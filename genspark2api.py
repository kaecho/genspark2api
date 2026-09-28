"""Genspark 网页端反代 —— 多账号轮转版

架构（标准网页端反代，浏览器不在链路里）：
  浏览器只用于登录取 cookie（gs_login.py）→ curl_cffi 纯 HTTP 转发

多账号轮转：
  读 accounts.json → 每个号一份 cookie + 独立 proxy
  选号：least-recently-used（并发请求拿到不同的号）
  失败/限额 → 按原因冷却该号，本请求换下一个号重试

代理池（三选一，优先级从高到低）：
  1. 账号条目里的 proxy            —— 该号专用，最高优先级
  2. proxy_pool.json / GS_PROXY_POOL —— 代理池，mode 决定怎么分配
  3. accounts.json 顶层 proxy_default / GS_PROXY —— 单代理兜底

  代理池 mode：
    sticky  账号下标 N → 池内槽位 N（重启后仍固定，便于按出口排查）
    rotate  每次上游请求换一个出口
    random  每次随机
    off     不使用代理池

冷却（环境变量可覆盖，单位秒）：
  GS_QUOTA_COOLDOWN=86400        积分耗尽（默认 24 小时）
  GS_RATE_COOLDOWN=3600          触发上游频率限制
  GS_NOTLOGIN_COOLDOWN=300       cookie 失效 / 未登录
  GS_ERROR_COOLDOWN=30           网络与传输错误
  GS_PLACEHOLDER_COOLDOWN=60     上游返回占位回复
  GS_STATE_FILE=cooldown_state.json  冷却状态落盘，重启后不丢

端点：
  POST /v1/chat/completions   (OpenAI 兼容，支持 stream / tools)
  GET  /v1/models
  GET  /health                每个账号的 ready / 冷却原因 / 出口 / 计数
  GET  /state                 汇总视图：可用数、冷却分布、池状态
  POST /admin/reload          重新读取 accounts.json 与代理池
  POST /admin/quota-check     批量探测额度，耗尽的直接冷却 24h
"""
import hashlib
import json
import os
import random
import re
import threading
import time
import uuid

from curl_cffi import requests as cffi
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

BASE = os.path.dirname(os.path.abspath(__file__))
MAP_FILE = os.environ.get("GS_ACCOUNTS", os.path.join(BASE, "accounts.json"))
PORT = int(os.environ.get("GS_PORT", "8899"))

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
UPSTREAM = "https://www.genspark.ai/api/agent/ask_proxy"
REFERER = "https://www.genspark.ai/agents?type=ai_chat"
CREDIT_API = "https://www.genspark.ai/api/credit_audit/billing_cycle"
IS_LOGIN_API = "https://www.genspark.ai/api/is_login"


def _int_env(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"{name} 必须是整数，收到 {raw!r}")


# 冷却时长（秒）。按失败原因区分：积分耗尽要冷却一整天，网络抖动只冷却几十秒。
COOLDOWN = {
    "quota": _int_env("GS_QUOTA_COOLDOWN", 86400),
    "rate": _int_env("GS_RATE_COOLDOWN", 3600),
    "notlogin": _int_env("GS_NOTLOGIN_COOLDOWN", 300),
    "error": _int_env("GS_ERROR_COOLDOWN", 30),
    "placeholder": _int_env("GS_PLACEHOLDER_COOLDOWN", 60),
}
MAX_ATTEMPTS = _int_env("GS_MAX_ATTEMPTS", 5)          # 一个请求最多换几个号
TRANSPORT_RETRIES = _int_env("GS_TRANSPORT_RETRIES", 2)  # 同一个号的传输层重试次数
POOL_FILE = os.environ.get("GS_PROXY_POOL_FILE", os.path.join(BASE, "proxy_pool.json"))
ADMIN_TOKEN = os.environ.get("GS_ADMIN_TOKEN", "")
_state_env = os.environ.get("GS_STATE_FILE", os.path.join(BASE, "cooldown_state.json"))
STATE_FILE = "" if _state_env.strip().lower() in ("", "off", "none", "0") else _state_env

# 模型清单（2026-09-23 实测 53 个中 50 个可用）
MODELS = [
    # openai (23)
    "gpt-5", "gpt-5.1", "gpt-5.2", "gpt-5.4", "gpt-5.5", "gpt-5.6", "gpt-6",
    "gpt-5-pro", "gpt-5.1-high", "gpt-5.1-low", "gpt-5.1-medium",
    "gpt-5.4-mini", "gpt-5.4-nano", "gpt-5.4-pro", "gpt-5.5-pro",
    "gpt-5.6-luna", "gpt-5.6-sol", "gpt-5.6-terra", "gpt-6-luna", "gpt-6-sol",
    # anthropic (10)
    "claude-4-5-haiku", "claude-opus-4-6", "claude-opus-4-7", "claude-opus-4-8",
    "claude-opus-5", "claude-opus-5-5", "claude-sonnet-4", "claude-sonnet-4-5",
    "claude-sonnet-4-6", "claude-sonnet-5",
    # google (6)
    "gemini-2.5-flash", "gemini-3.1-flash-lite-preview", "gemini-3.1-pro-preview",
    "gemini-3.6-flash", "gemini-3.7-flash", "gemini-3.8-flash",
    # genspark (11)
    "GLM-5.3", "deep-seek-v4-flash", "deep-seek-v4.1-flash", "glm-5p3",
    "glm-5p3-flash-baseten", "grok-4.5", "grok-4.6", "grok-4.7",
    "kimi-k3", "minimax-m3", "nemotron-3-ultra",
]
# 别名映射（用户可能用 API 风格的名字）
ALIAS = {
    "claude-haiku-4-5": "claude-4-5-haiku",
    "claude-opus-4-5": "claude-opus-4-6",
    "gpt-5.4-mini": "gpt-5.4-mini",
}

LOCK = threading.Lock()

PROXY_SCHEMES = ("http://", "https://", "socks4://", "socks4a://",
                 "socks5://", "socks5h://")


def validate_proxy(url):
    """Fail closed on a malformed proxy URL.

    Ignoring a bad URL would send the request over the default route instead,
    which leaks the host IP and defeats the whole point of the egress pool.
    """
    if not url:
        return ""
    u = str(url).strip()
    if not u.lower().startswith(PROXY_SCHEMES):
        raise RuntimeError(
            f"代理地址缺少 scheme: {u[:32]!r}（应为 http:// 或 socks5h:// 之类）")
    hostport = u.split("://", 1)[1].rsplit("@", 1)[-1]
    if not hostport or ":" not in hostport:
        raise RuntimeError(f"代理地址缺少 host:port: {u[:32]!r}")
    return u


def mask_proxy(url):
    """Hide proxy credentials before they reach /health or a response body."""
    if not url:
        return ""
    if "@" in url:
        scheme, rest = url.split("://", 1)
        return f"{scheme}://***@{rest.rsplit('@', 1)[1]}"
    return url


def proxies_dict(url):
    if not url:
        return None
    return {"https": url, "http": url}


class ProxyPool:
    """Egress pool. mode: off / sticky / rotate / random."""

    def __init__(self, proxies, mode="", source=""):
        self.proxies = [p for p in (validate_proxy(x) for x in proxies) if p]
        mode = (mode or "").strip().lower()
        if not self.proxies:
            mode = "off"
        elif not mode:
            mode = "sticky"
        if mode not in ("off", "sticky", "rotate", "random"):
            raise RuntimeError(f"未知代理池模式 {mode!r}（可选 off/sticky/rotate/random）")
        self.mode = mode
        self.source = source
        self._n = 0

    @property
    def enabled(self):
        return self.mode != "off" and bool(self.proxies)

    def sticky(self, index):
        """Account index -> slot. Stable across restarts by design."""
        if not self.proxies:
            return ""
        return self.proxies[index % len(self.proxies)]

    def rotate(self):
        with LOCK:
            url = self.proxies[self._n % len(self.proxies)]
            self._n += 1
        return url

    def pick(self, index):
        if not self.enabled:
            return ""
        if self.mode == "sticky":
            return self.sticky(index)
        if self.mode == "rotate":
            return self.rotate()
        return random.choice(self.proxies)

    def label(self, index):
        """Readable egress label for /health (not fixed under rotate/random)."""
        if not self.enabled:
            return ""
        if self.mode == "sticky":
            return self.sticky(index)
        return f"<pool:{self.mode}, {len(self.proxies)} proxies>"

    def info(self):
        return {"mode": self.mode, "size": len(self.proxies), "source": self.source,
                "proxies": [mask_proxy(p) for p in self.proxies[:8]]}


def load_pool(accounts_doc=None):
    """Pool sources, highest priority first.

    GS_PROXY_POOL > proxy_pool.json > accounts.json (proxy_pool/proxy_default) >
    GS_PROXY. An empty pool means accounts without their own `proxy` go direct.
    """
    raw = os.environ.get("GS_PROXY_POOL", "")
    proxies = [p for p in re.split(r"[,\s]+", raw) if p.strip()]
    mode = os.environ.get("GS_PROXY_MODE", "")
    source = "env GS_PROXY_POOL" if proxies else ""

    if not proxies and os.path.exists(POOL_FILE):
        doc = json.load(open(POOL_FILE, encoding="utf-8"))
        if isinstance(doc, list):
            proxies = [str(x) for x in doc]
        elif isinstance(doc, dict):
            proxies = [str(x) for x in (doc.get("proxies") or [])]
            mode = mode or str(doc.get("mode") or "")
        source = POOL_FILE

    if not proxies and accounts_doc:
        doc = accounts_doc.get("proxy_pool")
        if isinstance(doc, list):
            proxies = [str(x) for x in doc]
            source = MAP_FILE
        elif isinstance(doc, dict):
            proxies = [str(x) for x in (doc.get("proxies") or [])]
            mode = mode or str(doc.get("mode") or "")
            source = MAP_FILE
        if not proxies and accounts_doc.get("proxy_default"):
            proxies = [str(accounts_doc["proxy_default"])]
            source = f"{MAP_FILE} (proxy_default)"

    if not proxies and os.environ.get("GS_PROXY"):
        proxies = [os.environ["GS_PROXY"]]
        source = "env GS_PROXY"

    pool = ProxyPool(proxies, mode, source)
    print(f"[init] 代理池: mode={pool.mode} size={len(pool.proxies)} "
          f"source={pool.source or '-'}", flush=True)
    return pool


class StateStore:
    """Cooldown persistence.

    Without it every restart re-admits accounts whose credits are gone, and the
    pool burns attempts rediscovering them. Keyed by a cookie hash, not by seq,
    so regenerating accounts.json does not resurrect a cooled-down account.
    """

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.data = {}
        self.writable = bool(path)
        if not path:
            return
        try:
            if os.path.exists(path):
                doc = json.load(open(path, encoding="utf-8"))
                if isinstance(doc, dict):
                    self.data = doc
        except Exception as e:
            print(f"[state] 冷却状态读取失败，忽略: {type(e).__name__}: {e}", flush=True)

    def get(self, key):
        if not key:
            return 0.0, ""
        ent = self.data.get(key)
        if not isinstance(ent, dict):
            return 0.0, ""
        try:
            return float(ent.get("until") or 0), str(ent.get("reason") or "")
        except (TypeError, ValueError):
            return 0.0, ""

    def clear(self, key):
        if not key or not self.writable or key not in self.data:
            return
        with self.lock:
            self.data.pop(key, None)
            self._flush()

    def set(self, key, until, reason):
        if not key or not self.writable:
            return
        with self.lock:
            self.data[key] = {"until": round(until, 3), "reason": reason,
                              "at": round(time.time(), 3)}
            horizon = time.time() - 30 * 86400
            for k, v in list(self.data.items()):
                if isinstance(v, dict):
                    try:
                        if float(v.get("at") or 0) < horizon:
                            del self.data[k]
                    except (TypeError, ValueError):
                        del self.data[k]
            self._flush()

    def _flush(self):
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False)
            os.replace(tmp, self.path)
        except Exception as e:
            self.writable = False
            print(f"[state] 写入 {self.path} 失败，本次运行不再落盘: "
                  f"{type(e).__name__}: {e}", flush=True)


STATE = StateStore(STATE_FILE)


class Account:
    def __init__(self, d, index=0):
        self.seq = d.get("seq")
        self.email = d.get("email")
        self.cogen_id = d.get("cogen_id")
        self.cookie_file = d.get("cookie_file")
        # 账号自带出口优先于代理池；这里只解析它自己的，池留到请求时再取
        self.proxy = validate_proxy(str(d.get("proxy") or "").strip())
        self.index = index
        self.cookie = ""
        self.cooldown_until = 0.0
        self.cooldown_reason = ""
        self.last_used = 0.0
        self.state_key = ""
        self.stats = {"ok": 0, "fail": 0, "throttle": 0, "quota": 0}
        self._session = None
        self._session_proxy = None
        self.load()

    def load(self):
        self.cookie = ""
        path = self.resolve_cookie_file()
        if path:
            d = json.load(open(path, encoding="utf-8"))
            self.cookie = "; ".join(f"{c['name']}={c['value']}"
                                    for c in d.get("cookies", []) if c.get("name"))
        self.state_key = ("ck:" + hashlib.md5(self.cookie.encode()).hexdigest()[:12]
                          if self.cookie else "")
        until, reason = STATE.get(self.state_key)   # 重启后沿用落盘的冷却
        if until > time.time():
            self.cooldown_until, self.cooldown_reason = until, reason

    def resolve_cookie_file(self):
        """Find the cookie file, looking next to accounts.json as a fallback.

        Relative paths were resolved against the process working directory only,
        which silently drops every account when the bridge is started from
        somewhere else (or when GS_ACCOUNTS points at another directory).
        """
        cf = self.cookie_file
        if not cf:
            return ""
        if os.path.isabs(cf):
            return cf if os.path.exists(cf) else ""
        if os.path.exists(cf):
            return cf
        alt = os.path.join(os.path.dirname(os.path.abspath(MAP_FILE)), cf)
        return alt if os.path.exists(alt) else ""

    @property
    def ready(self):
        return bool(self.cookie) and time.time() >= self.cooldown_until

    def cooldown(self, reason, secs=None):
        """Mark the account unusable and remember why.

        `quota` is the 24h one: the account still answers `/api/is_login`, but
        the upstream refuses every chat, so retrying it minutes later only
        wastes attempts.
        """
        secs = COOLDOWN.get(reason, COOLDOWN["error"]) if secs is None else secs
        self.cooldown_until = time.time() + secs
        self.cooldown_reason = reason
        if reason in ("quota", "rate"):
            self.stats["throttle"] += 1
        if reason == "quota":
            self.stats["quota"] += 1
        else:
            self.stats["fail"] += 1
        STATE.set(self.state_key, self.cooldown_until, reason)

    def clear_cooldown(self):
        self.cooldown_until, self.cooldown_reason = 0.0, ""
        STATE.clear(self.state_key)

    def proxy_for(self, pool):
        """Per-account proxy first, then the pool. Empty means direct."""
        return self.proxy or pool.pick(self.index)

    def egress_label(self, pool):
        return mask_proxy(self.proxy) or mask_proxy(pool.label(self.index)) or "direct"

    def session(self, proxy):
        """HTTP session for one upstream call.

        Cached per account when the egress is stable (sticky pool or per-account
        proxy) so connections are reused. Under rotate/random the egress changes
        every call, so a fresh session is required. LRU selection hands distinct
        accounts to concurrent requests, so a cached session never has two
        in-flight users.
        """
        if self._session is None or self._session_proxy != proxy:
            self._session = cffi.Session(impersonate="chrome",
                                         proxies=proxies_dict(proxy))
            self._session_proxy = proxy
        return self._session

    def headers(self):
        rid = "|" + uuid.uuid4().hex + "." + uuid.uuid4().hex[:16]
        p = rid.lstrip("|").split(".")
        return {
            "User-Agent": UA, "Content-Type": "application/json",
            "Accept": "text/event-stream", "Origin": "https://www.genspark.ai",
            "Referer": REFERER, "request-id": rid,
            "traceparent": f"00-{p[0]}-{p[1]}-01", "Cookie": self.cookie,
        }


POOL = ProxyPool([], "off", "")
ACCOUNTS = []


def load_accounts():
    """(Re)read accounts.json plus the proxy pool, and swap the pool in place."""
    global ACCOUNTS, POOL
    if not os.path.exists(MAP_FILE):
        raise RuntimeError(f"缺少 {MAP_FILE}")
    doc = json.load(open(MAP_FILE, encoding="utf-8"))
    POOL = load_pool(doc)
    accts = []
    skipped = 0
    for i, a in enumerate(doc.get("accounts", [])):
        if a.get("status") == "disabled":
            continue
        acc = Account(a, i)
        if acc.cookie:
            accts.append(acc)
        else:
            skipped += 1
            print(f"[init] 跳过 seq={a.get('seq')} {a.get('email')}: "
                  f"读不到 cookie（{a.get('cookie_file')}）", flush=True)
    ACCOUNTS = accts
    if skipped:
        print(f"[init] 共跳过 {skipped} 个无 cookie 的账号", flush=True)
    return accts


load_accounts()
print(f"[init] 加载 {len(ACCOUNTS)} 个账号: "
      f"{[(a.seq, (a.email or '')[:22]) for a in ACCOUNTS[:8]]}"
      + ("…" if len(ACCOUNTS) > 8 else ""), flush=True)


def pick(exclude=()):
    """Least-recently-used pick.

    Not a round-robin cursor: `ready` shrinks as accounts cool down, and an
    index cursor then keeps re-serving whatever sits at the front of the list.
    LRU spreads load over the pool, and because `last_used` is stamped under the
    lock, two concurrent requests never get the same account.
    """
    with LOCK:
        skip = set(exclude)
        ready = [a for a in ACCOUNTS if a.ready and a.seq not in skip]
        if not ready:
            return None
        a = min(ready, key=lambda x: x.last_used)
        a.last_used = time.time()
        return a


# ---------------------------------------------------------------- tool emulation
# The upstream web session does NOT accept OpenAI-style `tools`. Measured
# 2026-09-23: the parameter is accepted (HTTP 200) but ignored, and every model
# answers "I can't call a tool here" (verified on gpt-6-luna, claude-opus-5-5,
# gemini-3.8-flash, GLM-5.3).
#
# So tools are emulated at the gateway, the usual approach for web bridges:
#   1. render the tool schemas into a system prompt with a strict output contract
#   2. parse the model's reply back into OpenAI `tool_calls`
#   3. flatten tool-protocol messages the upstream rejects (HTTP 422)
#
# Verified prerequisites: the upstream honours role=system, and the model
# follows the output contract exactly (3/3 cases, including correctly NOT
# calling a tool when none applies).

TOOL_CONTRACT = """You have access to the following tools.

To call a tool, reply with EXACTLY one line of JSON and nothing else:
{"tool_call": {"name": "<tool_name>", "arguments": {<arguments>}}}

If no tool is needed, reply normally in plain text. Never emit the JSON line
unless you actually need a tool.

Available tools:
%s

Rules:
- Emit ONLY the JSON line when calling a tool: no prose, no markdown fences.
- "arguments" must be a valid JSON object matching that tool's parameters.
- One tool call per reply. If several are needed, call the first one now; the
  remaining ones will be requested after its result comes back."""


def render_tools(tools):
    """Render OpenAI tool schemas into the contract prompt."""
    lines = []
    for t in tools:
        fn = t.get("function") if isinstance(t, dict) else None
        if not fn:
            fn = t if isinstance(t, dict) else {}
        name = fn.get("name")
        if not name:
            continue
        desc = (fn.get("description") or "").strip().replace("\n", " ")
        params = fn.get("parameters") or {}
        lines.append(f"- {name}: {desc}\n  arguments schema: "
                     f"{json.dumps(params, ensure_ascii=False)}")
    return TOOL_CONTRACT % ("\n".join(lines) if lines else "(none)")


def inject_tools(messages, tools):
    """Prepend the tool contract as a system message."""
    prompt = render_tools(tools)
    out = list(messages or [])
    if out and out[0].get("role") == "system":
        out[0] = {"role": "system",
                  "content": prompt + "\n\n" + str(out[0].get("content") or "")}
    else:
        out.insert(0, {"role": "system", "content": prompt})
    return out


def normalize_tool_messages(messages):
    """Translate OpenAI tool-protocol messages into plain chat messages.

    The upstream rejects the native shapes with HTTP 422 (measured 2026-09-23):
    an assistant message carrying `tool_calls`, or a message with role="tool".
    Both must be flattened:

      assistant{tool_calls:[...]}  -> assistant{content: <contract JSON line>}
      tool{tool_call_id, content}  -> user{content: "TOOL RESULT ..."}

    Verified: the flattened form round-trips and the model uses the result.
    """
    out = []
    for m in messages or []:
        if not isinstance(m, dict):
            continue
        role = m.get("role")

        if role == "tool":
            name = m.get("name") or "tool"
            cid = m.get("tool_call_id") or ""
            body = m.get("content")
            if not isinstance(body, str):
                body = json.dumps(body, ensure_ascii=False)
            head = f"TOOL RESULT for {name}" + (f" (call id {cid})" if cid else "")
            out.append({"role": "user",
                        "content": f"{head}: {body}\n"
                                   "Use this result to answer the user's question."})
            continue

        if role == "assistant" and m.get("tool_calls"):
            rendered = []
            for tc in m["tool_calls"]:
                fn = tc.get("function") or {}
                args = fn.get("arguments")
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except Exception:
                        args = {}
                if not isinstance(args, dict):
                    args = {}
                rendered.append(json.dumps(
                    {"tool_call": {"name": fn.get("name"), "arguments": args}},
                    ensure_ascii=False))
            content = "\n".join(rendered)
            if m.get("content"):
                content = str(m["content"]) + "\n" + content
            out.append({"role": "assistant", "content": content})
            continue

        clean = {"role": role, "content": m.get("content")}
        if m.get("name") and role != "assistant":
            clean["name"] = m["name"]
        out.append(clean)
    return out


def _extract_braced(s, start):
    """Return the substring from `start` (a '{') through its matching '}'.

    A non-greedy regex is wrong here: the payload nests objects
    ({"name":..., "arguments":{...}}), so `\\{.*?\\}` stops at the first inner
    brace and yields truncated JSON. Track depth, and respect string literals
    so a brace inside a string does not throw off the count.

    If the string ends before the depth returns to zero, the model dropped its
    closing brace(s) -- observed in practice, e.g. 65 chars where 66 were
    expected. Close them so the payload still parses.
    """
    if start < 0 or start >= len(s) or s[start] != "{":
        return None
    depth, in_str, esc = 0, False, False
    for i in range(start, len(s)):
        c = s[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return s[start:i + 1]
    # unterminated: close the open object(s) and retry. A dangling string is
    # NOT repaired -- closing it would invent content the model never produced.
    if depth > 0 and not in_str:
        return s[start:] + "}" * depth
    return None


TOOLCALL_KEY_RE = re.compile(r'\{\s*"tool_call"\s*:\s*\{')


def parse_toolcall(text):
    """Extract a tool call from the model's reply.

    Returns (name, arguments_dict) or (None, None). Tolerates markdown fences
    and surrounding prose, since models sometimes add either despite the
    contract.
    """
    if not text:
        return None, None
    s = text.strip()
    s = re.sub(r"^```(?:json)?\s*|\s*```$", "", s).strip()

    m = TOOLCALL_KEY_RE.search(s)
    if not m:
        return None, None
    outer = _extract_braced(s, m.start())
    if not outer:
        return None, None
    try:
        obj = json.loads(outer)
    except Exception:
        return None, None
    inner = obj.get("tool_call")
    if not isinstance(inner, dict):
        return None, None
    name = inner.get("name")
    args = inner.get("arguments")
    if not isinstance(name, str) or not name:
        return None, None
    if args is None:
        args = {}
    if not isinstance(args, dict):
        try:
            args = json.loads(args)
        except Exception:
            return None, None
        if not isinstance(args, dict):
            return None, None
    return name, args


def build_body(payload):
    m = payload.get("model") or "claude-4-5-haiku"
    m = ALIAS.get(m, m)
    msgs = payload.get("messages") or []
    # Always flatten OpenAI tool-protocol messages: the upstream rejects
    # assistant.tool_calls and role="tool" with HTTP 422.
    msgs = normalize_tool_messages(msgs)
    tools = payload.get("tools") or []
    if tools:
        msgs = inject_tools(msgs, tools)
    return {
        "ai_chat_model": m,
        "ai_chat_enable_search": False,
        "ai_chat_disable_personalization": False,
        "use_moa_proxy": False, "moa_models": [], "writingContent": None,
        "sas_ask_origin": "typed", "type": "ai_chat", "is_private": True,
        "messages": msgs,
    }


def is_upstream_error(text):
    """Detect the upstream's canned failure strings.

    The web session occasionally answers with a placeholder instead of a real
    reply, e.g. "Sorry, I couldn't produce a response this turn." Returning that
    as normal content misleads the caller, and it silently breaks tool emulation
    (no contract line is produced). Treat it as a failure so the caller retries
    on another account.
    """
    if not text:
        return False
    t = text.strip().lower()
    if len(t) > 300:
        return False
    return any(s in t for s in (
        "couldn't produce a response",
        "could not produce a response",
        "unable to produce a response",
        "please try again",
        "something went wrong",
        "an error occurred",
        "服务异常", "请稍后再试", "出了点问题",
    ))


QUOTA_MARKERS = ("积分已用完", "积分用完", "credit exhausted", "credits exhausted",
                 "out of credits", "insufficient credit", "run out of credit")
RATE_MARKERS = ("too quickly", "rate limit", "rate-limit", "too many requests",
                "请求过于频繁", "频率限制")
NOTLOGIN_MARKERS = ("not login", "not logged in", "unauthorized", "登录已过期",
                    "please log in", "session expired")
CANNED_MAX = 300


def classify_reason(text, status=None):
    """Map an upstream reply to a cooldown reason.

    Only the upstream's own refusal text is classified, never a model's prose: a
    long answer that happens to mention credits must not cool the account for a
    day. Hence the length gate.

    Returns "quota" | "rate" | "notlogin" | "error" | None.
    """
    if status in (401, 403):
        return "notlogin"
    if status == 429:
        return "rate"
    if isinstance(status, int) and status >= 500:
        return "error"
    if not text:
        return None
    t = text.strip()
    if len(t) > CANNED_MAX:
        return None
    low = t.lower()
    for m in QUOTA_MARKERS:
        if m in t or m in low:
            return "quota"
    for m in RATE_MARKERS:
        if m in low:
            return "rate"
    for m in NOTLOGIN_MARKERS:
        if m in low:
            return "notlogin"
    return None


def post_upstream(acct, body, proxy, stream=False, timeout=120):
    """POST to the upstream, retrying transport failures on the same account.

    A dropped connection or an empty 5xx reply is worth one more try before
    burning a whole account rotation on it. The retry happens before any byte of
    a streaming response is handed to the client, so it is always safe.
    """
    tries = max(1, TRANSPORT_RETRIES)
    last = None
    for i in range(tries):
        try:
            s = acct.session(proxy)
            r = s.post(UPSTREAM, headers=acct.headers(),
                       data=json.dumps(body), timeout=timeout, stream=stream)
            if not stream and r.status_code >= 500 and i + 1 < tries:
                last = f"http {r.status_code}"
                time.sleep(0.4 * (i + 1))
                continue
            return r
        except Exception as e:
            last = e
            if i + 1 < tries:
                time.sleep(0.4 * (i + 1))
                continue
            raise
    if isinstance(last, Exception):
        raise last
    raise RuntimeError(str(last))


def parse_sse(text):
    """Parse the upstream SSE body.

    Returns (content, joined_deltas, reason, err). `reason` is a cooldown reason
    when the body carries one of the upstream's canned refusals, which is what
    lets the caller cool the account down for the right amount of time.
    """
    content, deltas, reason, err = None, [], None, None
    for line in text.split("\n"):
        if not line.startswith("data: "):
            continue
        try:
            j = json.loads(line[6:])
        except Exception:
            continue
        t = j.get("type")
        if t == "message_field" and j.get("field_name") == "content":
            content = j.get("field_value")
        if t == "message_field_delta" and j.get("field_name") == "content":
            deltas.append(j.get("delta") or "")
        if t == "message_result" and isinstance(j.get("message"), dict):
            mc = j["message"].get("content") or ""
            r = classify_reason(mc)
            if r:
                reason = reason or r
            elif not content:
                content = mc
        if t == "error":
            err = json.dumps(j)[:300]
    return content, "".join(deltas), reason, err


app = FastAPI()
START = time.time()


def _admin_guard(request):
    if ADMIN_TOKEN and request.headers.get("x-admin-token") != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="admin token required")


@app.get("/health")
def health():
    now = time.time()
    return {
        "ok": True, "uptime_s": round(now - START, 1),
        "pool": POOL.info(),
        "accounts": [{
            "seq": a.seq, "email": (a.email or "")[:26],
            "ready": a.ready,
            "cooldown_left_s": max(0, round(a.cooldown_until - now)),
            "cooldown_reason": a.cooldown_reason,
            "egress": a.egress_label(POOL),
            "stats": a.stats,
        } for a in ACCOUNTS],
    }


@app.get("/state")
def state():
    """Aggregate view: how much of the pool is usable right now, and why not."""
    now = time.time()
    cooling = {}
    for a in ACCOUNTS:
        if a.ready:
            continue
        r = a.cooldown_reason or "unknown"
        cooling[r] = cooling.get(r, 0) + 1
    return {
        "uptime_s": round(now - START, 1),
        "pool": POOL.info(),
        "accounts": {"total": len(ACCOUNTS),
                     "ready": sum(1 for a in ACCOUNTS if a.ready),
                     "cooling": cooling},
        "settings": {"cooldown_s": COOLDOWN,
                     "max_attempts": MAX_ATTEMPTS,
                     "transport_retries": TRANSPORT_RETRIES,
                     "state_file": STATE_FILE or None,
                     "accounts_file": MAP_FILE},
        "quota_exhausted": [a.seq for a in ACCOUNTS
                            if a.cooldown_reason == "quota" and a.cooldown_until > now][:200],
    }


@app.post("/admin/reload")
def admin_reload(request: Request):
    """Re-read accounts.json and the proxy pool without restarting the bridge."""
    _admin_guard(request)
    before = len(ACCOUNTS)
    try:
        accts = load_accounts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    return {"ok": True, "accounts_before": before, "accounts_after": len(accts),
            "ready": sum(1 for a in accts if a.ready), "pool": POOL.info()}


def probe_credit(acct):
    """Remaining credits for one account, or None when unknown.

    The billing endpoint spends nothing (unlike a chat request), which is what
    makes a whole-pool quota sweep affordable.
    """
    try:
        h = acct.headers()
        h["Accept"] = "application/json, text/plain, */*"
        r = acct.session(acct.proxy_for(POOL)).get(CREDIT_API, headers=h, timeout=25)
        if r.status_code != 200:
            return None
        data = (r.json() or {}).get("data") or {}
        rem = data.get("remaining")
        return rem if isinstance(rem, int) else None
    except Exception:
        return None


@app.post("/admin/quota-check")
def admin_quota_check(request: Request):
    """Probe credit balances and park exhausted accounts for the quota cooldown.

    A single 0 can be a stale cache read (the upstream syncs lazily), so a 0 is
    confirmed with a second read before the account is parked. Accounts that
    read fine are released from an earlier quota cooldown.

    Query: scope=ready|cooling|all (default all), limit=1..500 (default 50).
    Runs serially in a worker thread; the response arrives when the batch ends.
    """
    _admin_guard(request)
    q = request.query_params
    try:
        limit = max(1, min(int(q.get("limit") or 50), 500))
    except ValueError:
        raise HTTPException(status_code=400, detail="limit 必须是整数")
    scope = (q.get("scope") or "all").lower()
    if scope == "ready":
        todo = [a for a in ACCOUNTS if a.ready][:limit]
    elif scope == "cooling":
        todo = [a for a in ACCOUNTS if not a.ready][:limit]
    elif scope == "all":
        todo = ACCOUNTS[:limit]
    else:
        raise HTTPException(status_code=400, detail="scope 必须是 ready/cooling/all")

    checked = exhausted = recovered = unknown = 0
    for a in todo:
        rem = probe_credit(a)
        if rem is None:
            unknown += 1
            continue
        if rem == 0:
            time.sleep(0.2)
            if probe_credit(a) == 0:
                if a.ready or a.cooldown_reason != "quota":
                    a.cooldown("quota")
                exhausted += 1
                continue
        if a.cooldown_reason == "quota":
            a.clear_cooldown()
            recovered += 1
        checked += 1
    return {"ok": True, "scope": scope, "probed": len(todo), "checked": checked,
            "quota_exhausted": exhausted, "recovered": recovered, "unknown": unknown,
            "ready_now": sum(1 for a in ACCOUNTS if a.ready)}


@app.get("/v1/models")
def models():
    return {"object": "list", "data": [
        {"id": m, "object": "model", "owned_by": "genspark-web"} for m in MODELS]}


@app.post("/v1/chat/completions")
async def chat(req: Request):
    payload = await req.json()
    model = payload.get("model") or "claude-4-5-haiku"
    want_stream = bool(payload.get("stream"))
    body = build_body(payload)
    cid = "chatcmpl-" + uuid.uuid4().hex[:24]
    created = int(time.time())

    # 一个请求最多换 MAX_ATTEMPTS 个号。上游偶发返回占位符（约 1/3 概率），
    # 加上积分耗尽的号，需要留足重试余量。
    tried = set()
    last_err = None
    for attempt in range(MAX_ATTEMPTS):
        acct = pick(exclude=tried)
        if acct is None:
            return JSONResponse(
                {"error": {"message": "所有账号都在冷却中（配额耗尽）",
                           "type": "no_account"}}, status_code=429)
        tried.add(acct.seq)
        proxy = acct.proxy_for(POOL)

        if not want_stream:
            try:
                r = post_upstream(acct, body, proxy)
                t = r.text
            except Exception as e:
                acct.cooldown("error")
                last_err = f"{type(e).__name__}: {e}"
                continue

            reason = classify_reason(t, r.status_code)
            if reason:
                acct.cooldown(reason)
                last_err = f"{reason}: {t[:120]}"
                continue

            content, joined, reason, err = parse_sse(t)
            if reason:
                acct.cooldown(reason)
                last_err = f"{reason}: {err or ''}"
                continue
            full = content or joined or ""

            # Retry on another account rather than passing off a placeholder or
            # an empty reply as a real answer. With tools requested, an empty
            # reply also means no contract line was produced.
            if is_upstream_error(full) or not full.strip():
                acct.cooldown("placeholder")
                last_err = (f"upstream_placeholder: {full[:80]}"
                            if is_upstream_error(full) else "empty reply")
                continue

            acct.stats["ok"] += 1
            msg = {"role": "assistant", "content": full}
            finish = "stop"

            # tool emulation: turn a parsed contract line into OpenAI tool_calls
            if payload.get("tools"):
                tname, targs = parse_toolcall(full)
                if tname:
                    msg["content"] = None
                    msg["tool_calls"] = [{
                        "id": "call_" + uuid.uuid4().hex[:24],
                        "type": "function",
                        "function": {"name": tname,
                                     "arguments": json.dumps(targs, ensure_ascii=False)},
                    }]
                    finish = "tool_calls"

            return JSONResponse({
                "id": cid, "object": "chat.completion", "created": created,
                "model": model,
                "choices": [{"index": 0, "finish_reason": finish, "message": msg}],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                "x_genspark": {"account": acct.seq, "email": acct.email[:22],
                               "upstream_status": r.status_code, "raw_len": len(t),
                               "attempts": attempt + 1,
                               "egress": mask_proxy(proxy) or "direct",
                               "tool_emulated": bool(payload.get("tools"))},
            })

        # 流式
        # 内部自己重试：换号只在"还没有任何字节发给客户端"时才安全，所以带 tools
        # 的请求全程缓冲，不带 tools 的请求一旦发出第一个 delta 就锁定该号。
        def gen(i=cid, cr=created, mo=model):
            has_tools = bool(payload.get("tools"))
            yield f'data: {json.dumps({"id": i, "object": "chat.completion.chunk", "created": cr, "model": mo, "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]})}\n\n'
            last = None
            tried_stream = set()
            for _ in range(MAX_ATTEMPTS):
                a = pick(exclude=tried_stream)
                if a is None:
                    yield f'data: {json.dumps({"error": {"message": "所有账号都在冷却中（配额耗尽）"}})}\n\n'
                    return
                tried_stream.add(a.seq)
                pxy = a.proxy_for(POOL)
                buf, emitted, collected = "", 0, ""
                seen_reason, stopped = None, False
                try:
                    r = post_upstream(a, body, pxy, stream=True)
                    for chunk in r.iter_content(chunk_size=None):
                        if not chunk:
                            continue
                        buf += chunk.decode("utf-8", "replace")
                        while "\n" in buf:
                            line, buf = buf.split("\n", 1)
                            line = line.strip()
                            if not line.startswith("data: "):
                                continue
                            try:
                                j = json.loads(line[6:])
                            except Exception:
                                continue
                            if j.get("type") == "message_field_delta" and j.get("field_name") == "content":
                                d = j.get("delta") or ""
                                if d:
                                    collected += d
                                    # With tools we cannot retract content
                                    # already sent, so buffer and decide later.
                                    if not has_tools:
                                        emitted += 1
                                        yield f'data: {json.dumps({"id": i, "object": "chat.completion.chunk", "created": cr, "model": mo, "choices": [{"index": 0, "delta": {"content": d}, "finish_reason": None}]})}\n\n'
                            elif j.get("type") == "message_result" and isinstance(j.get("message"), dict):
                                mc = j["message"].get("content") or ""
                                rr = classify_reason(mc)
                                if rr:
                                    seen_reason = rr
                                    stopped = True
                                    break
                        if stopped:
                            break
                except Exception as e:
                    last = f"{type(e).__name__}: {e}"
                    a.cooldown("error")
                    if emitted == 0:
                        continue
                    yield f'data: {json.dumps({"error": {"message": last}})}\n\n'
                    return

                if seen_reason:
                    # 上游直接拒绝（额度耗尽/限流/掉登录）：换个号再试一次。
                    a.cooldown(seen_reason)
                    last = seen_reason
                    if emitted:
                        yield f'data: {json.dumps({"error": {"message": seen_reason}})}\n\n'
                        yield "data: [DONE]\n\n"
                        return
                    continue

                placeholder = is_upstream_error(collected)
                empty = not collected.strip()

                if has_tools:
                    tname, targs = parse_toolcall(collected)
                    if tname:
                        tcid = "call_" + uuid.uuid4().hex[:24]
                        head = {"index": 0, "delta": {"tool_calls": [{
                            "index": 0, "id": tcid, "type": "function",
                            "function": {"name": tname, "arguments": ""}}]}}
                        yield f'data: {json.dumps({"id": i, "object": "chat.completion.chunk", "created": cr, "model": mo, "choices": [head]})}\n\n'
                        argchunk = {"index": 0, "delta": {"tool_calls": [{
                            "index": 0,
                            "function": {"arguments": json.dumps(targs, ensure_ascii=False)}}]}}
                        yield f'data: {json.dumps({"id": i, "object": "chat.completion.chunk", "created": cr, "model": mo, "choices": [argchunk]})}\n\n'
                        a.stats["ok"] += 1
                        fin = "tool_calls"
                    elif placeholder or empty:
                        # A tool was offered but no contract line came back, and
                        # the reply is either a canned failure or empty. Retry on
                        # another account -- returning an empty stop here would
                        # look like a successful answer.
                        a.cooldown("placeholder")
                        last = ("upstream_placeholder: " + collected[:80]) if placeholder \
                            else "empty reply with tools requested"
                        continue
                    else:
                        # model answered in prose instead of calling a tool
                        yield f'data: {json.dumps({"id": i, "object": "chat.completion.chunk", "created": cr, "model": mo, "choices": [{"index": 0, "delta": {"content": collected}, "finish_reason": None}]})}\n\n'
                        a.stats["ok"] += 1
                        fin = "stop"
                else:
                    if (placeholder or empty) and emitted == 0:
                        a.cooldown("placeholder")
                        last = ("upstream_placeholder: " + collected[:80]) if placeholder \
                            else "empty reply"
                        continue
                    a.stats["ok"] += 1
                    fin = "stop"

                yield f'data: {json.dumps({"id": i, "object": "chat.completion.chunk", "created": cr, "model": mo, "choices": [{"index": 0, "delta": {}, "finish_reason": fin}]})}\n\n'
                yield "data: [DONE]\n\n"
                return

            yield f'data: {json.dumps({"error": {"message": f"所有账号都失败: {last}"}})}\n\n'
            yield "data: [DONE]\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")

    return JSONResponse({"error": {"message": f"所有账号都失败: {last_err}"}},
                        status_code=502)


if __name__ == "__main__":
    import uvicorn
    print(f"[main] serving on :{PORT}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
