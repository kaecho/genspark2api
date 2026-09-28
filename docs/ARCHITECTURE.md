# Integration & Architecture Notes

Design notes for wiring this bridge into an LLM gateway and isolating per-account egress.
**No deployment-specific values here** — no real hostnames, IPs, keys, or account data.

---

## 1. Gateway integration (OpenAI-compatible gateway)

This bridge speaks the OpenAI Chat Completions protocol, so it plugs into any gateway that
supports `openai-compatibility`-style upstreams. The gateway sits in front and gives you a
single endpoint, model aliases, priority/failover, and usage accounting.

```
client ──► gateway ──► this bridge ──► upstream web session
             │              │
             │              └── round-robin over the account pool
             └── alias mapping + priority + failover
```

### Layering

| Layer | Responsibility |
|---|---|
| **Client** | Sends OpenAI-shaped requests, names models by the gateway's alias |
| **Gateway** | Auth, alias→model mapping, priority tiers, retry, usage stats |
| **This bridge** | Account selection, cookie/proxy attachment, upstream protocol translation |
| **Upstream** | The web session endpoint (SSE) |

### Config shape (placeholders)

The gateway entry looks like this — note that a gateway key is *not* the same thing as an
upstream account credential; the gateway key only authenticates the client to the gateway.

```yaml
- name: "<channel-name>"
  base-url: "http://127.0.0.1:<bridge-port>/v1"
  priority: <int>            # higher wins; lower tier acts as fallback
  api-key-entries:
    - api-key: "<gateway-local-key>"    # arbitrary; the bridge does not verify it
  models:
    - name: "<upstream-model-id>"       # the id the bridge accepts
      alias: "<prefix>/<friendly-name>" # what the client asks for
```

### Rules worth knowing

- **Alias prefix**: use a channel-distinctive prefix so aliases from different channels
  never collide in the gateway's flat namespace.
- **`priority` semantics**: the gateway picks the highest-priority bucket that has an
  available upstream; lower buckets serve as fallback. Useful when the same model name
  exists on a paid channel and a free channel — the free one can lead.
- **The bridge does not authenticate its callers.** Bind it to loopback and let the
  gateway own client auth. Never expose it on a public interface.
- **One model per channel at first.** Register a single model, verify end-to-end, then add
  the rest. Registering 50 models before proving the path works makes failures ambiguous.

### Verifying an integration

```
1. bridge alone     POST /v1/chat/completions   → a real completion
2. through gateway  POST /v1/chat/completions   → same, with the gateway alias
3. streaming        stream: true                → incremental deltas, then [DONE]
```

Test 1 isolates the bridge; test 2 proves alias mapping; test 3 proves the gateway does not
buffer the stream.

---

## 2. Per-account egress isolation

### Why it matters

Accounts that share one egress IP are correlated by the upstream. Rate limits, risk
scoring, and account restrictions tend to apply **per IP**, so a single noisy account can
degrade every account behind the same address. Isolating egress turns "all accounts share
one fate" into "one account degrades at a time".

This is not a theoretical concern — on similar bridges, accounts with fully isolated
device identifiers and distinct email addresses were still restricted together, and the
only shared attribute was the egress IP.

### Design

```
        ┌──────────────────────────────────┐
        │  egress pool definition          │  single source of truth:
        │  slot N → local listener :PORT_N │  account index → egress index
        └───────────────┬──────────────────┘
                        │  all channels read the same mapping
        ┌───────────────┴──────────────────┐
        │  proxy engine                    │
        │  one listener per slot           │
        └───────────────┬──────────────────┘
                        │
   ┌────────────┬───────┴───────┬────────────┐
 account 1    account 2      account N   (other channels)
 → :PORT_1    → :PORT_2       → :PORT_N   → same slot index
```

**Core rule: account index N → egress slot N.** Using the index (not a name) means
"which egress does this account use?" is answered by arithmetic, with no lookup table to
drift out of sync.

### Design decisions and their rationale

| Decision | Rationale |
|---|---|
| **Pool decoupled from the bridge** | Adding a channel does not rebuild the pool; egress resources stay centrally managed |
| **Cross-channel slot sharing is intentional** | The isolation requirement is *per-channel* (account N of a channel ↔ slot N), not global exclusivity |
| **One listener per slot** (not one config per account) | The bridge only needs `proxy: http://127.0.0.1:<port>`; it never learns node details |
| **Bind listeners to loopback** | Otherwise the proxy is an open relay reachable from the local network |
| **Fail closed on a bad proxy URL** | Silently falling back to a direct connection leaks the real IP — worse than an error |

### Implementation in this bridge

- Each account entry carries an optional `proxy`. Empty means "use the pool".
- A **proxy pool** can be configured in `proxy_pool.json`, in the top level of
  `accounts.json`, or via `GS_PROXY_POOL`, with `mode` selecting the assignment rule:
  `sticky` (account index → slot index, the rule described above), `rotate` (a different
  entry per upstream request), `random`, or `off`.
- A **separate HTTP session per account**, so connection pools and cookie jars never mix.
- A session is cached per account when its egress is stable; under `rotate` the egress
  changes every call, so each call gets its own session.
- The proxy is attached to **every** upstream call, including the credit probe, so no
  request escapes over the default route.
- A malformed proxy URL raises at startup rather than degrading to a direct connection.
- `/health` reports the egress per account and `/state` reports the pool, so "is the proxy
  actually in use?" is answerable without reading traffic.

```json
{
  "proxy_pool": {
    "mode": "sticky",
    "proxies": ["socks5h://user:pass@host-a:1080", "http://host-b:8080"]
  },
  "accounts": [
    { "seq": 1, "cookie_file": "cookies1.json", "proxy": "" },
    { "seq": 2, "cookie_file": "cookies2.json", "proxy": "http://127.0.0.1:<port2>" }
  ]
}
```

Account 1 draws from the pool (slot 0), account 2 pins its own listener and never touches
the pool.

### Operational notes

- **Single-node slots are a single point of failure.** A slot whose node dies is simply
  unavailable; the bridge should be restarted or the slot reassigned.
- **Slot health drifts.** Different probe targets report different "down" sets at the same
  moment — a slot that fails an IP-echo probe may still reach the actual upstream. Always
  probe **the upstream you care about**, not a generic echo service.
- **Verify the proxy is actually used**, don't assume. A field that is stored but never
  applied looks configured while traffic goes direct.

---

## 3. Failure semantics

| Condition | Behavior |
|---|---|
| Credits exhausted (`您的积分已用完`) | Park the account for 24 h, try the next one |
| Upstream rate-limits | Park the account for 1 h, try the next one |
| Upstream says not logged in | Park the account for 5 min, try the next one |
| Canned failure reply or empty body | Park the account for 1 min, try the next one |
| Network error / HTTP 5xx | Retry the same account twice, then park it for 30 s |
| All attempts used | Fail the request with the last reason; `429` when nothing is usable |
| No cookie for an account | Skip it at load time rather than failing at request time |

All cooldowns are configurable (`GS_*_COOLDOWN`) and persisted to `GS_STATE_FILE`, so a
restart resumes the pool's state instead of re-admitting accounts that just failed.

**Distinguish "rate-limited" from "model unavailable".** A rate-limit message is a
throttle signal, not a model failure — misclassifying it produces a model list full of
false negatives. When probing availability, go serial with a generous interval, and treat
a throttle response as "unknown", not "broken".

**Distinguish "used up" from "never granted".** An account whose credits are gone still
answers `/api/is_login` with `is_login: true` and a valid `cogen_id`; only a chat request
reveals the refusal. That is why session validity is not a health check for the pool, and
why the cooldown is keyed on the chat outcome (or, for bulk sweeps, on the credit
endpoint).
