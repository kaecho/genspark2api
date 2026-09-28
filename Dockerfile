# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    GS_PORT=8899 \
    GS_ACCOUNTS=/app/accounts.json \
    GS_STATE_FILE=/app/state/cooldown_state.json

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY genspark2api.py /app/genspark2api.py

# Code only. accounts.json, the cookie files, the proxy pool and the cooldown
# state are mounted, so the same image serves any account set.
#
# The container is intentionally not dropped to a non-root user: the account
# files are usually bind-mounted 0600 from the host, and a non-root process
# cannot read them. If you chown the mounts to a fixed uid, add
# `USER <uid>` here and keep the state directory writable by it.
RUN mkdir -p /app/state

EXPOSE 8899

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8899/health', timeout=5).status==200 else 1)"

LABEL org.opencontainers.image.source="https://github.com/kaecho/genspark2api" \
      org.opencontainers.image.description="OpenAI-compatible bridge for the Genspark web session: multi-account rotation, proxy pool, quota-aware cooldowns" \
      org.opencontainers.image.licenses="MIT"

CMD ["sh", "-c", "exec uvicorn genspark2api:app --host 0.0.0.0 --port ${GS_PORT:-8899} --log-level ${GS_LOG_LEVEL:-warning}"]
