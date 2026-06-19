# syntax=docker/dockerfile:1
# Trixie ships OpenSSL 3.5; the older bookworm (OpenSSL 3.0) produces a TLS
# ClientHello fingerprint that Cloudflare's bot management blocks with 403
# (e.g. www.tivolivredenburg.nl), even though the host's OpenSSL 3.5 passes.
FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH" \
    HOST=0.0.0.0 \
    PORT=8000 \
    CONCERTO_DB_PATH=/data/concerto.db

WORKDIR /app

# Install dependencies first (cached layer that ignores app source changes).
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev --no-editable

# Install the project itself. --no-editable copies the package into
# site-packages instead of installing an editable .pth that points at
# /app/src; the editable layout depends on the source tree and on the
# .pth surviving across build layers, which is fragile under different
# BuildKit cache behaviour (e.g. "No module named concerto" on some hosts).
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

# Run as a non-root user; the SQLite database lives in a persisted volume.
RUN useradd --uid 1000 --create-home app \
    && mkdir -p /data \
    && chown app:app /data
USER app
VOLUME ["/data"]

EXPOSE 8000

CMD ["python", "-m", "concerto"]
