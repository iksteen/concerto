# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

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
    uv sync --locked --no-install-project --no-dev

# Install the project itself.
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Run as a non-root user; the SQLite database lives in a persisted volume.
RUN useradd --uid 1000 --create-home app \
    && mkdir -p /data \
    && chown app:app /data
USER app
VOLUME ["/data"]

EXPOSE 8000

CMD ["python", "-m", "concerto"]
