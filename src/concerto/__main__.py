import logging
import os

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

logger = logging.getLogger("concerto")


def _create_app() -> FastAPI:
    # Import the selected platform lazily so a Slack deploy needn't install the
    # optional `discord` extra (and vice versa).
    platform = os.getenv("CONCERTO_PLATFORM", "slack").strip().lower()
    if platform == "discord":
        from concerto.discord_bot import create_app  # noqa: PLC0415
    elif platform == "slack":
        from concerto.slack_bot import create_app  # noqa: PLC0415
    else:
        msg = f"Unknown CONCERTO_PLATFORM: {platform!r} (use 'slack' or 'discord')"
        raise RuntimeError(msg)
    return create_app()


def main() -> None:
    load_dotenv()
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(_create_app(), host=host, port=port)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Scope LOG_LEVEL to our logger so DEBUG does not enable noisy library logs.
    logging.getLogger("concerto").setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    main()
