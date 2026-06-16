import logging
import os

import uvicorn
from dotenv import load_dotenv

from concerto.slack_bot import create_app

logger = logging.getLogger("concerto")


def main() -> None:
    load_dotenv()
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(create_app(), host=host, port=port)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
