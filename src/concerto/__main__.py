import logging

logger = logging.getLogger("concerto")


def main() -> None:
    logger.info("Hello from concerto!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
