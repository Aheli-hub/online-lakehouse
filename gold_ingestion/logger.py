import logging
import sys

LOGGER_NAME = "GoldPipeline"


def get_logger():

    logger = logging.getLogger(LOGGER_NAME)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s"
    )

    console = logging.StreamHandler(sys.stdout)

    console.setFormatter(formatter)

    logger.addHandler(console)

    logger.propagate = False

    return logger