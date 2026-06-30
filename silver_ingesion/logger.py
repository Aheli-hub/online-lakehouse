import logging

# ==========================================================
# LOGGER
# ==========================================================

def get_logger():

    logger = logging.getLogger("SilverPipeline")

    if not logger.handlers:

        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        console = logging.StreamHandler()

        console.setFormatter(formatter)

        logger.addHandler(console)

    return logger