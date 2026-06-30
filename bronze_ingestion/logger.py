import os
import logging
from datetime import datetime
from config import LOG_FOLDER, LOG_LEVEL

def get_logger():

    if not os.path.exists(LOG_FOLDER):
        os.makedirs(LOG_FOLDER)

    log_file = os.path.join(
        LOG_FOLDER,
        f"bronze_{datetime.now().strftime('%Y%m%d')}.log"
    )

    logger = logging.getLogger("BronzePipeline")

    if logger.handlers:
        return logger

    logger.setLevel(LOG_LEVEL)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_file)

    console_handler = logging.StreamHandler()

    file_handler.setFormatter(formatter)

    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    logger.addHandler(console_handler)

    return logger