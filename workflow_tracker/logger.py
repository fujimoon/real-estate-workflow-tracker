import logging
import os
from logging import FileHandler, Formatter, StreamHandler, getLogger


def get_log_level():
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    return level_map.get(log_level_str, logging.INFO)


logger = getLogger("workflow_tracker")
logger.setLevel(get_log_level())

formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

console_handler = StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
file_handler = FileHandler(os.path.join(log_dir, "application.log"))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
