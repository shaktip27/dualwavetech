import logging
import sys
import os
from logging.handlers import TimedRotatingFileHandler

def setup_logger(log_file: str = "app.log", level=logging.INFO):
    log_folder = "app_logs"
    os.makedirs(log_folder, exist_ok=True)  # Create folder if not exists
    log_path = os.path.join(log_folder, log_file)

    logger = logging.getLogger()
    logger.setLevel(level)

    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Timed rotating file handler - rotates daily, keeps 7 days in simple , older logs of days are deleted
        file_handler = TimedRotatingFileHandler(
            filename=log_path,
            when="midnight",    # rotate every day at midnight
            interval=1,
            backupCount=7,      # keep last 7 days logs
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str):
    return logging.getLogger(name)
