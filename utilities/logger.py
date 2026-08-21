import logging
import os


def configure_logger(
    log_name,
    log_file,
    log_dir="/logs"
):
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"{log_file}.log")
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger