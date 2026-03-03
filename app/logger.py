import logging
import os

def setup_logger(log_file: str = "app.log") -> logging.Logger:
    """Sets up a logger that writes to a file and console."""
    logger = logging.getLogger("MedallionLogger")
    logger.setLevel(logging.INFO)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add handlers if not already added
    if not logger.hasHandlers():
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger
