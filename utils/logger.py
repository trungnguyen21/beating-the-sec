import logging
import sys

from config import LOGGING_LEVEL

def get_logger(name: str) -> logging.Logger:
    """
    Creates or retrieves a logger with universal formatting.
    """
    logger = logging.getLogger(name)
    
    # Avoid adding handlers multiple times if get_logger is called repeatedly
    if not logger.handlers:
        logger.setLevel(LOGGING_LEVEL)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(LOGGING_LEVEL)
        
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Optional: Add file handler here if needed
        # file_handler = logging.FileHandler('beating-sec.log')
        # file_handler.setFormatter(formatter)
        # logger.addHandler(file_handler)
        
    return logger
