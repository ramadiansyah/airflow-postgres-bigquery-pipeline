# utils/logger.py

import logging

class CustomFormatter(logging.Formatter):
    """
    A custom logging formatter with colored output based on log level.
    
    This formatter changes the log message color depending on the log level:
    - DEBUG: grey
    - INFO: green
    - WARNING: yellow
    - ERROR: red
    - CRITICAL: red

    Attributes:
        grey (str): ANSI escape code for grey.
        green (str): ANSI escape code for green.
        yellow (str): ANSI escape code for yellow.
        red (str): ANSI escape code for red.
        reset (str): ANSI escape code to reset formatting.
        format (str): Log message format template.
        FORMATS (dict[int, str]): Mapping of log levels to their respective colored formats.
    """
    grey: str = "\x1b[38;21m"
    green: str = "\x1b[32m"
    yellow: str = "\x1b[33;21m"
    red: str = "\x1b[31;21m"
    reset: str = "\x1b[0m"
    format: str = "[%(asctime)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"

    FORMATS: dict[int, str] = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: red + format + reset,
    }

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the specified record as text with color based on its log level.

        Args:
            record (logging.LogRecord): The log record to be formatted.

        Returns:
            str: The formatted log message.
        """
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def setup_logger(name: str = "etl_logger", level: int = logging.INFO) -> logging.Logger:
    """
    Set up and return a logger with the custom colored formatter.

    Args:
        name (str, optional): Name of the logger. Defaults to "etl_logger".
        level (int, optional): Logging level (e.g., logging.INFO). Defaults to logging.INFO.

    Returns:
        logging.Logger: Configured logger instance.
    """
    handler: logging.StreamHandler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter())
    
    logger: logging.Logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.hasHandlers():
        logger.addHandler(handler)
    
    return logger
