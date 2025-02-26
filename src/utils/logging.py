import logging
import json
from datetime import datetime
from pythonjsonlogger import jsonlogger
import sys
from typing import Optional, Dict, Any
import os


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter for structured logging with enhanced metadata"""

    def __init__(self, time_format: str = "%Y.%m.%d T %H:%M"):
        fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
        super().__init__(fmt=fmt)
        self.time_format = time_format

    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]) -> None:
        """Add custom fields to the log record"""
        super().add_fields(log_record, record, message_dict)

        # Format timestamp according to specified format
        if record.created:
            timestamp = datetime.fromtimestamp(record.created)
            log_record['timestamp'] = timestamp.strftime(self.time_format)

        # Add standard fields
        log_record['level'] = record.levelname
        log_record['logger'] = record.name

        # Add correlation ID for request tracing
        log_record['correlation_id'] = getattr(record, 'correlation_id', None)

        # Add process and thread information
        log_record['process_id'] = os.getpid()
        log_record['thread_id'] = record.thread
        log_record['thread_name'] = record.threadName

        # Add any extra fields from the record
        if hasattr(record, 'extras'):
            for key, value in record.extras.items():
                log_record[key] = value


def setup_logging(
        service_name: str,
        log_level: str = "INFO",
        time_format: str = "%Y.%m.%d T %H:%M",
        log_dir: str = "logs"
) -> logging.Logger:
    """Configure application-wide logging with structured output"""

    # Create logger
    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, log_level.upper()))

    # Clear any existing handlers
    logger.handlers = []

    # Create formatter
    formatter = CustomJsonFormatter(time_format=time_format)

    # Console handler with color support
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler with rotation
    try:
        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)

        # Create file handler with daily rotation
        from logging.handlers import TimedRotatingFileHandler
        file_handler = TimedRotatingFileHandler(
            filename=f"{log_dir}/{service_name}.log",
            when='midnight',
            interval=1,
            backupCount=30,  # Keep 30 days of logs
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    except Exception as e:
        # Log to console if file handler creation fails but continue
        console_handler.setLevel(logging.WARNING)
        logger.warning(
            f"Could not create file handler: {str(e)}. Continuing with console logging only."
        )

    # Add custom log levels if needed
    logging.addLevelName(logging.INFO + 1, 'AUDIT')
    logging.addLevelName(logging.ERROR + 1, 'SECURITY')

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the current configuration"""
    return logging.getLogger(name)


class LogContext:
    """Context manager for temporarily adding context to logs"""

    def __init__(self, logger: logging.Logger, **context):
        self.logger = logger
        self.context = context
        self.old_context = {}

    def __enter__(self):
        # Save current context and update with new values
        for key, value in self.context.items():
            if hasattr(self.logger, key):
                self.old_context[key] = getattr(self.logger, key)
            setattr(self.logger, key, value)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore previous context
        for key in self.context:
            if key in self.old_context:
                setattr(self.logger, key, self.old_context[key])
            else:
                delattr(self.logger, key)