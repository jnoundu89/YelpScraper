import logging
import logging.config
import sys
from enum import Enum
from pathlib import Path


class LoggerManager:
    """Singleton for log configuration"""
    _instance = None

    def __new__(cls, log_level='INFO', process_name='app'):
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)
            cls._instance._configure(log_level, process_name)
        return cls._instance

    def _configure(self, log_level: str, process_name: str) -> None:
        s_path = './logs'
        Path(s_path).mkdir(parents=True, exist_ok=True)
        s_filename_path = f'{s_path}/{process_name}.log'

        # Set default terminal color to green
        sys.stdout.write("\033[38;5;76m")
        sys.stdout.flush()

        dc_config_logger = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'simple': {
                    'format': '%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(message)s'
                },
                'colored': {
                    '()': 'utilities.logging_utils.ColorFormatter'
                }
            },
            'handlers': {
                'log_file': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'level': log_level,
                    'formatter': 'simple',
                    'filename': s_filename_path,
                    'maxBytes': 10000000,
                    'backupCount': 1,
                    'encoding': 'utf8'
                },
                'log_console': {
                    'class': 'logging.StreamHandler',
                    'level': log_level,
                    'formatter': 'colored'
                }
            },
            'root': {
                'level': log_level,
                'handlers': ['log_file', 'log_console']
            }
        }

        logging.config.dictConfig(dc_config_logger)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logger configured successfully.")

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Factory method to get a named logger"""
        return logging.getLogger(name)


class Color(Enum):
    """
    Color class to define colors for the logger messages
    """
    SUCCESS = '38;5;76'
    ERROR = '91'
    WARNING = '38;5;214'


class ColorFormatter(logging.Formatter):
    """Custom formatter to add colors to log messages"""
    COLORS = {
        'DEBUG': Color.SUCCESS.value,
        'INFO': Color.SUCCESS.value,
        'WARNING': Color.WARNING.value,
        'ERROR': Color.ERROR.value,
        'CRITICAL': Color.ERROR.value,
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, Color.SUCCESS.value)
        record.msg = f"\033[{log_color}m{record.asctime} | {record.levelname} | {record.module} | {record.funcName} | {record.msg}\033[0m"
        return super().format(record)
