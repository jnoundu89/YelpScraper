import logging
import logging.config
from pathlib import Path


def configure_logging(s_log_level: str, s_process_name: str) -> None:
    """
    Set up the logging configuration for the process name and log level provided as arguments to the function call
    :param s_log_level: str - log level
    :param s_process_name: str - process name
    :return: None
    """

    s_path = './logs'

    Path(s_path).mkdir(parents=True, exist_ok=True)

    s_filename_path = f'{s_path}/{s_process_name}.log'

    dc_config_logger = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(message)s'
            }
        },
        'handlers': {
            'log_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': s_log_level,
                'formatter': 'simple',
                'filename': s_filename_path,
                'maxBytes': 50000000,
                'backupCount': 1,
                'encoding': 'utf8'
            },
            'log_console': {
                'class': 'logging.StreamHandler',
                'level': s_log_level,
                'formatter': 'simple'
            }
        },
        'root': {
            'level': s_log_level,
            'handlers': ['log_file', 'log_console']
        }
    }

    logging.config.dictConfig(dc_config_logger)
