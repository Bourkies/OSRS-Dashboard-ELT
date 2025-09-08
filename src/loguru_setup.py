# src/loguru_setup.py
from loguru import logger
import os
from pathlib import Path
import sys

def loguru_setup(config, project_root):
    '''Configures the logging setup based on the setting inside the config file under the [logging] section'''

    logger.trace('Start function "loguru_setup"')

    log_config = config.get('logging', {})
    log_file = project_root / log_config.get('file_path', 'logs/default.log')
    log_dir = Path(log_file).parent
    os.makedirs(log_dir, exist_ok=True)
    logger.remove()

    # Define a colorized format for the console. This ensures console output is
    # always colored, while the format string in the config file is used for the
    # plain-text log file.
    console_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name: <10.10}:{function: <10.10}:{line: <4}</cyan> | "
        "<level>{message}</level>"
    )

    # Add a handler for console output
    logger.add(
        sys.stderr,
        level=log_config.get('level', 'INFO'),
        format=console_format,
        colorize=True
    )

    # Add a handler for file output using settings from the config file
    logger.add(
        log_file,
        level=log_config.get('level', 'INFO'),
        format=log_config.get('format'),
        rotation=log_config.get('rotation', '10 MB'),
        retention=log_config.get('retention', '7 days'),
        enqueue=True,
        backtrace=True
    )

    logger.debug(f'Log settings applied: {log_config}')
    logger.trace('End function "loguru_setup"')
    