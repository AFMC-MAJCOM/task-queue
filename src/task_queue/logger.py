"""This file provides the construction and configuration of the logger
"""
import logging
import sys
import datetime
import os


def assign_handlers(_logger):
    """Assigns the file output and stream out put to a logger.

    Parameters:
    -----------
    _logger: Logger
        The logger that will have its handlers assigned.
    """
    file_handler = logging.FileHandler(get_log_fp())
    stream_handler = logging.StreamHandler(sys.stdout)
    # Add formatting to the statements
    fmt = logging.Formatter(fmt="%(asctime)s [%(levelname)s]: %(message)s")
    stream_handler.setFormatter(fmt)
    file_handler.setFormatter(fmt)
    # Add handlers to logger
    _logger.addHandler(stream_handler)
    _logger.addHandler(file_handler)


def create_logger(module_name: str, logger_level=logging.DEBUG):
    """Constructs and setups a logger for the task_queue module

    Parameters:
    -----------
    module_name: str
       The module name that will be logged
    logger_level: str | int
        The level that the logger will output to file and stream

    Returns:
    -----------
    The logger for the module
    """
    _logger = logging.getLogger(module_name)
    assign_handlers(_logger)
    # Set log level
    _logger.setLevel(logger_level)
    return _logger


def get_log_fp():
    """Constructs the modules log directory if none exists and returns
    the new log file filepath in that directory.

    Returns:
    -----------
    The filepath the new current log file
    """
    log = str(datetime.datetime.now())
    log = log.replace(" ", "_")
    log = log.replace(":", "_")
    log = log.split(".", maxsplit=1)[0]
    log = log.replace("-", "_")
    log = log + ".log"

    log_dir = os.getcwd() + "/logs"
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)
    return os.path.join(log_dir, log)


def set_logger_level(logger_level):
    """Verifies that the logger level providied is a valid logger level and
    then sets the logger to that level.

    Parameters:
    -----------
    logger_level: str | int
        The level that the logger will output to file and stream
    """
    if not isinstance(logger_level, int):
        try:
            logger_level = getattr(logging, logger_level)
        except AttributeError:
            logger.error("Incorrect logger level %s, defaulting to DEBUG",\
                         logger_level)
            return
    logger.setLevel(logger_level)


logger = create_logger(__name__)
