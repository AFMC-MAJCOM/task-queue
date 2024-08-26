"""Test the functionality of the logger
"""
import os
from logging import FileHandler, StreamHandler

from task_queue.logger import get_log_fp, create_logger


def assert_log_fp(fp):
    log_dir = os.getcwd() + '/log'
    assert log_dir in fp
    assert ".log" in fp
    

def test_log_dir_created():
    new_log_fp = get_log_fp()
    assert_log_fp(new_log_fp)


def test_logger_construction():
    logger_level = "INFO"
    name = __name__

    logger = create_logger(name, logger_level)
    for h in logger.handlers:
        if isinstance(h, FileHandler):
            assert_log_fp(h.baseFilename)
        elif isinstance(h, StreamHandler):
            assert h.stream.name == '<stdout>'
