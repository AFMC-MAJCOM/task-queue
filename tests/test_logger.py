"""Test the functionality of the logger
"""
import os
import pytest

from task_queue.logger import get_log_fp


def assert_log_fp(fp):
    """A helper method to verify that a given file path (fp) has the correct
    directory path and the file ends in fp.
    """
    log_dir = os.getcwd() + '/log'
    assert log_dir in fp
    assert ".log" in fp

@pytest.mark.unit
def test_log_dir():
    """Tests the get_log_fp returns the correct file path for a new log.
    """
    new_log_fp = get_log_fp()
    assert_log_fp(new_log_fp)
