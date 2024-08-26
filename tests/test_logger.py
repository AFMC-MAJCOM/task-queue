"""Test the functionality of the logger
"""
import os

from task_queue.logger import get_log_fp


def assert_log_fp(fp):
    log_dir = os.getcwd() + '/log'
    assert log_dir in fp
    assert ".log" in fp
    

def test_log_dir_created():
    new_log_fp = get_log_fp()
    assert_log_fp(new_log_fp)
