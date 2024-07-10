"""Configurations for pytests.
"""
import os

import pytest


os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['FSSPEC_S3_ENDPOINT_URL'] = 'http://localhost:9000'
os.environ['S3_ENDPOINT_URL'] = 'http://localhost:9000'


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Hook wrapper for testing.
    """
    # Execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # Set a report attribute for each phase of a call, which can
    # Be "setup", "call", "teardown"
    setattr(item, "rep_" + rep.when, rep)
