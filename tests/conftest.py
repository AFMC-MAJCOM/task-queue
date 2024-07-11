"""Functions used to setup and help testing"""
# Source: https://stackoverflow.com/questions/69281822
# /how-to-only-run-a-pytest-fixture-cleanup-on-test-error-or-failure

import pytest
import s3fs


@pytest.fixture(scope="session", autouse=True)
def setup_s3_bucket():
    """Create a 'unit-tests' S3 bucket for testing purposes.
    """
    fs = s3fs.S3FileSystem()

    test_bucket_name = 'unit-tests'
    if fs.exists(test_bucket_name):
        fs.rm(test_bucket_name, recursive=True)
    fs.mkdir(test_bucket_name)

    yield
    cleanup_bucket(test_bucket_name, fs)


def cleanup_bucket(test_bucket_name, fs):
    """Delete the created bucket that was used for testing.
    """
    if fs.exists(test_bucket_name):
        fs.rm(test_bucket_name)


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, "rep_" + rep.when, rep)
