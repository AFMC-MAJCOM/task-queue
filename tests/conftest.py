# Source: https://stackoverflow.com/questions/69281822
# /how-to-only-run-a-pytest-fixture-cleanup-on-test-error-or-failure

import s3fs
import pytest
import os
from data_pipeline import config

# make sure environment variables are all good
# kind of a hack
# os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
# os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
# os.environ['FSSPEC_S3_ENDPOINT_URL'] = os.environ["S3_ENDPOINT"]
# os.environ['FSSPEC_S3_ENDPOINT_URL'] = 'http://localhost:9000'
# os.environ['S3_ENDPOINT_URL'] = 'http://localhost:9000'
# os.environ['S3_ENDPOINT_URL'] = os.environ["S3_ENDPOINT"]

#  s3 = s3fs.S3FileSystem(
#             client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
# )



@pytest.fixture(scope="session", autouse=True)
def setup_s3_bucket():
    """
    """
    # if os.environ["S3_ENDPOINT"] and os.environ["AWS_ACCESS_KEY_ID"] and os.environ["AWS_SECRET_ACCESS_KEY"]:
    #     print('we made it heregfdgsdfgdsFDSFDS')
    #     os.environ['AWS_ACCESS_KEY_ID'] = os.environ['AWS_ACCESS_KEY_ID']
    #     os.environ['AWS_SECRET_ACCESS_KEY'] = os.environ['AWS_SECRET_ACCESS_KEY']
    #     os.environ['FSSPEC_S3_ENDPOINT_URL'] = os.environ["S3_ENDPOINT"]
    #     os.environ['S3_ENDPOINT_URL'] = os.environ["S3_ENDPOINT"]
    # else:
    #     print('the environ vars dont exist')
    #     os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
    #     os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
    #     os.environ['FSSPEC_S3_ENDPOINT_URL'] = 'http://localhost:9000'
    #     os.environ['S3_ENDPOINT_URL'] = 'http://localhost:9000'
    # fs = s3fs.S3FileSystem(
    #     client_kwargs={'endpoint_url': os.environ['S3_ENDPOINT']}
    # )
    fs = config.get_s3fs_connection()

    test_bucket_name = 'unit-tests'
    if fs.exists(test_bucket_name):
        fs.rm(test_bucket_name, recursive=True)
    fs.mkdir(test_bucket_name)

    yield
    cleanup_bucket(test_bucket_name, fs)


def cleanup_bucket(test_bucket_name, fs):
    """
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
