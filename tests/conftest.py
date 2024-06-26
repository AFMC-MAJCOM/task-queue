# Source: https://stackoverflow.com/questions/69281822/how-to-only-run-a-pytest-fixture-cleanup-on-test-error-or-failure

import pytest
import os

# make sure environment variables are all good
# kind of a hack
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['FSSPEC_S3_ENDPOINT_URL'] = 'http://localhost:9000'
os.environ['S3_ENDPOINT_URL'] = 'http://localhost:9000'


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, "rep_" + rep.when, rep)
    