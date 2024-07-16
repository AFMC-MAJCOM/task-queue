"""Configurations to setup and help testing.
"""
# Source: https://stackoverflow.com/questions/69281822
# /how-to-only-run-a-pytest-fixture-cleanup-on-test-error-or-failure
import pytest


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
