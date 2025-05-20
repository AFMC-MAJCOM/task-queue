"""Configurations to setup and help testing.
"""
import pytest

from task_queue.workers import work_queue
from task_queue.queues import memory_queue
from task_queue.workers.queue_worker_interface import DummyWorkerInterface
from tests.common_queue import default_items

# Source: https://stackoverflow.com/questions/69281822
# /how-to-only-run-a-pytest-fixture-cleanup-on-test-error-or-failure
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

# Fixtures
@pytest.fixture
def default_work_queue() -> work_queue.WorkQueue:
    """This is a fixture to create a work_queue for testing.

    Returns:
    -----------
    A default work_queue to be used for pytests.

    """
    queue = memory_queue()
    queue.put(default_items)
    interface = DummyWorkerInterface()
    return work_queue.WorkQueue(queue, interface)

