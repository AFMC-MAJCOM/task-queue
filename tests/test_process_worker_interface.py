"""Pytests for process queue worker.
"""
import random
import pytest
import time

from task_queue.workers.process_queue_worker import ProcessWorkerInterface
from task_queue.queues.queue_base import QueueItemStage

@pytest.fixture(scope="module")
def temp_dir(tmp_path_factory):
    """Create temp directory for testing."""
    # Use tmp_path to create a temporary directory
    temp_dir = tmp_path_factory.mktemp("scripts")
    return temp_dir

@pytest.fixture(scope="module")
def temp_script_good(temp_dir):
    """Create python script used for testing."""
    # Create a file inside the temporary directory
    temp_file = temp_dir / "my_script.py"
    temp_file.write_text("def dummy_script(dummy_input):\n    pass\ndummy_script(input_data)")
    return temp_file

@pytest.fixture(scope="module")
def temp_script_bad(temp_dir):
    """Create python script used for testing."""
    # Create a file inside the temporary directory
    temp_file = temp_dir / "my_script.py"
    temp_file.write_text("def dummy_script(dummy_param)print('this has a syntax error')")
    return temp_file


def make_queue_item(fail=False):
    """Creates a random queue item for testing.

    Paramters:
    ----------
    fail: boolean (default=false)
        Allows fail to be forced.

    Returns:
    ----------
    Returns a queue item id and body to use for testing.
    """
    queue_item_id = f"test-item-{random.randint(0, 9999999)}"

    queue_item_body = {
        "file_name" : "my_script.py",
        "parameters": fail
        }

    return (queue_item_id, queue_item_body)

@pytest.fixture(scope="module")
def process_worker(temp_dir):
    """Creates process worker interface used for testing."""
    return ProcessWorkerInterface(temp_dir)

def wait_for_finish(worker, queue_item_id):
    """Runs until item moves out of processing stage.

    Returns:
    -----------
    New status of the item.
    """
    while True:
        results = worker.poll_all_status()
        status = results[queue_item_id]

        if status != QueueItemStage.PROCESSING:
            break

        time.sleep(1)
    worker.delete_job(queue_item_id)
    return status

def test_process_interface_success(process_worker, temp_script_good):
    """Tests job success."""
    queue_item_id, queue_item_body = make_queue_item()

    process_worker.send_job(
        queue_item_id,
        queue_item_body
    )
    status = wait_for_finish(process_worker, queue_item_id)

    assert status == QueueItemStage.SUCCESS

def test_process_interface_fail(process_worker, temp_script_bad):
    """Tests job failure."""
    queue_item_id, queue_item_body = make_queue_item()
    process_worker.send_job(
        queue_item_id,
        queue_item_body
    )
    status = wait_for_finish(process_worker, queue_item_id)

    assert status == QueueItemStage.FAIL
