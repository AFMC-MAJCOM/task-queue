"""Pytests for process queue worker.
"""
import random
import pytest
import time
from pydantic import ValidationError

from task_queue.workers.process_queue_worker import ProcessQueueWorker
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
    temp_file.write_text("import sys\ndef do_nothing():\n    pass")
    return temp_file

@pytest.fixture(scope="module")
def temp_script_bad(temp_dir):
    """Create python script used for testing with a syntax error."""
    # Create a file inside the temporary directory
    temp_file = temp_dir / "my_bad_script.py"
    temp_file.write_text("syntax error")
    return temp_file


def make_queue_item(fail=False):
    """Creates a random queue item for testing.

    Returns:
    ----------
    Returns a queue item id and body to use for testing.
    """
    queue_item_id = f"test-item-{random.randint(0, 9999999)}"

    if not fail:
        queue_item_body = {
            "file_name" : "my_script.py",
            "args": ['arg1','arg2']
            }
    else:
        queue_item_body = {
            "file_name" : "my_bad_script.py",
            "args": ['arg1','arg2']
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

@pytest.mark.unit
def test_process_worker_success_with_arg(process_worker, temp_script_good):
    """Tests job success with at least one arg."""
    queue_item_id, queue_item_body = make_queue_item()

    process_worker.send_job(
        queue_item_id,
        queue_item_body
    )
    status = wait_for_finish(process_worker, queue_item_id)

    assert status == QueueItemStage.SUCCESS

@pytest.mark.unit
def test_process_worker_success_no_arg(process_worker, temp_script_good):
    """Tests job success with no args."""
    queue_item_id, _ = make_queue_item()
    queue_item_body = {"file_name" : "my_script.py"}

    process_worker.send_job(
        queue_item_id,
        queue_item_body
    )
    status = wait_for_finish(process_worker, queue_item_id)

    assert status == QueueItemStage.SUCCESS

@pytest.mark.unit
def test_process_worker_fail(process_worker, temp_script_bad):
    """Tests job failure."""
    queue_item_id, queue_item_body = make_queue_item(fail=True)
    process_worker.send_job(
        queue_item_id,
        queue_item_body
    )
    status = wait_for_finish(process_worker, queue_item_id)

    assert status == QueueItemStage.FAIL

@pytest.mark.unit
def test_process_invalid_body(process_worker):
    """Tests pydantic model catches bad queue_item_body."""
    queue_item_id, _ = make_queue_item()
    queue_item_body = {"bad_key" : "bad_value"}

    with pytest.raises(ValidationError):
        process_worker.send_job(
            queue_item_id,
            queue_item_body
        )
        status = wait_for_finish(process_worker, queue_item_id)

        assert status == QueueItemStage.FAIL

@pytest.mark.unit
def test_process_no_processes(process_worker):
    """Test poll_all_status with no processes does not break.
    """
    statuses = process_worker.poll_all_status()

    assert len(statuses) == 0

@pytest.mark.unit
def test_process_rerun_item(process_worker, temp_script_good, temp_script_bad):
    """Tests process can rerun a job.
    """
    # Set up the first job to fail
    queue_item_id, queue_item_body = make_queue_item(fail=True)
    process_worker.send_job(queue_item_id, queue_item_body)
    wait_for_finish(process_worker, queue_item_id)

    # And the second job to succeed
    _, queue_item_body = make_queue_item()
    process_worker.send_job(queue_item_id, queue_item_body)
    status = wait_for_finish(process_worker, queue_item_id)

    assert status == QueueItemStage.SUCCESS

@pytest.mark.unit
def test_process_worker_end_to_end_concurrent(
    process_worker,
    temp_script_good,
    temp_script_bad
):
    """Test multiple items running concurrently, with some succeeding and some
    failing.
    """
    n_processes = 10
    n_fail = 0

    for i in range(n_processes):
        should_fail = i % 3 == 0

        if should_fail:
            n_fail += 1

        queue_item_id, queue_item_body = make_queue_item(fail=should_fail)

        process_worker.send_job(
            queue_item_id,
            queue_item_body
        )

    while True:
        results = process_worker.poll_all_status()

        statuses = list(results.values())

        if all(s != QueueItemStage.PROCESSING for s in statuses):
            break

        time.sleep(1)

    assert sum(s == QueueItemStage.SUCCESS for s in statuses) == \
        n_processes - n_fail
    assert sum(s == QueueItemStage.FAIL for s in statuses) == n_fail
