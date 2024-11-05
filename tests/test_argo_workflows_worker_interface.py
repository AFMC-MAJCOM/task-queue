"""Pytests for argo workflow queue worker.
"""
import random
import time
import requests

import pytest

from task_queue.workers.argo_workflows_queue_worker import (
                                                    ArgoWorkflowsQueueWorker)
from task_queue.queues.queue_base import QueueItemStage
from .test_config import TaskQueueTestSettings

run_argo_tests = TaskQueueTestSettings().run_argo_tests

if not run_argo_tests:
    pytest.skip(allow_module_level=True)

def make_queue_item(fail=False, run_time=1):
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
        "submit_body": {
            "resourceKind": "WorkflowTemplate",
            "resourceName": "queue-test-template",
            "submitOptions": {
                "parameters": [
                    f"bin_file=fake_bin_file_{random.randint(0, 9999999)}",
                    f"force-fail={fail}",
                    f"run-time-sec={run_time}"
                ]
            }
        }
    }

    return (queue_item_id, queue_item_body)


def port_forwarded_worker():
    """Creates a test worker that connects to an argo workflows instance that
    is port forwarded to this host on port 2746 (the default port).

    Returns:
    -----------
    Test worker.
    """
    return ArgoWorkflowsQueueWorker(
        f"test-worker-{random.randint(0, 9999999)}",
        "http://localhost:2746",
        "pivot"
    )

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

def get_workflow_ids(worker):
    """Returns the workflow ids of all jobs in the argo workflows.
    """
    url = "http://localhost:2746/api/v1/workflows/pivot"
    wf = requests.get(url).json()

    item_ids = []
    for item in wf.get("items",[]):
        labels = worker.get_labels(item)
        item_ids.append(labels['work-queue.queue-item-id'])
    return item_ids

@pytest.mark.integration
def test_argo_worker_end_to_end_success():
    """Test item succeeds.
    """
    worker = port_forwarded_worker()

    queue_item_id, queue_item_body = make_queue_item()

    worker.send_job(
        queue_item_id,
        queue_item_body
    )

    status = wait_for_finish(worker, queue_item_id)

    assert status == QueueItemStage.SUCCESS

@pytest.mark.integration
def test_argo_worker_end_to_end_fail():
    """Test item fails.
    """
    worker = port_forwarded_worker()

    queue_item_id, queue_item_body = make_queue_item(fail=True)

    worker.send_job(
        queue_item_id,
        queue_item_body
    )

    status = wait_for_finish(worker, queue_item_id)

    assert status == QueueItemStage.FAIL

@pytest.mark.integration
def test_argo_worker_end_to_end_concurrent():
    """Test multiple items running concurrently, with some succeeding and some
    failing.
    """
    worker = port_forwarded_worker()

    n_processes = 10
    n_fail = 0

    for i in range(n_processes):
        should_fail = i % 3 == 0

        if should_fail:
            n_fail += 1

        queue_item_id, queue_item_body = make_queue_item(fail=should_fail)
        worker.send_job(
            queue_item_id,
            queue_item_body
        )

    while True:
        results = worker.poll_all_status()

        statuses = list(results.values())

        if all(s != QueueItemStage.PROCESSING for s in statuses):
            break

        time.sleep(1)

    assert sum(s == QueueItemStage.SUCCESS for s in statuses) == \
        n_processes - n_fail
    assert sum(s == QueueItemStage.FAIL for s in statuses) == n_fail

@pytest.mark.integration
def test_argo_worker_no_workflows():
    """Test worker with no workflows works as expected.
    """
    worker = port_forwarded_worker()

    statuses = worker.poll_all_status()

    assert len(statuses) == 0

@pytest.mark.integration
def test_argo_worker_rerun_item():
    """Tests argo can rerun a job.
    """
    worker = port_forwarded_worker()

    # Set up the first job to fail
    queue_item_id, queue_item_body = make_queue_item(fail=True)
    worker.send_job(queue_item_id, queue_item_body)
    wait_for_finish(worker, queue_item_id)

    # And the second job to succeed
    _, queue_item_body = make_queue_item(fail=False)
    worker.send_job(queue_item_id, queue_item_body)
    status = wait_for_finish(worker, queue_item_id)

    assert status == QueueItemStage.SUCCESS

@pytest.mark.integration
def test_argo_worker_delete_workflows():
    """Test that a completed argo workflow is deleted after updating Queue."""
    worker = port_forwarded_worker()

    queue_item_id, queue_item_body = make_queue_item()
    worker.send_job(
        queue_item_id,
        queue_item_body
    )
    assert queue_item_id in get_workflow_ids(worker)
    wait_for_finish(worker, queue_item_id)
    assert queue_item_id not in get_workflow_ids(worker)

    queue_item_id, queue_item_body = make_queue_item(fail=True)
    worker.send_job(
        queue_item_id,
        queue_item_body
    )
    assert queue_item_id in get_workflow_ids(worker)
    wait_for_finish(worker, queue_item_id)
    assert queue_item_id not in get_workflow_ids(worker)
