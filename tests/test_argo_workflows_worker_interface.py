from data_pipeline.argo_workflows_queue_worker import ArgoWorkflowsQueueWorker
from data_pipeline.queue_base import QueueItemStage
import random
import time
import os
import pytest

random_number = lambda: random.randint(0, 9999999)

run_argo_tests = os.environ.get('RUN_ARGO_TESTS', False)

if not run_argo_tests:
    pytest.skip(allow_module_level=True)

def make_queue_item(fail=False):
    queue_item_id = f"test-item-{random_number()}"

    queue_item_body = {
        "submit_body": {
            "resourceKind": "WorkflowTemplate",
            "resourceName": "queue-test-template",
            "submitOptions": {
                "parameters": [
                    f"bin_file=fake_bin_file_{random_number()}",
                    f"force-fail={fail}"
                ]
            }
        }
    }

    return (queue_item_id, queue_item_body)


def port_forwarded_worker():
    return ArgoWorkflowsQueueWorker(
        f"test-worker-{random_number()}",
        "http://localhost:2746",
        "pivot"
    )


def wait_for_finish(worker, queue_item_id):
    while True:
        results = worker.poll_all_status()
        status = results[queue_item_id]

        if status != QueueItemStage.PROCESSING:
            break

        time.sleep(1)

    return status


def test_argo_worker_end_to_end_success():
    worker = port_forwarded_worker()

    queue_item_id, queue_item_body = make_queue_item()

    worker.send_job(
        queue_item_id,
        queue_item_body
    )

    status = wait_for_finish(worker, queue_item_id)

    assert status == QueueItemStage.SUCCESS


def test_argo_worker_end_to_end_fail():
    worker = port_forwarded_worker()

    queue_item_id, queue_item_body = make_queue_item(fail=True)

    worker.send_job(
        queue_item_id,
        queue_item_body
    )

    status = wait_for_finish(worker, queue_item_id)

    assert status == QueueItemStage.FAIL


def test_argo_worker_end_to_end_concurrent():
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


def test_argo_worker_no_workflows():
    worker = port_forwarded_worker()

    statuses = worker.poll_all_status()

    assert len(statuses) == 0

def test_argo_worker_rerun_item():
    worker = port_forwarded_worker()

    # set up the first job to fail
    queue_item_id, queue_item_body = make_queue_item(fail=True)
    worker.send_job(queue_item_id, queue_item_body)
    wait_for_finish(worker, queue_item_id)

    # and the second job to succeed
    _, queue_item_body = make_queue_item(fail=False)
    worker.send_job(queue_item_id, queue_item_body)
    status = wait_for_finish(worker, queue_item_id)

    assert status == QueueItemStage.SUCCESS
