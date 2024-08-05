"""Test the API endpoints.
"""

import os
import pytest

from fastapi.testclient import TestClient

import tests.common_queue as qtest
from task_queue.queue_base import QueueItemStage
# Disable the wrong import position warning because we only want to import
# work_queue_web_api after setting the environment variable for testing.
# pylint: disable=wrong-import-position
# ruff: noqa: E402
os.environ['QUEUE_IMPLEMENTATION'] = "in-memory"
from task_queue.work_queue_web_api import app, queue

client = TestClient(app)

n_items = 20
default_items = dict([qtest.random_item() for _ in range(n_items)])

@pytest.fixture(autouse=True)
def clean_queue():
    # Moves all queue items to WAITING stage before each pytest.
    queue.wait()

def test_v1_queue_sizes():
    """Tests the sizes endpoint.
    """
    queue.put(default_items)

    proc, succ, fail = queue.get(3)

    queue.success(succ[0])
    queue.fail(fail[0])

    sizes = {
        'WAITING' : n_items - 3,
        'PROCESSING' : 1,
        'SUCCESS' : 1,
        'FAIL' : 1,
    }

    response = client.get("/api/v1/queue/sizes")
    assert response.status_code == 200
    assert response.json() == sizes

def test_v1_queue_status():
    """Tests the status/{item_id} endpoint.
    """
    queue.put(default_items)

    proc, succ, fail = queue.get(3)

    queue.success(succ[0])
    queue.fail(fail[0])

    response = client.get(f"/api/v1/queue/status/{proc[0]}")
    assert response.status_code == 200
    assert response.json() == QueueItemStage.PROCESSING.value

    response = client.get(f"/api/v1/queue/status/{succ[0]}")
    assert response.status_code == 200
    assert response.json() == QueueItemStage.SUCCESS.value

    response = client.get(f"/api/v1/queue/status/{fail[0]}")
    assert response.status_code == 200
    assert response.json() == QueueItemStage.FAIL.value

    response = client.get(f"/api/v1/queue/status/{'bad-item-id'}")
    assert response.status_code == 400
    assert response.json() == {"detail":"bad-item-id not in Queue"}

def test_v1_queue_describe():
    """Tests the describe endpoint.
    """
    desc = {'implementation': 'InMemoryQueue',
            'arguments': {}}

    response = client.get("/api/v1/queue/describe")
    assert response.status_code == 200
    assert response.json() == desc

def test_v1_queue_lookup_state():
    """Tests the lookup_state endpoint.
    """
    queue.put(default_items)

    # Waiting test
    waiting_id_list = [x for x in default_items]
    queue_item_stage = QueueItemStage.WAITING.name
    print(queue_item_stage)
    response = client.get(f"/api/v1/queue/lookup_state/{queue_item_stage}")
    print(response.json())
    assert response.status_code == 200
    assert sorted(response.json()) == sorted(waiting_id_list)

    # Processing tests
    proc = queue.get(2)
    proc_id_list = [x for x,_ in proc]
    queue_item_stage = QueueItemStage.PROCESSING.name
    response = client.get(f"/api/v1/queue/lookup_state/{queue_item_stage}")
    print(sorted(proc_id_list))
    print()
    print(sorted(response.json()))
    assert response.status_code == 200
    assert sorted(response.json()) == sorted(proc_id_list)

    # Success test
    succ = queue.get(2)
    succ_id_list = [x for x,_ in succ]
    for i in succ:
        queue.success(i[0])
    queue_item_stage = QueueItemStage.SUCCESS.name
    response = client.get(f"/api/v1/queue/lookup_state/{queue_item_stage}")
    assert response.status_code == 200
    assert sorted(response.json()) == sorted(succ_id_list)

    # Fail test
    fail = queue.get(2)
    fail_id_list = [x for x,_ in fail]
    for i in fail:
        queue.fail(i[0])
    queue_item_stage = QueueItemStage.FAIL.name
    response = client.get(f"/api/v1/queue/lookup_state/{queue_item_stage}")
    assert response.status_code == 200
    assert sorted(response.json()) == sorted(fail_id_list)

def test_v1_queue_lookup_state_fail():
    response = client.get("/api/v1/queue/lookup_state/bad-stage")
    assert response.status_code == 400
    assert response.json() == {"detail": "bad-stage not a Queue Item Stage"}
