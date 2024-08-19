"""Test the API endpoints.
"""
import pytest
import os

from fastapi.testclient import TestClient

import tests.common_queue as qtest
import task_queue.queues.in_memory_queue as imq
from task_queue.queues.queue_base import QueueItemStage

# Disable the wrong import position warning because we only want to import
# work_queue_web_api after setting the environment variable for testing.
# pylint: disable=wrong-import-position
# ruff: noqa: E402
os.environ['QUEUE_IMPLEMENTATION'] = "in-memory"
from task_queue.api.work_queue_web_api import app, queue

client = TestClient(app)

n_items = 20
default_items = dict([qtest.random_item() for _ in range(n_items)])

@pytest.fixture(autouse=True)
def clean_queue():
    # Moves all queue items to WAITING stage before each pytest.
    proc_ids = queue.lookup_state(QueueItemStage.PROCESSING)
    for item in proc_ids:
        imq.move_dict_item(
            queue.memory_queue.processing,
            queue.memory_queue.waiting,
            item
        )
    succ_ids = queue.lookup_state(QueueItemStage.SUCCESS)
    for item in succ_ids:
        imq.move_dict_item(
            queue.memory_queue.success,
            queue.memory_queue.waiting,
            item
        )
    fail_ids = queue.lookup_state(QueueItemStage.FAIL)
    for item in fail_ids:
        imq.move_dict_item(
            queue.memory_queue.fail,
            queue.memory_queue.waiting,
            item
        )

def test_v1_queue_size():
    """Tests the size endpoint for all 4 stages.
    """
    queue.put(default_items)

    proc, succ, fail = queue.get(3)

    queue.success(succ[0])
    queue.fail(fail[0])

    wait_actual_size = 17
    proc_actual_size = 1
    succ_actual_size = 1
    fail_actual_size = 1

    wait_response_size = client.get("/api/v1/queue/size/WAITING")
    proc_response_size = client.get("/api/v1/queue/size/PROCESSING")
    succ_response_size = client.get("/api/v1/queue/size/SUCCESS")
    fail_response_size = client.get("/api/v1/queue/size/FAIL")

    assert wait_response_size.status_code == 200
    assert wait_response_size.json() == wait_actual_size

    assert proc_response_size.status_code == 200
    assert proc_response_size.json() == proc_actual_size

    assert succ_response_size.status_code == 200
    assert succ_response_size.json() == succ_actual_size

    assert fail_response_size.status_code == 200
    assert fail_response_size.json() == fail_actual_size


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
    response = client.get(f"/api/v1/queue/lookup_state/{queue_item_stage}")
    assert response.status_code == 200
    assert sorted(response.json()) == sorted(waiting_id_list)

    # Processing tests
    proc = queue.get(2)
    proc_id_list = [x for x,_ in proc]
    queue_item_stage = QueueItemStage.PROCESSING.name
    response = client.get(f"/api/v1/queue/lookup_state/{queue_item_stage}")
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
    """Tests the lookup_state endpoint failure.
    """
    response = client.get("/api/v1/queue/lookup_state/bad-stage")
    assert response.status_code == 400
    assert response.json() == {"detail": "bad-stage not a Queue Item Stage"}

def test_v1_queue_requeue_list():
    """Tests the requeue endpoint works when given a list.
    """
    queue.put(default_items)

    fail_ids = [fail_id for fail_id, _ in queue.get(3)]
    for fail_id in fail_ids:
        queue.fail(fail_id)

    # Check correct response when items are valid list
    response = client.post("/api/v1/queue/requeue", json=fail_ids)
    assert response.status_code == 200
    assert queue.size(QueueItemStage.FAIL) == 0
    assert queue.size(QueueItemStage.WAITING) == len(default_items)

def test_v1_queue_requeue_str():
    """Tests the requeue endpoint works when given a string.
    """
    queue.put(default_items)

    get = queue.get(1)

    queue.fail(get[0][0])

    # Check correct response when item is only a string
    response = client.post("/api/v1/queue/requeue", json=get[0][0])
    assert response.status_code == 200
    assert queue.size(QueueItemStage.FAIL) == 0
    assert queue.size(QueueItemStage.WAITING) == len(default_items)

def test_v1_queue_requeue_invalid():
    """Tests the requeue endpoint works when given an invalid input.
    """
    queue.put(default_items)

    # Check that it does not fail when input is invalid
    with pytest.warns(UserWarning):
        response = client.post("/api/v1/queue/requeue", json='bad-item-id')
        assert response.status_code == 200

def test_v1_queue_lookup_item():
    """Tests the lookup_item endpoint.
    """
    queue.put(default_items)

    get = queue.get(1)

    response_dict = {
        "item_id":get[0][0],
        "status":QueueItemStage.PROCESSING.value,
        "item_body":get[0][1],
    }
    # Check correct response when item_id is valid
    response = client.get(f"/api/v1/queue/lookup_item/{get[0][0]}")
    assert response.status_code == 200
    assert response.json() == response_dict

    # Check correct response when item_id is invalid
    response = client.get(f"/api/v1/queue/lookup_item/{'bad-item-id'}")
    assert response.status_code == 400
    assert response.json() == {"detail":"bad-item-id not in Queue"}

def test_get_success():
    """Test getting n_items successfully
    """
    queue.put(default_items)
    n = round(n_items / 2)
    response = client.get(f"/api/v1/queue/get/{n}")
    processing = queue.size(QueueItemStage.PROCESSING)

    assert response.status_code == 200
    assert len(response.json()) == n
    assert n == processing

def test_put_valid_items():
    response = client.post("/api/v1/queue/put", json=default_items)
    assert queue.size(QueueItemStage.WAITING) == len(default_items)
    assert response.status_code == 200

def test_put_invalid_items():
    total_items_before = queue.size(QueueItemStage.WAITING)

    response = client.post("/api/v1/queue/put", json="bad-items")
    assert response.status_code == 422

    total_items_after = queue.size(QueueItemStage.WAITING)
    assert total_items_before == total_items_after

def test_put_partial_invalid_items():
    bad_item = object()
    with pytest.raises(TypeError):
        client.post("/api/v1/queue/put", json={'good_item': bad_item})
