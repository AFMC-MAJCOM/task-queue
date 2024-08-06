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
    items = client.get(f"/api/v1/queue/get/{n}").json()
    processing = queue.size(QueueItemStage.PROCESSING)

    assert items.status_code == 200
    assert len(items) == n
    assert n == processing
