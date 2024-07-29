"""Test the API endpoints.
"""
import json
import pytest

from fastapi.testclient import TestClient
from fastapi import FastAPI

import tests.common_queue as qtest
from task_queue.in_memory_queue import in_memory_queue
# from task_queue.work_queue_web_api import app
app = FastAPI()

client = TestClient(app)

n_items = 15
default_items = dict([qtest.random_item() for _ in range(n_items)])

def test_v1_queue_sizes():
    """Tests the sizes endpoint.
    """
    queue = in_memory_queue()

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

    response = client.get(f"/api/v1/queue/sizes")

    assert response.status_code == 200
    assert response.json() = sizes
    # is there a case where this will fail?

def test_v1_queue_status():
    """Tests the status/{item_id} endpoint.
    """
    queue.put(default_items)

    proc, succ, fail = queue.get(3)

    queue.success(succ[0])
    queue.fail(fail[0])

    response = client.get(f"/api/v1/queue/status/{proc[0]}")
    assert response.status_code == 200
    assert response.json() == qb.QueueItemStage.PROCESSING
    response = client.get(f"/api/v1/queue/status/{succ[0]}")
    assert response.status_code == 200
    assert response.json() == qb.QueueItemStage.SUCCESS
    response = client.get(f"/api/v1/queue/status/{fail[0]}")
    assert response.status_code == 200
    assert response.json() == qb.QueueItemStage.FAIL
    response = client.get(f"/api/v1/queue/status/{'bad-item-id'}")
    # insert response type
    # something about key error

def test_v1_queue_describe():
    """Tests the describe endpoint.
    """
    desc = {"implementation": "memory"}

    response = client.get(f"/api/v1/queue/describe")
    assert response.status_code == 200
    assert response.json() == desc
