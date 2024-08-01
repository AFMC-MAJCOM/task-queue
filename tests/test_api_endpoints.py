"""Test the API endpoints.
"""

import os

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

def test_put_valid_items():
    client.post("/api/v1/queue/put", json=default_items)
    assert queue.size(QueueItemStage.WAITING) == len(default_items)

def test_put_invalid_items():
    total_items_before = queue.size(QueueItemStage.WAITING) + \
        queue.size(QueueItemStage.PROCESSING) + \
        queue.size(QueueItemStage.FAIL) + \
        queue.size(QueueItemStage.SUCCESS)

    response = client.post("/api/v1/queue/put", json="bad-items")
    assert response.status_code == 422

    total_items_after = queue.size(QueueItemStage.WAITING) + \
        queue.size(QueueItemStage.PROCESSING) + \
        queue.size(QueueItemStage.FAIL) + \
        queue.size(QueueItemStage.SUCCESS)
    assert total_items_before == total_items_after
