"""Tests for validating the work queue api client
"""

from task_queue.work_queue_api_client import ApiClient

def test_constructor():
    client = ApiClient("localhost:8080")

    assert client.api_base_url == "localhost:8080/api/v1/queue/"
