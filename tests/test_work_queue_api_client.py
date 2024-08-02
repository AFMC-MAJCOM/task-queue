"""Tests for validating the work queue api client
"""
import os
from unittest import mock
from requests.exceptions import RequestException

from task_queue.work_queue_api_client import ApiClient

# Disable the wrong import position warning because we only want to import
# work_queue_web_api after setting the environment variable for testing.
# pylint: disable=wrong-import-position
# ruff: noqa: E402
os.environ['QUEUE_IMPLEMENTATION'] = "in-memory"
from task_queue.work_queue_web_api import app

url = "http://localhost:8000"
test_client = ApiClient(url)

def mocked_requests_put(*args, **kwargs):
    class MockResponse:
        def __init__(self, status_code):
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RequestException("ERROR!")

    if args[0] in [f"{url}{x.path}" for x in app.routes]:
        return MockResponse(200)

    return MockResponse(400)

def test_constructor():
    assert test_client.api_base_url == f"{url}/api/v1/queue/"

@mock.patch('requests.post', side_effect=mocked_requests_put)
def test_put_route_exists(mock_post):
    # This method is just asserting that no exception is raised
    response = test_client.put({})
    assert response is None
