"""Tests for validating the Work Queue API Client
"""
import pytest
import re
from unittest import mock
from requests.exceptions import RequestException

from task_queue.work_queue_api_client import ApiClient
from task_queue.queue_base import QueueItemStage
from task_queue.work_queue_web_api import app

url = "http://localhost:8000"
test_client = ApiClient(url)

# Get list of all possible endpoints
api_routes = [url + x.path for x in app.routes]
print("\n-----------------------------------------")
print(api_routes)

def mocked_requests(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RequestException("ERROR ", self.status_code)
    ## Redo this to be the opposite - look for api_route in my string and regex out the {---}
    # if args[0] not in api_routes:
    #     print(args[0])
    #     return MockResponse("Invalid URL", 404)
    # Mock response for lookup_status
    if args[0] == f"{url}/api/v1/queue/status/good-item-id":
        return MockResponse(QueueItemStage.WAITING,200)
    elif args[0] == f"{url}/api/v1/queue/status/bad-item-id":
        return MockResponse("bad-item-id not in Queue", 400)
    # Mock response for description
    elif args[0] == f"{url}/api/v1/queue/describe":
        return MockResponse({"good":"description"},200)
    # Mock response for get_queue_sizes
    elif args[0] == f"{url}/api/v1/queue/sizes":
        return MockResponse({"good":"sizes"},200)
    return MockResponse("", 400)

def test_constructor():
    assert test_client.api_base_url == f"{url}/api/v1/queue/"

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_status(mock_get):
    item_id = 'good-item-id'
    response = test_client.lookup_status(item_id)
    assert isinstance(response, QueueItemStage)

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_status_invalid(mock_get):
    with pytest.raises(RequestException):
        test_client.lookup_status(123)

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_status_fail(mock_get):
    response = test_client.lookup_status('bad-item-id')
    assert response == "bad-item-id not in Queue"

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_description(mock_get):
    response = test_client.description()
    assert isinstance(response, dict)

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_get_queue_sizes(mock_get):
    response = test_client.get_queue_sizes()
    assert isinstance(response, dict)