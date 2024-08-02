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

def mocked_requests(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.status_code = status_code
            self.json_data = json_data

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RequestException("ERROR ", self.status_code)

    # Replace item_id passed into url
    if 'good-item-id' in args[0]:
        route = re.sub('good-item-id','{item_id}',args[0])

    if route in api_routes:
        return MockResponse({"good":"dictionary"},200)

    return MockResponse("Bad URL", 404)

def test_constructor():
    assert test_client.api_base_url == f"{url}/api/v1/queue/"

@mock.patch('requests.get', side_effect=mocked_requests)
def test_bad_url(mock_get):
    # It does not matter what endpoint is used here, we are just testing that
    # it is properly throwing an error when testing a bad url.
    with pytest.raises(RequestException):
        test_client.lookup_status('bad-url')

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_status(mock_get):
    response = test_client.lookup_status('good-item-id')
    assert isinstance(response, dict)

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_description(mock_get):
    response = test_client.description()
    assert isinstance(response, dict)

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_get_queue_sizes(mock_get):
    response = test_client.get_queue_sizes()
    assert isinstance(response, dict)
