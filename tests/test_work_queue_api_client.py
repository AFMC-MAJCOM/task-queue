"""Tests for validating the Work Queue API Client
"""
import pytest
import re
from unittest import mock
from requests.exceptions import RequestException

from task_queue.work_queue_api_client import ApiClient
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

    route = args[0]
    # Replace item_id passed into url
    if 'good-item-id' in route:
        route = re.sub('good-item-id','{item_id}',route)

    if route in api_routes:
        return MockResponse({"good":"dictionary"},200)

    return MockResponse("Bad URL", 404)

def test_constructor():
    assert test_client.api_base_url == f"{url}/api/v1/queue/"

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_item(mock_get):
    response = test_client.lookup_item('good-item-id')
    assert isinstance(response, dict)
