"""Tests for validating the work queue api client
"""

import pytest
import unittest
from unittest import mock
from requests.exceptions import RequestException

from task_queue.work_queue_api_client import ApiClient

url = "http://localhost:8000"
put_valid_body = {
    1: [1, 2, 3],
    2: [4, 5, 6]
}
put_invalid_body = "THIS_IS_INVALID"
test_client = ApiClient(url)

def mocked_requests_put(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RequestException("ERROR!")

    if args[0] == f"{url}/api/v1/queue/put":
        if 'json' in kwargs and type(kwargs['json']) == dict:
            return MockResponse("", 200)

    return MockResponse("", 400)

def test_constructor():
    assert test_client.api_base_url == f"{url}/api/v1/queue/"

@mock.patch('requests.post', side_effect=mocked_requests_put)
def test_put_valid_body(mock_post):
    response = test_client.put(put_valid_body)
    assert response == None

@mock.patch('requests.post', side_effect=mocked_requests_put)
def test_put_invalid_body(mock_post):
    with pytest.raises(RequestException):
        test_client.put(put_invalid_body)
