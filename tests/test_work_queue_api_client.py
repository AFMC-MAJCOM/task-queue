"""Tests for validating the Work Queue API Client
"""
import pytest
import re
from unittest import mock
from requests.exceptions import RequestException
from pydantic import ValidationError

from task_queue.api.work_queue_api_client import ApiClient
from task_queue.api.work_queue_web_api import app
from task_queue.queues.queue_base import QueueItemStage

url = "http://localhost:8000"
test_client = ApiClient(url)

# Get list of all possible endpoints
api_routes = [url + x.path for x in app.routes]

class MockResponse:
    def __init__(self, json_data, status_code):
        self.status_code = status_code
        self.json_data = json_data

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RequestException("ERROR ", self.status_code)

def mocked_requests_fail(*args, **kwargs):
    return MockResponse("Bad Response", 400)

def mocked_requests(*args, **kwargs):
    route = args[0]
    # Replace params passed into url, force expected return types for pydantic
    if 'good-item-id' in route:
        route = re.sub('good-item-id','{item_id}',route)
        if '/status/' in route:
            return MockResponse(QueueItemStage.WAITING,200)
        elif '/lookup_item/' in route:
            return MockResponse({"item_id":"good-item-id",
                                "status":QueueItemStage.WAITING,
                                "item_body":"good-item-body"},
                                200)
    if '/get/' in route:
        split = route.split('/')
        split[len(split) - 1] = '{n_items}'
        route = '/'.join(split)

    if "WAITING" in route:
        route = re.sub('WAITING','{queue_item_stage}',route)

    if route in api_routes:
        return MockResponse({"good":"dictionary"},200)

    return MockResponse("Bad URL", 404)

def test_constructor():
    assert test_client.api_base_url == f"{url}/api/v1/queue/"

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_status(mock_get):
    test_client.lookup_status('good-item-id')
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}status/good-item-id"

@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_lookup_status_fail(mock_get):
    with pytest.raises(RequestException):
        test_client.lookup_status('bad-item-id')

def test_client_lookup_status_invalid_parameter():
    with pytest.raises(ValidationError):
        test_client.lookup_status(1)

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_description(mock_get):
    test_client.description()
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}describe"

@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_description_fail(mock_get):
    with pytest.raises(RequestException):
        test_client.description()

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_get_queue_size(mock_get):
    test_client.get_queue_size(QueueItemStage.WAITING)
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}size/{QueueItemStage.WAITING}"

@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_get_queue_size(mock_get):
    with pytest.raises(RequestException):
        test_client.get_queue_size(QueueItemStage.WAITING)

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_get_queue_sizes(mock_get):
    test_client.get_queue_sizes()
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}sizes"

@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_get_queue_sizes_fail(mock_get):
    with pytest.raises(RequestException):
        test_client.get_queue_sizes()

@mock.patch('requests.post', side_effect=mocked_requests)
def test_client_requeue(mock_get):
    test_client.requeue('good-item-id')
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}requeue"

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_state(mock_get):
    response = test_client.lookup_state(QueueItemStage.WAITING)
    assert isinstance(response, dict)

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_item(mock_get):
    test_client.lookup_item('good-item-id')
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}lookup_item/good-item-id"

@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_lookup_item_fail(mock_get):
    with pytest.raises(RequestException):
        test_client.lookup_item('bad-item-id')

def test_client_lookup_item_invalid_parameter():
    with pytest.raises(ValidationError):
        test_client.lookup_status(1)

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_get(mock_get):
    get_test_value = 165475
    test_client.get(get_test_value)
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}get/{get_test_value}"

@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_get_fail(mock_get):
    with pytest.raises(RequestException):
        test_client.get()

def test_client_get_invalid_parameter():
    with pytest.raises(ValidationError):
        test_client.get(-1)

@mock.patch('requests.post', side_effect=mocked_requests)
def test_client_put(mock_post):
    test_client.put({})
    route = mock_post.call_args[0][0]
    assert route == f"{test_client.api_base_url}put"

@mock.patch('requests.post', side_effect=mocked_requests_fail)
def test_client_put_fail(mock_post):
    with pytest.raises(RequestException):
        test_client.put({})

def test_client_put_invalid_parameter():
    with pytest.raises(ValidationError):
        test_client.put("{}")
