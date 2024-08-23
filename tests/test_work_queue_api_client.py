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
    if '/requeue' in route:
        return MockResponse(None,200)

    if '/get/' in route:
        split = route.split('/')
        split[len(split) - 1] = '{n_items}'
        route = '/'.join(split)

    if "WAITING" in route:
        route = re.sub('WAITING','{queue_item_stage}',route)

    if route in api_routes:
        return MockResponse({"good":"dictionary"},200)

    return MockResponse("Bad URL", 404)

@pytest.mark.unit
def test_constructor():
    """Tests that the Client constructor buils a proper base url."""
    assert test_client.api_base_url == f"{url}/api/v1/queue/"

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_status(mock_get):
    """Tests that lookup_status hits the correct endpoint."""
    test_client.lookup_status('good-item-id')
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}status/good-item-id"

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_lookup_status_fail(mock_get):
    """Tests that Client handles error if lookup_status input is bad."""
    with pytest.raises(RequestException):
        test_client.lookup_status('bad-item-id')

@pytest.mark.unit
def test_client_lookup_status_invalid_parameter():
    """Tests that Client throws pydantic error for lookup_status."""
    with pytest.raises(ValidationError):
        test_client.lookup_status(1)

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_description(mock_get):
    """Tests that Client description hits the correct endpoint."""
    test_client.description()
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}describe"

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_description_fail(mock_get):
    """Tests that Client handles error if description has a bad response."""
    with pytest.raises(RequestException):
        test_client.description()

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_size(mock_get):
    response = test_client.size(QueueItemStage.WAITING)
    assert isinstance(response, dict)

@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_size_fail(mock_get):
    with pytest.raises(RequestException):
        test_client.size(QueueItemStage.WAITING)

def test_client_size_invalid_parameter():
    with pytest.raises(ValidationError):
        test_client.size("BAD_STAGE")

@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_sizes(mock_get):
    """Tests that Client sizes hits the correct endpoint."""
    test_client.sizes()
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}sizes"

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_sizes_fail(mock_get):
    """Tests that Client handles error if sizes has a bad response.
    """
    with pytest.raises(RequestException):
        test_client.sizes()

@pytest.mark.unit
@mock.patch('requests.post', side_effect=mocked_requests)
def test_client_requeue(mock_post):
    """Tests that Client requeue hits the correct endpoint."""
    test_client.requeue('good-item-id')
    route = mock_post.call_args[0][0]
    assert route == f"{test_client.api_base_url}requeue"

@pytest.mark.unit
@mock.patch('requests.post', side_effect=mocked_requests_fail)
def test_client_requeue_fail(mock_post):
    """Tests that Client handles error if requeue has a bad response"""
    with pytest.raises(RequestException):
        test_client.requeue('')

@pytest.mark.unit
def test_client_requeue_invalid_parameter():
    """Tests that Client Client throws pydantic error for requeue."""
    with pytest.raises(ValidationError):
        test_client.requeue(1)

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_state(mock_get):
    """Tests that Client lookup_state hits the correct endpoint."""
    response = test_client.lookup_state(QueueItemStage.WAITING)
    assert isinstance(response, dict)

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_lookup_state_fail(mock_get):
    """Tests that Client handles error if lookup_state has a bad response"""
    with pytest.raises(RequestException):
        test_client.lookup_state(QueueItemStage.WAITING)

@pytest.mark.unit
def test_client_lookup_state_invalid_parameter():
    """Tests that Client throws pydantic error for lookup_state."""
    with pytest.raises(ValidationError):
        test_client.lookup_state([])

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_lookup_item(mock_get):
    """Tests that Client lookup_item hits the correct endpoint."""
    test_client.lookup_item('good-item-id')
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}lookup_item/good-item-id"

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_lookup_item_fail(mock_get):
    """Tests that Client handles error if lookup_item has a bad response."""
    with pytest.raises(RequestException):
        test_client.lookup_item('bad-item-id')

@pytest.mark.unit
def test_client_lookup_item_invalid_parameter():
    """Tests that Client throws pydantic error for lookup_item."""
    with pytest.raises(ValidationError):
        test_client.lookup_item(1)

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests)
def test_client_get(mock_get):
    """Tests that Client get hits the correct endpoint."""
    get_test_value = 165475
    test_client.get(get_test_value)
    route = mock_get.call_args[0][0]
    assert route == f"{test_client.api_base_url}get/{get_test_value}"

@pytest.mark.unit
@mock.patch('requests.get', side_effect=mocked_requests_fail)
def test_client_get_fail(mock_get):
    """Tests that Client handles error if get has a bad response."""
    with pytest.raises(RequestException):
        test_client.get()

@pytest.mark.unit
def test_client_get_invalid_parameter():
    """Tests that Client throws pydantic error for get."""
    with pytest.raises(ValidationError):
        test_client.get(-1)

@pytest.mark.unit
@mock.patch('requests.post', side_effect=mocked_requests)
def test_client_put(mock_post):
    """Tests that Client put hits the correct endpoint."""
    test_client.put({})
    route = mock_post.call_args[0][0]
    assert route == f"{test_client.api_base_url}put"

@pytest.mark.unit
@mock.patch('requests.post', side_effect=mocked_requests_fail)
def test_client_put_fail(mock_post):
    """Tests that Client handles error if put has a bad response."""
    with pytest.raises(RequestException):
        test_client.put({})

@pytest.mark.unit
def test_client_put_invalid_parameter():
    """Tests that Client throws pydantic error for put."""
    with pytest.raises(ValidationError):
        test_client.put("{}")
