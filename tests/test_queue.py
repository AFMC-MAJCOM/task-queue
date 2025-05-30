"""Pytests for queue functionality.
"""
import random
import os

import pytest

from task_queue.queues import memory_queue
from task_queue.queues import event_queue
from task_queue.queues import json_sql_queue
from task_queue.queues import json_s3_queue
from task_queue.events import InMemoryEventStore
import tests.common_queue as qtest
from .test_config import TaskQueueTestSettings
from task_queue.queues.queue_base import QueueItemStage


UNIT_TEST_QUEUE_BASE = TaskQueueTestSettings().UNIT_TEST_QUEUE_BASE

ALL_QUEUE_TYPES = [
    pytest.param("memory", marks=pytest.mark.unit),
    pytest.param("with_events", marks=pytest.mark.unit)
]
try:
    import sqlalchemy as sqla
    from .utils import PytestSqlEngine
    param = pytest.param(
        "sql",
        marks=[pytest.mark.integration, pytest.mark.uses_sql]
    )
    ALL_QUEUE_TYPES.append(param)
except ModuleNotFoundError:
    pass

try:
    import s3fs
    param = pytest.param(
        "s3",
        marks=[pytest.mark.integration, pytest.mark.uses_s3]
    )
    ALL_QUEUE_TYPES.append(param)
except ModuleNotFoundError:
    pass


@pytest.fixture(scope="session")
def setup_s3_bucket():
    """Create a 'integration-tests' S3 bucket for testing purposes.
    """
    fs = s3fs.S3FileSystem()

    test_bucket_name = 'integration-tests'
    if fs.exists(test_bucket_name):
        fs.rm(test_bucket_name, recursive=True)
    fs.mkdir(test_bucket_name)

    yield
    cleanup_bucket(test_bucket_name, fs)

def cleanup_bucket(test_bucket_name, fs):
    """Delete the created bucket that was used for testing.
    """
    if fs.exists(test_bucket_name):
        fs.rm(test_bucket_name)

def new_s3_queue(request):
    """Creates a new s3 queue for tests and prints results.
    """
    queue_base = os.path.join(UNIT_TEST_QUEUE_BASE,
                              str(random.randint(0, 9999999)))
    yield json_s3_queue(queue_base)
    fs = s3fs.S3FileSystem()

    # If the test passes
    if request.node.rep_call.passed:
        print("Cleaning up results")
        # Clean up the queue to reduce clutter
        if fs.exists(queue_base):
            fs.rm(queue_base, recursive=True)
    elif request.node.rep_call.failed:
        print(f"Failed results at {queue_base}")

def new_in_memory_queue():
    """Returns an in-memory queue.
    """
    return memory_queue()

@pytest.fixture(scope="session")
def cleanup_sql_queue():
    """"""

def new_sql_queue():
    """Returns a SQL queue.
    """
    queue_name = "TEST_QUEUE_" + str(random.randint(0, 9999999999))
    test_sql_engine = PytestSqlEngine()
    return json_sql_queue(
        test_sql_engine.test_sql_engine,
        queue_name,
        table_name="test_sql_queue",
        constraint_name="_test_queue_name_index_key_uc"
    )

@pytest.fixture(scope="session")
def setup_fixture(request):
    uses_s3_marker = request.node.get_closest_marker("uses_s3")
    uses_sql_marker = request.node.get_closest_marker("uses_sql")

    if uses_sql_marker is not None:
        # Deletes the SQL table made during the pytests.
        yield

        tablename = 'test_sql_queue'
        test_sql_engine = PytestSqlEngine()
        with test_sql_engine.test_sql_engine.connect() as connection:
            connection.execute(sqla.text(f"DROP TABLE IF EXISTS {tablename};"))
            connection.commit()
    elif uses_s3_marker is not None:
        fs = s3fs.S3FileSystem()

        test_bucket_name = 'integration-tests'
        if fs.exists(test_bucket_name):
            fs.rm(test_bucket_name, recursive=True)
        fs.mkdir(test_bucket_name)

        yield
        # cleanup_bucket(test_bucket_name, fs)
    else:
        yield

@pytest.fixture
def new_empty_queue(request, setup_fixture):
    """Fixture to create an empty queue of one given type.

    This is then broadcasted across ALL_QUEUE_TYPES for each test by way of the
    pytest.mark.parametrize decorators, so each test is run four times, one for
    each queue type in the list.
    """
    if request.param == "sql":
        yield new_sql_queue()
    elif request.param == "s3":
        yield from new_s3_queue(request)
    elif request.param == "memory":
        yield new_in_memory_queue()
    elif request.param == "with_events":
        store = InMemoryEventStore()
        queue = new_in_memory_queue()
        yield event_queue(queue, store, "TEST_EVENT_QUEUE")

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_put_get(new_empty_queue):
    """Tests put and get function as expected.
    """
    qtest.test_put_get(new_empty_queue)

@pytest.mark.filterwarnings("ignore:Item .* already in queue. Skipping.")
@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_mixed_duplicates(new_empty_queue):
    """Tests that duplicates are not added to queue but new items are.
    """
    qtest.test_mixed_duplicates(new_empty_queue)

@pytest.mark.filterwarnings("ignore:Item .* already in queue. Skipping.")
@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_add_to_queue_no_duplicates(new_empty_queue):
    """Tests that no duplicates are added to the queue.
    """
    qtest.test_add_to_queue_no_duplicates(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_get_empty_queue(new_empty_queue):
    """Tests the results of calling get on an empty queue.
    """
    qtest.test_get_empty_queue(new_empty_queue)

@pytest.mark.filterwarnings("ignore:Item .* already in queue. Skipping.")
@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_put_exception(new_empty_queue):
    """Tests that put will not add non-JSON serializable items.
    """
    qtest.test_put_exception(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_queue_size(new_empty_queue):
    """Test that sizes are properly tracked.
    """
    qtest.test_queue_size(new_empty_queue)

@pytest.mark.filterwarnings("ignore:Item .* already in queue. Skipping.")
@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_out_of_order(new_empty_queue):
    """Tests out of order.
    """
    qtest.test_out_of_order(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_get_zero_items(new_empty_queue):
    """Tests get 0 items works as expected.
    """
    qtest.test_get_zero_items(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_lookup(new_empty_queue):
    """Tests that lookup_status works as expected.
    """
    qtest.test_lookup(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_multiple_get(new_empty_queue):
    """Test multiple get calls.
    """
    qtest.test_multiple_get(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_multiple_get_empty(new_empty_queue):
    """Test multiple get calls.
    """
    qtest.test_multiple_get_empty(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_put_empty(new_empty_queue):
    """Tests put with an empty dict does not throw an error.
    """
    new_empty_queue.put({})

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_requeue_empty(new_empty_queue):
    """Tests requeue with no FAILED items does not raise error.
    """
    qtest.test_requeue_empty(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_requeue_list_input(new_empty_queue):
    """Tests requeue with an input list of strings.
    """
    qtest.test_requeue_list_input(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_requeue_string_input(new_empty_queue):
    """Tests requeue with an input string.
    """
    qtest.test_requeue_string_input(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_requeue_invalid_ids(new_empty_queue):
    """Tests requeue with an an invalid id.
    """
    qtest.test_requeue_invalid_ids(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_lookup_state(new_empty_queue):
    """Tests that lookup_state works as expected.
    """
    qtest.test_lookup_state(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_lookup_state_fail(new_empty_queue):
    """Tests that lookup_state works as expected.
    """
    qtest.test_lookup_state_fail(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_lookup_item(new_empty_queue):
    """Test that lookup_item works as expected.
    """
    qtest.test_lookup_item(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_lookup_item_fail(new_empty_queue):
    """Tests that the proper error is thrown when lookup_item fails.
    """
    qtest.test_lookup_item_fail(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_peek_items_not_moved(new_empty_queue):
    """Tests that peeked items are not moved to PROCESSING
    """
    new_empty_queue.put(qtest.default_items)

    NUM_PEEK = 5
    _ = new_empty_queue.peek(NUM_PEEK)
    waiting = new_empty_queue.size(QueueItemStage.WAITING)
    assert waiting == len(qtest.default_items)
    assert new_empty_queue.size(QueueItemStage.PROCESSING) == 0

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_peek_items_same_as_get(new_empty_queue):
    """Tests that the items returned by `peek` are the same as returned by a
    subsequent `get`
    """
    new_empty_queue.put(qtest.default_items)

    NUM_PEEK = 5
    items_peek = new_empty_queue.peek(NUM_PEEK)
    items_get = new_empty_queue.get(NUM_PEEK)

    assert items_peek == items_get
