"""Top file docstring
"""
import pytest

import data_pipeline.s3_queue as s3q
from data_pipeline.in_memory_queue import InMemoryQueue
import data_pipeline.sql_queue as sqlq
import data_pipeline.queue_with_events as eq
from data_pipeline.events.in_memory_event_store import InMemoryEventStore


import data_pipeline.queue_base as qb
import random
import os
import pytest
import tests.common_queue as qtest
from sqlalchemy import create_engine
from .utils import test_sql_engine


import os
AWS_UNIT_TEST_QUEUE_BASE = "s3://data-dev-998806663306/unit-test-space/s3_queue_"
LOCAL_UNIT_TEST_QUEUE_BASE = "s3://unit-tests/queue/queue_"
UNIT_TEST_QUEUE_BASE = LOCAL_UNIT_TEST_QUEUE_BASE

def new_s3_queue(request):
    """Docstring
    """
    queue_base = os.path.join(UNIT_TEST_QUEUE_BASE, str(random.randint(0, 9999999)))
    yield s3q.JsonS3Queue(queue_base)

    #If the test passes
    if request.node.rep_call.passed:
        print("Cleaning up results")
        # clean up the queue to reduce clutter
        if s3q.fs.exists(queue_base):
            s3q.fs.rm(queue_base, recursive=True)
    elif request.node.rep_call.failed:
        print(f"Failed results at {queue_base}")

def new_in_memory_queue(request):
    """Docstring
    """
    return InMemoryQueue()

def new_sql_queue(request):
    """Docstring
    """
    queue_name = "TEST_QUEUE_" + str(random.randint(0, 9999999999))
    return sqlq.JsonSQLQueue(test_sql_engine, queue_name)


ALL_QUEUE_TYPES = ["memory", "sql", "s3", "with_events"]
@pytest.fixture
def new_empty_queue(request):
    """Docstring
    """
    if request.param == "sql":
        yield new_sql_queue(request)
    elif request.param == "s3":
        yield from new_s3_queue(request)
    elif request.param == "memory": 
        yield new_in_memory_queue(request)
    elif request.param == "with_events":
        store = InMemoryEventStore()
        queue = InMemoryQueue()
        yield eq.QueueWithEvents(queue, store, "TEST_EVENT_QUEUE")


@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_put_get(new_empty_queue):
    """Docstring
    """
    qtest.test_put_get(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_mixed_duplicates(new_empty_queue):
    """Docstring
    """
    qtest.test_mixed_duplicates(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_add_to_queue_no_duplicates(new_empty_queue):
    """Docstring
    """
    qtest.test_add_to_queue_no_duplicates(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_get_empty_queue(new_empty_queue):
    """Docstring
    """
    qtest.test_get_empty_queue(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_put_exception(new_empty_queue):
    """Docstring
    """
    qtest.test_put_exception(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_queue_size(new_empty_queue):
    """Docstring
    """
    qtest.test_queue_size(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_out_of_order(new_empty_queue):
    """Docstring
    """
    qtest.test_out_of_order(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_get_zero_items(new_empty_queue):
    """Docstring
    """
    qtest.test_get_zero_items(new_empty_queue)
    
@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_lookup(new_empty_queue):
    """Docstring
    """
    qtest.test_lookup(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_multiple_get(new_empty_queue):
    """Docstring
    """
    qtest.test_multiple_get(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_multiple_get_empty(new_empty_queue):
    """Docstring
    """
    qtest.test_multiple_get_empty(new_empty_queue)

@pytest.mark.parametrize("new_empty_queue", ALL_QUEUE_TYPES, indirect=True)
def test_put_empty(new_empty_queue):
    """Docstring
    """
    new_empty_queue.put({})
