"""Pytest for event store functionality.
"""
import random
import datetime
from typing import Iterator, List

import pytest
from pydantic import BaseModel

from data_pipeline.events.in_memory_event_store import InMemoryEventStore
from data_pipeline.events.sql_event_store import SqlEventStore
from data_pipeline.events.event_store_interface import EventStoreInterface
from data_pipeline.events.event import Event
from ..utils import test_sql_engine


random_number = random.randint(0, 999999999)

test_event_names = [
    f"test-event-store-{random_number}-1",
    f"test-event-store-{random_number}-2",
    f"test-event-store-{random_number}-3",
]
n_event_types = len(test_event_names)

start_time = datetime.datetime.now()

class TestEventData(BaseModel):
    """This class is used for the pytests to create test event data.
    """
    __test__ = False

    some_number : int
    some_string : str
    some_dict : dict

def random_event(event_name, time=True, time_offset_sec=0):
    """Creates a random event with the test event data.

    Parameters:
    -----------
    event_name: str
        Desired name of the event.
    time: boolean (default=True)
        ?????
    time_offset_sec: int (default=0)
        Amount of time to offset event time.

    Returns:
    -----------
    Event object with random data.
    """
    data = TestEventData(
        some_number=random.randint(0, 1000000),
        some_string=chr(random.randint(ord('a'), ord('z'))),
        some_dict = {
            "a_float": random.random(),
            "a_const": 6443
        }
    )

    return Event(
        name=event_name,
        version="0.0.1",
        data=data.model_dump(),
        time=start_time+datetime.timedelta(seconds=time_offset_sec)
    )

n_events_per_type = 20
n_events = n_events_per_type*n_event_types

@pytest.fixture
def random_events():
    """Fixture to create events for testing.

    `n_events_per_type` of each event type, with each event of that type
    occuring one second after the previous event of that type.

    Returns:
    -----------
    List of events.
    """ 
    return [
        random_event(test_event_names[i%n_event_types], (i // n_event_types))
        for i in range(0, n_events)
    ]

ALL_EVENT_STORE_TYPES = ["memory", "sql"]

@pytest.fixture
def new_empty_store(request):
    """Fixture to return type Iterator[EventStoreInterface]
    """
    if request.param == "memory":
        yield InMemoryEventStore()
    if request.param == "sql":
        yield SqlEventStore(test_sql_engine)


@pytest.mark.parametrize("new_empty_store",
                         ALL_EVENT_STORE_TYPES,
                         indirect=True)
def test_add_events(new_empty_store, random_events):
    """Tests that adding events to empty store does not throw error.
    """
    new_empty_store.add(random_events)


@pytest.mark.parametrize("new_empty_store",
                         ALL_EVENT_STORE_TYPES,
                         indirect=True)
def test_get_events(new_empty_store, random_events):
    """Tests get_events functions as expected.
    """
    event_name = test_event_names[0]

    # Compare before/after in case tests are run out of order
    events_before = new_empty_store.get(event_name)

    new_empty_store.add(random_events)

    events_after = new_empty_store.get(event_name)

    assert len(events_after) - len(events_before) == n_events_per_type
    assert all( e.name == event_name for e in events_after )


@pytest.mark.parametrize("new_empty_store",
                         ALL_EVENT_STORE_TYPES,
                         indirect=True)
def test_get_events_time_since(new_empty_store, random_events):
    """Tests get_events with time_since param functions as expected.
    """
    new_empty_store.add(random_events)

    event_name = test_event_names[0]

    seconds_added = 5
    time_since = start_time + datetime.timedelta(seconds=seconds_added)

    events = new_empty_store.get(event_name, time_since=time_since)

    assert all( e.time > time_since for e in events )
    assert all( e.name == event_name for e in events )


@pytest.mark.parametrize("new_empty_store",
                         ALL_EVENT_STORE_TYPES,
                         indirect=True)
def test_event_default_time_now(new_empty_store):
    """Tests that the time assigned is accurate and does not throw errors when
    added to empty store.
    """
    evt = Event(
        name="doesnt_matter",
        version="0.0.1",
        data={}
    )

    now = datetime.datetime.now()

    # Should be less than a millisecond
    assert (now - evt.time).microseconds < 1000

    # Make sure this event is added to the store without errors
    new_empty_store.add(evt)


@pytest.mark.parametrize("new_empty_store",
                         ALL_EVENT_STORE_TYPES,
                         indirect=True)
def test_add_empty(new_empty_store):
    """Tests adding empty list to store does not throw errors.
    """
    new_empty_store.add([])
