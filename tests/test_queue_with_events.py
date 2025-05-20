"""Pytests for the queue_with_events.py functionality.
"""
import random

import pytest

from task_queue.events import InMemoryEventStore
from task_queue.queues import memory_queue
from task_queue.queues import queue_with_events
from task_queue.queues.queue_with_events import QueueAddEventData
from task_queue.queues.queue_with_events import QueueMoveEventData
from task_queue.queues import QueueItemStage
from .common_queue import default_items


random_number = random.randint(0, 999999999)

ADD_EVENT_NAME = f"TEST_QUEUE_ADD_EVENT_{random_number}"
MOVE_EVENT_NAME = f"TEST_QUEUE_MOVE_EVENT_{random_number}"

@pytest.fixture
def queue_with_events_fixture():
    """Fixture to create queue with events for testing.
    """
    q = memory_queue()
    s = InMemoryEventStore()

    return q, s, queue_with_events(
        q,
        s,
        add_event_name=ADD_EVENT_NAME,
        move_event_name=MOVE_EVENT_NAME
    )

@pytest.mark.unit
def test_event_queue_add(queue_with_events_fixture):
    """Tests that every event was added to the queue and every item has an
    event.
    """
    _, s, eq = queue_with_events_fixture

    eq.put(default_items)

    add_events = s.get(ADD_EVENT_NAME)

    for e in add_events:
        queue_add_event = QueueAddEventData(**e.data)

        # Make sure every item has an event
        assert queue_add_event.queue_index_key in default_items

    # Make sure every item has an event
    assert len(add_events) == len(default_items)

@pytest.mark.unit
def test_event_queue_lifecycle(queue_with_events_fixture):
    """Test event queue lifestyle works as expected.
    """
    _, s, eq = queue_with_events_fixture

    eq.put(default_items)

    # Move half of the items from `waiting` to `processing`.
    get_amount = len(default_items) // 2

    items = eq.get(get_amount)

    # Move half of the items in `processing` to `success` and half to `fail`.
    success_fail_amount = get_amount // 2
    success_items = items[:success_fail_amount]
    fail_items = items[success_fail_amount:]


    for k, _ in success_items:
        eq.success(k)

    for k, _ in fail_items:
        eq.fail(k)

    move_events = [
        QueueMoveEventData(**e.data)
        for e in s.get(MOVE_EVENT_NAME)
    ]

    process_events = [
        e for e in move_events if e.stage_to == QueueItemStage.PROCESSING
    ]

    success_events = [
        e for e in move_events if e.stage_to == QueueItemStage.SUCCESS
    ]

    fail_events = [
        e for e in move_events if e.stage_to == QueueItemStage.FAIL
    ]

    # Make sure the right number of events happened for each stage
    assert len(process_events) == len(success_items) + len(fail_items)
    assert len(success_events) == eq.size(QueueItemStage.SUCCESS)
    assert len(fail_events) == eq.size(QueueItemStage.FAIL)

    for e in move_events:
        if e.stage_to != QueueItemStage.PROCESSING:
            # If this item was moved, make sure it is in the proper spot now
            assert e.stage_to == eq.lookup_status(e.queue_index_key)
