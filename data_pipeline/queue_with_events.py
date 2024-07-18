"""Contains functions and classes concerning Queue with Event.
"""
from typing import Dict
from functools import partial

import pydantic

from data_pipeline.queue_base import QueueBase, QueueItemStage
from data_pipeline.events.event import Event


QUEUE_EVENT_SCHEMA_VERSION="0.0.1"


class QueueAddEventData(pydantic.BaseModel):
    """Class with Queue information and Item data to be used when logging Event
    in Event Store.
    """
    queue_index_key : str
    queue_item_data : pydantic.JsonValue
    queue_info : Dict[str, str]


class QueueMoveEventData(pydantic.BaseModel):
    """Class concerning tracking movment of Items on Queue.
    """
    queue_index_key : str
    stage_from : QueueItemStage
    stage_to : QueueItemStage

    # will automatically convert the QueueItemStage fields to their integer
    # values when dumped with `model_dump`
    @pydantic.field_validator('stage_from', 'stage_to')
    def _internal_validator(cls, v):
        """Validates that the input is a QueueItemStage object.

        Parameters:
        -----------
        v: QueueItemStage object or int.

        Returns:
        -----------
        QueueItemStage object.
        """
        if isinstance(v, QueueItemStage):
            return v
        return QueueItemStage(v)

    @pydantic.field_serializer('stage_from', 'stage_to')
    def _internal_serializer(v):
        """Serializes QueueItemStage by returning the int representation.

        Parameters:
        -----------
        v: QueueItemStage object.

        Returns:
        -----------
        Integer representing stage.
        """
        return v.value


def add_to_queue_with_event(
    queue,
    event_store,
    queue_add_event_name,
    new_items
):
    """Adds Item to the Queue and logs the Event to an Event Store.

    Parameters:
    -----------
    queue: QueueBase
        Queue to add Items to.
    event_store: EventStoreInterface
        Event Store to add Events to.
    queue_add_event_name: str
        Name of Event.
    new_items: Dict
        New Items to add to queue.
    Returns:
    -----------
    Results of putting filtered_items in queue in the WAITING stage.
    """
    # The flow control of this function looks really weird to satisfy the
    # `test_put_with_exception` test.
    queue_event_data = []
    filtered_items = {}
    exc = None

    for k, v in new_items.items():
        try:
            queue_event_data.append(
                Event(
                    name=queue_add_event_name,
                    version=QUEUE_EVENT_SCHEMA_VERSION,
                    data=QueueAddEventData(
                        queue_index_key=k,
                        queue_item_data=v,
                        queue_info=queue.description
                    ).model_dump()
                )
            )

            filtered_items[k] = v
        except BaseException as e:
            exc = e

    event_store.add(queue_event_data)

    out = queue.put(filtered_items)

    if exc is not None:
        raise exc

    return out


def get_from_queue_with_event(
    queue,
    event_store,
    queue_move_event_name,
    n_items = 1
):
    """Retrieves Items in WAITING stage, logs Event, then adds to the Store.

    Parameters:
    -----------
    queue: QueueBase
        Queue to get Items from.
    event_store: EventStoreInterface
        Event Store to add Events to.
    queue_move_event_name: str
        Name of Event.
    n_items: int (default=1)
        Number of Items to get from Queue.
    Returns:
    -----------
    List of Items retrieved from the Queue and moved to PROCESSING stage.
    """
    n_items = max(n_items, 0)

    items = queue.get(n_items)

    queue_event_data = [
        Event(
            name=queue_move_event_name,
            version=QUEUE_EVENT_SCHEMA_VERSION,
            data=QueueMoveEventData(
                queue_index_key=k,
                stage_from=QueueItemStage.WAITING,
                stage_to=QueueItemStage.PROCESSING
            ).model_dump()
        )

        for k, _ in items
    ]

    event_store.add(queue_event_data)

    return items


def record_queue_move_event(
    event_store,
    queue_move_event_name,
    item_id,
    from_stage,
    to_stage
):
    """Tracks the movement of Items in Queue via Event Store.

    Parameters:
    -----------
    event_store: EventStoreInterface
        Event Store to track item movement.
    queue_move_event_name: str
        name of Event
    item_id: str
        ID of Queue Item
    from_stage: QueueItemStage
        Stage Item is being moved from.
    to_stage: QueueItemStage
        Stage Item is being moved to.
    """
    queue_event_data = Event(
        name = queue_move_event_name,
        version = QUEUE_EVENT_SCHEMA_VERSION,
        data = QueueMoveEventData(
            queue_index_key=item_id,
            stage_from=from_stage,
            stage_to=to_stage
        ).model_dump()
    )

    event_store.add(queue_event_data)


def queue_success_with_event(
    queue,
    event_store,
    queue_move_event_name,
    item_id
):
    """Moves Queue Item from PROCESSING to SUCCESS and logs the Event.

    Parameters:
    -----------
    queue: QueueBase
        Queue to add Event to SUCCESS stage.
    event_store: EventStoreInterface
        Event Store to track item movement.
    queue_move_event_name: str
        Name of Event.
    item_id: str
        ID of Queue Item.
    """
    record_queue_move_event(
        event_store,
        queue_move_event_name,
        item_id,
        QueueItemStage.PROCESSING,
        QueueItemStage.SUCCESS
    )

    queue.success(item_id)

def queue_fail_with_event(
    queue,
    event_store,
    queue_move_event_name,
    item_id
):
    """Moves Queue Item from PROCESSING to FAIL and logs the Event.

    Parameters:
    -----------
    queue: QueueBase
        Queue to add Event to Fail stage.
    event_store: EventStoreInterface
        Event Store to track item movement.
    queue_move_event_name: str
        Name of Event.
    item_id: str
        ID of Queue Item.
    """
    record_queue_move_event(
        event_store,
        queue_move_event_name,
        item_id,
        QueueItemStage.PROCESSING,
        QueueItemStage.FAIL
    )

    queue.fail(item_id)


def QueueWithEvents(
    queue,
    event_store,
    event_base_name = None,
    add_event_name = None,
    move_event_name = None
):
    """Returns a QueueBase object with added functionality to log events as
    queue items are retrieved and moved.

    Parameters:
    -----------
    queue: QueueBase
    event_store: EventStoreInterface
        Event Store to log item movement.
    event_base_name: str (default=None)
        Name of Base Event.
    add_event_name: str (default=None)
        Add Event name.
    move_event_name: str (default=None)
        Move Event name.

    Returns:
    -----------
    QueueBase object.
    """
    if event_base_name:
        add_event_name = f"{event_base_name}_ADD"
        move_event_name = f"{event_base_name}_MOVE"

    # validate arguments
    assert bool(add_event_name) and bool(move_event_name), \
        "No event name supplied for queue"

    return QueueBase(
        partial(add_to_queue_with_event, queue, event_store, add_event_name),
        partial(get_from_queue_with_event, queue,
                event_store, move_event_name),
        partial(queue_success_with_event, queue, event_store, move_event_name),
        partial(queue_fail_with_event, queue, event_store, move_event_name),
        queue.size,
        queue.lookup_status,

        {
            "implementation": "event",
            "event_base_name": event_base_name,
            "base_queue_description": queue.description
        }
    )
