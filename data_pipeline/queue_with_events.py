from data_pipeline.queue_base import QueueBase, QueueItemStage
from data_pipeline.events.event_store_interface import EventStoreInterface
from data_pipeline.events.event import Event

from typing import Dict
from typing_extensions import Annotated
import pydantic
from pydantic import BeforeValidator
from functools import partial

QUEUE_EVENT_SCHEMA_VERSION="0.0.1"

class QueueAddEventData(pydantic.BaseModel):
    queue_index_key : str
    queue_item_data : pydantic.JsonValue
    queue_info : Dict[str, str]


class QueueMoveEventData(pydantic.BaseModel):
    queue_index_key : str
    stage_from : QueueItemStage
    stage_to : QueueItemStage

    # will automatically convert the QueueItemStage fields to their integer
    # values when dumped with `model_dump`
    @pydantic.field_validator('stage_from', 'stage_to')
    def _internal_validator(cls, v):
        if isinstance(v, QueueItemStage):
            return v
        else:
            return QueueItemStage(v)

    @pydantic.field_serializer('stage_from', 'stage_to')
    def _internal_serializer(v):
        return v.value


def add_to_queue_with_event(
    queue : QueueBase,
    event_store : EventStoreInterface,
    queue_add_event_name : str,
    new_items
):
    # the flow control of this function looks really weird to satisfy the
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
                        queue_info=queue._description
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
    queue : QueueBase,
    event_store : EventStoreInterface,
    queue_move_event_name : str,
    n_items = 1
):
    # you can never be too careful
    if n_items < 0:
        n_items = 0

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
    event_store : EventStoreInterface,
    queue_move_event_name : str,
    item_id : str,
    from_stage : QueueItemStage,
    to_stage : QueueItemStage
):
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
    queue : QueueBase,
    event_store : EventStoreInterface,
    queue_move_event_name : str,
    item_id : str
):
    record_queue_move_event(
        event_store,
        queue_move_event_name,
        item_id,
        QueueItemStage.PROCESSING,
        QueueItemStage.SUCCESS
    )

    queue.success(item_id)

def queue_fail_with_event(
    queue : QueueBase,
    event_store : EventStoreInterface,
    queue_move_event_name : str,
    item_id : str
):
    record_queue_move_event(
        event_store,
        queue_move_event_name,
        item_id,
        QueueItemStage.PROCESSING,
        QueueItemStage.FAIL
    )

    queue.fail(item_id)


def QueueWithEvents(
    queue : QueueBase,
    event_store : EventStoreInterface,
    event_base_name : str = None,
    add_event_name : str = None,
    move_event_name : str = None
):
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
            "base_queue_description": queue._description
        }
    )
