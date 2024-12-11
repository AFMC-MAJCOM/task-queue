"""Contains functions and classes concerning Queue with Event.
"""
from typing import Dict

import pydantic

from task_queue import logger
from task_queue.events.event import Event
from .queue_base import QueueBase, QueueItemStage


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
    # Pylint no-self-argument diabled because @field_validator
    # cannot be applied to instance methods
    # pylint: disable=no-self-argument
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

class QueueWithEvents(QueueBase):
    """Class for QueueWithEvents.
    """
    # Pylint does not like more than 5 parameters
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        queue,
        event_store,
        event_base_name,
        add_event_name,
        move_event_name
    ):
        """Initializes the QueueWithEvents class.
        """
        if event_base_name:
            add_event_name = f"{event_base_name}_ADD"
            move_event_name = f"{event_base_name}_MOVE"

        # validate arguments
        assert bool(add_event_name) and bool(move_event_name), \
            "No event name supplied for queue"

        self.queue = queue
        self.event_store = event_store
        self.event_base_name = event_base_name
        self.add_event_name = add_event_name
        self.move_event_name = move_event_name

        self.event_schema_version = "0.0.1"

    # Pylint disabled because BaseException is used to set exc
    # pylint: disable=broad-exception-caught
    def put(self, items):
        """Adds a new Item to the Queue in the WAITING stage and logs the
        Event to an Event Store.

        Parameters:
        -----------
        items: dict
            Dictionary of Queue Items to add Queue, where Item is a key:value
            pair, where key is the item ID and value is the queue item body.

        Returns:
        -----------
        Results of putting filtered_items in queue in the WAITING stage.
        """
        # The flow control of this function looks really weird to satisfy the
        # `test_put_with_exception` test.
        queue_event_data = []
        filtered_items = {}
        exc = None

        for k, v in items.items():
            try:
                queue_event_data.append(
                    Event(
                        name=self.add_event_name,
                        version=self.event_schema_version,
                        data=QueueAddEventData(
                            queue_index_key=k,
                            queue_item_data=v,
                            queue_info=self.queue.description()
                        ).model_dump()
                    )
                )

                filtered_items[k] = v
            except BaseException as e:
                exc = e

        self.event_store.add(queue_event_data)

        out = self.queue.put(filtered_items)

        if exc is not None:
            logger.error(exc)
            raise exc

        return out

    def get(self, n_items=1):
        """Gets the next n items from the queue, moving them to PROCESSING and
        logs the Event.

        Parameters:
        -----------
        n_items: int (default=1)
            Number of items to retrieve from queue.

        Returns:
        ------------
        Returns a list of n_items from the queue, as
        List[(queue_item_id, queue_item_body)]
        """
        n_items = max(n_items, 0)

        items = self.queue.get(n_items)

        queue_event_data = [
            Event(
                name=self.move_event_name,
                version=self.event_schema_version,
                data=QueueMoveEventData(
                    queue_index_key=k,
                    stage_from=QueueItemStage.WAITING,
                    stage_to=QueueItemStage.PROCESSING
                ).model_dump()
            )

            for k, _ in items
        ]

        self.event_store.add(queue_event_data)

        return items

    def peek(self, n_items=1):
        return self.queue.peek(n_items)

    def success(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to SUCCESS and logs the Event.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        self.record_queue_move_event(
            queue_item_id,
            QueueItemStage.PROCESSING,
            QueueItemStage.SUCCESS
        )

        self.queue.success(queue_item_id)
        logger.info("Job %s successfully completed", queue_item_id)

    def fail(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to FAIL and logs the Event.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        self.record_queue_move_event(
            queue_item_id,
            QueueItemStage.PROCESSING,
            QueueItemStage.FAIL
        )

        self.queue.fail(queue_item_id)
        logger.info("Job %s failed", queue_item_id)

    def size(self, queue_item_stage):
        """Determines how many items are in some stage of the queue.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage object
            The specific stage of the queue (PROCESSING, FAIL, etc.).

        Returns:
        ------------
        Returns the number of items in that stage of the queue as an integer.
        """
        return self.queue.size(queue_item_stage)

    def lookup_status(self, queue_item_id):
        """Lookup which stage in the Queue Item is currently in.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item

        Returns:
        ------------
        Returns the current stage of the Item as a QueueItemStage object, will
        raise an error if Item is not in Queue.
        """
        return self.queue.lookup_status(queue_item_id)

    def lookup_state(self, queue_item_stage):
        """Lookup which item ids are in the current Queue stage.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage
            stage of Queue Item

        Returns:
        ------------
        Returns a list of all item ids in the current queue stage.
        """
        return self.queue.lookup_state(queue_item_stage)

    def lookup_item(self, queue_item_id):
        """Lookup an Item currently in the Queue.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item

        Returns:
        ------------
        Returns a dictionary with the Queue Item ID, the status of that Item,
        and the body, or it will raise an error if Item is not in Queue.
        """
        return self.queue.lookup_item(queue_item_id)

    def description(self):
        """A brief description of the Queue.

        Returns:
        ------------
        Returns a dictionary with relevant information about the Queue.
        """
        desc = {
            "implementation": "event",
            "event_base_name": self.event_base_name,
            "base_queue_description": self.queue.description()
        }
        return desc

    def requeue(self, item_ids):
        """Move input queue items from FAILED to WAITING.

        Parameters:
        -----------
        item_ids: [str]
            ID of Queue Item
        """
        item_ids = self._requeue(item_ids)
        self.queue.requeue(item_ids)
        for item in item_ids:
            self.record_queue_move_event(
                item,
                QueueItemStage.FAIL,
                QueueItemStage.WAITING
            )


    def record_queue_move_event(
        self,
        item_id,
        from_stage,
        to_stage
    ):
        """Tracks the movement of Items in Queue via Event Store

        Parameters:
        -----------
        item_id: str
            ID of Queue Item
        from_stage: QueueItemStage
            Stage Item is being moved from.
        to_stage: QueueItemStage
            Stage Item is being moved to.
        """
        queue_event_data = Event(
            name = self.move_event_name,
            version = self.event_schema_version,
            data = QueueMoveEventData(
                queue_index_key=item_id,
                stage_from=from_stage,
                stage_to=to_stage
            ).model_dump()
        )

        self.event_store.add(queue_event_data)

def queue_with_events(
    queue,
    event_store,
    event_base_name = None,
    add_event_name = None,
    move_event_name = None
):
    """Creates a QueueWithEvents object.
    """
    return QueueWithEvents(
        queue,
        event_store,
        event_base_name,
        add_event_name,
        move_event_name
    )
