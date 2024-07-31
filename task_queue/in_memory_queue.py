"""Wherein is contained the class and functions for the In Memory Queue.
"""
from dataclasses import dataclass, field
from typing import Dict, Any
import itertools
import json
import warnings

from task_queue.queue_base import QueueBase, QueueItemStage


@dataclass
class MemoryQueue():
    """Queue items are objects in a python dictionary.

    Primarily used for prototyping and testing.
    """
    waiting : Dict[str, Any] = field(default_factory=dict)
    processing : Dict[str, Any] = field(default_factory=dict)
    success : Dict[str, Any] = field(default_factory=dict)
    fail : Dict[str, Any] = field(default_factory=dict)
    index : set[str] = field(default_factory=set)


    def regenerate_index(self):
        """Regenerates the Index of the InMemoryQueue.
        """
        ids_in_queue = (
            list(self.waiting.keys())
            + list(self.processing.keys())
            + list(self.success.keys())
            + list(self.fail.keys())
        )

        self.index = set(ids_in_queue)

        assert len(ids_in_queue) == len(self.index), \
            "There are duplicates IDs in the work queue"

    def get_for_stage(self, stage):
        """Get the InMemoryQueue stage given a QueueItemStage.

        Parameters:
        -----------
        stage: QueueItemStage
            Stage of the Item in Queue.

        Returns:
        -----------
        Returns InMemoryQueue field associated with current stage.
        """
        match stage:
            case QueueItemStage.WAITING:
                return self.waiting
            case QueueItemStage.PROCESSING:
                return self.processing
            case QueueItemStage.SUCCESS:
                return self.success
            case QueueItemStage.FAIL:
                return self.fail


class InMemoryQueue(QueueBase):
    """Creates the In Memory Queue.
    """
    def __init__(self):
        """Initializes the QueueBase class.
        """
        self.memory_queue = MemoryQueue()

    def put(self, items):
        """Adds a new Item to the Queue in the WAITING stage.

        Parameters:
        -----------
        items: dict
            Dictionary of Queue Items to add Queue, where Item is a key:value
            pair, where key is the item ID and value is the queue item body.
        """
        # Filter out IDs that already exist in the index
        filtered_items = {
            k:v
            for k,v in items.items()
            if k not in self.memory_queue.index
            if is_json_serializable(v)
        }

        # Add to queue
        self.memory_queue.waiting.update(filtered_items)

    def get(self, n_items=1):
        """Gets the next n items from the queue, moving them to PROCESSING.

        Parameters:
        -----------
        n_items: int (default=1)
            Number of items to retrieve from queue.

        Returns:
        ------------
        Returns a list of n_items from the queue, as
        List[(queue_item_id, queue_item_body)]
        """
        # islice does not support negative values
        # `list` is necessary to freeze this iterator - now it won't break
        # When the dictionary changes size while iterating.
        next_ids = list(itertools.islice(self.memory_queue.waiting, n_items))

        queue_items = []

        for i in next_ids:
            queue_item = move_dict_item(
                self.memory_queue.waiting,
                self.memory_queue.processing,
                i
            )

            queue_items.append((i, queue_item))

        return queue_items

    def success(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to SUCCESS.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        move_dict_item(
            self.memory_queue.processing,
            self.memory_queue.success,
            queue_item_id
        )

    def fail(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to FAIL.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        move_dict_item(
            self.memory_queue.processing,
            self.memory_queue.fail,
            queue_item_id)

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
        return len(self.memory_queue.get_for_stage(queue_item_stage))

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
        for item_stage in QueueItemStage:
            dict_for_stage = self.memory_queue.get_for_stage(item_stage)
            if queue_item_id in dict_for_stage:
                return item_stage

        raise KeyError(queue_item_id)

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
        if queue_item_stage in QueueItemStage:
            dict_for_stage = self.memory_queue.get_for_stage(queue_item_stage)
            return list(dict_for_stage.keys())

        raise AttributeError(queue_item_stage)

    def lookup_item(self, queue_item_id):
        """Lookup an Item currently in the Queue.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item

        Returns:
        ------------
        Returns the Queue Item ID, the status of that Item, and the body, or it
        will raise an error if Item is not in Queue.
        """
        # Get item stage
        item_stage = self.lookup_status(queue_item_id)
        # Get item body
        dict_for_stage = self.memory_queue.get_for_stage(item_stage)
        item_body = dict_for_stage[queue_item_id]

        return (queue_item_id, item_stage, item_body)

    def description(self):
        """A brief description of the Queue.

        Returns:
        ------------
        Returns a dictionary with relevant information about the Queue.
        """
        desc = {"implementation": "memory"}
        return desc

    def requeue(self, item_ids):
        """Move input queue items from FAILED to WAITING.

        Parameters:
        -----------
        item_ids: [str]
            ID of Queue Item
        """
        item_ids = self._requeue(item_ids)
        for item in item_ids:
            move_dict_item(
                self.memory_queue.fail,
                self.memory_queue.waiting,
                item
            )


# Pylint is disabled because the goal is to just have
# the function return False and not fail
# pylint: disable=broad-exception-caught
def is_json_serializable(o):
    """Determines if the object passed in is JSON serializable.

    Parameters:
    -----------
    o: JSON value
        Queue Item data.

    Returns:
    -----------
    Returns True or False if the object is JSON serializable.
    """
    try:
        json.dumps(o)
    except Exception as e:
        print(e)
        return False
    return True

def move_dict_item(dict_from, dict_to, key):
    """Moves Item from one dictionary to another.

    Parameters:
    -----------
    dict_from: Dict
        Dictionary that Item is being moved from.
    dict_to: Dict
        Dictionary that Item is being moved to.
    key: str
        Key for the Item moving from dictionary.

    Returns:
    -----------
    Returns Item moved.
    """
    # Pop removes the key from the dictionary and returns the value
    item = dict_from.pop(key)
    dict_to[key] = item

    return item

def in_memory_queue():
    """Creates and returns an InMemoryQueue object.
    """
    return InMemoryQueue()
