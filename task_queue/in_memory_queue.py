"""Wherein is contained the class and functions for the In Memory Queue.
"""
from dataclasses import dataclass, field
from typing import Dict, Any
from functools import partial
import itertools
import json

from task_queue.queue_base import QueueBase, QueueItemStage


@dataclass
class InMemoryQueue():
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


def add_to_memory_queue(memory_queue,new_items):
    """Adds Items to In Memory Queue.

    Parameters:
    -----------
    memory_queue: InMemoryQueue
        InMemoryQueue object
    new_items: Dict[str, Any]
        Dictionary of new Items to add to Queue, where the key is the item_id
        and the value is the Queue Item Body.
    """
    # Filter out IDs that already exist in the index
    filtered_items = {
        k:v
        for k,v in new_items.items()
        if k not in memory_queue.index
        if is_json_serializable(v)
    }

    # Add to queue
    memory_queue.waiting.update(filtered_items)


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


def get_from_memory_queue(memory_queue, n_items = 1):
    """Gets n number of Items from In Memory Queue.

    Parameters:
    -----------
    memory_queue: InMemoryQueue
    n_items: int (default=1)
        Number of items to get.

    Returns:
    -----------
    List of Queue Items retrieved.
    """
    # islice does not support negative values
    # `list` is necessary to freeze this iterator - now it won't break
    # When the dictionary changes size while iterating.
    next_ids = list(itertools.islice(memory_queue.waiting, n_items))

    queue_items = []

    for i in next_ids:
        queue_item = move_dict_item(
            memory_queue.waiting,
            memory_queue.processing,
            i
        )

        queue_items.append((i, queue_item))

    return queue_items


def lookup_status(memory_queue, item_id):
    """Looks up the status of an Item in InMemoryQueue.

    Parameters:
    -----------
    memory_queue: InMemoryQueue
    item_id: str
        ID of Queue Item.

    Returns:
    -----------
    Returns Item stage or will raise an error.
    """
    for item_stage in QueueItemStage:
        dict_for_stage = memory_queue.get_for_stage(item_stage)
        if item_id in dict_for_stage:
            return item_stage

    raise KeyError(item_id)


def in_memory_queue():
    """Creates and returns InMemoryQueue using Base Queue Class.
    """
    memory_queue = InMemoryQueue()
    print(memory_queue)

    return QueueBase(
        partial(add_to_memory_queue, memory_queue),
        partial(get_from_memory_queue, memory_queue),
        lambda item_id: move_dict_item(memory_queue.processing,
                                       memory_queue.success,
                                       item_id),
        lambda item_id: move_dict_item(memory_queue.processing,
                                       memory_queue.fail,
                                       item_id),
        lambda stage: len(memory_queue.get_for_stage(stage)),
        partial(lookup_status, memory_queue),
        {"implementation": "memory"}
    )
