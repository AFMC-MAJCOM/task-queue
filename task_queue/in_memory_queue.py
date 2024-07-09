from task_queue.queue_base import QueueBase, QueueItemStage
from queue import Queue

from dataclasses import dataclass, field
from typing import Dict, Any
import itertools
from functools import partial
import json

@dataclass
class InMemoryQueue_():
    waiting : Dict[str, Any] = field(default_factory=dict)
    processing : Dict[str, Any] = field(default_factory=dict)
    success : Dict[str, Any] = field(default_factory=dict)
    fail : Dict[str, Any] = field(default_factory=dict)
    index : set[str] = field(default_factory=set)

    def regenerate_index(self):
        ids_in_queue = (
            list(self.waiting.keys())
            + list(self.processing.keys())
            + list(self.success.keys())
            + list(self.fail.keys())
        )

        self.index = set(ids_in_queue)

        assert len(ids_in_queue) == len(self.index), \
            "There are duplicates IDs in the work queue"

    def get_for_stage(self, stage:QueueItemStage):
        match stage:
            case QueueItemStage.WAITING:
                return self.waiting
            case QueueItemStage.PROCESSING:
                return self.processing
            case QueueItemStage.SUCCESS:
                return self.success
            case QueueItemStage.FAIL:
                return self.fail


def is_json_serializable(o):
    try:
        json.dumps(o)
        return True
    except:
        pass
    return False


def add_to_memory_queue(
    in_memory_queue:InMemoryQueue_,
    new_items:Dict[str, Any]
):
    # filter out ids that already exist in the index
    filtered_items = {
        k:v
        for k,v in new_items.items()
        if k not in in_memory_queue.index
        if is_json_serializable(v)
    }

    # add to queue
    in_memory_queue.waiting.update(filtered_items)

    # fix index
    in_memory_queue.regenerate_index


def move_dict_item(
    dict_from,
    dict_to,
    key
):
    # pop removes the key from the dictionary and returns the value
    item = dict_from.pop(key)
    dict_to[key] = item

    return item


def get_from_memory_queue(
    in_memory_queue : InMemoryQueue_,
    n_items = 1
):
    # islice does not support negative values
    # `list` is necessary to freeze this iterator - now it won't break
    # when the dictionary changes size while iterating
    next_ids = list(itertools.islice(in_memory_queue.waiting, n_items))

    queue_items = []

    for id in next_ids:
        queue_item = move_dict_item(
            in_memory_queue.waiting,
            in_memory_queue.processing,
            id
        )

        queue_items.append((id, queue_item))

    return queue_items


def lookup_status(
    in_memory_queue : InMemoryQueue_,
    item_id : str
):
    for item_stage in QueueItemStage:
        dict_for_stage = in_memory_queue.get_for_stage(item_stage)
        if item_id in dict_for_stage:
            return item_stage

    raise KeyError(item_id)


def InMemoryQueue():
    in_memory_queue = InMemoryQueue_()
    print(in_memory_queue)

    return QueueBase(
        partial(add_to_memory_queue, in_memory_queue),
        partial(get_from_memory_queue, in_memory_queue),
        lambda item_id: move_dict_item(in_memory_queue.processing,
                                       in_memory_queue.success,
                                       item_id),
        lambda item_id: move_dict_item(in_memory_queue.processing,
                                       in_memory_queue.fail,
                                       item_id),
        lambda stage: len(in_memory_queue.get_for_stage(stage)),
        partial(lookup_status, in_memory_queue),
        {"implementation": "memory"}
    )
