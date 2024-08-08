"""This file contains pydantic models used for pydantic type checking of the
inputs and outputs of the API and Client, when applicable."""
from pydantic import BaseModel
from typing import Any, Dict

from task_queue.queue_base import QueueItemStage

class QueueGetSizesModel(BaseModel):
    """A Pydantic model representing the return dictionary for the /sizes
    endpoint and get_queue_sizes() in client."""
    WAITING : int
    PROCESSING : int
    SUCCESS : int
    FAIL : int


class LookupQueueItemModel(BaseModel):
    """A Pydantic model representing the return dictionary for the /lookup_item
    endpoint and lookup_item() in client."""

    item_id : str
    status : QueueItemStage
    item_body : Any


class QueueDescribeModel(BaseModel):
    """A Pydantic model representing the return dictionary for the /describe
    endpoint and description() in client."""

    implementation : str
    arguments : Dict[str, Any]
