"""This file contains pydantic models used for pydantic type checking of the
inputs and outputs of the API and Client, when applicable."""
import json
from typing import Any, Annotated

from pydantic import BaseModel
from pydantic.functional_validators import AfterValidator

from task_queue.queues.queue_base import QueueItemStage

def json_serializable_validator(o):
    """This function is used to determine if an object is JSON serializable for
    the purposes of pydantic type checking.

    The original object is returned unaltered, this function is just being used
    as a validator for certain pydantic models.
    """
    json.dumps(o)
    return o

QueueItemBodyType = Annotated[Any, \
                    AfterValidator(json_serializable_validator)]


class QueueGetSizesModel(BaseModel):
    """A Pydantic model representing the return dictionary for the /sizes
    endpoint and get_queue_sizes() in client."""
    WAITING : int
    PROCESSING : int
    SUCCESS : int
    FAIL : int
    TOTAL: int


class LookupQueueItemModel(BaseModel):
    """A Pydantic model representing the return dictionary for the /lookup_item
    endpoint and lookup_item() in client."""
    item_id : str
    status : QueueItemStage
    item_body : QueueItemBodyType
