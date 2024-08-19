"""This file contains pydantic models used for pydantic type checking of the
inputs and outputs of the API and Client, when applicable."""
import json
from typing import Any, Annotated, Optional, List

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


class LookupQueueItemModel(BaseModel):
    """A Pydantic model representing the return dictionary for the /lookup_item
    endpoint and lookup_item() in client."""
    item_id : str
    status : QueueItemStage
    item_body : QueueItemBodyType


# class ArgoOwnerRefernceModel(BaseModel):
#     """A Pydantic model representing the owner refernce options for submitting
#     jobs to the argo workflows."""
#     apiVersion: Optional[str] = None
#     blockOwnerDeletion: Optional[bool] = True
#     controller: Optional[bool] = True
#     kind: Optional[str] = None
#     name: Optional[str] = None
#     uid: Optional[str] = None


# class ArgoSubmitOptionsModel(BaseModel):
#     """A Pydantic model representing the submit options for submitting jobs to
#     the argo workflows."""
#     annotations : Optional[str] = None
#     dryRun : Optional[bool] = True
#     entryPoint : Optional[str] = None
#     generateName : Optional[str] = None
#     labels : Optional[str] = None
#     name : Optional[str] = None
#     ownerReference : Optional[ArgoOwnerRefernceModel] = None
#     parameters : Optional[List[str]] = None
#     podPriorityClassName : Optional[str] = None
#     priority : Optional[int] = 0
#     serverDryRun : Optional[bool] = True
#     serviceAccount : Optional[str] = None


class ArgoSubmitBodyModel(BaseModel):
    """A Pydantic model representing the submit body schema for submitting jobs
    to the argo workflows."""
    namespace : Optional[str] = None
    resourceKind : Optional[str] = None
    resourceName : Optional[str] = None
    submitOptions : Optional[Dict[str,Any]] = None

class ArgoQueueItemBodyModel(BaseModel):
    """A Pydantic model representing the queue item body schema for submitting
    jobs to the argo workflows"""
    submit_body : ArgoSubmitBodyModel

