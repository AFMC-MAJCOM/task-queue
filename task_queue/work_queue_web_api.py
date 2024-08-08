"""Wherein is contained the functions and classes concering the Work Queue Web
API.
"""
from dataclasses import dataclass, asdict
<<<<<<< HEAD
from typing import Dict, Any, Annotated, Union, List
=======
from typing import Dict, Any, Annotated, Union, Tuple, List
>>>>>>> a6ee40908971f1e2d2fa343278da020647ddd0a7
from annotated_types import Ge, Le, MinLen
from pydantic import BaseModel
import os

from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine

from task_queue.queue_base import QueueItemStage
from task_queue.s3_queue import json_s3_queue
from task_queue.sql_queue import json_sql_queue
from task_queue.in_memory_queue import in_memory_queue

app = FastAPI()


@dataclass
class QueueSettings():
    """Class concerning the Queue Settings.
    """
    # Disabled because this method is inherited
    # and uses env_dict as a param later
    # pylint: disable=unused-argument
    @staticmethod
    def from_env(env_dict):
        """Returns instance of QueueSettings

        Parameters:
        -----------
        env_dict: dict
            Dictionary of environment variables.
        """
        return QueueSettings()

    def make_queue(self):
        """Returns QueueBase object.
        """
        raise NotImplementedError("make_queue not yet implemented.")


@dataclass
class S3QueueSettings(QueueSettings):
    """Class concerning the s3 Queue settings.
    """
    s3_base_path : str

    @staticmethod
    def from_env(env_dict):
        """Returns an S3QueueSettings object given an s3 Queue Base path.

        Parameters:
        -----------
        env_dict: dict
            Dictionary of environment variables.
        """
        return S3QueueSettings(
            env_dict['S3_QUEUE_BASE_PATH']
        )

    def make_queue(self):
        """Creates and returns a JsonS3Queue.
        """
        return json_s3_queue(self.s3_base_path)


@dataclass
class SqlQueueSettings(QueueSettings):
    """Class concerning the SQL queue settings.
    """
    connection_string : str
    queue_name : str

    @staticmethod
    def from_env(env_dict):
        """Creates and returns an instance of SqlQueueSettings based on the
        given env_dict.

        Parameters:
        -----------
        env_dict: dict
            Dictionary of environment variables.

        Returns:
        -----------
        Returns an instance of SQLQueueSettings.
        """
        if "SQL_QUEUE_CONNECTION_STRING" in env_dict:
            conn_str = env_dict["SQL_QUEUE_CONNECTION_STRING"]
        else:
            user = env_dict['SQL_QUEUE_POSTGRES_USER']
            password = env_dict['SQL_QUEUE_POSTGRES_PASSWORD']
            host = env_dict['SQL_QUEUE_POSTGRES_HOSTNAME']
            database = env_dict['SQL_QUEUE_POSTGRES_DATABASE']

            conn_str = \
                f"postgresql+psycopg2://{user}:{password}@{host}/{database}"

        return SqlQueueSettings(
            conn_str,
            env_dict['SQL_QUEUE_NAME'],
        )

    def make_queue(self):
        """Creates and returns a JSONSQLQueue.
        """
        return json_sql_queue(
            create_engine(self.connection_string),
            self.queue_name
        )


@dataclass
class InMemoryQueueSettings(QueueSettings):
    """Class concerning the In Memory Queue settings.
    The only implementation of this class so far is for testing.
    """
    @staticmethod
    def from_env(env_dict):
        """Returns instance of QueueSettings
        Parameters:
        -----------
        env_dict: dict
            Dictionary of environment variables.
        """
        return InMemoryQueueSettings()

    def make_queue(self):
        """Returns QueueBase object.
        """
        return in_memory_queue()


def queue_settings_from_env(env_dict):
    """Creates an instance of QueueSettings from an environment dictionary.

    Parameters:
    -----------
    env_dict: dict
        Dictionary of environment variables.

    Returns:
    -----------
    Returns QueueSettings.
    """
    impl = env_dict['QUEUE_IMPLEMENTATION']
    if impl == "s3-json":
        return S3QueueSettings.from_env(env_dict)
    if impl == "sql-json":
        return SqlQueueSettings.from_env(env_dict)
    if impl == "in-memory":
        return InMemoryQueueSettings.from_env(env_dict)
    return None

queue_settings = queue_settings_from_env(os.environ)
queue = queue_settings.make_queue()

class QueueGetSizesModel(BaseModel):
    """A Pydantic model representing the return dictionary for the /sizes
    endpoint and get_queue_sizes() in client."""
    WAITING : int
    PROCESSING : int
    SUCCESS : int
    FAIL : int


@app.get("/api/v1/queue/sizes")
async def get_queue_sizes() -> QueueGetSizesModel:
    """API endpoint to get the number of jobs in each stage.

    Returns:
    ----------
    Returns the number of jobs in each stage of the queue.
    """
    return {
        s.name : queue.size(s)
        for s in QueueItemStage
    }

@app.get("/api/v1/queue/status/{item_id}")
async def lookup_queue_item_status(item_id:str)->Annotated[int, Ge(0), Le(3)]:
    """API endpoint to look up the status of a specific item in queue.

    Parameters:
    -----------
    item_id: str
        ID of Item.

    Returns:
    -----------
    Returns the status of Item passed in.
    """
    try:
        return queue.lookup_status(item_id)
    except KeyError as exc:
        raise HTTPException(status_code=400,
                            detail=f"{item_id} not in Queue") from exc

@app.get("/api/v1/queue/lookup_state/{queue_item_stage}")
async def lookup_queue_item_state(queue_item_stage: str) -> List[str]:
    """API endpoint to look up all item ids from a specific stage.

    Parameters:
    -----------
    queue_item_stage: str
        Desired Queue Item Stage.

    Returns:
    -----------
    Returns a list of item ids.
    """
    try:
        queue_item_stage_enum = QueueItemStage[queue_item_stage]
        result = queue.lookup_state(queue_item_stage_enum)
        return result
    except Exception as exc:
        raise HTTPException(status_code=400,
              detail=f"{queue_item_stage} not a Queue Item Stage") from exc

class LookupQueueItemModel(BaseModel):
    """A Pydantic model representing the return dictionary for the /lookup_item
    endpoint and lookup_item() in client."""

    item_id : str
    status : QueueItemStage
    item_body : Any

@app.get("/api/v1/queue/lookup_item/{item_id}")
async def lookup_queue_item(item_id:str) -> LookupQueueItemModel:
    """API endpoint to lookup an Item currently in the Queue.

    Parameters:
    -----------
    item_id: str
        ID of Queue Item

    Returns:
    ------------
    Returns a dictionary with the Queue Item ID, the status of that Item, and
    the body, or it will raise an error if Item is not in Queue.
    """
    try:
        response = queue.lookup_item(item_id)
        return response
    except KeyError as exc:
        raise HTTPException(status_code=400,
                            detail=f"{item_id} not in Queue") from exc

class QueueDescribeModel(BaseModel):
    """A Pydantic model representing the return dictionary for the /describe
    endpoint and description() in client."""

    implementation : str
    arguments : Dict[str, Any]

@app.get("/api/v1/queue/describe")
async def describe_queue() -> QueueDescribeModel:
    """API endpoint to descibe the Queue.

    Returns:
    ----------
    Returns a dictionary description of the Queue.
    """
    return {
        "implementation": queue.__class__.__name__,
        "arguments": asdict(queue_settings)
    }

@app.get("/api/v1/queue/get/{n_items}")
async def get(n_items:int) ->  List[Tuple[str, Any]]:
    """API endpoint to get the next n Items from the Queue
    and move them to PROCESSING.

    Parameters:
    -----------
    n_items: int
        Number of items to retrieve from Queue.

    Returns:
    ----------
    Returns a list of n_items from the Queue, as
    List[(queue_item_id, queue_item_body)]
    """
    return queue.get(n_items)

@app.post("/api/v1/queue/requeue")
def requeue(item_ids:Union[str,list[str]]):
    """API endpoint to move input queue items from FAILED to WAITING.

    Parameters:
    -----------
    item_ids: [str]
        ID of Queue Item
    """
    queue.requeue(item_ids)

class QueuePutModel(BaseModel):
    """A Pydantic model representing the input dictionary for the /put
    endpoint and put() in client.
    """

@app.post("/api/v1/queue/put")
async def put(items:Annotated[Dict[str,Any], MinLen(1)]) -> None:
    """API endpoint to add items to the Queue.

    Parameters:
    -----------
    items: dict
        Dictionary of Queue Items to add Queue, where Item is a key:value
        pair, where key is the item ID and value is the queue item body.
    """
    queue.put(items)
