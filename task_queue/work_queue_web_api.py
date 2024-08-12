"""Wherein is contained the functions and classes concering the Work Queue Web
API.
"""
import logging
from dataclasses import dataclass, asdict
from typing import Dict, Any, Annotated, Union, Tuple, List
import json
from annotated_types import Ge, Le

from pydantic import PositiveIn
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine

from task_queue.queue_base import QueueItemStage
from task_queue.s3_queue import json_s3_queue
from task_queue.sql_queue import json_sql_queue
from task_queue import config
from task_queue.in_memory_queue import in_memory_queue
from task_queue.queue_pydantic_models import QueueGetSizesModel, \
    LookupQueueItemModel, QueueItemBodyType


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

api_settings = config.TaskQueueApiSettings()
api_settings.log_settings()
app = FastAPI()


@dataclass
class QueueSettings():
    """Class concerning the Queue Settings.
    """

    @staticmethod
    def from_env():
        """Returns instance of QueueSettings

        Returns:
        -----------
        A QueueSettings object.
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
    def from_env():
        """Returns an S3QueueSettings object given an s3 Queue Base path.

        Returns:
        -----------
        S3 Queue instance.
        """
        s3_settings = config.get_task_queue_settings(
            setting_class = config.TaskQueueS3Settings
        )
        s3_settings.log_settings()
        return S3QueueSettings(s3_settings.S3_QUEUE_BASE_PATH)

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
    def from_env():
        """Creates and returns an instance of SqlQueueSettings.

        Returns:
        -----------
        Returns an instance of SQLQueueSettings.
        """
        sql_settings = config.get_task_queue_settings(
            setting_class = config.TaskQueueSqlSettings
        )
        sql_settings.log_settings()
        if sql_settings.SQL_QUEUE_CONNECTION_STRING is not None:
            conn_str = sql_settings.SQL_QUEUE_CONNECTION_STRING
        else:
            user = sql_settings.SQL_QUEUE_POSTGRES_USER
            password = sql_settings.SQL_QUEUE_POSTGRES_PASSWORD
            host = sql_settings.SQL_QUEUE_POSTGRES_HOSTNAME
            database = sql_settings.SQL_QUEUE_POSTGRES_DATABASE

            conn_str = \
                f"postgresql+psycopg2://{user}:{password}@{host}/{database}"

        return SqlQueueSettings(
            conn_str,
            sql_settings.SQL_QUEUE_NAME,
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
    def from_env():
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


def queue_settings_from_env():
    """Creates an instance of QueueSettings from an environment dictionary.

    Returns:
    -----------
    Returns QueueSettings.
    """
    if api_settings.QUEUE_IMPLEMENTATION  \
        == config.QueueImplementations.S3_JSON:
        return S3QueueSettings.from_env()
    if api_settings.QUEUE_IMPLEMENTATION \
        == config.QueueImplementations.SQL_JSON:
        return SqlQueueSettings.from_env()
    if api_settings.QUEUE_IMPLEMENTATION \
        == config.QueueImplementations.IN_MEMORY:
        return InMemoryQueueSettings.from_env()
    return None

queue_settings = queue_settings_from_env()
queue = queue_settings.make_queue()


def json_serializable_validator(o):
    """Raises a value error if a given item is not serializable.

    Parameters:
    -----------
    o: JSON value
        An item being validated.

    Returns:
    -----------
    Returns the original input.
    """
    json.dumps(o)
    return o


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
        Desired Queue Item Stage (i.e. WAITING, FAIL)

    Returns:
    -----------
    Returns a list of item ids in that stage.
    """
    try:
        queue_item_stage_enum = QueueItemStage[queue_item_stage]
        result = queue.lookup_state(queue_item_stage_enum)
        return result
    except Exception as exc:
        raise HTTPException(status_code=400,
              detail=f"{queue_item_stage} not a Queue Item Stage") from exc

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

@app.get("/api/v1/queue/describe")
async def describe_queue() -> Dict[str, Union[str, Dict[str,Any]]]:
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
async def get(n_items:PositiveInt=1) ->  List[Tuple[str, Any]]:
    """API endpoint to get the next n Items from the Queue
    and move them to PROCESSING.

    Parameters:
    -----------
    n_items: int (default=1)
        Number of items to retrieve from Queue.

    Returns:
    ----------
    Returns a list of n_items from the Queue, as
    List[(queue_item_id, queue_item_body)]
    """
    return queue.get(n_items)

@app.post("/api/v1/queue/requeue")
def requeue(item_ids:Union[str,list[str]]) -> None:
    """API endpoint to move input queue items from FAILED to WAITING.

    Parameters:
    -----------
    item_ids: [str]
        ID of Queue Item
    """
    queue.requeue(item_ids)

@app.post("/api/v1/queue/put")
async def put(items:Dict[str,QueueItemBodyType]) -> None:
    """API endpoint to add items to the Queue.

    Parameters:
    -----------
    items: dict
        Dictionary of Queue Items to add Queue, where Item is a key:value
        pair, where key is the item ID and value is the queue item body.
        The item ID must be a string and the item body must be serializable.
    """
    queue.put(items)
