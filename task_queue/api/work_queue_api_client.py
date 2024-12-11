"""Wherein is contained the ApiClient class.
"""
from typing import Dict, Any, Union, List, Tuple
import warnings

from pydantic import validate_call, PositiveInt
import requests

from task_queue.queue_pydantic_models import QueueGetSizesModel, \
    LookupQueueItemModel, QueueItemBodyType
from ..queues.queue_base import QueueBase, QueueItemStage

warnings.filterwarnings(
                    "always",
                    category=UserWarning,
                    module=r'.*work_queue_api_client'
                )


class ApiClient(QueueBase):
    """Class for the ApiClient initialization and supporting functions.

    Parameters:
    -----------
    api_base_url: str
        The base url for all api endpoints.
    """
    api_base_url: str
    timeout: float = 5

    def __init__(self, api_base_url: str, timeout: float = 5):
        self.api_base_url = api_base_url + "/api/v1/queue/"
        self.timeout = timeout

    @validate_call
    def put(self, items: Dict[str, QueueItemBodyType]) -> None:
        """Adds a new Item to the Queue in the WAITING stage.

        Parameters:
        -----------
        items: dict
            Dictionary of Queue Items to add Queue, where Item is a key:value
            pair, where key is the item ID and value is the queue item body.
            The item ID must be a string and the item body must be
            serializable.
        """
        response = requests.post(f"{self.api_base_url}put", json=items, \
                                 timeout=self.timeout)
        response.raise_for_status()
        # Notify user if there were any items skipped.
        if response.json():
            for er in response.json()['detail']:
                warnings.warn(er)

    @validate_call
    def get(self, n_items:PositiveInt=1) -> List[Tuple[str, Any]]:
        """Gets the next n Items from the Queue, moving them to PROCESSING.

        Parameters:
        -----------
        n_items: int (default=1)
            Number of items to retrieve from Queue.

        Returns:
        ------------
        Returns a list of n_items from the Queue, as
        List[(queue_item_id, queue_item_body)]
        """
        response = requests.get(f"{self.api_base_url}get/{n_items}",
                               timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    @validate_call
    def peek(self, n_items:PositiveInt=1) -> List[Tuple[str, Any]]:
        return None

    @validate_call
    def success(self, queue_item_id:str) -> None:
        """Moves a Queue Item from PROCESSING to SUCCESS.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        return None

    @validate_call
    def fail(self, queue_item_id:str) -> None:
        """Moves a Queue Item from PROCESSING to FAIL.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        return None

    @validate_call
    def size(self, queue_item_stage:QueueItemStage) -> int:
        """Determines how many Items are in some stage of the Queue.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage object
            The specific stage of the Queue (PROCESSING, FAIL, etc.).

        Returns:
        ------------
        Returns the number of Items in that stage of the Queue as an integer.
        """
        response = requests.get(f"{self.api_base_url}size/"
                                f"{queue_item_stage.name}",
                                timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    @validate_call
    def sizes(self) -> QueueGetSizesModel:
        """Gets the number of Items in each Stage of the Queue.

        Returns:
        Returns a dictionary where the key is the name of the stage and the
        value is the number of Items currently in that Stage.
        """
        response = requests.get(f"{self.api_base_url}sizes",
                               timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    @validate_call
    def lookup_status(self, queue_item_id:str) -> QueueItemStage:
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
        response = requests.get(f"{self.api_base_url}status/{queue_item_id}",
                               timeout=self.timeout)
        response.raise_for_status()
        return QueueItemStage(response.json())

    @validate_call
    def lookup_state(self, queue_item_stage:QueueItemStage) -> List[str]:
        """Lookup which item ids are in the current Queue stage.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage
            stage of Queue Item

        Returns:
        ------------
        Returns a list of all item ids in the current queue stage.
        """
        stage = queue_item_stage.name
        response = requests.get(
            f"{self.api_base_url}lookup_state/{stage}",
            timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    @validate_call
    def lookup_item(self, queue_item_id:str) -> LookupQueueItemModel:
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
        response = requests.get(
            f"{self.api_base_url}lookup_item/{queue_item_id}",
            timeout=self.timeout)
        response.raise_for_status()
        response = response.json()
        response['status'] = QueueItemStage(response['status'])
        return response

    @validate_call
    def requeue(self, item_ids:Union[str, List[str]]) -> None:
        """Move input queue items from FAILED to WAITING.

        Parameters:
        -----------
        item_ids: [str]
            ID of Queue Item
        """
        response = requests.post(f"{self.api_base_url}requeue",
                               timeout=self.timeout,
                               json=item_ids
                              )
        response.raise_for_status()
        # Notify user if there were any items skipped.
        if response.json():
            for er in response.json()['detail']:
                warnings.warn(er)

    @validate_call
    def description(self) -> Dict[str, Union[str, Dict[str,Any]]]:
        """A brief description of the Queue.

        Returns:
        ------------
        Returns a dictionary with relevant information about the Queue.
        """
        response = requests.get(f"{self.api_base_url}describe",
                                timeout=self.timeout)
        response.raise_for_status()
        return response.json()
