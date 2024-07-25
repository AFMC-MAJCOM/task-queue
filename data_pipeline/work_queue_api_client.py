"""Wherein is contained the ApiClient class.
"""
# import abstract QueueBase when complete
from .queue_base import QueueItemStage

class ApiClient(): #TODO inherit from QueueBaseABC
    """Class for the ApiClient initialization and supporting functions.
    """
    def __init__(self, base_url):
        self.base_url = base_url
    
    def put(item_id: str, body):
        """Adds a new item to the queue in the WAITING state.

        Parameters:
        -----------
        item_id: string
            The id of the item being added to the queue.
        body: any
            The body of the item being added to the queue.
        """
        return None

    def get(n_items: int):
        """Returns n items from the queue.

        Parameters:
        -----------
        n_items: int
            The number of items that will be returned.

        Returns:
        -----------
        Returns a list of items from the queue.
        """
        return None # return List[Tuple[str, Any]]

    def success(item_id: str):
        """Classifies an item as successfully completed.

        Parameters:
        -----------
        item_id: str
            The id of the item that needs to be moved to success.
        """
        return None

    def fail(item_id: str):
        """Classifies an item as failed.

        Parameters:
        -----------
        item_id: str
            The id of the item that needs to be moved to failed.
        """
        return None

    def size(item_stage: QueueItemStage):
        """Returns the number of items in specific queue stage.

        Parameters:
        -----------
        item_stage: QueueItemStage
            The stage being requested.

        Returns:
        -----------
        Returns the size of the requested item stage.
        """
        return None # return int

    def lookup_status(item_id: str):
        """Returns the item stage of a given item.

        Parameters:
        -----------
        item_id: str
            The id of the item being requested.

        Returns:
        -----------
        Returns the queue stage of the requested item.
        """
        return None # return QueueItemStage
        
    def describe():
        """Returns a description of the queue.

        Returns:
        -----------
        Returns a dictionary description of the queue.
        """
        return None # return Dict[str, str]
        