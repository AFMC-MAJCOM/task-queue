"""Wherein is contained the ApiClient class.
"""

from .queue_base import QueueBase

class ApiClient(QueueBase):
    """Class for the ApiClient initialization and supporting functions.

    Parameters:
    -----------
    api_base_url: str
        The base url for all api endpoints.
    """
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url + "/api/v1/queue/"

    def put(self, items):
        """Adds a new Item to the Queue in the WAITING stage.

        Parameters:
        -----------
        items: dict
            Dictionary of Queue Items to add Queue, where Item is a key:value
            pair, where key is the item ID and value is the queue item body.
        """
        return None

    def get(self, n_items=1):
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
        return None

    def success(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to SUCCESS.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        return None

    def fail(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to FAIL.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        return None

    def size(self, queue_item_stage):
        """Determines how many Items are in some stage of the Queue.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage object
            The specific stage of the Queue (PROCESSING, FAIL, etc.).

        Returns:
        ------------
        Returns the number of Items in that stage of the Queue as an integer.
        """
        return None

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
        return None

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
        return None

    def requeue(self, item_ids):
        """Move input queue items from FAILED to WAITING.

        Parameters:
        -----------
        item_ids: [str]
            ID of Queue Item

        Returns:
        ------------
        Returns a list of IDs that were moved from FAIL to WAITING
        """
        return None

    def description(self):
        """A brief description of the Queue.

        Returns:
        ------------
        Returns a dictionary with relevant information about the Queue.
        """
        return None
