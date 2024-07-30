"""Wherein is contained the Abstract Base Classes for Queue.
"""
from abc import ABC, abstractmethod
from enum import Enum


class QueueItemStage(Enum):
    """The different stages that a queue item can be in.
    """
    WAITING = 0
    PROCESSING = 1
    SUCCESS = 2
    FAIL = 3


class QueueBase(ABC):
    """Abstract Base Class for Queue.
    """

    @abstractmethod
    def put(self, items):
        """Adds a new Item to the Queue in the WAITING stage.

        Parameters:
        -----------
        items: dict
            Dictionary of Queue Items to add Queue, where Item is a key:value
            pair, where key is the item ID and value is the queue item body.
        """

    @abstractmethod
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

    @abstractmethod
    def success(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to SUCCESS.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """

    @abstractmethod
    def fail(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to FAIL.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
    def description(self):
        """A brief description of the Queue.

        Returns:
        ------------
        Returns a dictionary with relevant information about the Queue.
        """
