"""Wherein is contained the Abstract Base Classes for Queue.
"""
import warnings
from abc import ABC, abstractmethod
from enum import Enum

from task_queue import logger


class QueueItemStage(Enum):
    """The different stages that a queue item can be in.
    """
    WAITING = 0
    PROCESSING = 1
    SUCCESS = 2
    FAIL = 3

warnings.filterwarnings(
                "always",
                category=UserWarning,
                module=r'.*queue_base'
            )

class QueueBase(ABC):
    """Abstract Base Class for Queue.
    """

    def _put(self, items):
        """Remove Item from items if the Item ID exists in the queue.

        Parameters:
        ------------
        items: dict
            Dictionary of Queue Items where Item is a key:value pair, where key
            is the item ID and value is the queue item body.
            The item ID must be a string and the item body must be
            serializable.

        Returns:
        ------------
        Returns a dictionary of items.
        """
        # Get all IDs in queue
        queue_ids = (self.lookup_state(QueueItemStage.FAIL)
        + self.lookup_state(QueueItemStage.SUCCESS)
        + self.lookup_state(QueueItemStage.WAITING)
        + self.lookup_state(QueueItemStage.PROCESSING))

        item_ids = items.keys()
        duplicate_ids = list(set(item_ids).intersection(set(queue_ids)))

        for id_ in duplicate_ids:
            logger.warning("Item %s already in queue. Skipping.", id_)
            warnings.warn(f"Item {id_!r} already in queue. Skipping.")

        no_duplicate_items = items.copy()
        for k in duplicate_ids:
            no_duplicate_items.pop(k)
        return no_duplicate_items

    @abstractmethod
    def put(self, items):
        """Adds a new Item to the Queue in the WAITING stage.

        Parameters:
        -----------
        items: dict
            Dictionary of Queue Items to add Queue, where Item is a key:value
            pair, where key is the item ID and value is the queue item body.
            The item ID must be a string and the item body must be
            serializable.
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
    def peek(self, n_items=1):
        """Return the next queue items without moving anything from WAITING to
        PROCESSING.

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

    def sizes(self):
        """Determines how many Items are in each stage of the Queue.

        Returns:
        ------------
        Returns the number of Items in each stage of the Queue as an integer.
        """
        waiting_size = self.size(QueueItemStage.WAITING)
        processing_size = self.size(QueueItemStage.PROCESSING)
        success_size = self.size(QueueItemStage.SUCCESS)
        fail_size = self.size(QueueItemStage.FAIL)

        sizes_dict = {"WAITING": waiting_size, "PROCESSING": processing_size,
                      "SUCCESS": success_size, "FAIL": fail_size}

        return sizes_dict

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
        Returns a dictionary with the Queue Item ID, the status of that Item,
        and the body, or it will raise an error if Item is not in Queue.
        """

    @abstractmethod
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

    @abstractmethod
    def description(self):
        """A brief description of the Queue.

        Returns:
        ------------
        Returns a dictionary with relevant information about the Queue.
        """

    @abstractmethod
    def requeue(self, item_ids):
        """Move input queue items from FAILED to WAITING.

        Parameters:
        -----------
        item_ids: [str]
            ID of Queue Item
        """

    def _requeue(self, item_ids):
        """Remove ids from item_ids that are not in the FAIL state.

        Parameters:
        -----------
        item_ids: [str]
            ID of Queue Item

        Returns:
        ------------
        Returns a list of IDs that to be requeued
        """
        if isinstance(item_ids, str):
            item_ids = [item_ids]

        failed_ids = self.lookup_state(QueueItemStage.FAIL)
        missing_ids = list(set(item_ids) - set(failed_ids))
        for id_ in missing_ids:
            logger.warning("Item %s not in a FAIL state. Skipping.", id_)
            warnings.warn(f"Item {id_!r} not in a FAIL state. Skipping.")

        item_ids = [id_ for id_ in item_ids if id_ in failed_ids]
        return item_ids
