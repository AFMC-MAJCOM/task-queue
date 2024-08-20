"""Wherein is contained the functionality for the s3 Queue.
"""
import json
import os
from functools import reduce

import s3fs

from .queue_base import QueueBase
from .. import s5fs
from . import queue_base


fs = s3fs.S3FileSystem()


class JsonS3Queue(QueueBase):
    """Class for the JsonS3Queue.
    """
    def __init__(self, queue_base_s3_path):
        self.queue_base_path = queue_base_s3_path
        fs.mkdir(self.queue_base_path)
        self.queue_path = os.path.join(
            queue_base_s3_path,
            queue_base.QueueItemStage.WAITING.name
        )
        self.processing_path = os.path.join(
            queue_base_s3_path,
            queue_base.QueueItemStage.PROCESSING.name
        )
        self.queue_index_path = os.path.join(queue_base_s3_path, "index.txt")
        self.success_path = os.path.join(
            queue_base_s3_path,
            queue_base.QueueItemStage.SUCCESS.name
        )
        self.fail_path = os.path.join(
            queue_base_s3_path,
            queue_base.QueueItemStage.FAIL.name
        )

        if s5fs.HAS_S5CMD:
            print("S3 Queue is using S5CMD")
        else:
            print("S3 Queue is using S3FS")

    # BaseExeption is used to tell the user the failed items
    # pylint: disable=broad-exception-raised
    def put(self, items):
        """Adds a new Item to the Queue in the WAITING stage.

        Parameters:
        -----------
        items: dict
            Dictionary of Queue Items to add Queue, where Item is a key:value
            pair, where key is the item ID and value is the queue item body.
            The item ID must be a string and the item body must be
            serializable.

        Returns:
        -----------
        Returns the length of the list of added items.
        """
        # Get a list of item keys that are already in the index, and remove
        # them from the incoming items list
        in_index = get_queue_index_items(self.queue_index_path)
        item_keys_to_add = subtract_duplicates(items.keys(), in_index)

        # # These are only items whose keys are not in the index
        items_to_add = { k:items[k] for k in item_keys_to_add }
        # items_to_add = self._put(items)

        # Path of each item is its ID
        item_paths = {
            k: os.path.join(self.queue_path, id_to_fname(k))
            for k in items_to_add.keys()
        }

        # Write item data to each file
        queue_write_success = [
            maybe_write_s3_json(item_paths[item], items_to_add[item])
            for item in items_to_add.keys()
        ]

        # Check successes
        added_items = [
            item
            for item, success in zip(items_to_add, queue_write_success)
            if success
        ]

        # Add successful queue item writes to the index
        add_items_to_index(self.queue_index_path, added_items)

        # Check for any failures and raise if there were
        if not all(queue_write_success):
            fail_items = [
                item
                for item, success in zip(items_to_add, queue_write_success)
                if not success
            ]
            raise BaseException(
                "Error writing at least one queue object to S3:",
                fail_items
            )

        return len(added_items)

    def get(self, n_items=1):
        """Gets the next n items from the queue, moving them to PROCESSING.

        Parameters:
        -----------
        n_items: int
            Number of items to retrieve from queue.

        Returns:
        ------------
        Returns a list of n_items from the queue, as
        List[(queue_item_id, queue_item_body)]
        """
        n_items = max(n_items, 0)

        queue_items = safe_s3fs_ls(
            fs,
            self.queue_path,
            detail=False,
            refresh=True
        )

        to_get = queue_items[:n_items]

        output = []

        for item_path in to_get:
            # get item data and add to list
            with fs.open(item_path) as f:
                item_data = json.load(f)

            # move item to processing
            s3_move(item_path, self.processing_path)
            output.append((fname_to_id(item_path), item_data))

        return output

    def success(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to SUCCESS.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        s3_move(
            os.path.join(self.processing_path, id_to_fname(queue_item_id)),
            self.success_path
        )

    def fail(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to FAIL.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        s3_move(
            os.path.join(self.processing_path, id_to_fname(queue_item_id)),
            self.fail_path
        )

    def size(self, queue_item_stage):
        """Determines how many items are in some stage of the queue.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage object
            The specific stage of the queue (PROCESSING, FAIL, etc.).

        Returns:
        ------------
        Returns the number of items in that stage of the queue as an integer.
        """
        item_stage_size = len(
            safe_s3fs_ls(
                fs,
                os.path.join(self.queue_base_path, queue_item_stage.name)
            )
        )
        return item_stage_size

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
        paths_with_status = [
            (self.queue_path, queue_base.QueueItemStage.WAITING),
            (self.success_path, queue_base.QueueItemStage.SUCCESS),
            (self.fail_path, queue_base.QueueItemStage.FAIL),
            (self.processing_path, queue_base.QueueItemStage.PROCESSING)
        ]

        for p, s in paths_with_status:
            item_ids = map(fname_to_id, safe_s3fs_ls(fs, p))
            if queue_item_id in item_ids:
                return s

        raise KeyError(queue_item_id)

    def lookup_state(self,
                 queue_item_stage
                 ):
        """Lookup which item ids are in the current Queue stage.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage
            stage of Queue Item

        Returns:
        ------------
        Returns a list of all item ids in the current queue stage.
        """
        item_ids = []
        for item_id in get_queue_index_items(self.queue_index_path):
            if queue_item_stage == self.lookup_status(item_id):
                item_ids.append(item_id)
        return item_ids


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
        # Get item stage
        item_stage = self.lookup_status(queue_item_id)
        # Get item body
        item_body = []
        for item_id in get_queue_index_items(self.queue_index_path):
            if queue_item_id == item_id:
                fname = os.path.join(
                    self.queue_base_path,
                    item_stage.name,
                    id_to_fname(item_id)
                )
                with fs.open(fname) as f:
                    item_body = json.load(f)

        return {
            'item_id':queue_item_id,
            'status':item_stage,
            'item_body':item_body
        }

    def requeue(self, item_ids):
        """Move input queue items from FAILED to WAITING.

        Parameters:
        -----------
        item_ids: [str]
            ID of Queue Item
        """
        item_ids = self._requeue(item_ids)
        for item in item_ids:
            s3_move(
                os.path.join(self.fail_path, id_to_fname(item)),
                self.queue_path
            )

    def description(self):
        """A brief description of the Queue.

        Returns:
        ------------
        Returns a dictionary with relevant information about the Queue.
        """
        desc = {
            "implementation": "s3",
            "s3_path": self.queue_base_path
        }
        return desc

def ensure_s3_prefix(path:str):
    """Returns a valid s3 path.
    """
    if not path.startswith("s3://"):
        return "s3://" + path
    return path


def safe_s5fs_move(source, dest):
    """Uses s5fs to move from source to destination.

    Parameters:
    -----------
    source: str
        Path of source
    dest: str
        Path of destination
    """
    s5fs.move(
        ensure_s3_prefix(source),
        ensure_s3_prefix(dest)
    )

def safe_s3fs_ls(filesystem, path, *args, **kwargs):
    """Lists the contents of the file.

    Parameters:
    -----------
    filesystem: s3fs.S3FileSystem
        Instantiated s3fs.S3FileSystem Object.
    path: str
        Path to directory.
    *args: str
        Optional additional arguments.
    **kwargs: str
        More arguments.

    Returns:
    -----------
    Returns a list of the contents in the directory if the directory exists,
    otherwise returns an empty list instead of throwing an exception
    """
    fs.invalidate_cache()
    path = str(path)
    if filesystem.exists(path):
        try:
            return filesystem.ls(path, *args, **kwargs)
        except FileNotFoundError:
            pass
    return []

if s5fs.HAS_S5CMD:
    move = safe_s5fs_move
else:
    move = fs.move

def check_queue_index(index_path, item):
    """Check if Item is in the Queue index.

    Parameters:
    -----------
    index_path: str
        Path to index.
    item: str
        Item ID to look for.

    Returns:
    -----------
    Returns True if Item ID is in index file, or else False.
    """
    func_fs = s3fs.S3FileSystem(default_cache_type="none")
    if func_fs.exists(index_path):
        with func_fs.open(index_path, 'r') as f:
            line = f.readline()
            while line != '':
                line = line.replace("\n", '')
                if line == item:
                    return True
                line = f.readline()

    return False

def get_queue_index_items(index_path):
    """Gets all contents from the Queue index file.

    The queue index file is a text file with one item entry per line

    Parameters:
    -----------
    index_path: str
        Path to index file.

    Returns:
    -----------
    Returns list of Item IDs in file.
    """
    func_fs = s3fs.S3FileSystem(default_cache_type="none")
    if not func_fs.exists(index_path):
        return []
    with func_fs.open(index_path, "r") as f:
        return [
            line.strip()
            for line in f.readlines()
            if line != ''
        ]

def subtract_duplicates(main_list, *other_lists):
    """Remove duplicate items in `main_list`.

    Optionally subtract duplicates from `other_lists` as well.

    Parameters:
    -----------
    main_list: List
        List to remove dupicate items from
    *other_lists: List (optional)
        Optional other lists to remove duplicates from.

    Returns:
    -----------
    List of items with no duplicates.
    """
    others_set = reduce(
        set.union,
        (set(sublist) for sublist in other_lists),
        set()
    )

    return list(set(main_list) - others_set)

def add_items_to_index(index_path, items):
    """Add items to the Queue index file.

    Parameters:
    -----------
    index_path: str
        Path to index file.
    items: List of Queue Items.
        List of Queue Items to add to file, where Item is a key:value pair,
        where key is the item ID and value is the queue item body.
    """
    func_fs = s3fs.S3FileSystem(default_cache_type="none")
    with func_fs.open(index_path, 'a') as f:
        f.write("\n".join(items))
        # Trailing newline so the next add doesn't append to the end of the
        # Last item line we put there
        f.write("\n")


def add_item_to_index(index_path, item):
    """Add an item to the Queue index file.

    Parameters:
    -----------
    index_path: str
        Path to index file.
    item: Queue Item
        Queue Item to add to file, where Item is a key:value pair, where key is
        the item ID and value is the queue item body.
    """
    add_items_to_index(index_path, [item])


def id_to_fname(item_id):
    """Converts an Item ID into a filename.

    Parameters:
    -----------
    item_id: str
        ID of Item.

    Returns:
    -----------
    Returns a valid JSON filename with the Item ID.
    """
    return f"{item_id}.json"

def fname_to_id(item_fname):
    """Converts a filename to an Item ID.

    Parameters:
    -----------
    item_fname: str
        Filename of Item.

    Returns:
    -----------
    Returns a valid Item ID using the filename.
    """
    return os.path.splitext(os.path.basename(item_fname))[0]

# Pylint exception caught is disabled so that anything writen before
# an exception was caught will be correctly deleted from S3
# pylint: disable=broad-exception-caught
def maybe_write_s3_json(s3_path, json_data):
    """Fault-tolerant write to S3.

    If the write fails, return False instead of raising an exception.

    Parameters:
    -----------
    s3_path: str
        s3 path
    json_data: Dict
        Dictionary of data.

    Returns:
    -----------
    Returns boolean to show if it successfully wrote file.
    """
    try:
        with fs.open(s3_path, "wt") as f:
            json.dump(json_data, f, indent=4)
    except Exception as e:
        print(e)
        # What was written to the S3 file before the exception will still show
        # Up in S3, so let's just delete that
        fs.rm(s3_path)
        return False

    # If the output file exists, we succeeded.
    return fs.exists(s3_path)

def s3_move(item_path, dest_path):
    """Moves an item from one place to another.

    Parameters:
    -----------
    item_path: str
        Current Item path.
    dest_path: str
        Path of destination.

    Returns:
    -----------
    Returns the new path of the Item.
    """
    name = os.path.basename(item_path)
    dest = os.path.join(dest_path, name)

    move(item_path, dest)

    return dest

def json_s3_queue(queue_base_s3_path):
    """Creates and returns the S3 Queue.
    """
    return JsonS3Queue(queue_base_s3_path)
