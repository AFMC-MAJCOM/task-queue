"""Wherein is contained the functionality for the s3 Queue.
"""
import json
import os
from functools import partial, reduce

import s3fs

from . import s5fs
from . import queue_base


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

fs = s3fs.S3FileSystem(default_cache_type="none")

if s5fs.HAS_S5CMD:
    move = safe_s5fs_move
    print("S3 Queue is using S5CMD")
else:
    move = fs.move
    print("S3 Queue is using S3FS")


def check_queue_index(index_path, item):
    """Check if Item is in the Queue index.

    Parameters:
    -----------
    index_path: str
        Path to index.
    item: str
        Item to look for.

    Returns:
    -----------
    Returns True if Item is in index file, or else False.
    """
    fs = s3fs.S3FileSystem()
    if (fs.exists(index_path)):
        with fs.open(index_path, 'r') as f:
            line = f.readline()
            while (line != ''):
                line = line.replace("\n", '')
                if (line == item):
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
    Returns list of contents of file.
    """
    fs = s3fs.S3FileSystem()
    if not fs.exists(index_path):
        return []
    with fs.open(index_path, "r") as f:
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
        (set(l) for l in other_lists),
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
    fs = s3fs.S3FileSystem()
    with fs.open(index_path, 'a') as f:
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


def add_json_to_s3_queue(queue_path, queue_index_path, items):
    """Add Items to the s3 Queue.

    Parameters:
    -----------
    queue_path: str
        Path to s3 Queue.
    queue_index_path: str
        Path to s3 Queue Index.
    items: dict
        Dictionary of Queue Items to add Queue, where Item is a key:value pair,
        where key is the item ID and value is the queue item body.

    Returns:
    -----------
    Returns the length of the list of added items.
    """
    # Get a list of item keys that are already in the index, and remove them
    # From the incoming items list
    in_index = get_queue_index_items(queue_index_path)
    item_keys_to_add = subtract_duplicates(items.keys(), in_index)

    # These are only items whose keys are not in the index
    items_to_add = { k:items[k] for k in item_keys_to_add }

    # Path of each item is its ID
    item_paths = {
        k: os.path.join(queue_path, id_to_fname(k))
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
    add_items_to_index(queue_index_path, added_items)

    # Check for any failures and raise if there were
    if not all(queue_write_success):
        fail_items = [
            item
            for item, success in zip(items_to_add, queue_write_success)
            if not success
        ]
        raise BaseException("Error writing at least one queue object to S3:",
                            fail_items)

    return len(added_items)


def get_json_from_s3_queue(queue_path, processing_path, n_items=1):
    """Grabs Items from s3 Queue.

    Parameters:
    -----------
    queue_path: str
        Path to s3 Queue.
    processing_path: str
        Path to processing, where items are moved to after being moved from
        the Queue.
    n_items: int (default=1)
        Number of items to get from Queue.

    Returns:
    -----------
    Returns list containing item IDs and data.
    """
    if n_items < 0:
        n_items = 0

    queue_items = safe_s3fs_ls(
        fs,
        queue_path,
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
        destination = s3_move(item_path, processing_path)
        output.append((fname_to_id(item_path), item_data))

    return output


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


def lookup_status(waiting_path,
                  success_path,
                  fail_path,
                  processing_path,
                  item_id
                 ):
    """Look up the status of Item.

    Parameters:
    waiting_path: str
        Path of WAITING Items.
    success_path: str
        Path of SUCCESS Items.
    fail_path: str
        Path of FAIL Items.
    processing_path: str
        Path of PROCESSING Items.
    item_id: str
        ID of Item.

    Returns:
    -----------
    Returns the status of desired Item.
    """
    paths_with_status = [
        (waiting_path, queue_base.QueueItemStage.WAITING),
        (success_path, queue_base.QueueItemStage.SUCCESS),
        (fail_path, queue_base.QueueItemStage.FAIL),
        (processing_path, queue_base.QueueItemStage.PROCESSING)
    ]

    for p, s in paths_with_status:
        item_ids = map(fname_to_id, safe_s3fs_ls(fs, p))
        if item_id in item_ids:
            return s

    raise KeyError(item_id)


index_name = "index.txt"

def JsonS3Queue(queue_base_s3_path):
    """Returns the s3 Queue.
    """
    queue_index_path = os.path.join(queue_base_s3_path, index_name)
    queue_path = os.path.join(queue_base_s3_path,
                              queue_base.QueueItemStage.WAITING.name)
    processing_path = os.path.join(queue_base_s3_path,
                                   queue_base.QueueItemStage.PROCESSING.name)
    success_path = os.path.join(queue_base_s3_path,
                                queue_base.QueueItemStage.SUCCESS.name)
    fail_path = os.path.join(queue_base_s3_path,
                             queue_base.QueueItemStage.FAIL.name)

    return queue_base.QueueBase(
        partial(add_json_to_s3_queue, queue_path, queue_index_path),
        partial(get_json_from_s3_queue, queue_path, processing_path),

        lambda item_id: s3_move(
            os.path.join(processing_path, id_to_fname(item_id)),
            success_path
        ),

        lambda item_id: s3_move(
            os.path.join(processing_path, id_to_fname(item_id)),
            fail_path
        ),

        lambda queue_item_stage: len(
            safe_s3fs_ls(
                fs,
                os.path.join(queue_base_s3_path, queue_item_stage.name)
            )
        ),

        partial(lookup_status, queue_path, success_path,
                fail_path, processing_path),

        {
            "implementation": "s3",
            "s3_path": queue_base_s3_path
        }
    )
