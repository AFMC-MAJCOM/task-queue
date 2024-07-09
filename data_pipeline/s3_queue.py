import json
import s3fs
import os
import datetime
from functools import partial, reduce
from . import s5fs
from . import queue_base
from . import config

def ensure_s3_prefix(path:str):
    if not path.startswith("s3://"):
        return "s3://" + path
    return path


def safe_s5fs_move(source, dest):
    s5fs.move(
        ensure_s3_prefix(source),
        ensure_s3_prefix(dest)
    )

def safe_s3fs_ls(filesystem:s3fs.S3FileSystem, path, *args, **kwargs):
    fs.invalidate_cache()
    path = str(path)
    if filesystem.exists(path):
        try:
            return filesystem.ls(path, *args, **kwargs)
        except FileNotFoundError:
            pass
    return []

fs = config.get_s3fs_connection(default_cache_type="none")

if s5fs.HAS_S5CMD:
    move = safe_s5fs_move
    print("S3 Queue is using S5CMD")
else:
    move = fs.move
    print("S3 Queue is using S3FS")


def check_queue_index(index_path, item):
    fs = config.get_s3fs_connection()
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
    """
    the queue index file is a text file with one item entry per line
    """
    fs = config.get_s3fs_connection()
    if not fs.exists(index_path):
        return []
    with fs.open(index_path, "r") as f:
        return [
            line.strip()
            for line in f.readlines()
            if line != ''
        ]

def subtract_duplicates(main_list, *other_lists):
    """
    remove duplicate items in `main_list`, and optionally subtract duplicates
    from `other_lists` as well
    """
    others_set = reduce(
        set.union,
        (set(l) for l in other_lists),
        set()
    )

    return list(set(main_list) - others_set)

def add_items_to_index(index_path, items):
    fs = config.get_s3fs_connection()
    with fs.open(index_path, 'a') as f:
        # note: for some reason `f.writelines` didn't work here
        f.write("\n".join(items))
        # trailing newline so the next add doesn't append to the end of the
        # last item line we put there
        f.write("\n")


def add_item_to_index(index_path, item):
    add_items_to_index(index_path, [item])


def id_to_fname(item_id):
    return f"{item_id}.json"

def fname_to_id(item_fname:str):
    return os.path.splitext(os.path.basename(item_fname))[0]


def maybe_write_s3_json(s3_path, json_data):
    """
    Fault-tolerant write to S3: if the write fails, simply return False instead
    of raising an exception.

    """
    try:
        with fs.open(s3_path, "wt") as f:
            json.dump(json_data, f, indent=4)
    except Exception as e:
        print(e)
        # what was written to the S3 file before the exception will still show
        # up in S3, so let's just delete that
        fs.rm(s3_path)
        return False

    # If the output file exists, we succeeded.
    return fs.exists(s3_path)


def add_json_to_s3_queue(queue_path, queue_index_path, items:dict):
    # get a list of item keys that are already in the index, and remove them
    # from the incoming items list
    in_index = get_queue_index_items(queue_index_path)
    item_keys_to_add = subtract_duplicates(items.keys(), in_index)

    # these are only items whose keys are not in the index
    items_to_add = { k:items[k] for k in item_keys_to_add }

    # path of each item is its ID
    item_paths = {
        k: os.path.join(queue_path, id_to_fname(k))
        for k in items_to_add.keys()
    }

    # write item data to each file
    queue_write_success = [
        maybe_write_s3_json(item_paths[item], items_to_add[item])
        for item in items_to_add.keys()
    ]

    # check successes
    added_items = [
        item
        for item, success in zip(items_to_add, queue_write_success)
        if success
    ]

    # add successful queue item writes to the index
    add_items_to_index(queue_index_path, added_items)

    # check for any failures and raise if there were
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
    # once bitten, twice shy
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
    name = os.path.basename(item_path)
    dest = os.path.join(dest_path, name)

    move(item_path, dest)

    return dest


def lookup_status(
    waiting_path,
    success_path,
    fail_path,
    processing_path,
    item_id
):
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
