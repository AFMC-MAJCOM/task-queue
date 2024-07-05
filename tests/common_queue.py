"""Top docstring
"""
import pytest
import random
import data_pipeline.queue_base as qb

pytestmark = pytest.mark.skip()

def random_item():
    """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
    key = chr(random.randint(ord('a'), ord('z')))
    val = [
        random.randint(0, 100)
        for _ in range(random.randint(0, 10))
    ]

    return key,val

n_items = 20
default_items = dict([random_item() for _ in range(n_items)])

def test_put_get(queue):
    """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
    put = queue.put(default_items)
    get = queue.get(len(default_items))

    print(get)

    for k, v1 in get:
        assert any([
            v1 == v2
            for v2 in default_items.values()
        ])

def test_add_to_queue_no_duplicates(queue):
    """Docstring
    """
    queue.put(default_items)
    first_put = queue.size(qb.QueueItemStage.WAITING)
    queue.put(default_items)
    second_put = queue.size(qb.QueueItemStage.WAITING)

    assert first_put == len(default_items)
    assert second_put == len(default_items)

    # get all items from the queue
    get_count = len(default_items)
    items_from_queue = queue.get(get_count)
    assert get_count == len(items_from_queue)

def test_multiple_get_empty(queue):
    """Docstring
    """
    first_item = list(default_items.keys())[0]
    put = queue.put({first_item:default_items[first_item]})

    get = queue.get()
    get_2 = queue.get()

    assert len(get_2) == 0

def test_multiple_get(queue):
    """Docstring
    """
    queue.put(default_items)

    queue.get()
    queue.get()

def test_out_of_order(queue):
    """Docstring
    """
    queue.put(default_items)
    get = queue.get()
    queue.put(default_items)
    queue.success(get[0][0])

def test_mixed_duplicates(queue):
    """Docstring
    """
    half = len(default_items) // 2
    half_items = dict(list(default_items.items())[half:])
    queue.put(half_items)
    queue.put(default_items)
    assert queue.size(qb.QueueItemStage.WAITING) == len(default_items)

def test_get_empty_queue(queue):
    """Docstring
    """
    out = queue.get(10)

    assert out == []

def test_get_zero_items(queue):
    """Docstring
    """
    queue.put(default_items)

    nothing = queue.get(0)

    assert len(nothing) == 0
    assert queue.size(qb.QueueItemStage.WAITING) == len(default_items)
    assert queue.size(qb.QueueItemStage.PROCESSING) == 0

def test_put_exception(queue):
    """Docstring
    """
    items = {
        "a": [1, 2, 3],
        "b": random.Random(), # something that is not json serializable
        "c": [7, 8, 9]
    }

    # make sure the good items are still added
    try:
        queue.put(items)
    except:
        pass

    first_len = queue.size(qb.QueueItemStage.WAITING)
    assert first_len == len(items) - 1 

    # make sure the good items are not duplicated
    try:
        queue.put(items)
    except:
        pass

    second_len = queue.size(qb.QueueItemStage.WAITING)
    assert second_len == len(items) - 1
    
def test_queue_size(queue):
    """Docstring
    """
    queue.put(default_items)
    waiting_size = queue.size(qb.QueueItemStage.WAITING)
    assert len(default_items) == waiting_size

    # there are 4 stages that a queue item can be in, so let's move 1/4 of the
    # items to each stage
    move_amount = len(default_items) // 4

    # move 3/4 of the items to waiting
    move_count = move_amount * 3
    processing_items = queue.get(move_count)
    processing_size = queue.size(qb.QueueItemStage.PROCESSING)
    assert processing_size == move_count

    # move 1/4 of the items to success
    for key, data in processing_items[:move_amount]:
        queue.success(key)
    success_size = queue.size(qb.QueueItemStage.SUCCESS)
    assert success_size == move_amount

    # move 1/4 of the items to fail
    for key, data in processing_items[move_amount:2*move_amount]:
        queue.fail(key)
    fail_size = queue.size(qb.QueueItemStage.FAIL)
    assert fail_size == move_amount

def test_lookup(queue: qb.QueueBase):
    """Docstring
    """
    queue.put(default_items)

    proc, succ, fail = queue.get(3)

    print(proc, succ, fail)

    queue.success(succ[0])
    queue.fail(fail[0])

    assert queue.lookup_status(proc[0]) == qb.QueueItemStage.PROCESSING
    assert queue.lookup_status(succ[0]) == qb.QueueItemStage.SUCCESS
    assert queue.lookup_status(fail[0]) == qb.QueueItemStage.FAIL

def test_lookup_fail(queue: qb.QueueBase):
    """Docstring
    """
    with pytest.raises(KeyError):
        queue.lookup_status("does-not-exist")
