"""Pytests for the common queue functionalities.
"""
import random

import pytest

import task_queue.queue_base as qb


pytestmark = pytest.mark.skip()

def random_item():
    """Creates random item for tests.

    Returns:
    -----------
    Random key and value.

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
    """Tests put and get function as expected.
    """
    queue.put(default_items)
    get = queue.get(len(default_items))

    print(get)

    for k, v1 in get:
        assert any([
            v1 == v2
            for v2 in default_items.values()
        ])

def test_add_to_queue_no_duplicates(queue):
    """Tests that no duplicated are added to the queue.
    """
    queue.put(default_items)
    first_put = queue.size(qb.QueueItemStage.WAITING)
    queue.put(default_items)
    second_put = queue.size(qb.QueueItemStage.WAITING)

    assert first_put == len(default_items)
    assert second_put == len(default_items)

    # Get all items from the queue
    get_count = len(default_items)
    items_from_queue = queue.get(get_count)
    assert get_count == len(items_from_queue)

def test_multiple_get_empty(queue):
    """Tests multiple get calls.
    """
    first_item = list(default_items.keys())[0]
    queue.put({first_item:default_items[first_item]})

    queue.get()
    get_2 = queue.get()

    assert len(get_2) == 0

def test_multiple_get(queue):
    """Tests multiple get calls.
    """
    queue.put(default_items)

    queue.get()
    queue.get()

def test_out_of_order(queue):
    """Tests out of order.
    """
    queue.put(default_items)
    get = queue.get()
    queue.put(default_items)
    queue.success(get[0][0])

def test_mixed_duplicates(queue):
    """Tests that duplicates are not added to queue but new items are.
    """
    half = len(default_items) // 2
    half_items = dict(list(default_items.items())[half:])
    queue.put(half_items)
    queue.put(default_items)
    assert queue.size(qb.QueueItemStage.WAITING) == len(default_items)

def test_get_empty_queue(queue):
    """Tests the results of calling get on an empty queue.
    """
    out = queue.get(10)

    assert out == []

def test_get_zero_items(queue):
    """Tests get 0 items works as expected.
    """
    queue.put(default_items)

    nothing = queue.get(0)

    assert len(nothing) == 0
    assert queue.size(qb.QueueItemStage.WAITING) == len(default_items)
    assert queue.size(qb.QueueItemStage.PROCESSING) == 0

def test_put_exception(queue):
    """Tests that put will not add non-JSON serializable items.
    """
    # Add items that are not JSON serializable
    items = {
        "a": [1, 2, 3],
        "b": random.Random(),
        "c": [7, 8, 9]
    }

    # Make sure the good items are still added
    try:
        queue.put(items)
    except BaseException as e:
        print(e)

    first_len = queue.size(qb.QueueItemStage.WAITING)
    assert first_len == len(items) - 1

    # Make sure the good items are not duplicated
    try:
        queue.put(items)
    except BaseException as e:
        print(e)

    second_len = queue.size(qb.QueueItemStage.WAITING)
    assert second_len == len(items) - 1

def test_queue_size(queue):
    """Test that sizes are properly tracked.
    """
    queue.put(default_items)
    waiting_size = queue.size(qb.QueueItemStage.WAITING)
    assert len(default_items) == waiting_size

    # There are 4 stages that a queue item can be in, so let's move 1/4 of the
    # Items to each stage
    move_amount = len(default_items) // 4

    # Move 3/4 of the items to waiting
    move_count = move_amount * 3
    processing_items = queue.get(move_count)
    processing_size = queue.size(qb.QueueItemStage.PROCESSING)
    assert processing_size == move_count

    # Move 1/4 of the items to success
    for key, data in processing_items[:move_amount]:
        queue.success(key)
    success_size = queue.size(qb.QueueItemStage.SUCCESS)
    assert success_size == move_amount

    # Move 1/4 of the items to fail
    for key, data in processing_items[move_amount:2*move_amount]:
        queue.fail(key)
    fail_size = queue.size(qb.QueueItemStage.FAIL)
    assert fail_size == move_amount

def test_lookup(queue: qb.QueueBase):
    """Tests that lookup_status works as expected.
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
    """Test that proper error is thrown when lookup_status fails.
    """
    with pytest.raises(KeyError):
        queue.lookup_status("does-not-exist")

def test_lookup_state(queue: qb.QueueBase):
    """Tests that lookup_state works as expected with status-based lookup.
    """
    queue.put(default_items)

    # wait test
    wait_id_list = [x for x in default_items]
    assert wait_id_list.sort() == \
    queue.lookup_state(qb.QueueItemStage.WAITING).sort()

    # proc tests
    proc = queue.get(3)
    proc_id_list = [x for x,_ in proc]

    # succ test
    succ = queue.get(3)
    succ_id_list = [x for x,_ in succ]
    for i in succ:
        queue.success(i[0])

    # fail test
    fail = queue.get(2)
    fail_id_list = [x for x,_ in fail]
    for i in fail:
        queue.fail(i[0])

    assert proc_id_list.sort() == \
    queue.lookup_state(qb.QueueItemStage.PROCESSING).sort()
    assert succ_id_list.sort() == \
    queue.lookup_state(qb.QueueItemStage.SUCCESS).sort()
    assert fail_id_list.sort() == \
    queue.lookup_state(qb.QueueItemStage.FAIL).sort()

def test_lookup_state_fail(queue: qb.QueueBase):
    """Test that proper error is thrown when lookup_state fails.
    """
    result = queue.lookup_state("does-not-exist")
    assert result == []

