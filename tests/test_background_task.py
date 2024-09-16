import asyncio
import concurrent.futures
import time

import pytest

from mpservice.background_task import BackgroundTask


class MyTask(BackgroundTask):
    @classmethod
    def run(cls, x, y, *, _cancelled, _info, wait=1.2):
        t0 = time.perf_counter()
        while True:
            time.sleep(0.01)
            if time.perf_counter() - t0 >= wait:
                break
            if _cancelled.is_set():
                raise concurrent.futures.CancelledError('cancelled per request')
            if not _info.empty():
                _ = _info.get_nowait()
            _info.put_nowait({'x': x, 'y': y, 'time_waited': time.perf_counter() - t0})
        return x + y

    @classmethod
    def get_task_id(cls, x, y, wait):
        # `wait` does not participate in task ID.
        return (x, y)


def test_task():
    tasks = MyTask()
    task = tasks.submit(3, 4, wait=1.5)
    print(task.task_id)
    assert task.exception() is None

    with pytest.raises(concurrent.futures.TimeoutError):
        _ = task.result(0)

    task2 = tasks.submit(3, 4, wait=1.6)
    assert task2.task_id == task.task_id
    assert task.callers == 2

    t0 = time.perf_counter()
    while not task.done():
        time.sleep(0.001)
    t1 = time.perf_counter()

    assert task.exception() is None
    assert 1.5 - 0.1 < t1 - t0 < 1.5 + 0.1
    assert task.result() == 7
    assert task.task_id in tasks

    assert task2.result() == 7
    assert task2.task_id not in tasks


def test_error():
    tasks = MyTask()
    task = tasks.submit(3, 4, wait='abc')
    time.sleep(0.5)
    assert task.done()
    with pytest.raises(TypeError):
        _ = task.result()
    e = task.exception()
    assert isinstance(e, TypeError)


def test_cancel():
    tasks = MyTask()
    task = tasks.submit(3, 4, wait=4)
    print(task.info())
    for _ in range(10):
        time.sleep(0.2)
        print(task.info())

    assert task.task_id in tasks

    assert not task.cancelled()
    assert not task.done()
    z = task.cancel()
    assert z

    assert task.cancelled()
    assert task.done()
    assert task.task_id not in tasks

    with pytest.raises(concurrent.futures.CancelledError):
        _ = task.result()

    with pytest.raises(concurrent.futures.CancelledError):
        _ = task.exception()


@pytest.mark.asyncio
async def test_async():
    tasks = MyTask()

    task = tasks.submit(3, 4, wait=3)
    z = await task
    assert z == 7

    task = tasks.submit(3, 4, wait=10)
    await asyncio.sleep(1)
    assert not task.done()
    assert task.exception() is None
    assert not task.cancelled()

    await task.a_cancel()

    with pytest.raises(concurrent.futures.CancelledError):
        _ = await task

    assert task.cancelled()
