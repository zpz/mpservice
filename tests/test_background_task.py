import asyncio
import time
import pytest
from mpservice.background_task import BackgroundTask


class MyTask(BackgroundTask):
    @classmethod
    def run(cls, x, y, *, wait=1.2):
        time.sleep(wait)
        return x + y

    @classmethod
    def get_task_id(cls, x, y, wait):
        # `wait` does not participate in task ID.
        return (x, y)


@pytest.mark.asyncio
async def test_a_task():
    tasks = MyTask()
    loop = asyncio.get_running_loop()
    tid = tasks.submit(3, 4, wait=1.5, loop=loop)
    print(tid)
    t0 = time.perf_counter()
    while not tasks.done(tid):
        await asyncio.sleep(0.001)
    t1 = time.perf_counter()

    tid2 = tasks.submit(3, 4, wait=1.6, loop=loop)
    assert tasks.get(tid) is tasks.get(tid2)
    assert tasks[tid]['callers'] == 2

    assert tid2 == tid
    assert tasks._tasks
    assert 1.5 - 0.01 < t1 - t0 < 1.5 + 0.01
    assert tasks.result(tid) == 7
    assert tid in tasks
    assert tasks.result(tid2) == 7
    assert tid not in tasks


def test_task():
    tasks = MyTask()
    tid = tasks.submit(3, 4, wait=1.5)
    print(tid)

    tid2 = tasks.submit(3, 4, wait=1.6)
    assert tasks.get(tid) is tasks.get(tid2)
    assert tasks[tid]['callers'] == 2
    tasks.cancel(tid2)
    assert tasks[tid]['callers'] == 1

    t0 = time.perf_counter()
    while not tasks.done(tid):
        time.sleep(0.001)
    t1 = time.perf_counter()

    assert 1.5 - 0.01 < t1 - t0 < 1.5 + 0.01
    assert tasks.result(tid) == 7
    assert tid not in tasks
