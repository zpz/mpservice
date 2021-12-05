import time
from mpservice.background_task import BackgroundTask


class MyTask(BackgroundTask):
    @classmethod
    def run(cls, x, y, *, _cancelled, _status, wait=1.2):
        time.sleep(wait)
        return x + y

    @classmethod
    def get_task_id(cls, x, y, wait):
        # `wait` does not participate in task ID.
        return (x, y)


def test_task():
    tasks = MyTask()
    task = tasks.submit(3, 4, wait=1.5)
    print(task.task_id)

    t0 = time.perf_counter()
    while not task.done():
        time.sleep(0.001)
    t1 = time.perf_counter()

    task2 = tasks.submit(3, 4, wait=1.6)
    assert task2.task_id == task.task_id
    assert task.callers == 2

    assert tasks._tasks
    assert 1.5 - 0.01 < t1 - t0 < 1.5 + 0.01
    assert task.result() == 7
    assert task.task_id in tasks
    assert task2.result() == 7
    assert task2.task_id not in tasks
