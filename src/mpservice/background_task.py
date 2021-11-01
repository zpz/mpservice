import asyncio
import concurrent.futures
import functools
from abc import ABC, abstractmethod
from collections.abc import Hashable
from typing import Optional

from ._streamer import MAX_THREADS


class Task:
    # Objects of this class are created by `BackgroundTask`.

    def __init__(self, task_id: str, task_info: dict, task_catalog: dict):
        self.task_id = task_id
        self._task_info = task_info
        self._task_catalog = task_catalog

    def cancel(self) -> bool:
        self._task_info['callers'] -= 1
        if self._task_info['callers'] > 0:
            return False
        return self._task_info['fut'].cancel()

    def done(self) -> bool:
        return self._task_info['fut'].done()

    def result(self):
        if self._task_info['fut'].done():
            self._task_info['callers'] -= 1
            if self._task_info['callers'] < 1:
                del self._task_catalog[self.task_id]
            return self._task_info['fut'].result()

        if isinstance(self._task_info['fut'], concurrent.futures.Future):
            raise concurrent.futures.InvalidStateError('task is not done yet')
        else:
            raise asyncio.InvalidStateError('task is not done yet')

    def exception(self):
        if self._task_info['fut'].done():
            self._['callers'] -= 1
            if self._task_info['callers'] < 1:
                del self._task_catalog[self.task_id]
            return self._task_info['fut'].exception()
            # This returns `None` if no exception was raised.

        if isinstance(self._task_info['fut'], concurrent.futures.Future):
            raise concurrent.futures.InvalidStateError('task is not done yet')
        else:
            raise asyncio.InvalidStateError('task is not done yet')


class BackgroundTask(ABC):
    def __init__(self, executor: Optional[concurrent.futures.Executor] = None):
        if executor is None:
            executor = concurrent.futures.ThreadPoolExecutor(MAX_THREADS)
        self._executor = executor
        self._tasks = {}

    @classmethod
    @abstractmethod
    def run(cls, *args, **kwargs):
        # Subclass implementation may want to make the parameter
        # more specific.
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_task_id(cls, *args, **kwargs) -> Hashable:
        # The parameter list should be identical to that
        # of `run`.
        raise NotImplementedError

    def submit(self, *args, loop=None, **kwargs) -> Task:
        task_id = hash(self.get_task_id(*args, **kwargs))
        task_info = self._tasks.get(task_id)
        if task_info:
            task_info['callers'] += 1
            fut = task_info['fut']
            if isinstance(fut, asyncio.Future):
                fut.remove_done_callback(self._cb_remove_once_done)
            else:
                try:
                    fut._done_callbacks.remove(self._cb_remove_once_done)
                except ValueError:
                    pass
            return Task(task_id, task_info, self._tasks)

        func = self.run
        if kwargs:
            func = functools.partial(self.run, **kwargs)
        if loop is None:
            fut = self._executor.submit(func, *args)
        else:
            fut = loop.run_in_executor(self._executor, func, *args)
        task_info = {'fut': fut, 'callers': 1}
        self._tasks[task_id] = task_info
        return Task(task_id, task_info, self._tasks)

    def _cb_remove_once_done(self, fut) -> None:
        t = None
        for task_id, task_info in self._tasks.item():
            if task_info['fut'] is fut:
                t = task_id
                break
        if t is not None:
            del self._tasks[t]

    def submit_and_forget(self, *args, loop=None, **kwargs) -> None:
        task = self.submit(*args, loop=loop, **kwargs)
        if task._task_info['callers'] == 1:
            task._task_info['fut'].add_done_callback(self._cb_remove_once_done)
        task._task_info['callers'] -= 1

    def __contains__(self, task_id: str) -> bool:
        return task_id in self._tasks

    def __getitem__(self, task_id: str) -> dict:
        task_info = self._tasks[task_id]
        return Task(task_id, task_info, self._tasks)

    def get(self, task_id: str, default=None) -> Optional[dict]:
        try:
            return Task(task_id, self._tasks[task_id], self._tasks)
        except KeyError:
            return default

    def __iter__(self):
        return iter(self._tasks)

    def keys(self):
        return self._tasks.keys()

    def values(self):
        for task_id, task_info in self._tasks.items():
            yield Task(task_id, task_info, self._tasks)

    def items(self):
        for task_id, task_info in self._tasks.items():
            yield task_id, Task(task_id, task_info, self._tasks)

    def __delitem__(self, task_id):
        task = self[task_id]
        if not task.done():
            task.cancel()
        else:
            del self._tasks[task_id]
