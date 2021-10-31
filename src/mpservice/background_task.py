import asyncio
import concurrent.futures
import functools
from abc import ABC, abstractmethod
from collections.abc import Hashable
from typing import Optional

from ._streamer import MAX_THREADS


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

    def submit(self, *args, loop=None, **kwargs) -> str:
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
            return task_id

        func = self.run
        if kwargs:
            func = functools.partial(self.run, **kwargs)
        if loop is None:
            fut = self._executor.submit(func, *args)
        else:
            fut = loop.run_in_executor(self._executor, func, *args)
        self._tasks[task_id] = {
            'fut': fut,
            'callers': 1,
        }
        return task_id

    def _cb_remove_once_done(self, fut) -> None:
        t = None
        for task_id, task_info in self._tasks.item():
            if task_info['fut'] is fut:
                t = task_id
                break
        if t is not None:
            del self._tasks[t]

    def submit_and_forget(self, *args, loop=None, **kwargs) -> None:
        task_id = self.submit(*args, loop=loop, **kwargs)
        task_info = self._tasks[task_id]
        if task_info['callers'] == 1:
            task_info['fut'].add_done_callback(self._cb_remove_once_done)
        task_info['callers'] -= 1

    def __contains__(self, task_id: str) -> bool:
        return task_id in self._tasks

    def __getitem__(self, task_id: str) -> dict:
        return self._tasks[task_id]

    def get(self, task_id: str, default=None) -> Optional[dict]:
        return self._tasks.get(task_id, default)

    def task_ids(self):
        return list(self._tasks)

    def cancel(self, task_id) -> bool:
        task_info = self._tasks[task_id]
        task_info['callers'] -= 1
        if task_info['callers'] > 0:
            return False

        cancelled = self._tasks[task_id].cancel()
        if cancelled:
            del self._tasks[task_id]
        return cancelled

    def remove(self, task_id):
        task_info = self._tasks.get(task_id)
        if task_info is None:
            return
        if not task_info['fut'].done():
            task_info['fut'].cancel()
        del self._tasks[task_id]

    def done(self, task_id: str) -> bool:
        return self._tasks[task_id]['fut'].done()

    def result(self, task_id: str):
        task_info = self._tasks[task_id]
        if task_info['fut'].done():
            task_info['callers'] -= 1
            if task_info['callers'] < 1:
                del self._tasks[task_id]
            return task_info['fut'].result()

        if isinstance(task_info['fut'], concurrent.futures.Future):
            raise concurrent.futures.InvalidStateError('task is not done yet')
        else:
            raise asyncio.InvalidStateError('task is not done yet')

    def exception(self, task_id: str):
        task_info = self._tasks[task_id]
        if task_info['fut'].done():
            task_info['callers'] -= 1
            if task_info['callers'] < 1:
                del self._tasks[task_id]
            return task_info['fut'].exception()
            # This returns `None` if no exception was raised.

        if isinstance(task_info['fut'], concurrent.futures.Future):
            raise concurrent.futures.InvalidStateError('task is not done yet')
        else:
            raise asyncio.InvalidStateError('task is not done yet')
