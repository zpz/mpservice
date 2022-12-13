from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import multiprocessing
import queue
import threading
import weakref
from abc import ABC, abstractmethod
from collections.abc import Hashable
from datetime import datetime
from typing import Optional

from .util import MAX_THREADS


class Task:
    """Objects of this class are created by ``BackgroundTask``."""

    _NOTSET_ = object()

    def __init__(
        self,
        task_id: Hashable,
        future: concurrent.futures.Future,
        callers: int,
        cancelled: threading.Event,
        info: queue.Queue | multiprocessing.queues.Queue,
        task_catalog: dict,
    ):
        self.task_id = task_id
        self.future = future
        self.callers = callers
        self._cancelled = cancelled
        self._info = info
        self._info_ = None
        self._task_catalog = task_catalog
        self._result_retrieved = 0

    def __del__(self):
        if not self.done() and not self.cancelled():
            self.cancel()

    def info(self):
        while True:
            # Get the last entry in the queue.
            try:
                self._info_ = self._info.get_nowait()
            except queue.Empty:
                break
        return self._info_

    def cancel(self, wait: bool = True) -> bool:
        if self.cancelled():
            return True

        self.callers -= 1
        if self.callers > 0:
            return False

        if self.future.done():
            return False

        self._cancelled.set()
        z = self.future.cancel()
        if not z:
            # Task has started execution, hence
            # `Future.cancel` can do nothing to stop it.
            # Now it relies on the user code in `BackgroundTask.run`
            # to check `_cancelled` and stop execution proactively.
            if wait:
                concurrent.futures.wait([self.future])
        del self._task_catalog[self.task_id]
        return True

    async def a_cancel(self, wait: bool = True) -> bool:
        z = self.cancel(False)
        if z and wait:
            while not self.future.done():
                await asyncio.sleep(0.0123)
        return z

    def done(self) -> bool:
        return self.future.done()

    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def result(self, timeout=None):
        if self.cancelled():
            raise concurrent.futures.CancelledError()

        err = None
        try:
            result = self.future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise
        except Exception as e:
            err = e

        self._result_retrieved += 1
        if self._result_retrieved >= self.callers:
            # Remove from the manager if every stakeholder
            # has retrieved the result.
            try:
                del self._task_catalog[self.task_id]
            except KeyError:
                pass
        if err is not None:
            raise err
        return result

    async def a_result(self, timeout=None):
        try:
            return self.result(timeout=0)
        except concurrent.futures.TimeoutError:
            if timeout == 0:
                raise
            if timeout is None:
                timeout = 3600 * 24
            while not self.future.done():
                if timeout <= 0:
                    raise concurrent.futures.TimeoutError()
                await asyncio.sleep(0.0123)
                timeout -= 0.0123

            self._result_retrieved += 1
            if self._result_retrieved >= self.callers:
                # Remove from the manager if every stakeholder
                # has retrieved the result.
                try:
                    del self._task_catalog[self.task_id]
                except KeyError:
                    pass
            return self.future.result(0)

    def __await__(self):
        return self.a_result().__await__()

    def exception(self):
        """
        Returns ``None`` if task is not yet completed
        or has completed successfully.

        Raises ``CancelledError`` if the task was cancelled.

        If the task raised an exception, this method will
        raise the same exception.
        """
        if self.cancelled():
            raise concurrent.futures.CancelledError()

        try:
            return self.future.exception(timeout=0)
            # This returns `None` if no exception was raised.
        except concurrent.futures.TimeoutError:
            return None


class BackgroundTask(ABC):
    """
    ``BackgroundTask`` provides a mechanism to manager "tasks" run in a
    thread pool or process pool. Given a user-specified way to determine
    a task based on the task parameters, a repeat submission of an
    existing task (i.e. a task with the same ID) will simply get
    access to the existing task rather than making a new submission.

    Facilities are provided to check task status, and cancel an ongoing task.
    """

    def __init__(self, executor: Optional[concurrent.futures.Executor] = None):
        """
        Parameters
        ----------
        executor
            If you provide your own *process* executor,
            it's recommended to use ``mpservice.util.MP_SPAWN_CTX`` for its
            ``ctx`` parameter.
        """
        self._own_executor = False
        if executor is None:
            executor = concurrent.futures.ThreadPoolExecutor(MAX_THREADS)
            self._own_executor = True
        self._executor = executor
        self._tasks: dict[Hashable, Task] = {}

        if self._own_executor:
            weakref.finalize(self, type(self)._shutdown_executor, executor)

    @staticmethod
    def _shutdown_executor(executor):
        executor.shutdown()

    @classmethod
    @abstractmethod
    def run(
        cls,
        *args,
        _cancelled: threading.Event,
        _info: queue.Queue | multiprocessing.Queue,
        **kwargs,
    ):
        """
        This method contains the operations of the user task.
        It runs in ``self._executor``.

        In order to make sure these operations do not keep state
        in an instance of this class, it is a classmethod.

        Subclass implementation may want to make the parameter listing
        more specific.

        The parameters ``_cancelled`` and ``_info`` are provided by
        the background-task mechanism rather than the user task itself,
        but are to be used by the user task.

        If the task tends to take long, and wants to support cancellation,
        then it should check ``_cancelled.is_set()`` periodically (preferrably
        rather frequently), and stops if the flag is set.
        Note that ``concurrent.futures.Future.cancel``
        can only cancel the execution of a scheduled task if it has not started
        running yet. Once the task is ongoing, ``Future.cancel`` can't stop
        the execution. Therefore user code needs to be proactive, using
        ``_cancelled`` to detect the cancellation request and act accordingly.

        The queue ``_info`` is used to pass info to the caller, e.g. periodic
        progress reports. Because there is no guarantee that this queue
        is being checked by the caller timely or at all,
        we can't let it have unlimited capacity and keep elements in it.
        In fact, it has length 1. Before pushing an element onto
        the queue, the user code should pop any existing elements.
        An element in this queue is typically a small dict.

        It is not mandatory that the user task code makes use of
        ``_cancelled`` and ``_info``.
        """
        raise NotImplementedError

    @classmethod
    def get_task_id(cls, *args, **kwargs) -> Hashable:
        """
        Determine an ID for the task based on the input parameters.
        Next time when a task is submitted, if its task ID
        turns out to be identical to an in-progress task,
        it will not be re-submitted; rather, it will just
        wait for the result of the in-progress task.

        There are some caveats in determination of this task ID.
        For example, if one of the input parameter is a large list,
        do we use the full content of the list to determine the ID?

        For another example, if the task takes the path of an input
        file, and the path is hard-coded and never changes, then
        this parameter value never changes; but does this mean
        the content of the file does not change?
        If we assume the file content could change anytime,
        we may consider each submission to constitute a unique task.
        Then, simply return a random task ID.

        The parameter list should be identical to that
        of ``run``, minus ``_cancelled`` and ``_status``.

        The default implementation returns a random value.
        """
        return str(datetime.utcnow())

    def submit(self, *args, **kwargs) -> Task:
        task_id = self.get_task_id(*args, **kwargs)
        task = self._tasks.get(task_id)
        if task is not None:
            # The same task was submitted before.
            task.callers += 1
            if task.callers == 1:
                # This task was as "submit_and_forget".
                # Now that another user is submitting
                # the same task, and we don't know whether
                # this user also wants to "forget", we
                # need to remove the "forget" callback.
                try:
                    task.future._done_callbacks.remove(self._cb_remove_once_done)
                except ValueError:
                    pass
            return task

        if isinstance(self._executor, concurrent.futures.ThreadPoolExecutor):
            cancelled = threading.Event()
            info = queue.Queue(1)
        else:
            cancelled = self._executor._mp_context.Manager().Event()
            info = self._executor._mp_context.Manager().Queue(1)

        func = functools.partial(self.run, *args, **kwargs)
        fut = self._executor.submit(func, _cancelled=cancelled, _info=info)
        fut.task_id = task_id
        task = Task(
            task_id=task_id,
            future=fut,
            callers=1,
            cancelled=cancelled,
            info=info,
            task_catalog=self._tasks,
        )
        self._tasks[task_id] = task
        return task

    def _cb_remove_once_done(self, fut) -> None:
        del self._tasks[fut.task_id]

    def submit_and_forget(self, *args, **kwargs) -> None:
        task = self.submit(*args, **kwargs)
        if task.callers == 1:
            task.future.add_done_callback(self._cb_remove_once_done)
        task.callers -= 1

    def __contains__(self, task_id: str) -> bool:
        return task_id in self._tasks

    def __getitem__(self, task_id: str) -> Task:
        return self._tasks[task_id]

    def get(self, task_id: str, default=None) -> Optional[Task]:
        return self._tasks.get(task_id, default)

    def __iter__(self):
        return iter(self._tasks)

    def keys(self):
        return self._tasks.keys()

    def values(self):
        return self._tasks.values()

    def items(self):
        return self._tasks.items()

    def __delitem__(self, task_id):
        del self._tasks[task_id]
