import concurrent.futures
import functools
import multiprocessing
import queue
import threading
from abc import ABC, abstractmethod
from collections.abc import Hashable
from datetime import datetime
from typing import Optional, Union, Dict

from ._streamer import MAX_THREADS


class Task:
    # Objects of this class are created by `BackgroundTask`.

    _NOTSET_ = object()

    def __init__(self,
                 task_id: Hashable,
                 future: concurrent.futures.Future,
                 callers: int,
                 cancelled: threading.Event,
                 status: Union[queue.Queue, multiprocessing.Queue],
                 task_catalog: dict):
        self.task_id = task_id
        self.future = future
        self.callers = callers
        self._cancelled = cancelled
        self._status = status
        self._task_catalog = task_catalog
        self._result_retrieved = 0

    def __del__(self):
        if not self.done():
            self.cancel()

    def status(self):
        s = None
        while True:
            # Get the last entry in the queue.
            try:
                s = self._status.get_nowait()
            except queue.Empty:
                break
        return s

    def cancel(self) -> bool:
        self.callers -= 1
        if self.callers > 0:
            return False
        self._cancelled.set()
        return self.future.cancel()

    def done(self) -> bool:
        return self.future.done()

    def cancelled(self) -> bool:
        return self.future.cancelled()

    def result(self):
        result = self.future.result()
        # If result is not available, hence the above
        # raises exception, the following will not
        # happen.

        self._result_retrieved += 1
        if self._result_retrieved >= self.callers:
            # Remove from the manager if very stakeholder
            # has retrieved the result.
            del self._task_catalog[self.task_id]
        return result

    def exception(self):
        return self.future.exception()
        # This returns `None` if no exception was raised.


class BackgroundTask(ABC):
    '''
    `BackgroundTask` provides a mechanism to manager "tasks" run in a
    thread pool or process pool. Given a user-specified way to determine
    a task based on the task parameters, a repeat submission of an
    existing task (i.e. a task with the same ID) will simply get
    access to the existing task rather than making a new submission.

    Facilities are provided to check task status, and cancel an ongoing task. 
    '''

    def __init__(self, executor: Optional[concurrent.futures.Executor] = None):
        self._own_executor = False
        if executor is None:
            executor = concurrent.futures.ThreadPoolExecutor(MAX_THREADS)
            self._own_executor = True
        self._executor = executor
        self._tasks: Dict[Hashable, Task] = {}

    def __del__(self):
        if self._own_executor:
            self._executor.shutdown()

    @classmethod
    @abstractmethod
    def run(cls,
            *args,
            _cancelled: threading.Event,
            _status: Union[queue.Queue, multiprocessing.Queue],
            **kwargs):
        '''
        This method contains the operations of the user task.
        It runs in `self._executor`.

        In order to make sure these operations do not keep state
        in an instance of this class, it is a `classmethod`.

        Subclass implementation may want to make the parameter listing
        more specific.

        The parameters `_cancelled` and `_status` are provided by
        the background-task mechanism rather than the user task itself,
        but are to be used by the user task.

        If the task tends to take
        long, and wants to support cancellation, then it should check
        `_cancelled.is_set()` periodically, and stops if the flag is set.

        `_status` is used to report progress periodically. Because
        there is no guarantee that these reports are retrieved timely
        or at all, user code should ensure this queue contains only
        one single element, that is, before pushing an element onto
        the queue, it should pop any existing elements. A report is
        typically a small dict.

        It is not mandatory that the user task code makes use of
        `_cancelled` and `_status`.
        '''
        raise NotImplementedError

    @classmethod
    def get_task_id(cls, *args, **kwargs) -> Hashable:
        '''
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
        of `run`, minus `_cancelled` and `_status`.

        The default implementation retruns a random value.
        '''
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
                    task.future._done_callbacks.remove(
                        self._cb_remove_once_done)
                except ValueError:
                    pass
            return task

        if isinstance(self._executor, concurrent.futures.ThreadPoolExecutor):
            cancelled = threading.Event()
            status = queue.Queue(1)
        else:
            cancelled = multiprocessing.Event()
            status = multiprocessing.Queue(1)

        func = functools.partial(self.run, *args, **kwargs)
        fut = self._executor.submit(func, _cancelled=cancelled, _status=status)
        fut.task_id = task_id
        task = Task(
            task_id=task_id,
            future=fut,
            callers=1,
            cancelled=cancelled,
            status=status,
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
