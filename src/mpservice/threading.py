from __future__ import annotations

import os
import threading

# Overhead of Thread:
# sequentially creating/running/joining
# threads with a trivial worker function:
#   20000 took 1 sec.


class TimeoutError(Exception):
    pass


MAX_THREADS = min(32, (os.cpu_count() or 1) + 4)
"""
This default is suitable for I/O bound operations.
This value is what is used by `concurrent.futures.ThreadPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_.
For others, you may want to specify a smaller value.
"""


class Thread(threading.Thread):
    """
    A subclass of the standard ``threading.Thread``,
    this class makes the result or exception produced in a thread
    accessible from the thread object itself. This makes the ``Thread``
    object's behavior similar to the ``Future`` object returned
    by ``concurrent.futures.ThreadPoolExecutor.submit``.

    .. seealso:: :class:`SpawnProcess`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._result_ = None
        self._exception_ = None

    def run(self):
        """
        This method represents the thread's activity.
        """
        try:
            if self._target is not None:
                self._result_ = self._target(*self._args, **self._kwargs)
        except SystemExit:
            # TODO: what if `e.code` is not 0?
            raise
        except BaseException as e:
            self._exception_ = e
            raise  # Standard threading will print error info here.
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs

    def join(self, timeout=None):
        '''
        Same behavior as the standard lib, except that if the thread
        terminates with an exception, the exception is raised.
        '''
        super().join(timeout=timeout)
        if self.is_alive():
            # Timed out
            return
        if self._exception_ is not None:
            raise self._exception_

    def done(self) -> bool:
        '''
        Return ``True`` if the thread has terminated.
        Return ``False`` if the thread is running or not yet started.
        '''
        if self.is_alive():
            return False
        return self._started.is_set()
        # Otherwise, not started yet.

    def result(self, timeout=None):
        '''
        Behavior is similar to ``concurrent.futures.Future.result``.
        '''
        super().join(timeout)
        if self.is_alive():
            raise TimeoutError
        if self._exception_ is not None:
            raise self._exception_
        return self._result_

    def exception(self, timeout=None):
        '''
        Behavior is similar to ``concurrent.futures.Future.exception``.
        '''
        super().join(timeout)
        if self.is_alive():
            raise TimeoutError
        return self._exception_
