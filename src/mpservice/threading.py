from __future__ import annotations

import concurrent.futures
import multiprocessing
import multiprocessing.connection
import multiprocessing.context
import multiprocessing.queues
import multiprocessing.util
import os
import sys
import threading
import traceback
import warnings
import weakref
from typing import Optional


# Overhead of Thread:
# sequentially creating/running/joining
# threads with a trivial worker function:
#   20000 took 1 sec.


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


def _loud_thread_function(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        print(
            f"Exception in process '{multiprocessing.current_process().name}' thread '{threading.current_thread().name}':",
            file=sys.stderr,
        )
        traceback.print_exception(*sys.exc_info())
        raise
        # https://stackoverflow.com/a/54295910


class ThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    """
    This class is a drop-in replacement of the standard
    `concurrent.futures.ThreadPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_.
    The parameter ``loud_exception`` controls whether to print out exception
    info if the submitted worker task fails with an exception.
    The default is ``True``, whereas ``False`` has the behavior of the standard library,
    which does not print exception info in the worker thread.
    """

    def submit(self, fn, /, *args, loud_exception: bool = True, **kwargs):
        if loud_exception:
            return super().submit(_loud_thread_function, fn, *args, **kwargs)
        return super().submit(fn, *args, **kwargs)


# References
#  https://thorstenball.com/blog/2014/10/13/why-threads-cant-fork/
_global_thread_pools_: dict[str, ThreadPoolExecutor] = weakref.WeakValueDictionary()
_global_thread_pools_lock: threading.Lock = threading.Lock()


def get_shared_thread_pool(
    name: str = "default", max_workers: Optional[int] = None
) -> ThreadPoolExecutor:
    """
    Get a globally shared "thread pool", that is,
    `concurrent.futures.ThreadPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_.

    This is often called with no argument, leaving both ``name`` and ``max_workers`` at their default.

    The default value of ``name`` is "default".
    Different values of ``name`` refer to independent executors.

    The default value of ``max_workers`` is the default value of ThreadPoolExecutor.
    (Since Python 3.8, it is ``min(32, (os.cpu_count() or 1) + 4)``.)
    If an executor with the requested ``name`` does not exist, it will be created
    with the specified ``max_workers`` argument (or using default if not specified).
    However, if the name is "default", the internal default size is always used; user specified ``max_workers``,
    if any, will be ignored.

    If the named executor exists, it will be returned, and ``max_workers`` will be ignored.

    User should assign the returned executor to a variable and keep the variable in scope
    as long as the executor is needed.
    Once all user references to a named executor (including the default one named "default")
    have been garbage collected, the executor is gone. When it is requested again,
    it will be created again.

    User should not call ``shutdown`` on the returned executor.

    This function is thread-safe, meaning it can be called safely in multiple threads with different
    or the same ``name``.
    """
    with _global_thread_pools_lock:
        executor = _global_thread_pools_.get(name)
        # If the named pool exists, it is returned; the input `max_workers` is ignored.
        if executor is None or executor._shutdown:
            # `executor._shutdown` is True if user inadvertently called `shutdown` on the executor.
            if name == "default":
                if max_workers is not None:
                    warnings.warn(
                        f"size of the 'default' thread pool is determined internally; the input {max_workers} is ignored"
                    )
                    max_workers = None
            else:
                if max_workers is not None:
                    assert 1 <= max_workers <= 64
            executor = ThreadPoolExecutor(max_workers)
            _global_thread_pools_[name] = executor
    return executor


if hasattr(os, 'register_at_fork'):  # not available on Windows

    def _clear_global_state():
        box = _global_thread_pools_
        for name in list(box.keys()):
            pool = box.get(name)
            if pool is not None:
                # TODO: if `pool` has locks, are there problems?
                pool.shutdown(wait=False)
            box.pop(name, None)
        global _global_thread_pools_lock
        try:
            _global_thread_pools_lock.release()
        except RuntimeError:  # 'release unlocked lock'
            pass
        _global_thread_pools_lock = threading.Lock()

    os.register_at_fork(after_in_child=_clear_global_state)
