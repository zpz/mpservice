from __future__ import annotations

import concurrent.futures
import multiprocessing
import os
import sys
import threading
import traceback
import warnings
import weakref

from mpservice.multiprocessing import MP_SPAWN_CTX

__all__ = [
    'ThreadPoolExecutor',
    'ProcessPoolExecutor',
    'get_shared_thread_pool',
    'get_shared_process_pool',
]


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


def _loud_process_function(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        print(
            f"Exception in process '{multiprocessing.current_process().name}':",
            file=sys.stderr,
        )
        traceback.print_exception(*sys.exc_info())
        raise


class ProcessPoolExecutor(concurrent.futures.ProcessPoolExecutor):
    '''
    This class is a drop-in replacement of the standard
    `concurrent.futures.ProcessPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor>`_.
    By default, it uses a "spawn" context and the process class :class:`SpawnProcess`.

    In addition, the parameter ``loud_exception`` controls whether to print out exception
    info if the submitted worker task fails with an exception.
    The default is ``True``, whereas ``False`` has the behavior of the standard library,
    which does not print exception info in the worker process.
    Although exception info can be obtained in the caller process via the
    `Future <https://docs.python.org/3/library/concurrent.futures.html#future-objects>`_ object
    returned from the method :meth:`submit`, the printing in the worker process
    is handy for debugging in cases where the user fails to check the Future object in a timely manner.
    '''

    # The loud-ness of this executor is different from the loud-ness of
    # ``SpawnProcess``. In this executor, loudness refers to each submitted function.
    # A process may stay on and execute many submitted functions.
    # The loudness of SpawnProcess plays a role only when that process crashes.

    def __init__(self, max_workers=None, mp_context=None, **kwargs):
        if mp_context is None:
            mp_context = MP_SPAWN_CTX
        super().__init__(max_workers=max_workers, mp_context=mp_context, **kwargs)

    def submit(self, fn, /, *args, loud_exception: bool = True, **kwargs):

        if loud_exception:
            return super().submit(_loud_process_function, fn, *args, **kwargs)
        return super().submit(fn, *args, **kwargs)


# References
#  https://thorstenball.com/blog/2014/10/13/why-threads-cant-fork/
_global_thread_pools_: dict[str, ThreadPoolExecutor] = weakref.WeakValueDictionary()
_global_thread_pools_lock: threading.Lock = threading.Lock()
_global_process_pools_: dict[str, ProcessPoolExecutor] = weakref.WeakValueDictionary()
_global_process_pools_lock: threading.Lock = threading.Lock()


def get_shared_thread_pool(
    name: str = "default", max_workers: int | None = None
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


def get_shared_process_pool(
    name: str = "default", max_workers: int = None
) -> ProcessPoolExecutor:
    """
    Get a globally shared "process pool", that is,
    `concurrent.futures.ProcessPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor>`_.

    Analogous to :func:`get_shared_thread_pool`.
    """
    with _global_process_pools_lock:
        executor = _global_process_pools_.get(name)
        # If the named pool exists, it is returned; the input `max_workers` is ignored.
        if executor is None or executor._processes is None:
            # `executor._processes` is None if user inadvertently called `shutdown` on the executor.
            if name == "default":
                if max_workers is not None:
                    warnings.warn(
                        f"size of the 'default' process pool is determined internally; the input {max_workers} is ignored"
                    )
                    max_workers = None
            else:
                if max_workers is not None:
                    assert 1 <= (os.cpu_count() or 1) * 2
            executor = ProcessPoolExecutor(max_workers)
            _global_process_pools_[name] = executor
    return executor


if hasattr(os, 'register_at_fork'):  # not available on Windows

    def _clear_global_state():
        for box in (_global_thread_pools_, _global_process_pools_):
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

        global _global_process_pools_lock
        try:
            _global_process_pools_lock.release()
        except RuntimeError:
            pass
        _global_process_pools_lock = threading.Lock()

    os.register_at_fork(after_in_child=_clear_global_state)
