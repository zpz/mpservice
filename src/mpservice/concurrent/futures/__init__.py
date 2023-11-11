from __future__ import annotations

import concurrent.futures
import multiprocessing
import os
import sys
import threading
import traceback
import weakref

from mpservice.multiprocessing import MP_SPAWN_CTX

wait = concurrent.futures.wait
as_completed = concurrent.futures.as_completed
ALL_COMPLETED = concurrent.futures.ALL_COMPLETED
FIRST_COMPLETED = concurrent.futures.FIRST_COMPLETED
FIRST_EXCEPTION = concurrent.futures.FIRST_EXCEPTION


__all__ = [
    'ThreadPoolExecutor',
    'ProcessPoolExecutor',
    'wait',
    'as_completed',
    'ALL_COMPLETED',
    'FIRST_COMPLETED',
    'FIRST_EXCEPTION',
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
    """
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
    """

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
    name: str, max_workers: int | None = None
) -> ThreadPoolExecutor:
    """
    Get a globally shared "thread pool", that is,
    `concurrent.futures.ThreadPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_.

    If an executor with the requested ``name`` does not exist, it will be created
    with the specified ``max_workers`` argument (or using default if not specified).

    If the named executor exists, it will be returned. However, if ``max_workers`` is specified
    but mismatches the "max workers" of the existing executor, ``ValueError`` is raised.

    User should assign the returned executor to a variable and keep the variable in scope
    as long as the executor is needed.
    Once all user references to a named executor have been garbage collected, the executor is gone.
    When it is requested again, it will be created again.

    User should not call ``shutdown`` on the returned executor, because it is *shared* with other
    users.

    This function is thread-safe, meaning it can be called safely in multiple threads with different
    or the same ``name``.

    Example use case: an instance of a class needs to start and use a ThreadPoolExecutor; user may
    have many such instances live at the same time although they are unlikely to use the ThreadPoolExecutor
    at the same time; to avoid having too many threads open, the class may choose to use a "shared"
    thread pool between the instances.
    """
    assert name
    with _global_thread_pools_lock:
        executor = _global_thread_pools_.get(name)
        if executor is None or executor._shutdown:
            # `executor._shutdown` is True if user inadvertently called `shutdown` on the executor.
            executor = ThreadPoolExecutor(max_workers)
            _global_thread_pools_[name] = executor
        else:
            if max_workers is not None and max_workers != executor._max_workers:
                raise ValueError(
                    f'`max_workers`, {max_workers}, mismatches the existing value, {executor._max_workers}'
                )
    return executor


def get_shared_process_pool(name: str, max_workers: int = None) -> ProcessPoolExecutor:
    """
    Get a globally shared "process pool", that is,
    `concurrent.futures.ProcessPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor>`_.

    Analogous to :func:`get_shared_thread_pool`.
    """
    assert name
    with _global_process_pools_lock:
        executor = _global_process_pools_.get(name)
        if executor is None or executor._processes is None:
            # `executor._processes` is None if user inadvertently called `shutdown` on the executor.
            executor = ProcessPoolExecutor(max_workers)
            _global_process_pools_[name] = executor
        else:
            if max_workers is not None and max_workers != executor._max_workers:
                raise ValueError(
                    f'`max_workers`, {max_workers}, mismatches the existing value, {executor._max_workers}'
                )
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
