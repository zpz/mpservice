from __future__ import annotations

__all__ = [
    'InvalidStateError',
    'Thread',
    'FIRST_COMPLETED',
    'FIRST_EXCEPTION',
    'ALL_COMPLETED',
    'wait',
    'as_completed',
]

import concurrent.futures
import ctypes
import threading
import traceback
from collections.abc import Iterator, Sequence
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, FIRST_EXCEPTION
from typing import Type

# Overhead of Thread:
# sequentially creating/running/joining
# threads with a trivial worker function:
#   20000 took 1 sec.
# https://stackoverflow.com/questions/36484151/throw-an-exception-into-another-thread
from .._common import TimeoutError


class InvalidStateError(RuntimeError):
    pass


class Thread(threading.Thread):
    """
    A subclass of the standard ``threading.Thread``,
    this class makes the result or exception produced in a thread
    accessible from the thread object itself. This makes the ``Thread``
    object's behavior similar to the ``Future`` object returned
    by ``concurrent.futures.ThreadPoolExecutor.submit``.

    .. seealso:: :class:`mpservice.multiprocessing.SpawnProcess`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._future_: concurrent.futures.Future = None

    @staticmethod
    def handle_exception(exc):
        # Subclass can customize this to log more info,
        # however, usually you should do that when calling `.join`.
        # This method should not raise exceptions.
        print(f'Exception in {threading.current_thread().name}: {repr(exc)}')

    def run(self):
        """
        This method represents the thread's activity.
        """
        self._future_ = concurrent.futures.Future()
        try:
            if self._target is not None:
                z = self._target(*self._args, **self._kwargs)
                self._future_.set_result(z)
            else:
                self._future_.set_result(None)
        except SystemExit as e:
            # TODO: what if the code has created other threads?
            if e.code is None:
                self._future_.set_result(None)
            else:
                if isinstance(e.code, int):
                    if e.code == 0:
                        self._future_.set_result(None)
                    else:
                        self.handle_exception(e)
                        self._future_.set_exception(e)
                else:
                    self.handle_exception(e)
                    self._future_.set_exception(e)
        except BaseException as e:
            self.handle_exception(e)

            tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            tb = f'[{threading.current_thread().name}] ' + tb
            e.__cause__ = type(e)(tb)
            e.__traceback__ = None

            self._future_.set_exception(e)
            # Sometimes somehow error is not visible (maybe it's a `pytest` issue?).

            if self._daemonic:
                traceback.print_exc()
            # In non-daemonic thread, let user control printing of exception
            # when they call `join`.
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs

    def join(self, timeout=None):
        """
        Same behavior as the standard lib, except that if the thread
        terminates with an exception, the exception is raised.
        """
        super().join(timeout=timeout)
        if self.is_alive():
            # Timed out
            return
        if self._future_.exception():
            raise self._future_.exception()

    def done(self) -> bool:
        """
        Return ``True`` if the thread has terminated.
        Return ``False`` if the thread is running or not yet started.
        """
        if self.is_alive():
            return False
        return self._started.is_set()
        # Otherwise, not started yet.

    def result(self, timeout=None):
        """
        Behavior is similar to ``concurrent.futures.Future.result``.
        """
        super().join(timeout)
        if self.is_alive():
            raise TimeoutError
        return self._future_.result()

    def exception(self, timeout=None):
        """
        Behavior is similar to ``concurrent.futures.Future.exception``.
        """
        super().join(timeout)
        if self.is_alive():
            raise TimeoutError
        return self._future_.exception()

    def throw(self, exc: BaseException | Type[BaseException]) -> None:
        """
        Raise exception ``exc`` inside the thread.

        Do not use this unless you know what you're doing.
        A main intended user is :meth:`terminate`.
        """
        # References:
        #  https://gist.github.com/liuw/2407154
        #  https://docs.python.org/3/c-api/init.html#c.PyThreadState_SetAsyncExc
        #  https://stackoverflow.com/a/65090035/6178706

        if not self.is_alive():
            raise InvalidStateError('The thread is not running')

        tid = self.ident
        ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(tid), ctypes.py_object(exc)
        )
        if ret == 0:
            raise ValueError(f'Invalid thread ID {tid}')
        elif ret > 1:
            # Huh? Why would we notify more than one threads?
            # Because we punch a hole into C level interpreter.
            # So it is better to clean up the mess.
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
            raise SystemError('PyThreadState_SetAsyncExc failed')

    def terminate(self) -> None:
        """
        There are many pitfalls in terminating a thread from outside.
        Use extra caution if the thread code is complex, e.g. if it creates
        more threads.
        """
        # There's a chance that the exception is missed in the thread.
        # I read about it but now can't find the reference.
        while self.is_alive():
            self.throw(SystemExit)
            super().join(0.01)


def wait(
    threads: Sequence[Thread], timeout=None, return_when=ALL_COMPLETED
) -> tuple[set[Thread], set[Thread]]:
    """See ``concurrent.futures.wait``."""

    futures = [t._future_ for t in threads]
    future_to_thread = {id(t._future_): t for t in threads}
    done, not_done = concurrent.futures.wait(
        futures, timeout=timeout, return_when=return_when.upper()
    )
    if done:
        done = set(future_to_thread[id(f)] for f in done)
    if not_done:
        not_done = set(future_to_thread[id(f)] for f in not_done)
    return done, not_done


def as_completed(threads: Sequence[Thread], timeout=None) -> Iterator[Thread]:
    """See ``concurrent.futures.as_completed``."""

    futures = [t._future_ for t in threads]
    future_to_thread = {id(t._future_): t for t in threads}
    for f in concurrent.futures.as_completed(futures, timeout=timeout):
        yield future_to_thread[id(f)]
