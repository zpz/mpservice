from __future__ import annotations
import errno
import functools
import inspect
import logging
import logging.handlers
import multiprocessing
import multiprocessing.connection
import multiprocessing.queues
import multiprocessing.context
import os
import subprocess
import threading
import traceback
from multiprocessing.util import Finalize
from types import TracebackType
from typing import Optional


MAX_THREADS = min(32, (os.cpu_count() or 1) + 4)
# This default is suitable for I/O bound operations.
# This value is what is used by `concurrent.futures.ThreadPoolExecutor`.
# For others, user may want to specify a smaller value.


TimeoutError = TimeoutError  # make local alias for the standard exception
# `concurrent.futures._base` does this.
# I guess the point is to ask user to import this class to check, instead of checking
# against the builtin class directly. This prepares for possible customization
# later, although currently it just re-uses the builtin one.


class _SpawnContext:
    # We want to use `SpawnProcess` as the process class when
    # the creation method is 'spawn'.
    # However, because `multiprocessing.get_context('spawn')`
    # is a global var, we shouldn't directly change its
    # `.Process` attribute like this:
    #
    #   ctx = multiprocessing.get_context('spawn')
    #   ctx.Process = SpawnProcess
    #
    # It would change the behavior of the spawn context in code
    # outside of our own.
    # To achieve the goal in a controlled way, we designed this class.

    def __init__(self):
        self._ctx = multiprocessing.get_context("spawn")

    def __getattr__(self, name):

        if name == "Process":
            return SpawnProcess
        return getattr(self._ctx, name)


MP_SPAWN_CTX = _SpawnContext()
"""
Use this as the ``mp_context`` or ``ctx`` arguments to various multiprocessing
functions.
"""


def get_docker_host_ip():
    """
    From within a Docker container, this function finds the IP address
    of the host machine.
    """
    # INTERNAL_HOST_IP=$(ip route show default | awk '/default/ {print $3})')
    # another idea:
    # ip -4 route list match 0/0 | cut -d' ' -f3
    #
    # Usually the result is '172.17.0.1'
    #
    # The command `ip` is provided by the Linux package `iproute2`.

    z = subprocess.check_output(["ip", "-4", "route", "list", "match", "0/0"])
    z = z.decode()[len("default via ") :]
    return z[: z.find(" ")]


def is_exception(e):
    # TODO: test showed the raised objects are always instances, not classes, even
    # if we do
    #   raise ValueError
    # the captured object is a ValueError instance, not the class.
    return isinstance(e, BaseException) or (
        (type(e) is type) and issubclass(e, BaseException)
    )


def is_async(func):
    while isinstance(func, functools.partial):
        func = func.func
    return inspect.iscoroutinefunction(func) or (
        not inspect.isfunction(func)
        and hasattr(func, "__call__")
        and inspect.iscoroutinefunction(func.__call__)
    )


def full_class_name(cls):
    if not isinstance(cls, type):
        cls = cls.__class__
    mod = cls.__module__
    if mod is None or mod == "builtins":
        return cls.__name__
    return mod + "." + cls.__name__


def is_remote_exception(e) -> bool:
    return isinstance(e, BaseException) and isinstance(e.__cause__, RemoteTraceback)


def get_remote_traceback(e) -> str:
    # Assuming `e` has passed the check of `is_remote_exception`.
    return e.__cause__.tb


class RemoteTraceback(Exception):
    def __init__(self, tb: str):
        self.tb = tb

    def __str__(self):
        return self.tb


def rebuild_exception(exc: BaseException, tb: str):
    exc.__cause__ = RemoteTraceback(tb)

    return exc


class RemoteException:
    """
    This class is a pickle helper for Exception objects to preserve some traceback info.
    This is needed because directly calling ``pickle.dumps`` on an Exception object will lose
    its ``__traceback__`` and ``__cause__`` attributes.

    One typical use case is to send an exception object across process boundaries,
    hence undergoing pickling/unpickling.

    This class preserves some traceback info simply by keeping it as a formatted string.
    Once unpickled (e.g. in another process), the object obtained, say ``obj``,
    is not an instance of ``RemoteException``, but rather of the original exception type.
    The object does not have ``__traceback__`` attribute, which is impossible to reconstruct,
    but rather has ``__cause__`` attribute, which is a custom Exception
    object (``RemoteTraceback``) that contains the string-form traceback info.
    The traceback string is ``obj.__cause__.traceback``. This string contains proper linebreaks.

    Most (probably all) methods and attributes of the unpickled exception object behave the same
    as the original, exception for ``__traceback__`` and ``__cause__``.

    Again, the unpickled object, say ``obj``, is not an instance of this class, hence
    we can't define methods on this class to help using `obj`.

    Note that ``RemoteException`` does not subclass ``BaseException``, so you can't "raise" this object.

    See also: ``is_remote_exception``, ``get_remote_traceback``.
    """

    # This takes the idea of `concurrent.futures.process._ExceptionWithTraceback`
    # with slightly tweaked traceback printout.
    # `pebble.common` uses the same idea.

    # check out
    #   https://github.com/ionelmc/python-tblib
    #   https://stackoverflow.com/questions/6126007/python-getting-a-traceback-from-a-multiprocessing-process
    # about pickling Exceptions with tracebacks.
    #
    # See also: boltons.tbutils
    # See also: package `eliot`: https://github.com/itamarst/eliot/blob/master/eliot/_traceback.py
    #
    # Also check out `sys.excepthook`.

    def __init__(self, exc: Exception, tb: Optional[TracebackType | str] = None):
        if isinstance(tb, str):
            pass
        elif isinstance(tb, TracebackType):
            tb = "".join(traceback.format_exception(type(exc), exc, tb))
        else:
            assert tb is None
            if exc.__traceback__ is not None:
                # This is the most common use case---in an exception handler block:
                #
                #   try:
                #       ...
                #   except Exception as e:
                #       ...
                #       ee = RemoteException(e)
                #       ...
                #
                # This includes the case where `e` has come from another process via `RemoteException`
                # (hence `is_remote_exception(e)` is True) and is raised again, and because
                # we intend to pickle it again (e.g. paassing it to another process via a queue),
                # hence we put it in `RemoteException`.
                tb = "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                )
            else:
                # This use case is not in an "except" block. Somehow there's an
                # exception object and we need to pickle it, so we put it in a
                # `RemoteException`.
                if is_remote_exception(exc):
                    tb = get_remote_traceback(exc)
                else:
                    raise ValueError(f"{repr(exc)} does not contain traceback info")
                    # In this case, don't use RemoteException. Pickle the exc object directly.

        self.exc = exc
        self.tb = tb

    def __reduce__(self):
        return rebuild_exception, (self.exc, self.tb)


class Thread(threading.Thread):
    """
    This class makes the result or exception produced in a thread
    accessible from the thread object itself. This makes the ``Thread``
    object's behavior somewhat similar to the ``Future`` object returned
    by ``concurrent.futures.ThreadPoolExecutor.submit``.
    """

    def __init__(self, *args, loud_exception: bool = True, **kwargs):
        """
        ``loud_exception``: if ``True``, it's the standard ``Thread`` behavior;
        if ``False``, it's the ``concurrent.futures`` behavior.
        """
        super().__init__(*args, **kwargs)
        self._result_ = None
        self._exception_ = None
        self._loud_exception_ = loud_exception

    def run(self):
        """
        This method represents the thread's activity.
        """
        try:
            if self._target is not None:
                self._result_ = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self._exception_ = e
            if self._loud_exception_:
                raise
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs

    def done(self) -> bool:
        if self.is_alive():
            return False
        return self._started.is_set()
        # Otherwise, not started yet.

    def result(self, timeout=None):
        self.join(timeout)
        if self.is_alive():
            raise TimeoutError
        if self._exception_ is not None:
            raise self._exception_
        return self._result_

    def exception(self, timeout=None):
        self.join(timeout)
        if self.is_alive():
            raise TimeoutError
        return self._exception_


class SpawnProcess(multiprocessing.context.SpawnProcess):
    """
    This customization adds two things to the standard class:

    - make result and exception available as attributes of the
      process object, similar to ``concurrent.futures.Future``.
    - make logs in the subprocess handled in the main process.
    """

    def __init__(self, *args, kwargs=None, loud_exception: bool = True, **moreargs):
        """
        ``loud_exception``: if True, it's the standard ``Process`` behavior;
        if False, it's the ``concurrent.futures`` behavior.
        """
        if kwargs is None:
            kwargs = {}
        else:
            kwargs = dict(kwargs)

        assert "__result_and_error__" not in kwargs
        reader, writer = multiprocessing.connection.Pipe(duplex=False)
        kwargs["__result_and_error__"] = writer

        assert "__worker_logger__" not in kwargs
        worker_logger = ProcessLogger()
        worker_logger.start()
        kwargs["__worker_logger__"] = worker_logger

        assert "__loud_exception__" not in kwargs
        kwargs["__loud_exception__"] = loud_exception

        super().__init__(*args, kwargs=kwargs, **moreargs)

        assert not hasattr(self, "__result_and_error__")
        self.__result_and_error__ = reader

        assert not hasattr(self, "__worker_logger__")
        self.__worker_logger__ = worker_logger

    def run(self):
        # This runs in a child process.
        worker_logger = self._kwargs.pop("__worker_logger__")
        worker_logger.start()
        result_and_error = self._kwargs.pop("__result_and_error__")
        # Upon completion, `result_and_error` will contain `result` and `exception`
        # in this order; both may be `None`.
        loud_exception = self._kwargs.pop("__loud_exception__")
        if self._target:
            try:
                z = self._target(*self._args, **self._kwargs)
            except BaseException as e:
                result_and_error.send(None)
                result_and_error.send(RemoteException(e))
                if loud_exception:
                    raise
            else:
                result_and_error.send(z)
                result_and_error.send(None)
        else:
            result_and_error.send(None)
            result_and_error.send(None)

    def done(self) -> bool:
        return self.exitcode is not None

    def result(self, timeout=None):
        self.join(timeout)
        if not self.done():
            raise TimeoutError
        if not hasattr(self, "__result__"):
            try:
                self.__result__ = self.__result_and_error__.recv()
                self.__error__ = self.__result_and_error__.recv()
            except EOFError as e:
                exitcode = self.exitcode
                if exitcode:
                    raise e from ChildProcessError(
                        f"exitcode {exitcode}, {errno.errorcode[exitcode]}"
                    )
                raise
        if self.__error__ is not None:
            raise self.__error__
        return self.__result__

    def exception(self, timeout=None):
        self.join(timeout)
        if not self.done():
            raise TimeoutError
        if not hasattr(self, "__result__"):
            try:
                self.__result__ = self.__result_and_error__.recv()
                self.__error__ = self.__result_and_error__.recv()
            except EOFError as e:
                exitcode = self.exitcode
                if exitcode:
                    raise e from ChildProcessError(
                        f"exitcode {exitcode}, {errno.errorcode[exitcode]}"
                    )
                raise
        return self.__error__


class ProcessLogger:
    """
    Logging messages produced in worker processes are tricky.
    First, some settings should be concerned in the main process only,
    including log formatting, log-level control, log handler (destination), etc.
    Specifically, these should be settled in the "launching script", and definitely
    should not be concerned in worker processes.
    Second, the terminal printout of loggings in multiple processes tends to be
    intermingled and mis-ordered.

    This class uses a queue to transmit all logging messages that are produced
    in a worker process to the main process/thread, to be handled there.

    Usage:

    1. In main process, create a ``ProcessLogger`` instance and start it::

            pl = ProcessLogger().start()

    2. Pass this object to other processes. (Yes, this object is pickle-able.)

    3. In the other process, start it. Suppose the object is also called ``pl``,
       then do

       ::

            pl.start()

    Calling ``stop`` in either the main or the child process is optional.
    The call will immediately stop processing logs in the respective process.

    Although user can use this class in their code, they are encouraged to
    use ``SpawnProcess``, which already handles logging via this class.
    """

    def __init__(self, *, ctx: Optional[multiprocessing.context.BaseContext] = None):
        self._ctx = ctx or MP_SPAWN_CTX
        self._t = None

    def __getstate__(self):
        assert self._t is not None
        return self._q

    def __setstate__(self, state):
        # In another process.
        self._q = state

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()

    def start(self):
        if hasattr(self, "_t"):
            self._start_in_main_process()
        else:
            self._start_in_other_process()
        return self

    def stop(self):
        if hasattr(self, "_t"):
            self._stop_in_main_process()
        else:
            self._stop_in_other_process()

    def _start_in_main_process(self):
        assert self._t is None
        self._q = self._ctx.Queue()

        self._t = threading.Thread(
            target=ProcessLogger._logger_thread,
            args=(self._q,),
            name="ProcessLoggerThread",
            daemon=True,
        )
        self._t.start()
        self._finalize = Finalize(
            self,
            type(self)._finalize_logger_thread,
            (self._t, self._q),
            exitpriority=10,
        )

    @staticmethod
    def _logger_thread(q: multiprocessing.queues.Queue):
        while True:
            record = q.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            if record.levelno >= logger.getEffectiveLevel():
                logger.handle(record)

    @staticmethod
    def _finalize_logger_thread(t: threading.Thread, q: multiprocessing.queues.Queue):
        q.put(None)
        t.join()

    def _stop_in_main_process(self):
        # assert self._t is not None
        # self._q.put(None)
        # self._t.join()
        # self._t = None
        self._stopped = True
        finalize = self._finalize
        if finalize:
            self._finalize = None
            finalize()

    def _start_in_other_process(self):
        """
        In a Process (created using the "spawn" method),
        run this function at the beginning to set up putting all log messages
        ever produced in that process into the queue that will be consumed
        in the main process by ``self._logger_thread``.

        During the execution of the process, logging should not be configured.
        Logging config should happen in the main process/thread.
        """
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        self._qh = logging.handlers.QueueHandler(self._q)
        root.addHandler(self._qh)

    def _stop_in_other_process(self):
        if self._q is not None:
            logging.getLogger().removeHandler(self._qh)
            self._q.close()
            self._q = None
