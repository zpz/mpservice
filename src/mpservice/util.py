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
"""
This default is suitable for I/O bound operations.
This value is what is used by `concurrent.futures.ThreadPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_.
For others, you may want to specify a smaller value.
"""


class TimeoutError(Exception):
    pass


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
`multiprocessing`_ has a "context", which has to do with how a process is created and started.
Multiprocessing objects like ``Process``, ``Queue``, ``Event``, etc., must be created from
the same context in order to work together. For example, if you send a ``Queue`` created out of
the "spawn" context to a ``Process`` created out of the "fork" context, it will not work.

Python's default "process start method" **on Linux** is "fork".
If you do

::

    import multiprocessing
    q = multiprocessing.Queue()

this is equivalent to

::

    q = multiprocessing.get_context().Queue()

`multiprocessing.get_context <https://docs.python.org/3/library/multiprocessing.html#multiprocessing.get_context>`_ takes the sole parameter ``method``,
which on Linux defaults to ``'fork'``.

However, it is advised to not use this default; rather, **always** use the "spawn" context.
There are some references on this topic; for example, see `this article <https://pythonspeed.com/articles/python-multiprocessing/>`_
and `this StackOverflow thread <https://stackoverflow.com/questions/64095876/multiprocessing-fork-vs-spawn>`_.

So, you should write multiprocessing code this way::

    import multiprocessing
    ctx = multiprocessing.get_context('spawn')
    q = ctx.Queue(...)
    e = ctx.Event(...)
    p = ctx.Process(..., args=(q, e))
    ...

The constant ``MP_SPAWN_CTX`` is a replacement of the standard spawn context.
Instead of the above, you are advised to write this::

    from mpservice.util import MP_SPAWN_CTX as ctx
    q = ctx.Queue(...)
    e = ctx.Event(...)
    p = ctx.Process(..., args=(q, e))
    ...

The difference between ``MP_SPAWN_CTX`` and the standard spawn context
is that the former uses the custom :class:`SpawnProcess` class in places of the standard
`Process <https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process>`_.

If you only need to start a process and don't need to create other objects
(like queue or event) from a context, then you can use :class:`SpawnProcess` directly.
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


def is_exception(e) -> bool:
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
    """Return ``True`` if the exception ``e`` was created by :class:`RemoteException`, and ``False`` otherwise."""
    return isinstance(e, BaseException) and isinstance(e.__cause__, RemoteTraceback)


def get_remote_traceback(e) -> str:
    """
    ``e`` must have checked ``True`` by :func:`is_remote_exception`.
    Suppose an Exception object is wrapped by :class:`RemoteException` and sent to another process
    through a queue.
    Taken out of the queue in the other process, the object will check ``True``
    by :func:`is_remote_exception`. Then this function applied on the object
    will return the traceback info as a string.
    """
    return e.__cause__.tb


class RemoteTraceback(Exception):
    """
    This class is used by :class:`RemoteException`.
    End-user does not use this class directly.
    """

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

    This is designed to be used to send an exception object to another process.
    It can also be used to pickle-persist an Exception object for some time.

    This class preserves some traceback info simply by keeping it as a formatted string during pickling.
    Once unpickled, the object obtained, say ``obj``,
    is **not** an instance of ``RemoteException``, but rather of the original Exception type.
    ``obj`` does not have the ``__traceback__`` attribute, which is impossible to reconstruct,
    but rather has the ``__cause__`` attribute, which is a custom Exception
    object (:class:`RemoteTraceback`) that contains the string-form traceback info, with proper line breaks.
    Most (if not all) methods and attributes of ``obj`` behave the same
    as the original Exception object, except that
    ``__traceback__`` is gone but ``__cause__`` is added.

    Because ``obj`` is not an instance of this class,
    we cannot define methods on this class to help using ``obj``.

    Note that ``RemoteException`` does not subclass ``BaseException``, hence you can't "raise" an instance of this class.

    .. seealso:: :func:`is_remote_exception`, :func:`get_remote_traceback`.

    Examples
    --------
    >>> from mpservice.util import RemoteException, is_remote_exception, get_remote_traceback
    >>> import pickle
    >>>
    >>>
    >>> def foo():
    ...     raise ValueError(38)
    >>>
    >>>
    >>> def gee():
    ...     foo()
    >>>
    >>>
    >>> gee()
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "<stdin>", line 2, in gee
      File "<stdin>", line 2, in foo
    ValueError: 38
    .
    38
    >>>
    >>> err = None
    >>> try:
    ...     gee()
    ... except Exception as e:
    ...     err = e
    >>>
    >>> err
    ValueError(38)
    >>> err.__traceback__
    <traceback object at 0x7fad69dff8c0>
    >>> err.__cause__ is None
    True
    >>>
    >>> e_remote = RemoteException(err)
    >>> e_remote
    <mpservice.util.RemoteException object at 0x7fad702cd280>
    >>> e_pickled = pickle.dumps(e_remote)
    >>> e_unpickled = pickle.loads(e_pickled)
    >>>
    >>> e_unpickled
    ValueError(38)
    >>> type(e_unpickled)
    <class 'ValueError'>
    >>> e_unpickled.__traceback__ is None
    True
    >>> e_unpickled.__cause__
    RemoteTraceback('Traceback (most recent call last):\n  File "<stdin>", line 2, in <module>\n  File "<stdin>", line 2, in gee\n  File "<stdin>", line 2, in foo\nValueError: 38\n')
    >>>
    >>> is_remote_exception(e_unpickled)
    True
    >>> get_remote_traceback(e_unpickled)
    'Traceback (most recent call last):\n  File "<stdin>", line 2, in <module>\n  File "<stdin>", line 2, in gee\n  File "<stdin>", line 2, in foo\nValueError: 38\n'
    >>> print(get_remote_traceback(e_unpickled))
    Traceback (most recent call last):
      File "<stdin>", line 2, in <module>
      File "<stdin>", line 2, in gee
      File "<stdin>", line 2, in foo
    ValueError: 38
    >>>
    >>>
    >>> raise e_unpickled
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
    ValueError: 38
    .
    38
    >>>
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
        """
        Parameters
        ----------
        exc
            An Exception object.
        tb
            Optional traceback info, if not already carried by ``exc``.
        """
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
    A subclass of the standard ``threading.Thread``,
    this class makes the result or exception produced in a thread
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
    A subclass of the standard ``multiprocessing.context.SpawnProcess``,
    this customization adds two things:

    1. Make result and exception available as attributes of the
       process object, hence letting you use a ``SpawnProcess`` object
       similarly to how you use the ``Future`` object returned by
       ``concurrent.futures.ProcessPoolExecutor.submit``.
    2. Make logs in the worker process handled in the main process.

    The logging enhancement makes use of :class:`ProcessLogger`.
    """

    def __init__(self, *args, kwargs=None, loud_exception: bool = True, **moreargs):
        """
        Parameters
        ----------
        *args
            Positional arguments passed on to the standard ``Process``.
        kwargs
            Passed on to the standard ``Process``.
        loud_exception
            If ``True``, behave like the standard multiprocessing ``Process`` in the case of exceptions;
            if ``False``, behave like ``concurrent.futures`` in the case of exceptions.
        **moreargs
            Additional keyword arguments passed on to the standard ``Process``.
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

    Calling :meth:`stop` in either the main or the child process is optional.
    The call will immediately stop processing logs in the respective process.

    .. note:: Although user can use this class in their code, they are encouraged to
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
