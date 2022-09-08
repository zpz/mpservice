import functools
import inspect
import logging
import logging.handlers
import multiprocessing
import multiprocessing.connection
import multiprocessing.queues
import multiprocessing.context
import subprocess
import threading
import traceback
from types import TracebackType
from typing import Optional, Union

import pebble


logger = logging.getLogger(__name__)


MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)
# This default is suitable for I/O bound operations.
# This value is what is used by `concurrent.futures.ThreadPoolExecutor`.
# For others, user may want to specify a smaller value.


class TimeoutError(RuntimeError):
    pass


def is_exception(e):
    return isinstance(e, BaseException) or (
        (type(e) is type) and issubclass(e, BaseException)
    )


def is_remote_exception(e) -> bool:
    return is_exception(e) and isinstance(e.__cause__, pebble.common.RemoteTraceback)


def get_remote_traceback(e) -> str:
    # Assuming `e` has passed the check of `is_remote_exception`.
    return e.__cause__.traceback


def is_async(func):
    while isinstance(func, functools.partial):
        func = func.func
    return inspect.iscoroutinefunction(func) or (
        not inspect.isfunction(func)
        and hasattr(func, '__call__')
        and inspect.iscoroutinefunction(func.__call__)
    )


def full_class_name(cls):
    if not isinstance(cls, type):
        cls = cls.__class__
    mod = cls.__module__
    if mod is None or mod == 'builtins':
        return cls.__name__
    return mod + '.' + cls.__name__


def exit_err_msg(obj, exc_type=None, exc_value=None, exc_tb=None):
    if exc_type is None:
        return
    if is_remote_exception(exc_value):
        msg = "Exiting {} with exception: {}\n{}".format(
            obj.__class__.__name__, exc_type, exc_value,
        )
        msg = "{}\n\n{}\n\n{}".format(
            msg, get_remote_traceback(exc_value),
            "The above exception was the direct cause of the following exception:")
        msg = f"{msg}\n\n{''.join(traceback.format_tb(exc_tb))}"
        return msg


class RemoteException(pebble.common.RemoteException):
    '''
    The purpose of this class is to send an exception object across process boundaries,
    hence undergoing pickling/unpickling, while keeping some traceback info.
    This is achieved simply by keeping the traceback info as a formatted string, because
    a traceback object is not pickle-able.

    Once unpickled in another process, the object obtained is not an instance of `RemoteException`,
    but rather of the original exception type. The object does not have `__traceback__` attribute,
    which is impossible to reconstruct, but has `__cause__` attribute, which is a custom Exception
    object that contains the string-form traceback info.

    Most (maybe all) methods and attributes of the exception object behave the same as the original,
    exception for `__traceback__`.

    Again, the unpickled object, say `obj`, is not an instance of this class, hence
    we can't define methods on this class to help using `obj`. The traceback string is
    `obj.__cause__.traceback`. This string with proper linebreaks contains all the traceback info
    except that in the "current process" (where `obj` comes into being via unpickling).
    I have yet to find a way to get this last missing part of the printout without doing `raise obj`.

    To determine that an exception object `obj` originates from another process via this mechanism, use
    `is_remote_exception`.
    '''

    # `pebble.common.RemoteException` is almost the same as `concurrent.futures.process._ExceptionWithTraceback`,
    # with slightly nicer printouts.
    # `pebble` is on LGPL license.

    # check out
    #   https://github.com/ionelmc/python-tblib
    #   https://stackoverflow.com/questions/6126007/python-getting-a-traceback-from-a-multiprocessing-process
    # about pickling Exceptions with tracebacks.
    #
    # See also: boltons.tbutils
    # See also: package `eliot`: https://github.com/itamarst/eliot/blob/master/eliot/_traceback.py
    #
    # Also check out `sys.excepthook`.

    def __init__(self, exc: Exception, tb: Optional[Union[TracebackType, str]] = None):
        if isinstance(tb, str):
            pass
        elif isinstance(tb, TracebackType):
            tb = ''.join(traceback.format_exception(type(exc), exc, tb))
        else:
            assert tb is None
            if exc.__traceback__ is not None:
                tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            else:
                if is_remote_exception(exc):
                    tb = get_remote_traceback(exc)
                else:
                    logger.warning(f"exc '{repr(exc)}' has a None __traceback__")
                    tb = ''.join(traceback.format_exception(type(exc), exc, None))
        super().__init__(exc, tb)


class Thread(threading.Thread):
    '''
    This class makes the result or exception produced in a thread
    accessible from the thread object itself. This makes the `Thread`
    object's behavior somewhat similar to the `Future` object returned
    by `concurrent.futures.ThreadPoolExecutor.submit`.
    '''
    def __init__(self, *args, auto_start=True, **kwargs):
        super().__init__(*args, **kwargs)
        self._auto_started = False
        if auto_start:
            self.start()

    def start(self):
        if self._auto_started:
            return
        super().start()
        self._auto_started = True

    def run(self):
        self._result_ = None
        self._exc_ = None
        try:
            if self._target is not None:
                self._result_ = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self._exc_ = e
            # raise
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs

    def done(self):
        if self.is_alive():
            return False
        return self._started.is_set()
        # Otherwise, not started yet.

    def result(self, timeout=None):
        self.join(timeout)
        if self.is_alive():
            raise TimeoutError
        if self._exc_ is not None:
            raise self._exc_
        return self._result_

    def exception(self, timeout=None):
        self.join(timeout)
        if self.is_alive():
            raise TimeoutError
        return self._exc_


class SpawnProcess(multiprocessing.context.SpawnProcess):
    '''
    This customization adds two things to the standard class:

        - make result and exception available as attributes of the
          process object, similar to `concurrent.futures.Future`.
        - make logs in the subprocess handled in the main process.
    '''
    def __init__(self, *args, kwargs=None, auto_start=True, **moreargs):
        if kwargs is None:
            kwargs = {}

        assert '__result_and_error__' not in kwargs
        reader, writer = multiprocessing.connection.Pipe(duplex=False)
        kwargs['__result_and_error__'] = writer

        assert '__worker_logger__' not in kwargs
        worker_logger = ProcessLogger(ctx=multiprocessing.get_context('spawn'))
        worker_logger.__enter__()
        kwargs['__worker_logger__'] = worker_logger

        super().__init__(*args, kwargs=kwargs, **moreargs)

        assert not hasattr(self, '__result_and_error__')
        self.__result_and_error__ = reader

        assert not hasattr(self, '__worker_logger__')
        self.__worker_logger__ = worker_logger

        self._auto_started = False
        if auto_start:
            self.start()

    def _stop_logger(self):
        if hasattr(self, '__worker_logger__') and self.__worker_logger__ is not None:
            self.__worker_logger__.__exit__()
            self.__worker_logger__ = None

    def __del__(self):
        self._stop_logger()

    def terminate(self):
        super().terminate()
        self._stop_logger()

    def kill(self):
        super().kill()
        self._stop_logger()

    def join(self, *args, **kwargs):
        super().join(*args, **kwargs)
        if self.done():
            self._stop_logger()

    def start(self):
        if self._auto_started:
            return
        super().start()
        self._auto_started = True

    def run(self):
        with self._kwargs.pop('__worker_logger__'):
            result_and_error = self._kwargs.pop('__result_and_error__')
            # Upon completion, `result_and_error` will contain `result` and `exception`
            # in this order; both may be `None`.
            if self._target:
                try:
                    z = self._target(*self._args, **self._kwargs)
                except BaseException as e:
                    result_and_error.send(None)
                    result_and_error.send(RemoteException(e))
                else:
                    result_and_error.send(z)
                    result_and_error.send(None)
            else:
                result_and_error.send(None)
                result_and_error.send(None)

    def done(self):
        return self.exitcode is not None

    def result(self, timeout=None):
        self.join(timeout)
        if not self.done():
            raise TimeoutError
        if not hasattr(self, '__result__'):
            self.__result__ = self.__result_and_error__.recv()
            self.__error__ = self.__result_and_error__.recv()
        if self.__error__ is not None:
            raise self.__error__
        return self.__result__

    def exception(self, timeout=None):
        self.join(timeout)
        if not self.done():
            raise TimeoutError
        if not hasattr(self, '__result__'):
            self.__result__ = self.__result_and_error__.recv()
            self.__error__ = self.__result_and_error__.recv()
        return self.__error__


def Process(*args, ctx, **kwargs):
    # This is a "factory" function.
    method = ctx.get_start_method()
    assert method == 'spawn', f"process start method '{method}' not implemented; the 'spwan' method is preferred"
    return SpawnProcess(*args, **kwargs)


class ProcessLogger:
    '''
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

        1. In main process, create a `ProcessLogger` instance and start it:

                pl = ProcessLogger(ctx=...).start()

        2. Pass this object to other processes. (Yes, this object is picklable.)

        3. In the other process, start it. Suppose the object is also called `pl`,
           then do

                pl.start()

    Remember to call the `stop` method in both main and the other process.
    For this reason, you may want to use the context manager.
    '''
    def __init__(self, *, ctx: multiprocessing.context.BaseContext):
        # assert ctx.get_start_method() == 'spawn'
        self._ctx = ctx
        self._t = None

    def __getstate__(self):
        assert self._t is not None
        return self._q

    def __setstate__(self, state):
        # In another process.
        self._q = state

    def __enter__(self):
        if hasattr(self, '_t'):
            self._start_in_main_process()
        else:
            self._start_in_other_process()
        return self

    def __exit__(self, *args):
        if hasattr(self, '_t'):
            self._stop_in_main_process()
        else:
            self._stop_in_other_process()

    def _start_in_main_process(self):
        assert self._t is None
        self._q = self._ctx.Queue()

        self._t = threading.Thread(target=self._logger_thread, args=(self._q, ))
        self._t.start()

    @staticmethod
    def _logger_thread(q: multiprocessing.queues.Queue):
        threading.current_thread().name = 'logger_thread'
        while True:
            record = q.get()
            if record is None:
                # This is put in the queue by `self.__exit__` in the other process.
                break
            logger = logging.getLogger(record.name)
            if record.levelno >= logger.getEffectiveLevel():
                logger.handle(record)

    def _start_in_other_process(self):
        '''
        In a Process (created using the "spawn" method),
        run this function at the beginning to set up putting all log messages
        ever produced in that process into the queue that will be consumed
        in the main process by `self._logger_thread`.

        During the execution of the process, logging should not be configured.
        Logging config should happen in the main process/thread.
        '''
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        qh = logging.handlers.QueueHandler(self._q)
        root.addHandler(qh)

    def _stop_in_main_process(self):
        assert self._t is not None
        self._q.put(None)
        self._t.join()
        self._t = None

    def _stop_in_other_process(self):
        self._q.close()


def get_docker_host_ip():
    '''
    From within a Docker container, this function finds the IP address
    of the host machine.
    '''
    # INTERNAL_HOST_IP=$(ip route show default | awk '/default/ {print $3})')
    # another idea:
    # ip -4 route list match 0/0 | cut -d' ' -f3
    #
    # Usually the result is '172.17.0.1'

    z = subprocess.check_output(['ip', '-4', 'route', 'list', 'match', '0/0'])
    z = z.decode()[len('default via '):]
    return z[: z.find(' ')]
