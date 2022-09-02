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
from concurrent.futures.process import _ExceptionWithTraceback


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


class Thread(threading.Thread):
    '''
    This class makes the result or exception produced in a thread
    accessible from the thread object itself. This makes the `Thread`
    object's behavior somewhat similar to the `Future` object returned
    by `concurrent.futures.ThreadPoolExecutor.submit`.
    '''
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
    def __init__(self, *args, kwargs=None, **moreargs):
        if kwargs is None:
            kwargs = {}
        assert '__result_and_error__' not in kwargs
        assert '__worker_logger__' not in kwargs
        reader, writer = multiprocessing.connection.Pipe(duplex=False)
        worker_logger = ProcessLogger(ctx=multiprocessing.get_context('spawn'))
        worker_logger.__enter__()
        kwargs['__result_and_error__'] = writer
        kwargs['__worker_logger__'] = worker_logger

        super().__init__(*args, kwargs=kwargs, **moreargs)

        assert not hasattr(self, '__result_and_error__')
        assert not hasattr(self, '__worker_logger__')
        self.__result_and_error__ = reader
        self.__worker_logger__ = worker_logger

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
                    result_and_error.send(_ExceptionWithTraceback(e, e.__traceback__))
                    raise
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
        self._creator = True

    def __getstate__(self):
        assert self._creator
        assert self._t is not None
        return self._q

    def __setstate__(self, state):
        # In another process.
        self._q = state
        self._creator = False

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    def start(self):
        if self._creator:
            self._start_in_main_process()
        else:
            self._start_in_other_process()
        return self

    def stop(self):
        if self._creator:
            assert self._t is not None
            self._q.put(None)
            self._t.join()
        else:
            self._q.close()

    def _start_in_main_process(self):
        assert self._t is None
        self._q = self._ctx.Queue()

        self._t = threading.Thread(target=self._logger_thread, args=(self._q, ))
        self._t.start()

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
        if root.handlers:
            print('root logger has handlers: {}; deleted'.format(root.handlers))
        root.setLevel(logging.DEBUG)
        qh = logging.handlers.QueueHandler(self._q)
        root.addHandler(qh)

    @staticmethod
    def _logger_thread(q: multiprocessing.queues.Queue):
        threading.current_thread().name = 'logger_thread'
        while True:
            record = q.get()
            if record is None:
                # This is put in the queue by `self.stop()`.
                break
            logger = logging.getLogger(record.name)
            if record.levelno >= logger.getEffectiveLevel():
                logger.handle(record)


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
