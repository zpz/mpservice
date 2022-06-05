import functools
import inspect
import logging
import logging.handlers
import multiprocessing
import multiprocessing.queues
import subprocess
import threading
import warnings


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
    object behavior somewhat similar to the `Future` object returned
    by `concurrent.futures.ThreadPoolExecutor.submit`.
    '''
    def run(self):
        self._result = None
        self._exc = None
        try:
            if self._target is not None:
                self._result = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self._exc = e
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
        if self._exc is not None:
            raise self._exc
        return self._result

    def exception(self, timeout=None):
        self.join(timeout)
        if self.is_alive():
            raise TimeoutError
        return self._exc


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

                pl = ProcessLogger().start()

        2. Pass this object to other processes. (Yes, this object is picklable.)

        3. In the other process, start it. Suppose the object is also called `pl`,
           then do

                pl.start()
    '''
    def __init__(self, *, ctx):
        assert ctx.get_start_method() == 'spawn'
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

        self._t = Thread(target=self._logger_thread, args=(self._q, ))
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
            warnings.warn('root logger has handlers: {}; deleted'.format(root.handlers))
            root.handlers = []
        root.setLevel(logging.DEBUG)
        qh = logging.handlers.QueueHandler(self._q)
        root.addHandler(qh)

    @staticmethod
    def _logger_thread(q: multiprocessing.queues.Queue):
        '''
        In main thread, start another thread with this function as `target`.
        '''
        threading.current_thread().name = 'logger_thread'
        while True:
            record = q.get()
            if record is None:
                # User should put a `None` in `q` to indicate stop.
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
