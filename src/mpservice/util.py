import functools
import inspect
import logging
import logging.handlers
import multiprocessing
import multiprocessing.queues
import multiprocessing.context
import subprocess
import threading


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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._result_ = multiprocessing.get_context('spawn').Queue()
        self._exc_ = multiprocessing.get_context('spawn').Queue()
        self._result_.put(None)
        self._exc_.put(None)

        print('process init in', multiprocessing.current_process().name)

    def run(self):
        print('process run in', multiprocessing.current_process().name)

        if self._target:
            try:
                z = self._target(*self._args, **self._kwargs)
                print('in run result:', z)
                self._result_.put(z)
            except BaseException as e:
                print('Error in Process', multiprocessing.current_process().name)
                traceback.print_exc()
                self._error_.put(e)

    def done(self):
        if self.is_alive():
            return False
        return self._started.is_set()
        # Otherwise, not started yet.

    def result(self, timeout=None):
        self.join(timeout)
        if self.is_alive():
            raise TimeoutError
        e = self._exc_.get()
        if e is not None:
            raise e
        return self._result_.get()

    def exception(self, timeout=None):
        self.join(timeout)
        if self.is_alive():
            raise TimeoutError
        return self._exc_.get()


def Process(*args, ctx=None, **kwargs):
    # This is a "factory" function.
    if ctx is None:
        ctx = multiprocessing.get_context('spawn')
    method = ctx.get_start_method()
    assert method == 'spawn', f"process start method '{method}' not implemented"
    return SpawnProcess(*args, **kwargs)


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
