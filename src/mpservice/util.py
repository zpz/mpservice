from __future__ import annotations
# Enable using a class in type annotations in the code
# that defines that class itself.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import functools
import inspect
import logging
import logging.handlers
import multiprocessing
import subprocess
import warnings


logger = logging.getLogger(__name__)


MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)
# This default is suitable for I/O bound operations.
# This value is what is used by `concurrent.futures.ThreadPoolExecutor`.
# For others, user may want to specify a smaller value.


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


def logger_thread(q: multiprocessing.Queue):
    '''
    In main thread, start another thread with this function as `target`.
    '''
    while True:
        record = q.get()
        if record is None:
            # User should put a `None` in `q` to indicate stop.
            break
        logger = logging.getLogger(record.name)
        if record.levelno >= logger.getEffectiveLevel():
            logger.handle(record)


def forward_logs(q: multiprocessing.Queue):
    '''
    In a Process (created using the "spawn" method),
    run this function at the beginning to set up putting all log messages
    ever produced in that process into the queue that will be consumed by
    `logger_thread`.

    During the execution of the process, logging should not be configured.
    Logging config should happen in the main process/thread.
    '''
    root = logging.getLogger()
    if root.handlers:
        warnings.warn('root logger has handlers: {}; deleted'.format(root.handlers))
        root.handlers = []
    root.setLevel(logging.DEBUG)
    qh = logging.handlers.QueueHandler(q)
    root.addHandler(qh)


def get_docker_host_ip():
    # INTERNAL_HOST_IP=$(ip route show default | awk '/default/ {print $3})')
    # another idea:
    # ip -4 route list match 0/0 | cut -d' ' -f3
    #
    # Usually the result is '172.17.0.1'

    z = subprocess.check_output(['ip', '-4', 'route', 'list', 'match', '0/0'])
    z = z.decode()[len('default via '):]
    return z[: z.find(' ')]
