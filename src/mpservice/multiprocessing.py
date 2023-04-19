"""
`multiprocessing`_ can be tricky.
``mpservice`` provides help to address several common difficulties.

First, it is a good idea to always use the non-default (on Linux) "spawn" method to start a process.
:data:`~mpservice.util.MP_SPAWN_CTX` is provided to make this easier.

Second, in well structured code, a **spawned** process will not get the logging configurations that have been set
in the main process. On the other hand, we should definitely not separately configure logging in
non-main processes. The class :class:`~mpservice.util.SpawnProcess` addresses this issue. In fact,
``MP_SPAWN_CTX.Process`` is a reference to ``SpawnProcess``. Therefore, when you use ``MP_SPAWN_CTX``,
logging in the non-main processes are covered---log messages are sent to the main process to be handled,
all transparently.

Third, one convenience of `concurrent.futures`_ compared to `multiprocessing`_ is that the former
makes it easy to get the results or exceptions of the child process via the object returned from job submission.
With `multiprocessing`_, in contrast, we have to pass the results or explicitly captured exceptions
to the main process via a queue. :class:`~mpservice.util.SpawnProcess` has this covered as well. It can be used in the
``concurrent.futures`` way.

Last but not least, if exception happens in a child process and we don't want the program to crash right there,
instead we send it to the main or another process to be investigated when/where we are ready to,
the traceback info will be lost in pickling. :class:`~mpservice.util.RemoteException` helps on this.
"""

from __future__ import annotations

import concurrent.futures
import errno
import logging
import logging.handlers
import multiprocessing
import multiprocessing.connection
import multiprocessing.context
import multiprocessing.queues
import multiprocessing.util
import multiprocessing.managers
import os
import sys
import threading
import traceback
import warnings
import weakref
from typing import Optional, Callable

from ._remote_exception import is_remote_exception, get_remote_traceback, RemoteTraceback, RemoteException
from .threading import Thread


class SpawnProcess(multiprocessing.context.SpawnProcess):
    """
    A subclass of the standard ``multiprocessing.context.SpawnProcess``,
    this customization adds two things:

    1. Make result and exception available as attributes of the
       process object, hence letting you use a ``SpawnProcess`` object
       similarly to how you use the ``Future`` object returned by
       `concurrent.futures.ProcessPoolExecutor.submit <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor.submit>`_.
    2. Make logs in the worker process handled in the main process.

        Logging messages produced in worker processes are tricky.
        First, some settings should be concerned in the main process only,
        including log formatting, log-level control, log handler (destination), etc.
        Specifically, these should be settled in the "launching script", and definitely
        should not be concerned in worker processes.
        Second, the terminal printout of loggings in multiple processes tends to be
        intermingled and mis-ordered.

        This class uses a queue to transmit all logging messages that are produced
        in the worker process to the main process/thread, to be handled there.

    Examples
    --------
    Let's use an example to show the logging behavior.
    First, use a spawn-context from the standard `multiprocessing`_:

    .. code-block:: python
        :linenos:

        # log.py
        import logging
        import multiprocessing as mp
        from mpservice.multiprocessing import SpawnProcess


        def worker():
            logging.getLogger('worker.error').error('worker error')
            logging.getLogger('worker.warn').warning('worker warning')
            logging.getLogger('worker.info').info('worker info')
            logging.getLogger('worker.debug').debug('worker debug')


        def main():
            logging.getLogger('main.error').error('main error')
            logging.getLogger('main.info').info('main info')
            p = mp.get_context('spawn').Process(target=worker)
            p.start()
            p.join()
            logging.getLogger('main.warn').warning('main warning')
            logging.getLogger('main.debug').debug('main debug')


        if __name__ == '__main__':
            logging.basicConfig(
                format='[%(asctime)s.%(msecs)02d; %(levelname)s; %(name)s; %(funcName)s, %(lineno)d] [%(processName)s]  %(message)s',
                level=logging.DEBUG,
            )
            main()

    Run it::

        $ python log.py
        [2022-12-20 17:29:54,386.386; ERROR; main.error; main, 15] [MainProcess]  main error
        [2022-12-20 17:29:54,386.386; INFO; main.info; main, 16] [MainProcess]  main info
        worker error
        worker warning
        [2022-12-20 17:29:54,422.422; WARNING; main.warn; main, 20] [MainProcess]  main warning
        [2022-12-20 17:29:54,423.423; DEBUG; main.debug; main, 21] [MainProcess]  main debug

    Clearly, the child process exhibits the default behavior---print the warning-and-above-level log messages to the console---unaware of the logging configuration set in the main process.
    **This is a show stopper.**

    On line 15, replace ``mp.get_context('spawn').Process`` by ``SpawnProcess``.
    Run it again::

        $ python log.py
        [2022-12-20 17:39:31,284.284; ERROR; main.error; main, 15] [MainProcess]  main error
        [2022-12-20 17:39:31,284.284; INFO; main.info; main, 16] [MainProcess]  main info
        [2022-12-20 17:39:31,321.321; ERROR; worker.error; worker, 8] [SpawnProcess-1]  worker error
        [2022-12-20 17:39:31,321.321; WARNING; worker.warn; worker, 9] [SpawnProcess-1]  worker warning
        [2022-12-20 17:39:31,321.321; INFO; worker.info; worker, 10] [SpawnProcess-1]  worker info
        [2022-12-20 17:39:31,322.322; DEBUG; worker.debug; worker, 11] [SpawnProcess-1]  worker debug
        [2022-12-20 17:39:31,327.327; WARNING; main.warn; main, 20] [MainProcess]  main warning
        [2022-12-20 17:39:31,327.327; DEBUG; main.debug; main, 21] [MainProcess]  main debug

    This time, logs in the child process respect the level and format configurations set in the main process
    (because they are sent to and handled in the main process).
    """

    def __init__(self, *args, kwargs=None, **moreargs):
        """
        Parameters
        ----------
        *args
            Positional arguments passed on to the standard ``Process``.
        kwargs
            Passed on to the standard ``Process``.
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

        assert "__logger_queue__" not in kwargs
        logger_queue = MP_SPAWN_CTX.Queue()
        kwargs['__logger_queue__'] = logger_queue

        super().__init__(*args, kwargs=kwargs, **moreargs)

        assert not hasattr(self, "__result_and_error__")
        self.__result_and_error__ = reader
        assert not hasattr(self, "__logger_queue__")
        self.__logger_queue__ = logger_queue
        assert not hasattr(self, '__logger_thread__')
        self.__logger_thread__ = None
        assert not hasattr(self, '__finalize__')
        self.__finalize__ = None

    def start(self):
        super().start()

        # The following must be *after* ``super().start``, otherwise will get error
        # "cannot pickle '_thread.lock' object" because `self`
        # will be passed to the other process in ``super().start()``,
        # going through pickling.
        self.__logger_thread__ = Thread(
            target=self._run_logger_thread,
            args=(self.__logger_queue__,),
            name="ProcessLoggerThread",
            daemon=True,
        )
        self.__logger_thread__.start()
        self.__finalize__ = multiprocessing.util.Finalize(
            self,
            type(self)._finalize_logger_thread,
            (self.__logger_thread__, self.__logger_queue__),
            exitpriority=5,
        )

    @staticmethod
    def _run_logger_thread(q: multiprocessing.queues.Queue):
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

    def run(self):
        """
        Overrides the standard ``Process.run``.

        ``start`` arranges for this to be run in a child process.
        """
        result_and_error = self._kwargs.pop("__result_and_error__")

        # Upon completion, `result_and_error` will contain `result` and `exception`
        # in this order; both may be `None`.
        if self._target:
            logger_queue = self._kwargs.pop("__logger_queue__")

            if not logging.getLogger().hasHandlers():
                # Set up putting all log messages
                # ever produced in this process into ``logger_queue``,
                # The log messages will be consumed
                # in the main process by ``self._run_logger_thread``.
                #
                # During the execution of this process, logging should not be configured.
                # Logging config should happen in the main process/thread.
                root = logging.getLogger()
                root.setLevel(logging.DEBUG)
                qh = logging.handlers.QueueHandler(logger_queue)
                root.addHandler(qh)
            else:
                # If logger is configured in this process, then do not start log forwarding,
                # but this is usually not recommended.
                # This sually happends because logging is configured on the module level rather than
                # in the ``if __name__ == '__main__':`` block.
                logger_queue = None
                qh = None

            try:
                z = self._target(*self._args, **self._kwargs)
            except SystemExit:
                # TODO: what if `e.code` is not 0?
                result_and_error.send(None)
                result_and_error.send(None)
                raise
            except BaseException as e:
                result_and_error.send(None)
                result_and_error.send(RemoteException(e))
                raise
            else:
                result_and_error.send(z)
                result_and_error.send(None)
            finally:
                result_and_error.close()
                if qh is not None:
                    logging.getLogger().removeHandler(qh)
                    logger_queue.close()
        else:
            result_and_error.send(None)
            result_and_error.send(None)
            result_and_error.close()

    def _get_result(self):
        # Error could happen below if the process has terminated in some
        # unusual ways.
        if not hasattr(self, '__result__'):
            self.__result__ = self.__result_and_error__.recv()
            self.__error__ = self.__result_and_error__.recv()
            self.__result_and_error__.close()
            self.__result_and_error__ = None

        finalize = self.__finalize__
        if finalize:
            self.__finalize__ = None
            finalize()

    def join(self, timeout=None):
        '''
        Same behavior as the standard lib, except that if the process
        terminates with an exception, the exception is raised.
        '''
        super().join(timeout=timeout)
        exitcode = self.exitcode
        if exitcode is None:
            # Not terminated. Timed out.
            return
        self._get_result()
        if exitcode == 0:
            # Terminated w/o error.
            return
        if exitcode == 1:
            raise self.__error__
        assert exitcode < 0
        if exitcode == -errno.ENOTBLK:  # 15
            raise ChildProcessError(
                f"exitcode {-exitcode}, {errno.errorcode[-exitcode]}; likely due to a forced termination"
            )
            # For example, ``self.terminate()`` was called. That's a code smell.
            # ``signal.Signals.SIGTERM`` is 15.
            # ``signal.Signals.SIGKILL`` is 9.
            # ``signal.Signals.SIGINT`` is 2.
        else:
            raise ChildProcessError(
                f"exitcode {-exitcode}, {errno.errorcode[-exitcode]}"
            )
        # For a little more info on the error codes, see
        #   https://www.gnu.org/software/libc/manual/html_node/Error-Codes.html

    def done(self) -> bool:
        """
        Return ``True`` if the process has terminated normally or with exception.
        Return ``False`` if the process is running or not yet started.
        """
        return self.exitcode is not None

    def result(self, timeout: Optional[float | int] = None):
        '''
        Behavior is similar to ``concurrent.futures.Future.result``.
        '''
        super().join(timeout)
        if not self.done():
            raise TimeoutError
        self._get_result()
        if self.__error__ is not None:
            raise self.__error__
        return self.__result__

    def exception(self, timeout: Optional[float | int] = None):
        '''
        Behavior is similar to ``concurrent.futures.Future.exception``.
        '''
        super().join(timeout)
        if not self.done():
            raise TimeoutError
        self._get_result()
        return self.__error__


Process = SpawnProcess
'''Alias to :class:`SpawnProcess`.'''


class SpawnContext(multiprocessing.context.SpawnContext):
    '''
    We want to use :class:`SpawnProcess` as the process class when
    the creation method is 'spawn'.
    However, because the return of ``multiprocessing.get_context('spawn')``
    is a global var, we shouldn't directly change its
    ``.Process`` attribute like this::

        ctx = multiprocessing.get_context('spawn')
        ctx.Process = SpawnProcess

    It would change the behavior of the spawn context in code
    outside of our own.
    To achieve the goal in a controlled way, we designed this class.
    '''

    Process = SpawnProcess


MP_SPAWN_CTX = SpawnContext()
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

So, multiprocessing code is often written this way::

    import multiprocessing
    ctx = multiprocessing.get_context('spawn')
    q = ctx.Queue(...)
    e = ctx.Event(...)
    p = ctx.Process(..., args=(q, e))
    ...

The constant ``MP_SPAWN_CTX`` is a replacement of the standard spawn context.
Instead of the above, you are advised to write this::

    from mpservice.multiprocessing import MP_SPAWN_CTX as ctx
    q = ctx.Queue(...)
    e = ctx.Event(...)
    p = ctx.Process(..., args=(q, e))
    ...

The difference between ``MP_SPAWN_CTX`` and the standard spawn context
is that ``MP_SPAWN_CTX.Process`` is the custom :class:`SpawnProcess` in place of the standard
`Process <https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process>`_.

If you only need to start a process and don't need to create other objects
(like queue or event) from a context, then you can use :class:`SpawnProcess` directly.

All multiprocessing code in ``mpservice`` uses either ``MP_SPAWN_CTX``, or ``SpawnProcess`` directly.

`concurrent.futures.ProcessPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor>`_
takes a parameter ``mp_context``.
You can provide ``MP_SPAWN_CTX`` for this parameter so that the executor will use ``SpawnProcess``.
"""


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


class SpawnProcessPoolExecutor(concurrent.futures.ProcessPoolExecutor):
    '''
    This class is a drop-in replacement of the standard
    `concurrent.futures.ProcessPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor>`_.
    It uses a "spawn" context and the process class :class:`SpawnProcess`, hence getting
    the benefits of SpawnProcess.

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

    def __init__(self, max_workers=None, **kwargs):
        assert 'mp_context' not in kwargs
        super().__init__(max_workers=max_workers, mp_context=MP_SPAWN_CTX, **kwargs)

    def submit(self, fn, /, *args, loud_exception: bool = True, **kwargs):
        if loud_exception:
            return super().submit(_loud_process_function, fn, *args, **kwargs)
        return super().submit(fn, *args, **kwargs)


ProcessPoolExecutor = SpawnProcessPoolExecutor


class Manager(multiprocessing.managers.SyncManager):

    """A "server process" provides a server running in one process,
    to be called from other processes for shared data or functionalities.

    This module corresponds to the standard
    `multiprocessing.managers <https://docs.python.org/3/library/multiprocessing.html#managers>`_ module
    with simplified APIs for targeted use cases. The basic workflow  is as follows.

    1. Register one or more classes with the :class:`Manager` class::

            class Doubler:
                def __init__(self, ...):
                    ...

                def double(self, x):
                    return x * 2

            class Tripler:
                def __init__(self, ...):
                    ...

                def triple(self, x):
                    return x * 3

            Manager.register(Doubler)
            Manager.register(Tripler)

    2. Create a manager object and start it::

            manager = Manager()
            manager.start()

    You can also use a context manager::

            with Manager() as manager:
                ...

    3. Create one or more proxies::

            doubler = manager.Doubler(...)
            tripler = manager.Tripler(...)

    A manager object has a "server process".
    The above causes corresponding class objects to be created
    in the server process; the returned objects are "proxies"
    for the real objects. These proxies can be passed to any other
    processes and used there.

    The arguments in the above calls are passed to the server process
    and used in the ``__init__`` methods of the corresponding classes.
    For this reason, the parameters to ``__init__`` of a registered class
    must all be pickle-able.

    Calling one registered class multiple times, like

    ::

            prox1 = manager.Doubler(...)
            prox2 = manager.Doubler(...)

    will create independent objects in the server process.

    Multiple manager objects will run their corresponding
    server processes independently.

    4. Pass the proxy objects to any process and use them there.

    Public methods (minus "properties") defined by the registered classes
    can be invoked on a proxy with the same parameters and get the expected
    result. For example,

    ::

            prox1.double(3)

    will return 6. Inputs and output of the public method
    should all be pickle-able.

    In each new thread or process, a proxy object will create a new
    connection to the server process (see``multiprocessing.managers.Server.accepter``,
    ...,
    ``Server.accept_connection``,
    and
    ``BaseProxy._connect``;
    all in `Lib/multiprocessing/managers.py <https://github.com/python/cpython/blob/main/Lib/multiprocessing/managers.py>`_);
    the server process then creates a new thread
    to handle requests from this connection (see ``Server.serve_client``
    also in `Lib/multiprocessing/managers.py <https://github.com/python/cpython/blob/main/Lib/multiprocessing/managers.py>`_).
    """

    def __init__(self):
        super().__init__(ctx=MP_SPAWN_CTX)

        # `self._ctx` is `MP_SPAWN_CTX`

    @classmethod
    def register(cls, typeid_or_callable: str | Callable, /, **kwargs):
        """
        ``typeid_or_callable`` is usually a class object.
        This method should be called before a :class:`Manager` object is "started".
        """
        if isinstance(typeid_or_callable, str):
            # This form allows the full API of the base class.
            # I have not encountered a need for this.
            # This is allowed just in case for experiments and expansions.
            # You almost always should use the other form.
            typeid = typeid_or_callable
            callable_ = kwargs.pop("callable", None)
        else:
            assert callable(typeid_or_callable)
            # Usually, `typeid_or_callable` is a class object and the sole argument.
            typeid = typeid_or_callable.__name__
            callable_ = typeid_or_callable
        if typeid in cls._registry:
            warnings.warn(
                '"%s" was registered; the existing registry is overwritten.' % typeid
            )
        super().register(typeid, callable_, **kwargs)






_global_process_pools_: dict[str, ProcessPoolExecutor] = weakref.WeakValueDictionary()
_global_process_pools_lock: threading.Lock = threading.Lock()


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
        box = _global_process_pools_
        for name in list(box.keys()):
            pool = box.get(name)
            if pool is not None:
                # TODO: if `pool` has locks, are there problems?
                pool.shutdown(wait=False)
            box.pop(name, None)
        global _global_process_pools_lock
        try:
            _global_process_pools_lock.release()
        except RuntimeError:
            pass
        _global_process_pools_lock = threading.Lock()

    os.register_at_fork(after_in_child=_clear_global_state)
