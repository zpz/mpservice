"""
`multiprocessing`_ can be tricky.
``mpservice`` provides help to address several common difficulties.

First, it is a good idea to always use the non-default (on Linux) "spawn" method to start a process.
:data:`~mpservice.multiprocessing.MP_SPAWN_CTX` is provided to make this easier.

Second, in well structured code, a **spawned** process will not get the logging configurations that have been set
in the main process. On the other hand, we should definitely not separately configure logging in
non-main processes. The class :class:`~mpservice.multiprocessing.SpawnProcess` addresses this issue. In fact,
``MP_SPAWN_CTX.Process`` is a reference to ``SpawnProcess``. Therefore, when you use ``MP_SPAWN_CTX``,
logging in the non-main processes are covered---log messages are sent to the main process to be handled,
all transparently.

Third, one convenience of `concurrent.futures`_ compared to `multiprocessing`_ is that the former
makes it easy to get the results or exceptions of the child process via the object returned from job submission.
With `multiprocessing`_, in contrast, we have to pass the results or explicitly captured exceptions
to the main process via a queue. :class:`~mpservice.multiprocessing.SpawnProcess` has this covered as well.
It can be used in the ``concurrent.futures`` way.

Last but not least, if exception happens in a child process and we don't want the program to crash right there,
instead we send it to the main or another process to be investigated when/where we are ready to,
the traceback info will be lost in pickling. :class:`~mpservice.multiprocessing.RemoteException` helps on this.
"""

from __future__ import annotations

import errno
import logging
import logging.handlers
import multiprocessing
import multiprocessing.connection
import multiprocessing.context
import multiprocessing.managers
import multiprocessing.queues
import multiprocessing.util
import threading
import warnings
from typing import Callable

import psutil

from ._remote_exception import (
    RemoteException,
    RemoteTraceback,
    get_remote_traceback,
    is_remote_exception,
)
from .threading import Thread

__all__ = [
    'RemoteException',
    'RemoteTraceback',
    'get_remote_traceback',
    'is_remote_exception',
    'TimeoutError',
    'SpawnProcess',
    'SpawnContext',
    'MP_SPAWN_CTX',
    'ServerProcess',
    'CpuAffinity',
]


class TimeoutError(Exception):
    pass


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
                logging.captureWarnings(True)
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
                print(f"{multiprocessing.current_process().name}: {repr(e)}")
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
        if exitcode >= 0:
            raise ValueError(f"expecting negative `exitcode` but got: {exitcode}")
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

    def result(self, timeout: float | int | None = None):
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

    def exception(self, timeout: float | int | None = None):
        '''
        Behavior is similar to ``concurrent.futures.Future.exception``.
        '''
        super().join(timeout)
        if not self.done():
            raise TimeoutError
        self._get_result()
        return self.__error__


class CpuAffinity:
    """
    ``CpuAffinity`` specifies which CPUs (or cores) a process should run on.

    This operation is known as "pinning a process to certain CPUs"
    or "setting the CPU/processor affinity of a process".

    Setting and getting CPU affinity is done via |psutil.cpu_affinity|_.

    .. |psutil.cpu_affinity| replace:: ``psutil.Process().cpu_affinity``
    .. _psutil.cpu_affinity: https://psutil.readthedocs.io/en/latest/#psutil.Process.cpu_affinity
    .. see https://jwodder.github.io/kbits/posts/rst-hyperlinks/
    """

    def __init__(self, target: int | list[int] | None = None, /):
        """
        Parameters
        ----------
        target
            The CPUs to pin the current process to.

            If ``None``, no pinning is done. This object is used only to query the current affinity.
            (I believe all process starts in an un-pinned status.)

            If an int, it is the zero-based index of the CPU. Valid values are 0, 1,...,
            the number of CPUs minus 1. If a list, the elements are CPU indices.
            Duplicate values will be removed. Invalid values will raise ``ValueError``.

            If ``[]``, pin to all eligible CPUs.
            On some systems such as Linux this may not necessarily mean all available logical
            CPUs as in ``list(range(psutil.cpu_count()))``.
        """
        if target is not None:
            if isinstance(target, int):
                target = [target]
            else:
                assert all(isinstance(v, int) for v in target)
                # `psutil` would truncate floats but I don't like that.
        self.target = target

    def __repr__(self):
        return f"{self.__class__.__name__}({self.target})"

    def __str__(self):
        return self.__repr__()

    def set(self, *, pid=None) -> None:
        """
        Set CPU affinity to the value passed into :meth:`__init__`.
        If that value was ``None``, do nothing.
        Use an empty list to cancel previous pin.
        """
        if self.target is not None:
            psutil.Process(pid).cpu_affinity(self.target)

    @classmethod
    def get(self, *, pid=None) -> list[int]:
        """Return the current CPU affinity."""
        return psutil.Process(pid).cpu_affinity()


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

    def Manager(self, *, name: str | None = None, cpu: int | list[int] | None = None):
        '''
        The counterpart in the standard lib does not have the parameters ``name`` and ``cpu``.
        '''
        m = super().Manager()
        if name:
            m._process.name = name
        if cpu is not None:
            CpuAffinity(cpu).set(pid=m._process.pid)
        return m

    def get_context(self, method=None):
        if method is None or method == 'spawn':
            return self
        return super().get_context(method)


# MP_SPAWN_CTX = multiprocessing.context.DefaultContext(SpawnContext())
# The version above would fail `tests/test_streamer.py::test_parmap`. I don't know why.
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


# Have this subclass to help debugging when needed.
class _ProcessServer(multiprocessing.managers.Server):
    pass


# Have this subclass to help debugging when needed.
class _SpawnManager(multiprocessing.managers.BaseManager):

    _Server = _ProcessServer

    def __init__(self):
        super().__init__(ctx=MP_SPAWN_CTX)
        # 3.11 got parameter `shutdown_timeout`, which may be useful.


class ServerProcess:
    """
    A "server process" provides a server running in one process,
    to be called from other processes for shared data or functionalities.

    The basic workflow is as follows.

    1. Register one or more classes with the :class:`ServerProcess` class::

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

        ServerProcess.register(Doubler)
        ServerProcess.register(Tripler)

    2. Start a "server process" object in a contextmanager::

        with ServerProcess() as server:
                ...


    3. Create one or more proxies::

            doubler = server.Doubler(...)
            tripler = server.Tripler(...)

        The above causes corresponding class objects to be created
        in the server process; the returned objects are "proxies"
        for the real objects. These proxies can be passed to any other
        processes and used there.

        The arguments in the above calls are passed into the server process
        and used in the ``__init__`` methods of the corresponding classes.
        For this reason, the parameters to ``__init__`` of a registered class
        must all be pickle-able.

        Calling one registered class multiple times, like

        ::

                prox1 = server.Doubler(...)
                prox2 = server.Doubler(...)

        will create independent objects in the server process.

        Multiple ServerProcess objects will run their corresponding
        server processes independently.

    4. Pass the proxy objects to any process and use them there.

        Public methods (minus "properties") defined by the registered classes
        can be invoked on a proxy with the same parameters and get the expected
        result. For example,

        ::

                prox1.double(3)

        will return 6. Inputs and output of the public method
        should all be pickle-able.

        Between the server process and the proxy object in a particular process/thread,
        a connection is established, which starts a new thread in the server process
        to handle all requests from that proxy object.
        These "requests" include all methods of the proxy, not just one particular method.

        Calls on a particular method of the proxy from multiple processes/threads
        become multi-threaded concurrent calls in the server process.
        We can design a simple example to observe this effect::


            class Doubler:
                def do(self, x):
                    time.sleep(0.5)
                    return x + x

            ServerProcess.register(Doubler)

            def main():
                with ServerProcess() as server:
                    doubler = server.Doubler()

                    ss = Stream(range(100)).parmap(doubler.do, executor='thread', num_workers=50)
                    t0 = time.perf_counter()
                    zz = list(ss)
                    t1 = time.perf_counter()
                    print(t1 - t0)
                    assert zz == [x + x for x in range(100)]

            if __name__ == '__main__':
                main()

        If the calls to ``double.do`` were sequential, then 100 calls would take 50 seconds.
        With concurrent calls in 50 threads as above, it took 1.05 seconds in one experiment.

        As a consequence, if the method mutates some shared state, it needs to guard things by locks.

    The class ``ServerProcess`` delegates most work to ``Manager`` in the standard ``multiprocessing``,
    but is specifically for the use case where user needs to design and register a custom class
    to be used in a "server process".
    If you don't need a custom class, but rather just need to use one of the standard classes,
    for example, ``Event``, you can use that via ``MP_SPAWN_CTX.Manager``, or better, import
    ``Manager`` from ``mpservice.multiprocessing``.

    In an environment that supports shared memory, ``ServerProcess`` has two other methods:
    :meth:`MemoryBlock` and :meth:`list_memory_blocks`. A simple example::

        with ServerProcess() as server:
            mem = server.MemoryBlock(size=1000)
            buffer = mem.buf  # memoryview
            # ... write data into `buffer`
            # pass `mem` to other processes and use its `.buf` again for the content.
            # Since it's a "shared" block of memory, any process and modify the data
            # via the memoryview.

    To release (or "destroy") the memory block, just make sure all references to ``mem``
    in this (the "creating") and worker (the "consuming") processes are cleared.
    """

    # In each new thread or process, a proxy object will create a new
    # connection to the server process (see``multiprocessing.managers.Server.accepter``,
    # ...,
    # ``Server.accept_connection``,
    # and
    # ``BaseProxy._connect``;
    # all in `Lib/multiprocessing/managers.py <https://github.com/python/cpython/blob/main/Lib/multiprocessing/managers.py>`_);
    # the server process then creates a new thread
    # to handle requests from this connection (see ``Server.serve_client``
    # also in `Lib/multiprocessing/managers.py <https://github.com/python/cpython/blob/main/Lib/multiprocessing/managers.py>`_).

    # Overhead:
    #
    # I made a naive example, where the registered class just returns ``x + x``.
    # I was able to finish 13K calls per second.

    _registry = set()

    @classmethod
    def register(cls, worker: Callable, /, name: str = None, proxytype=None):
        """
        ``worker`` is usually a class object (not an instance of the class).
        It can also be a function.

        Suppose ``worker`` is a class ``Worker``, then registering ``Worker`` will add a method
        named "Worker" to the class ``ServerProcess``. Later on a running ServerProcess object
        ``server``, calling::

            server.Worker(*args, **kwargs)

        will run the callable ``worker`` inside the server process, taking ``args`` and ``kwargs``
        as it normally would. The object resulted from that call will stay in the server process.
        (In the case where ``worker`` is the class ``Worker``, the call ``Worker(...)`` will result
        in an *instance* of the Worker class.)
        The call ``server.Worker(...)`` returns a "proxy" to that object; the proxy is going to be used
        from other processes or threads to communicate with the real object residing inside the server process.

        .. note:: This method must be called before a :class:`ServerProcess` object is "started".
        """
        if not callable(worker):
            raise ValueError("`worker` must be a callable")
        typeid = name or worker.__name__
        callable_ = worker
        if typeid in cls._registry:
            warnings.warn(
                '"%s" was registered; the existing registry is overwritten.' % typeid
            )
        else:
            cls._registry.add(typeid)
        _SpawnManager.register(typeid, callable_, proxytype=proxytype)

    def __init__(
        self,
        *,
        cpu: int | list[int] | None = None,
        name: str | None = None,
    ):
        '''
        Parameters
        ----------
        name
            Name of the process. If ``None``, a default will be used.
        '''
        self._manager = _SpawnManager()
        self._cpu = cpu
        self._name = name

    def __enter__(self):
        self._manager.__enter__()
        if self._cpu is not None:
            CpuAffinity(self._cpu).set(pid=self._manager._process.pid)
        if self._name:
            self._manager._process.name = self._name
        return self

    def __exit__(self, *args):
        self._manager.__exit__(*args)

    def __getattr__(self, name):
        # The main method names are the names of the classes that have been reigstered.
        if name in self._registry:
            return getattr(self._manager, name)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )


try:
    from multiprocessing.shared_memory import SharedMemory
except ImportError:
    pass
else:

    class MemoryBlock:
        '''
        This class is used within the "server process" of a ``ServerProcess`` to
        create and track shared memory blocks.

        The design of this class is largely dictated by the need of its corresponding "proxy" class.
        '''

        _blocks_ = set()

        def __init__(self, size: int):
            self._mem = SharedMemory(create=True, size=size)
            self.__class__._blocks_.add(self._mem.name)

        def _info(self):
            return self._mem.name, self._mem.size

        def list_memory_blocks(self):
            return self._blocks_

        def __del__(self):
            '''
            The garbage collection of this object happens when its refcount
            reaches 0, which in turn happens when all references to its
            corresponding proxy object (outside of the server process) have been
            removed.

            According to the class ``multiprocessing.shared_memory.SharedMemory``,
            all SharedMemory objects in the proxy objects have called :meth:`SharedMemory.close`.

            As a result, we assume that all uses of this particular memory block have finished
            by this time, hence we destroy the memory block by calling ``unlink``.
            '''
            try:
                name = self._mem.name
                self._mem.close()
                self._mem.unlink()
                self.__class__._blocks_.remove(name)
            except OSError:
                pass

    BaseProxy = multiprocessing.managers.BaseProxy

    def _rebuild_memory_block_proxy(func, args, name, size, mem):
        obj = func(*args)
        obj._name, obj._size, obj._mem = name, size, mem

        # We have called ``incref`` for the ``MemoryBlock``
        # when pickling, hence we don't call ``incref`` when
        # reconstructing the proxy here during unpickling.
        # However, we still need to set up calling of ``decref``
        # when this reconstructed proxy is garbage collected.

        # The code below is part of ``BaseProxy._incref``.

        obj._idset.add(obj._id)
        state = obj._manager and obj._manager._state
        obj._close = multiprocessing.util.Finalize(
            obj,
            BaseProxy._decref,
            args=(obj._token, obj._authkey, state, obj._tls, obj._idset, obj._Client),
            exitpriority=10,
        )

        return obj

    class MemoryBlockProxy(BaseProxy):
        _exposed_ = ('_info', 'list_memory_blocks')

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._name = None
            self._size = None
            self._mem = None

        def __reduce__(self):
            # The only sensible case of pickling this proxy object
            # transfering this object between processes in a queue.
            # (This proxy object is never pickled for storage.)
            # It is possible that at some time the only "reference"
            # to a certain shared memory block is in pickled form in
            # a queue. For example,
            #
            #   # main process
            #   with ServerProcess() as server:
            #        q = Queue()
            #        w = Process(target=..., args=(q, ))
            #        w.start()
            #
            #        for _ in range(1000):
            #           mem = server.MemoryBlock(100)
            #           # ... computations ...
            #           # ... write data into `mem` ...
            #           q.put(mem)
            #           ...
            #
            # During the loop, `mem` goes out of scope and only lives on in the queue.
            # A main design goal is to prevent the ``MemoryBlock`` object in the server process
            # from being garbage collected. This mean we need to do some ref-counting hacks.

            # Inc ref here to represent the pickled object in the queue.
            # When unpickling, we do not inc ref again. In effect, we move the call
            # to ``incref`` earlier from ``pickle.loads`` into ``pickle.dumps``
            # for this object.

            conn = self._Client(self._token.address, authkey=self._authkey)
            multiprocessing.managers.dispatch(conn, None, 'incref', (self._id,))
            # NOTE:
            # calling ``self._incref()`` would not work because it adds another `_decref`;
            # I don't totally understand that part.

            func, args = super().__reduce__()
            args[-1]['incref'] = False  # do not inc ref again during unpickling.
            return _rebuild_memory_block_proxy, (
                func,
                args,
                self._name,
                self._size,
                self._mem,
            )

        @property
        def name(self):
            '''
            Return the name of the ``SharedMemory`` object.
            '''
            if self._name is None:
                info = self._callmethod('_info')
                self._name, self._size = info
            return self._name

        @property
        def size(self) -> int:
            '''
            Return size of the memory block in bytes.
            '''
            if self._size is None:
                info = self._callmethod('_info')
                self._name, self._size = info
            return self._size

        @property
        def buf(self) -> memoryview:
            '''
            Return a ``memoryview`` into the context of the memory block.
            '''
            if self._mem is None:
                self._mem = SharedMemory(name=self.name, create=False)
            return self._mem.buf

        def list_memory_blocks(self) -> set[str]:
            '''
            Return the set of names of shared memory blocks being
            tracked by the ``ServerProcess`` that "ownes" the current
            proxy object.
            '''
            return self._callmethod('list_memory_blocks')

    ServerProcess.register(MemoryBlock, proxytype=MemoryBlockProxy)

    def list_memory_blocks(self):
        '''
        List names of MemoryBlock objects being tracked.
        '''
        m = self.MemoryBlock(1)
        blocks = m.list_memory_blocks()
        blocks.remove(m.name)
        return blocks

    ServerProcess.list_memory_blocks = list_memory_blocks


_names_ = [x for x in dir(MP_SPAWN_CTX) if not x.startswith('_')]
globals().update((name, getattr(MP_SPAWN_CTX, name)) for name in _names_)
# Names like `Process`, `Queue`, `Pool`, `Event`, `Manager` etc are directly import-able from this module.
# But they are not classes; rather they are bound methods of the context `MP_SPAWN_CTX`.
# This is the same behavior as the standard `multiprocessing`.
# With this, you can usually replace
#
#    from multiprocessing import ...
#
# by
#
#    from mpservice.multiprocessing import ...
