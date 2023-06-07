from __future__ import annotations

import errno
import logging
import logging.handlers
import multiprocessing
import multiprocessing.connection
import multiprocessing.context
import multiprocessing.managers
import multiprocessing.queues
import threading
from multiprocessing import util

import psutil

from ..threading import Thread
from ._remote_exception import RemoteException


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
        self.__finalize__ = util.Finalize(
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
