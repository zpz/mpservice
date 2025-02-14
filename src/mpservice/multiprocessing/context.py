from __future__ import annotations

import concurrent.futures
import errno
import logging
import logging.handlers
import multiprocessing
import multiprocessing.connection
import multiprocessing.context
import multiprocessing.managers
import multiprocessing.pool
import multiprocessing.queues
import multiprocessing.synchronize
import multiprocessing.util
import os
import sys
import time
import traceback

from .._common import TimeoutError
from ..threading import Thread
from .remote_exception import RemoteException

logger = logging.getLogger(__name__)


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

    This class is aliased by ``mpservice.multiprocessing.Process``.
    It is preferred to import by the aliased name.

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

        assert '_result_and_error_' not in kwargs
        assert '_logger_queue_' not in kwargs
        reader, writer = multiprocessing.connection.Pipe(duplex=False)
        kwargs['_result_and_error_'] = writer
        logger_queue = MP_SPAWN_CTX.Queue()
        kwargs['_logger_queue_'] = logger_queue

        super().__init__(*args, kwargs=kwargs, **moreargs)

        assert not hasattr(self, '_result_and_error_')
        assert not hasattr(self, '_logger_queue_')
        assert not hasattr(self, '_logger_thread_')
        assert not hasattr(self, '_collector_thread_')
        self._result_and_error_ = reader
        self._logger_queue_ = logger_queue
        self._logger_thread_ = None

    def start(self):
        super().start()

        # The following must be *after* ``super().start``, otherwise will get error
        # "cannot pickle '_thread.Lock' object" or "cannot pickle '_thread.RLock" because `self`
        # will be passed to the other process in ``super().start()``,
        # going through pickling.

        self._future_ = concurrent.futures.Future()

        self._logger_thread_ = Thread(
            target=self._run_logger,
            args=(self._logger_queue_,),
            name=f'{self.name}-LoggerThread',
            daemon=getattr(self, 'daemon', None),
        )
        self._logger_thread_.start()

        self._result_collector_thread_ = Thread(
            target=self._collect_result,
            name=f'{self.name}-ResultCollectorThread',
        )
        self._result_collector_thread_.start()

        self._finalizer_ = multiprocessing.util.Finalize(
            self,
            type(self)._finalize,
            args=(self._logger_thread_, self._logger_queue_),
        )

    @staticmethod
    def _run_logger(q: multiprocessing.queues.Queue):
        while True:
            record = q.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            if record.levelno >= logger.getEffectiveLevel():
                logger.handle(record)

    def _collect_result(self):
        result, error = None, None
        try:
            result = self._result_and_error_.recv()
            error = self._result_and_error_.recv()

        except EOFError as exc:
            # the process has been terminated by calling ``self.terminate()``
            while self.exitcode is None:
                time.sleep(0.001)

            exitcode = -self.exitcode
            if exitcode == errno.ENOTBLK:  # 15
                # warnings.warn(
                #     f'process exitcode {exitcode}, {errno.errorcode[exitcode]}; likely due to a forced termination by calling `.terminate()`',
                #     stacklevel=2,
                # )
                pass
                # For example, ``self.terminate()`` was called. That's a code smell.
                # ``signal.Signals.SIGTERM`` is 15.
                # ``signal.Signals.SIGKILL`` is 9.
                # ``signal.Signals.SIGINT`` is 2.
            else:
                msg = os.strerror(exitcode)
                if exitcode == 9:
                    msg += ': possibly out of memory'
                raise OSError(exitcode, msg) from exc

        self._logger_queue_.put(None)
        self._result_and_error_.close()
        self._result_and_error_ = None
        if error is not None:
            self._future_.set_exception(error)
        else:
            self._future_.set_result(result)

    @staticmethod
    def _finalize(logger_thread, q):
        q.put(None)
        logger_thread.join()

    @staticmethod
    def handle_exception(exc):
        # Subclass can customize this to log more info.
        # however, usually you should do that when calling `.join`.
        # This method must not raise exceptions.
        print(f'Exception in {multiprocessing.current_process().name}: {repr(exc)}')

    def run(self):
        """
        Overrides the standard ``Process.run``.

        ``start`` arranges for this to be run in a child process.
        """
        result_and_error = self._kwargs.pop('_result_and_error_')
        # Upon completion, `result_and_error` will contain `result` and `exception`
        # in this order; both may be `None`.

        if not self._target:
            result_and_error.send(None)
            result_and_error.send(None)
            result_and_error.close()
            return

        logger_queue = self._kwargs.pop('_logger_queue_')

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
            logger_queue.close()
            logger_queue = None
            qh = None

        self._mpservice_exitcode_ = 0

        try:
            z = self._target(*self._args, **self._kwargs)
        except SystemExit as e:
            if e.code is None:
                result_and_error.send(None)
                result_and_error.send(None)
            else:
                if isinstance(e.code, int):
                    if e.code == 0:
                        result_and_error.send(None)
                        result_and_error.send(None)
                    else:
                        self._mpservice_exitcode_ = e.code
                        self.handle_exception(e)
                        result_and_error.send(None)
                        result_and_error.send(RemoteException(e))
                else:
                    self._mpservice_exitcode_ = 1
                    self.handle_exception(e)
                    sys.stderr.write(f'exitcode: {e.code}' + '\n')
                    result_and_error.send(None)
                    result_and_error.send(RemoteException(e))
        except BaseException as e:
            self.handle_exception(e)
            # This must go before the two lines below, in case
            # user's custom `handle_exception` writes logs.
            self._mpservice_exitcode_ = 1
            if self.daemon:
                traceback.print_exc()
            result_and_error.send(None)
            result_and_error.send(RemoteException(e))
        else:
            result_and_error.send(z)
            result_and_error.send(None)
        finally:
            result_and_error.close()
            if qh is not None:
                logging.getLogger().removeHandler(qh)
                logger_queue.close()

    def _bootstrap(self, parent_sentinel=None):
        exitcode = super()._bootstrap(parent_sentinel)
        assert exitcode == 0
        return self._mpservice_exitcode_

    def join(self, timeout=None):
        """
        Same behavior as the standard lib, except that if the process
        terminates with an exception, the exception is raised.
        """
        super().join(timeout=timeout)
        if not self.done():
            # timed out
            return

        self._result_collector_thread_.join()

        # exitcode = self.exitcode
        # if exitcode is None:
        #     # Not terminated. Timed out.
        #     return
        # if exitcode == 0:
        #     # Terminated w/o error.
        #     return
        # if exitcode == 1:
        #     raise self._future_.exception()
        # if exitcode >= 0:
        #     raise ValueError(f'expecting negative `exitcode` but got {exitcode}')

        if self._future_.exception():
            raise self._future_.exception()
        # else:
        #     raise ChildProcessError(
        #         f'child process failed with exitcode {exitcode}, {errno.errorcode[exitcode]}'
        #     )
        # For a little more info on the error codes, see
        #   https://www.gnu.org/software/libc/manual/html_node/Error-Codes.html

    def done(self) -> bool:
        """
        Return ``True`` if the process has terminated normally or with exception.
        Return ``False`` if the process is running or not yet started.
        """
        return self.exitcode is not None

    def result(self, timeout: float | int | None = None):
        """
        Behavior is similar to ``concurrent.futures.Future.result``.
        """
        self.join(timeout)
        if not self.done():
            raise TimeoutError
        return self._future_.result()

    def exception(self, timeout: float | int | None = None):
        """
        Behavior is similar to ``concurrent.futures.Future.exception``.
        """
        super().join(timeout)
        if not self.done():
            raise TimeoutError
        self._result_collector_thread_.join()
        return self._future_.exception()


class SpawnContext(multiprocessing.context.SpawnContext):
    """
    The standard package ``multiprocessing`` has a
    `"context" <https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods>`_,
    which has to do with how a process is created and started.
    For example, a ``ForkContext`` (or ``SpawnContext``) will create processes by ``ForkProcess`` (or ``SpawnProcess``).
    To use a queue to communicate with the process, the queue needs to be created via ``ForkContext.Queue`` (or ``SpawnContext.Queue``),
    else it will not work.

    If you do

    ::

        import multiprocessing
        q = multiprocessing.Queue()

    this is equivalent to

    ::

        q = multiprocessing.get_context().Queue()

    `multiprocessing.get_context <https://docs.python.org/3/library/multiprocessing.html#multiprocessing.get_context>`_ takes the sole parameter ``method``,
    which **on Linux** defaults to ``'fork'``.

    However, it is advised to **not** use this default; rather, **always** use the "spawn" context.
    There are some references on this topic; for example, see `this article <https://pythonspeed.com/articles/python-multiprocessing/>`_
    and `this StackOverflow thread <https://stackoverflow.com/questions/64095876/multiprocessing-fork-vs-spawn>`_.

    So, multiprocessing code is better written this way::

        import multiprocessing
        ctx = multiprocessing.get_context('spawn')
        q = ctx.Queue(...)
        e = ctx.Event(...)
        p = ctx.Process(..., args=(q, e))
        ...

    where ``ctx`` is an instance of ``multiprocessing.context.SpawnContext``.

    ``mpservice.multiprocessing.SpawnContext`` inherits from the standard ``SpawnContext``
    and provides some enhancements. (The main enhancement is to use the custom :class:`mpservice.multiprocessing.SpawnProcess`
    for process creation.) The constant ``mpservice.multiprocessing.MP_SPAWN_CTX``
    points to an instance of this class. With these facilities, the code above can be replaced by this::

        from mpservice.multiprocessing import MP_SPAWN_CTX as ctx
        q = ctx.Queue(...)
        e = ctx.Event(...)
        p = ctx.Process(..., args=(q, e))
        ...

    However, there is an annoyance here: ``ctx.Queue``, ``ctx.Event``, and some others are not **classes**,
    but rather **factory methods**. As a result, they can not be used to annotate the type of the objects
    created by them. (Similarly, if you do ``from multiprocessing import Queue, Event``, you are getting
    factory methods, not classes. The actual classes, ``multiprocessing.queues.Queue`` and ``multiprocessing.synchronize.Event``,
    have a **required** parameter ``ctx``, hence are somewhat inconvenient to use.)

    The module ``mpservice.multiprocessing`` breaks from the standard ``multiprocessing`` API design to alleviate this problem.
    It provides custom classes :class:`Queue`, :class:`Event`, and some others, that have an **optional**
    rather than required parameter ``ctx``, which defaults to, you guessed it, ``mpservice.multiprocessing.MP_SPAWN_CTX``.
    With this, the code above is better yet written this way::

        from mpservice.multiprocessing import Queue, Event, Process
        q: Queue = Queue(...)
        e: Event = Event(...)
        p: Process = Process(..., args=(q, e))

    .. note:: Recommendations on the use of ``MP_SPAWN_CTX``: use the classes ``Process``, ``Manager``, ``Lock``,
        ``RLock``, ``Condition``, ``Semaphore``, ``BoundedSemaphore``, ``Event``, ``Barrier``,
        ``Queue``, ``JoinableQueue``, ``SimpleQueue``, ``Pool`` diretly to create objects and type-annote them;
        this is preferred over ``MP_SPAWN_CTX.Process``, ``MP_SPAWN_CTX.Manager``, etc, although they would work, too.
        A few other methods of ``SpawnContext`` are not exposed in ``mpservice.multiprocessing``;
        you may use them via the object ``MP_SPAWN_CTX``.
    """

    Process = SpawnProcess

    def get_context(self, method=None):
        if method is None or method == 'spawn':
            return self
        return super().get_context(method)


# MP_SPAWN_CTX = multiprocessing.context.DefaultContext(SpawnContext())
# The version above would fail `tests/test_streamer.py::test_parmap`. I don't know why.
MP_SPAWN_CTX = SpawnContext()
