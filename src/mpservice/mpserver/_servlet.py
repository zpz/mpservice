from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from time import sleep
from typing import Literal

from mpservice.multiprocessing import Process
from mpservice.multiprocessing.remote_exception import EnsembleError, RemoteException
from mpservice.threading import Thread

from ._worker import Worker, _SimpleProcessQueue, _SimpleThreadQueue

logger = logging.getLogger(__name__)


class Servlet(ABC):
    """
    A :class:`Servlet` manages the execution of one or more :class:`Worker`.
    We make a distinction between "simple" servlets, including :class:`ProcessServlet` and :class:`ThreadServlet`,
    and "compound" servlets, including :class:`SequentialServlet`, :class:`EnsembleServlet`,
    and :class:`SwitchServlet`.

    A simple servlet arranges to execute one :class:`Worker` in requested number of processes (or threads).
    Optionally, it can specify exactly which CPU(s) each worker process should use.
    Each input item is passed to and processed by exactly one of the processes (or threads).

    A compound servlet arranges to execute multiple :class:`Servlet`\\s as a sequence or an ensemble.
    In addition, there is :class:`SwitchServlet` that acts as a "switch"
    in front of a set of Servlets.
    There's a flavor of recursion in this definition in that a member servlet can very well be
    a compound servlet.

    Great power comes from this recursive definition.
    In principle, we can freely compose and nest the :class:`Servlet` types.
    For example, suppose `W1`, `W2`,..., are :class:`Worker` subclasses,
    then we may design such a workflow,

    ::

        s = SequentialServlet(
                ProcessServlet(W1),
                EnsembleServlet(
                    ThreadServlet(W2),
                    SequentialServlet(ProcessServlet(W3), ThreadServlet(W4)),
                    ),
                EnsembleServlet(
                    Sequetial(ProcessServlet(W5), ProcessServlet(W6)),
                    Sequetial(ProcessServlet(W7), ThreadServlet(W8), ProcessServlet(W9)),
                    ),
            )
    """

    @abstractmethod
    def start(self, q_in, q_out) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """
        When this method is called, there shouldn't be "pending" work
        in the servlet. If for whatever reason, the servlet is in a busy,
        messy state, it is not required to "wait" for things to finish.
        The priority is to ensure the processes and threads are stopped.

        The primary mechanism to stop a servlet is to put
        the special constant ``None`` in the input queue.
        The user should have done that; but just to be sure,
        this method may do that again.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def input_queue_type(self) -> Literal['thread', 'process']:
        """
        Indicate whether the input queue can be a "thread queue"
        (i.e. ``queue.Queue``) or needs to be a "process queue"
        (i.e. ``multiprocessing.queues.Queue``).
        If "thread", caller can provide either a thread queue or a
        process queue. If "process", caller must provide a process queue.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def output_queue_type(self) -> Literal['thread', 'process']:
        """
        Indicate whether the output queue can be a "thread queue"
        (i.e. ``queue.Queue``) or needs to be a "process queue"
        (i.e. ``multiprocessing.queues.Queue``).
        If "thread", caller can provide either a thread queue or a
        process queue. If "process", caller must provide a process queue.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def workers(self) -> list[Worker]:
        raise NotImplementedError

    @property
    @abstractmethod
    def children(self) -> list:
        raise NotImplementedError

    def _debug_info(self) -> dict:
        zz = []
        for ch in self.children:
            if hasattr(ch, '_debug_info'):
                zz.append(ch._debug_info())
            else:
                zz.append(
                    {
                        'type': ch.__class__.__name__,
                        'name': ch.name,
                        'is_alive': ch.is_alive(),
                    }
                )
        return {
            'type': self.__class__.__name__,
            'children': zz,
        }


class ProcessServlet(Servlet):
    """
    Use this class if the operation is CPU bound.
    """

    def __init__(
        self,
        worker_cls: type[Worker],
        *,
        cpus: None | int | Sequence[None | int | Sequence[int]] = None,
        worker_name: str = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        worker_cls
            A subclass of :class:`Worker`.
        cpus
            Specifies how many processes to create and how they are pinned
            to specific CPUs.

            The default is ``None``, indicating a single unpinned process.

            If an int, indicating number of (unpinned) processes.

            Otherwise, a list specifiying CPU pinning.
            Each element of the list specifies the CPU pinning of one process.
            The number of processes created is the number of elements in ``cpus``.
            The CPU spec is very flexible. For example,

            ::

                cpus=[[0, 1, 2], [0], [2, 3], [4, 5, 6], 4, None]

            This instructs the servlet to create 6 processes, each running an instance
            of ``worker_cls``. The CPU affinity of each process is as follows:

            1. CPUs 0, 1, 2
            2. CPU 0
            3. CPUs 2, 3
            4. CPUs 4, 5, 6
            5. CPU 4
            6. Any CPU, no pinning
        worker_name
            Prefix to the name of the worker processes. If not provided, a default is
            constructed based on the class name.
        **kwargs
            Passed to the ``__init__`` method of ``worker_cls``.

        Notes
        -----
        When the servlet has multiple processes, the output stream does not need to follow
        the order of the elements in the input stream.
        """
        self._worker_cls = worker_cls
        if cpus is None:
            self._cpus = [None]
        else:
            if isinstance(cpus, int):
                cpus = [None for _ in range(cpus)]
            self._cpus = cpus
        self._init_kwargs = kwargs
        self._workers = []
        self._worker_name = worker_name
        self._started = False

    def start(self, q_in: _SimpleProcessQueue, q_out: _SimpleProcessQueue):
        """
        Create the requested number of processes, in each starting an instance
        of ``self._worker_cls``.

        Parameters
        ----------
        q_in
            A queue with input elements. Each element will be passed to and processed by
            exactly one worker process.
        q_out
            A queue for results.
        """
        assert not self._started
        basename = self._worker_name or f'{self._worker_cls.__name__}-process'
        for worker_index, cpu in enumerate(self._cpus):
            # Create as many processes as the length of `cpus`.
            # Each process is pinned to the specified cpu core.
            sname = f'{basename}-{worker_index}'
            logger.info('adding worker <%s> at CPU %s ...', sname, cpu)
            p = Process(
                target=self._worker_cls.run,
                name=sname,
                kwargs={
                    'q_in': q_in,
                    'q_out': q_out,
                    'worker_index': worker_index,
                    'cpu_affinity': cpu,
                    **self._init_kwargs,
                },
            )
            p.start()
            name = q_out.get()
            if name is None:
                p.join()  # this will raise exception b/c worker __init__ failed
            self._workers.append(p)
            logger.debug('   ... worker <%s> is ready', name)

        self._q_in = q_in
        self._q_out = q_out
        logger.info('servlet %s is ready', self._worker_cls.__name__)
        self._started = True

    def stop(self):
        """Stop the workers."""
        assert self._started
        self._q_in.put(None)
        for w in self._workers:
            w.join()
        self._workers = []
        self._started = False

    @property
    def input_queue_type(self):
        return 'process'

    @property
    def output_queue_type(self):
        return 'process'

    @property
    def workers(self):
        return self._workers

    @property
    def children(self):
        return self._workers


class ThreadServlet(Servlet):
    """
    Use this class if the operation is I/O bound (e.g. calling an external service
    to get some info), and computation is very light compared to the I/O part.
    Another use-case of this class is to perform some very simple and quick
    pre-processing or post-processing.
    """

    def __init__(
        self,
        worker_cls: type[Worker],
        *,
        num_threads: None | int = None,
        worker_name: str | None = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        worker_cls
            A subclass of :class:`Worker`
        num_threads
            The number of threads to create. Each thread will host and run
            an instance of ``worker_cls``. Default ``None`` means 1.
        worker_name
            Prefix to the name of the worker threads. If not provided, a default is
            constructed based on the class name.
        **kwargs
            Passed on the ``__init__`` method of ``worker_cls``.

        Notes
        -----
        When the servlet has multiple threads, the output stream does not need to follow
        the order of the elements in the input stream.
        """
        self._worker_cls = worker_cls
        self._num_threads = num_threads or 1
        self._init_kwargs = kwargs
        self._workers = []
        self._worker_name = worker_name
        self._started = False

    def start(
        self,
        q_in: _SimpleProcessQueue | _SimpleThreadQueue,
        q_out: _SimpleProcessQueue | _SimpleThreadQueue,
    ):
        """
        Create the requested number of threads, in each starting an instance
        of ``self._worker_cls``.

        Parameters
        ----------
        q_in
            A queue with input elements. Each element will be passed to and processed by
            exactly one worker thread.
        q_out
            A queue for results.

            ``q_in`` and ``q_out`` are either :class:`_SimpleProcessQueue`\\s (for processes)
            or :class:`_SimpleThreadQueue`\\s (for threads). Because this servlet may be connected to
            either :class:`ProcessServlet`\\s or :class:`ThreadServlet`\\s, either type of queues may
            be appropriate. In contrast, for :class:`ProcessServlet`, the input and output
            queues are both :class:`_SimpleProcessQueue`\\s.
        """
        assert not self._started
        basename = self._worker_name or f'{self._worker_cls.__name__}-thread'
        for ithread in range(self._num_threads):
            sname = f'{basename}-{ithread}'
            logger.info('adding worker <%s> in thread ...', sname)
            w = Thread(
                target=self._worker_cls.run,
                name=sname,
                kwargs={
                    'q_in': q_in,
                    'q_out': q_out,
                    'worker_index': ithread,
                    **self._init_kwargs,
                },
            )
            w.start()
            name = q_out.get()
            if name is None:
                w.join()  # this will raise exception b/c worker __init__ failed
            self._workers.append(w)
            logger.debug('   ... worker <%s> is ready', name)

        self._q_in = q_in
        self._q_out = q_out
        logger.info('servlet %s is ready', self._worker_cls.__name__)
        self._started = True

    def stop(self):
        """Stop the worker threads."""
        assert self._started
        self._q_in.put(None)
        for w in self._workers:
            w.join()
        self._workers = []
        self._started = False

    @property
    def input_queue_type(self):
        return 'thread'

    @property
    def output_queue_type(self):
        return 'thread'

    @property
    def workers(self):
        return self._workers

    @property
    def children(self):
        return self._workers


class SequentialServlet(Servlet):
    """
    A ``SequentialServlet`` represents
    a sequence of operations performed in order,
    one operations's output becoming the next operation's input.

    Each operation is performed by a "servlet", that is, an instance of any subclass
    of :class:`Servlet`, including :class:`SequentialServlet`.
    However, a `SequentialServlet` as a direct member of another `SequentialServlet`
    may not be beneficial---you may as well flatten it out.

    If any member servlet has multiple workers (threads or processes),
    the output stream does not need to follow the order of the elements in the input stream.
    """

    def __init__(self, *servlets: Servlet):
        assert len(servlets) > 0
        self._servlets = servlets
        self._qs = []
        self._started = False

    def start(self, q_in, q_out):
        """
        Start the member servlets.

        A main concern is to connect the servlets by "pipes", or queues,
        for input and output, in addition to the very first ``q_in``,
        which carries input items from the "outside world" and the very last ``q_out``,
        which sends output items to the "outside world".

        Each item in ``q_in`` goes to the first member servlet;
        the result goes to the second servlet; and so on.
        The result out of the last servlet goes to ``q_out``.

        The types of ``q_in`` and ``q_out`` are decided by the caller.
        The types of intermediate queues are decided within this function.
        As a rule, use :class:`_SimpleThreadQueue` between two threads; use :class:`_SimpleProcessQueue`
        between two processes or between a process and a thread.
        """
        assert not self._started
        nn = len(self._servlets)
        q1 = q_in
        for i, s in enumerate(self._servlets):
            if i + 1 < nn:  # not the last one
                if (
                    s.output_queue_type == 'thread'
                    and self._servlets[i + 1].input_queue_type == 'thread'
                ):
                    q2 = _SimpleThreadQueue()
                else:
                    q2 = _SimpleProcessQueue()
                self._qs.append(q2)
            else:
                q2 = q_out
            s.start(q1, q2)
            q1 = q2
        self._q_in = q_in
        self._q_out = q_out
        self._started = True

    def stop(self):
        """Stop the member servlets."""
        assert self._started
        for s in self._servlets:
            s.stop()
        self._qs = []
        self._started = False

    @property
    def workers(self):
        return [w for s in self._servlets for w in s.workers]

    @property
    def children(self):
        return self._servlets

    @property
    def input_queue_type(self):
        return self._servlets[0].input_queue_type

    @property
    def output_queue_type(self):
        return self._servlets[-1].output_queue_type


class EnsembleServlet(Servlet):
    """
    A ``EnsembleServlet`` represents
    an ensemble of operations performed in parallel on each input item.
    The list of results, corresponding to the order of the operators,
    is returned as the result.

    Each operation is performed by a "servlet", that is, an instance of
    any subclass of :class:`Servlet`.

    If ``fail_fast`` is ``True`` (the default), once one ensemble member raises an
    Exception, an ``EnsembleError`` will be returned. Results of the other ensemble members
    that arrive afterwards will be ignored. This is not necessarily "fast"; the main point
    is that the item in question results in an Exception rather than a list that contains
    Exception(s).

    If ``fail_fast`` is ``False``, then Exception results, if any, are included
    in the result list. If all entries in the list are Exceptions, then the result list
    is replaced by an ``EnsembleError``. Here is the logic: if the result list contains
    some valid results and some Exceptions, user may consider it "partially" successful
    and may choose to make use of the valid results in some way; if every ensemble member has failed,
    then the ensemble has failed, hence the result is a single ``EnsembleError``.

    The output stream does not need to follow the order of the elements in the input stream.
    """

    def __init__(self, *servlets: Servlet, fail_fast: bool = True):
        assert len(servlets) > 1
        self._servlets = servlets
        self._started = False
        self._fail_fast = fail_fast

    def _reset(self):
        self._qin = None
        self._qout = None
        self._qins = []
        self._qouts = []
        self._uid_to_results = {}
        self._threads = []

    def start(self, q_in, q_out):
        """
        Start the member servlets.

        A main concern is to wire up the parallel execution of all the servlets
        on each input item.

        ``q_in`` and ``q_out`` contain inputs from and outputs to
        the "outside world". Their types, either :class:`_SimpleProcessQueue` or :class:`_SimpleThreadQueue`,
        are decided by the caller.
        """
        assert not self._started
        self._reset()
        self._qin = q_in
        self._qout = q_out
        for s in self._servlets:
            q1 = (
                _SimpleThreadQueue()
                if s.input_queue_type == 'thread'
                else _SimpleProcessQueue()
            )
            q2 = (
                _SimpleThreadQueue()
                if s.output_queue_type == 'thread'
                else _SimpleProcessQueue()
            )
            s.start(q1, q2)
            self._qins.append(q1)
            self._qouts.append(q2)
        t = Thread(target=self._dequeue, name=f'{self.__class__.__name__}._dequeue')
        t.start()
        self._threads.append(t)
        t = Thread(target=self._enqueue, name=f'{self.__class__.__name__}._enqueue')
        t.start()
        self._threads.append(t)
        self._started = True

    def _enqueue(self):
        qin = self._qin
        qout = self._qout
        qins = self._qins
        catalog = self._uid_to_results
        nn = len(qins)
        while True:
            z = qin.get()
            if z is None:  # sentinel for end of input
                for q in qins:
                    q.put(z)  # send the same sentinel to each servlet
                # qout.put(z)
                return

            uid, x = z
            if isinstance(x, BaseException):
                x = RemoteException(x)
            if isinstance(x, RemoteException):
                # short circuit exception to the output queue
                qout.put((uid, x))
                continue

            z = {'y': [None] * nn, 'n': 0}
            # `y` is the list of outputs from the servlets, in order.
            # `n` is number of outputs already received.
            catalog[uid] = z
            for q in qins:
                q.put((uid, x))
                # Element in the queue to each servlet is in the standard format,
                # i.e. a (ID, value) tuple.

    def _dequeue(self):
        qout = self._qout
        qouts = self._qouts
        catalog = self._uid_to_results
        fail_fast = self._fail_fast

        nn = len(qouts)
        while True:
            all_empty = True
            for idx, q in enumerate(qouts):
                while not q.empty():
                    # Get all available results out of this queue.
                    # They are for different requests.
                    # This is needed because the output elements in a member servlet
                    # may not follow the order of the input elements.
                    # If they did, we could just take the head of the output stream
                    # of each member servlet, and assemble them as the output for one
                    # input element.
                    all_empty = False
                    v = q.get()
                    if v is None:
                        qout.put(v)
                        return
                        # TODO: this is a little problematic---should we
                        # wait for all ensemble members to see `None`, thus
                        # "driving out" all regular work, before exiting?

                    uid, y = v
                    # `y` can be an exception object or a regular result.
                    z = catalog.get(uid)
                    if z is None:
                        # The entry has been removed due to "fail fast"
                        continue

                    if isinstance(y, BaseException):
                        y = RemoteException(y)

                    z['y'][idx] = y
                    z['n'] += 1

                    if fail_fast and isinstance(y, RemoteException):
                        # If fail fast, then the first exception causes
                        # this to return an EnsembleError as result.
                        catalog.pop(uid)
                        try:
                            raise EnsembleError(z)
                        except Exception as e:
                            qout.put((uid, RemoteException(e)))
                    elif z['n'] == nn:
                        # All results for this request have been collected.
                        # If the results contain any exception member, then
                        # ``fail_fast`` must be ``False``. In this case,
                        # the result list is replaced by an EnsembleError
                        # only if all members are exceptions.
                        catalog.pop(uid)
                        if all(isinstance(v, RemoteException) for v in z['y']):
                            try:
                                raise EnsembleError(z)
                            except Exception as e:
                                y = RemoteException(e)
                        else:
                            y = z['y']
                        qout.put((uid, y))

            if all_empty:
                sleep(0.005)  # TODO: what is a good duration?

    def stop(self):
        assert self._started
        for s in self._servlets:
            s.stop()
        self._qin.put(None)
        for t in self._threads:
            t.join()
        self._reset()
        self._started = False

    @property
    def workers(self):
        return [w for s in self._servlets for w in s.workers]

    @property
    def children(self):
        return self._servlets

    @property
    def input_queue_type(self):
        return 'thread'

    @property
    def output_queue_type(self):
        return 'thread'


class SwitchServlet(Servlet):
    """
    SwitchServlet contains multiple member servlets (which are provided to :meth:`__init__`).
    Each input element is passed to and processed by exactly one of the members
    based on the output of the method :meth:`switch`.

    This is somewhat analogous to the "switch" construct in some
    programming languages.
    """

    def __init__(self, *servlets: Servlet):
        assert len(servlets) > 0
        self._servlets = servlets
        self._started = False

    def _reset(self):
        self._qin = None
        self._qout = None
        self._qins = []
        self._thread_enqueue = None

    def start(self, q_in, q_out):
        assert not self._started
        self._reset()
        self._qin = q_in
        self._qout = q_out

        # Create one queue for each member servlet.
        # Any input element received from `q_in` is passed to
        # `self.switch` to determine which member servlet should
        # process this input; then the input is placed in
        # the appropriate queue.
        for s in self._servlets:
            q1 = (
                _SimpleThreadQueue()
                if s.input_queue_type == 'thread'
                else _SimpleProcessQueue()
            )
            s.start(q1, q_out)
            self._qins.append(q1)

        self._thread_enqueue = Thread(
            target=self._enqueue, name=f'{self.__class__.__name__}._enqueue'
        )
        self._thread_enqueue.start()
        self._started = True

    def stop(self):
        assert self._started
        for s in self._servlets:
            s.stop()
        self._qin.put(None)
        self._thread_enqueue.join()
        self._reset()
        self._started = False

    @abstractmethod
    def switch(self, x) -> int:
        """
        Parameters
        ----------
        x
          An element received via the input queue, that is,
          the parameter ``q_in`` to :meth:`start`. If the current servlet
          is preceded by another servlet, then ``x`` is an output of the
          other servlet.

          In principle, this method should not modify ``x``.
          It is expected to be a quick check on ``x`` to determine
          which member servlet should process it.

          ``x`` is never an instance of Exception or RemoteException.
          That case is already taken care of before this method is called.
          This method should not be used to handle such exception cases.

        Returns
        -------
        int
          The index of the member servlet that will receive and process ``x``.
          This number is between 0 and the number of member servlets minus 1,
          inclusive.
        """
        raise NotImplementedError

    def _enqueue(self):
        qin = self._qin
        qout = self._qout
        qins = self._qins
        while True:
            v = qin.get()
            if v is None:  # sentinel for end of input
                for q in qins:
                    q.put(v)  # send the same sentinel to each servlet
                # qout.put(v)
                return

            uid, x = v
            if isinstance(x, BaseException):
                x = RemoteException(x)
            if isinstance(x, RemoteException):
                # short circuit exception to the output queue
                qout.put((uid, x))
                continue

            # Determine which member servlet should process `x`:
            idx = self.switch(x)
            qins[idx].put((uid, x))

    @property
    def input_queue_type(self):
        return 'thread'

    @property
    def output_queue_type(self):
        if all(s.output_queue_type == 'thread' for s in self._servlets):
            return 'thread'
        return 'process'

    @property
    def workers(self):
        return [w for s in self._servlets for w in s.workers]

    @property
    def children(self):
        return self._servlets
