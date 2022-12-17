"""
The module ``mpservice.streamer`` provides utilities for stream processing with threading or multiprocessing concurrencies.

An input data stream goes through a series of operations.
The output from one operation becomes the input to the next operation.
One or more "primary" operations are so heavy
that they can benefit from concurrency via threading
(if they are I/O bound) or multiprocessing (if they are CPU bound).
These operations are called "transform"s.

The other operations are typically light weight and supportive
of the primary transforms. These operations perform batching,
unbatching, buffering, filtering, logging, etc.
"""


# Iterable vs iterator
#
# if we need to use
#
#   for v in X:
#       ...
#
# then `X.__iter__()` is called to get an "iterator".
# In this case, `X` is an "iterable", and it must implement `__iter__`.
#
# If we do not use it that way, but rather only directly call
#
#   next(X)
#
# then `X` must implement `__next__` (but does not need to implement `__iter__`).
# This `X` is an "iterator".
#
# Often we let `__iter__` return `self`, and implement `__next__` in the same class.
# This way the class is both an iterable and an iterator.

# Async generator returns an async iterator.

from __future__ import annotations

# Enable using a class in type annotations in the code
# that defines that class itself.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import concurrent.futures
import os
import queue
import threading
import traceback
from collections import deque
from collections.abc import Iterable, Iterator, Sequence
from multiprocessing.util import Finalize
from random import random
from typing import (
    Callable,
    TypeVar,
    Optional,
    Any,
)

from deprecation import deprecated
from overrides import EnforceOverrides, overrides, final

from .util import is_remote_exception, get_remote_traceback
from .util import is_exception, Thread, MP_SPAWN_CTX
from ._queues import SingleLane


FINISHED = "8d906c4b-1161-40cc-b585-7cfb012bca26"
STOPPED = "ceccca5e-9bb2-46c3-a5ad-29b3ba00ad3e"
CRASHED = "57cf8a88-434e-4772-9bca-01086f6c45e9"


T = TypeVar("T")  # indicates input data element
TT = TypeVar("TT")  # indicates output after an op on `T`


class Streamer(EnforceOverrides, Iterator):
    """
    The class ``Streamer`` is the "entry-point" for the "streamer" utilities.
    User constructs a ``Streamer`` object
    by passing an `Iterable`_ to it, then calls its methods to use it.
    """

    def __init__(self, instream: Iterator | Iterable, /):
        """
        Parameters
        ----------
        instream
            The input stream of elements. This is an `Iterable`_ or an `Iterator`_,
            which could be unlimited.
        """
        self.streamlets: list[Stream] = [Stream(instream)]
        self._started = False

    def __enter__(self):
        self._started = True
        return self

    def __exit__(self, *args, **kwargs):
        for s in self.streamlets:
            s._stop()

    @final
    def __iter__(self):
        if not self._started:
            raise RuntimeError(
                "iteration must be started within context manager, i.e. within a 'with ...:' block"
            )
        return self

    @final
    def __next__(self):
        return self.streamlets[-1].__next__()

    def drain(self) -> int:
        """Drain off the stream and return the number of elements processed.

        This method is for the side effect: the entire stream has been processed
        by all the operations and results have been taken care of, for example,
        the final operation may have saved results in a database.

        If you need more info about the processing, such as inspecting exceptions
        as they happen (if ``return_expections`` to :meth:`parmap` is ``True``),
        then don't use this method. Instead, iterate the streamer yourself
        and do whatever you need to do.
        """
        n = 0
        for _ in self:
            n += 1
        return n

    def map(self, func: Callable[[T], Any], /, *args, **kwargs):
        """Perform a simple transformation on each data element.

        This operation happens "inline"--there is no other threads or processes
        used. For this reason, the method is for "light-weight" transforms.

        This is a 1-to-1 transform from the input stream to the output stream.
        This method can neither add nor skip elements in the stream.

        If the logic needs to keep some state or history info, then define a class and implement
        its ``__call__`` method.
        For example, to print out every hundredth value for information::

            class Peek:
                def __init__(self):
                    self._count = 0

                def __call__(self, x):
                    self._count += 1
                    if self._count == 100:
                        print(x)
                        self._count = 0
                    return x

            obj.map(Peek())

        (This functionality is already provided by :meth:`peek`.)

        Parameters
        ----------
        func
            A function that takes a data element and returns a new value; the new values
            (which do not have to differ from the original) form the new stream going
            forward.
        *args
            Additional position arguments to ``func``, after the first argument, which
            is the data element.
        **kwargs
            Additional keyword arguments to ``func``.
        """
        self.streamlets.append(Mapper(self.streamlets[-1], func, *args, **kwargs))
        return self

    def filter(self, func: Callable[[T], bool], /, *args, **kwargs):
        """Select data elements to keep in the stream.

        This method can be used to either "keep" or "drop" elements according to various conditions.
        If the logic needs to keep some state or history info, then define a class and implement
        its ``__call__`` method.
        For example, to "drop the first 100 elements"::

            class Drop:
                def __init__(self, n):
                    self._n = n
                    self._count = 0

                def __call__(self, x):
                    self._count += 1
                    if self._count <= self._n:
                        return False
                    return True

            obj.filter(Drop(100))

        Parameters
        ----------
        func
            A function that takes a data element and returns ``True`` or ``False`` to indicate
            the element should be kept in or dropped from the stream.

            This function should not make changes to the input data element.
        *args
            Additional position arguments to ``func``, after the first argument, which
            is the data element.
        **kwargs
            Additional keyword arguments to ``func``.
        """
        self.streamlets.append(Filter(self.streamlets[-1], func, *args, **kwargs))
        return self

    def filter_exceptions(
        self,
        keep_exc_types: Optional[
            type[BaseException] | tuple[type[BaseException], ...]
        ] = None,
        drop_exc_types: Optional[
            type[BaseException] | tuple[type[BaseException], ...]
        ] = None,
    ):
        """
        If a call to :meth:`transform` upstream has specified ``return_exceptions=True``,
        then its output stream may contain ``Exception`` objects.

        This method works on the output stream and determines which ``Exception`` objects
        should be dropped, kept, or raised.

        The default behavior (both ``keep_exc_types`` and ``drop_exc_types`` are ``None``)
        is to drop all exception objects from the stream.

        Parameters
        ----------
        keep_exc_types
            These types of exceptions are kept in the stream.

            If ``None`` (the default), no exception object is kept in the stream.
        drop_exc_types
            These types of exceptions are dropped from the stream.

            If ``None`` (the default), then all exceptions except those specified by
            ``keep_exc_types`` are dropped.
            If you want to drop only subclasses of ``Exception`` , then provide ``Exception``.

            If you want no exception object to be dropped (then the only choice is between "kept" and "raised"),
            then provide ``()``.

            The members in ``keep_exc_types`` and ``drop_exc_types`` should be distinct.
            If there is any common member, then the one in ``keep_exc_types`` takes effect.

            An exception object that is neither dropped nor kept will be raised.
        """

        def foo(x):
            if is_exception(x):
                if keep_exc_types is not None and isinstance(x, keep_exc_types):
                    return True
                if drop_exc_types is None:
                    return False
                if isinstance(x, drop_exc_types):
                    return False
                raise x
            return True

        return self.filter(foo)

    def peek(
        self,
        *,
        print_func: Optional[Callable[[str], None]] = None,
        interval: int | float = 1000,
        exc_types: Optional[Sequence[type[BaseException]]] = None,
        with_exc_tb: bool = True,
    ):
        """Take a peek at the data element *before* it continues in the stream.

        This implements several info printouts. If this can not do what you need, just create your own
        function to pass to :meth:`map`.

        Parameters
        ----------
        print_func
            A function that will be used to print messages.
            This should take a str and return nothing.

            The default is the built-in ``print``. It's often useful
            to pass in logging function such as ``logger.info``.
        interval
            Print out the data element at this interval. The default is 1000,
            that is, print every 1000th elment.

            If it is a float, then it must be between 0 and 1 open-open.
            This will be take as the (target) fraction of elements that are printed.

            To print out every element, pass in ``1``.

            To turn off the printouts by this condition, pass in ``None``.
        exc_types
            If the element is an exception object of these types, print it out.

            If ``None`` (the default), all exception objects are printed.
            This is regardless of the ``interval`` value.

            To turn off the printouts by this condition, pass in ``()``.
            In that case, the value will still be printed according to the ``interval``
            condition, but the printout will not be tailored to the fact that
            it is an exception object; in other words, there will be no printing
            of the traceback.
        with_exc_tb
            When an exception object is printed, should the traceback be printed
            as well, if available? Default is ``True``.
        """
        if interval is not None:
            if isinstance(interval, float):
                assert 0 < interval < 1
            else:
                assert isinstance(interval, int)
                assert interval >= 1

        class Peeker:
            def __init__(self):
                self._idx = 0
                self._print_func = print if print_func is None else print_func
                self._interval = interval
                self._exc_types = exc_types
                self._with_trace = with_exc_tb

            def __call__(self, x):
                self._idx += 1
                if is_exception(x):
                    if self._exc_types is None or isinstance(x, self._exc_types):
                        trace = ""
                        if self._with_trace:
                            if is_remote_exception(x):
                                trace = get_remote_traceback(x)
                            else:
                                try:
                                    trace = "".join(
                                        traceback.format_tb(x.__traceback__)
                                    )
                                except AttributeError:
                                    pass
                        if trace:
                            self._print_func("#%d:  %r\n%s" % (self._idx, x, trace))
                        else:
                            self._print_func("#%d:  %r" % (self._idx, x))
                        return x
                if self._interval is not None:
                    if self._interval >= 1:
                        if self._idx % self._interval == 0:
                            self._print_func(f"#{self._idx}:  {x!r}")
                    else:
                        if random() < self._interval:
                            self._print_func(f"#{self._idx}:  {x!r}")
                return x

        return self.map(Peeker())

    @deprecated(
        deprecated_in="0.11.8", removed_in="0.12.0", details="Use ``peek`` instead."
    )
    def peek_every_nth(self, n: int):
        return self.peek(interval=n)

    def head(self, n: int):
        """
        Take the first ``n`` elements and ignore the rest.

        This does not delegate to ``filter``, because ``filter``
        would need to walk through the entire stream,
        which is not needed for ``head``.
        """
        self.streamlets.append(Header(self.streamlets[-1], n))
        return self

    def tail(self, n: int):
        """
        Keep the last ``n`` elements and ignores all the previous ones.
        If there are less than ``n`` data elements in total, then keep all of them.

        .. note:: ``n`` data elements need to be kept in memory, hence ``n`` should
            not be "too large" for the typical size of the data elements.
        """
        self.streamlets.append(Tailor(self.streamlets[-1], n))
        return self

    def groupby(self, func: Callable[[T], Any], /, *args, **kwargs):
        """
        ``func`` takes a data element and outputs a value.
        **Consecutive** elements that have the same value of this output
        will be grouped into a list.

        Following this operator, every element in the stream is a list of length 1 or more.

        The output of ``func`` is usually a str or int or a tuple of a few strings or ints.

        ``*args`` and ``**kwargs`` are additional arguments to ``func``.

        A group will be kept in memory until it is concluded (i.e. the next element
        starts a new group). For this reason, the groups should not be too large
        for the typical size of the data element.
        """
        self.streamlets.append(Groupby(self.streamlets[-1], func, *args, **kwargs))
        return self

    def batch(self, batch_size: int):
        """Bundle elements into batches, i.e. lists.

        Take elements from an input stream,
        and bundle them up into batches up to a size limit,
        and produce the batches.

        The output batches are all of the specified size,
        except possibly the final batch.
        There is no 'timeout' logic to proceed eagerly with a partial batch.
        For efficiency, this requires the input stream to have a steady supply.
        If that is a concern, having a ``buffer`` on the input stream
        prior to ``batch`` may help.
        """
        self.streamlets.append(Batcher(self.streamlets[-1], batch_size))
        return self

    def unbatch(self):
        """Reverse of ``batch``.

        Turn a stream of lists into a stream of individual elements.

        This is usually used to correspond with a previous
        ``.batch()``, but that is not required. The only requirement
        is that the input elements are lists.
        """
        self.streamlets.append(Unbatcher(self.streamlets[-1]))
        return self

    def buffer(self, maxsize: int):
        """Buffer is used to stabilize and improve the speed of data flow.

        A buffer is useful after any operation that can not guarantee
        (almost) instant availability of output. A buffer allows its
        output to "pile up" when the downstream consumer is slow,
        so that data *is* available when the downstream does come to request
        data. The buffer evens out irregularities in the speeds of upstream
        production and downstream consumption.
        """
        self.streamlets.append(Buffer(self.streamlets[-1], maxsize))
        return self

    @deprecated(
        deprecated_in="0.11.8", removed_in="0.12.0", details="Use ``parmap`` instead"
    )
    def transform(self, *args, **kwargs):
        return self.parmap(*args, **kwargs)

    def parmap(
        self,
        func: Callable[[T], TT],
        /,
        *,
        executor: str = "thread",
        concurrency: Optional[int] = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        **func_args,
    ):
        """Apply a transformation on each element of the data stream,
        producing a stream of corresponding results.

        ``func``: a sync function that takes a single input item
        as the first positional argument and produces a result.
        Additional keyword args can be passed in via ``func_args``.
        Async can be supported, but it's a little more involved than the sync case.
        Since the need seems to be low, it's not supported for now.

        The outputs are in the order of the input elements.

        The main point of ``func`` does not have to be the output.
        It could rather be some side effect. For example,
        saving data in a database. In that case, the output may be
        ``None``. Regardless, the output is yielded to be consumed by the next
        operator in the pipeline. A stream of ``None``\\s could be used
        in counting, for example. The output stream may also contain
        Exception objects (if ``return_exceptions`` is ``True``), which may be
        counted, logged, or handled in other ways.

        ``executor``: either 'thread' or 'process'.

        ``concurrency``: max number of concurrent calls to ``func``.
        If ``None``, a default value is used.

        ``return_x``: if True, output stream will contain tuples ``(x, y)``;
        if ``False``, output stream will contain ``y`` only.

        ``return_exceptions``: if True, exceptions raised by ``func`` will be
        in the output stream as if they were regular results; if False,
        they will propagate. Note that this does not absorb exceptions
        raised by previous components in the pipeline; it is concerned about
        exceptions raised by ``func`` only.

        User may want to add a ``buffer`` to the output of this method,
        esp if the ``func`` operations are slow.
        """
        self.streamlets.append(
            Parmapper(
                self.streamlets[-1],
                func,
                executor=executor,
                concurrency=concurrency,
                return_x=return_x,
                return_exceptions=return_exceptions,
                **func_args,
            )
        )
        return self


class Stream(EnforceOverrides):
    def __init__(self, instream: Stream | Iterator | Iterable, /):
        if hasattr(instream, "__next__"):
            self._instream = instream
        else:
            self._instream = iter(instream)
        self._stopped = threading.Event()

    def _stop(self):
        # Clean up and deal with early-stop situations.
        self._stopped.set()

    @final
    def __iter__(self):
        return self

    def _get_next(self):
        """Produce the next element in the stream.

        Subclasses refine this method rather than ``__next__``.
        In a subclass, almost always it will not get the next element
        from ``self._instream``, but rather from some object that holds
        results of transformations on ``self._instream``.
        In other words, subclass typically needs to override this method.
        Subclass should take care to handle exceptions in this method.
        """
        if self._stopped.is_set():
            raise StopIteration
        z = next(self._instream)
        return z

    @final
    def __next__(self):
        z = self._get_next()
        return z


class Mapper(Stream):
    def __init__(self, instream: Stream, func: Callable[[T], Any], *args, **kwargs):
        super().__init__(instream)
        self.func = func
        self._args = args
        self._kwargs = kwargs

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        return self.func(next(self._instream), *self._args, **self._kwargs)


class Filter(Stream):
    def __init__(self, instream: Stream, func: Callable[[T], bool], /, *args, **kwargs):
        super().__init__(instream)
        self.func = func
        self._args = args
        self._kwargs = kwargs

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        while True:
            z = next(self._instream)
            if not self.func(z, *self._args, **self._kwargs):
                continue
            return z


class Header(Stream):
    def __init__(self, instream: Stream, /, n: int):
        """
        Keeps the first ``n`` elements and ignores all the rest.
        """
        assert n > 0
        super().__init__(instream)
        self.n = n
        self._n = 0

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        if self._n >= self.n:
            raise StopIteration
        self._n += 1
        return next(self._instream)


class Tailor(Stream):
    def __init__(self, instream: Stream, /, n: int):
        """
        Keeps the last ``n`` elements and ignores all the previous ones.
        If there are less than ``n`` data elements in total, then keep all of them.

        .. note:: ``n`` data elements need to be kept in memory, hence ``n`` should
            not be "too large" for the typical size of the data elements.
        """
        super().__init__(instream)
        assert n > 0
        self.n = n
        self._data = deque(maxlen=n)
        self._data_collected = False

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        if not self._data_collected:
            data = self._data
            for v in self._instream:
                data.append(v)
            self._data_collected = True
        try:
            return self._data.popleft()
        except IndexError:
            # No more data.
            raise StopIteration


class Groupby(Stream):
    def __init__(self, instream: Stream, /, func: Callable[[T], Any], *args, **kwargs):
        super().__init__(instream)
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._z = object()
        self._group = None

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration

        while True:
            try:
                x = next(self._instream)
                z = self._func(x, *self._args, **self._kwargs)
                if z == self._z:
                    self._group.append(x)
                else:
                    if self._group is None:
                        self._group = [x]
                    else:
                        batch = self._group
                        self._group = [x]
                        self._z = z
                        return batch
            except StopIteration:
                self._stop()
                break
        if self._group:
            return self._group
        raise StopIteration


# Alternatively, this can be implemented by ``groupby``.
class Batcher(Stream):
    def __init__(self, instream: Stream, /, batch_size: int):
        """
        Aggregate the stream into batches (lists) of the specified size.
        The last batch may have less elements.
        """
        super().__init__(instream)
        assert 1 <= batch_size <= 10_000
        self.batch_size = batch_size

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        batch = []
        for _ in range(self.batch_size):
            try:
                batch.append(next(self._instream))
            except StopIteration:
                self._stop()
                break
        if batch:
            return batch
        raise StopIteration


class Unbatcher(Stream):
    def __init__(self, instream: Stream, /):
        """
        The incoming stream consists of lists.
        This object "expands" or "flattens" the lists into a stream
        of individual elements. Usually, the output stream
        is "longer" than the input stream.

        This may correspond to a "Batcher" operator upstream,
        but that is by no means a requirement.
        """
        super().__init__(instream)
        self._batch = None

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        if self._batch:
            return self._batch.pop(0)
        z = next(self._instream)
        if is_exception(z):
            return z
        self._batch = z
        if self._stopped.is_set():
            raise StopIteration
        return self._batch.pop(0)


class Buffer(Stream):
    def __init__(self, instream: Stream, /, maxsize: int):
        super().__init__(instream)
        assert 1 <= maxsize <= 10_000
        self.maxsize = maxsize
        self._tasks = SingleLane(maxsize)
        self._worker = Thread(target=self._start, loud_exception=False)
        self._worker.start()
        self._finalize_func = Finalize(
            self,
            type(self)._finalize,
            (self._stopped, self._tasks, self._worker),
            exitpriority=10,
        )

    def _start(self):
        threading.current_thread().name = "BufferThread"
        q = self._tasks
        while True:
            try:
                v = next(self._instream)
            except StopIteration:
                q.put(FINISHED)
                break
            except Exception:
                q.put(STOPPED)
                raise
            if self._stopped.is_set():
                break
            q.put(v)  # if `q` is full, will wait here

    @overrides
    def _stop(self):
        self._finalize_func()

    @staticmethod
    def _finalize(stopped, tasks, worker):
        stopped.set()
        while not tasks.empty():
            _ = tasks.get()
        # `tasks` is now empty. The thread needs to put at most one
        # more element into the queue, which is safe.
        worker.join()

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        while True:
            try:
                z = self._tasks.get(timeout=1)
                break
            except queue.Empty as e:
                if self._worker.is_alive():
                    continue
                if self._stopped.is_set():
                    raise StopIteration
                if self._worker.exception():
                    raise self._worker.exception()
                raise RuntimeError("unknown situation occurred") from e
        if z == FINISHED:
            self._stop()
            raise StopIteration
        if z == STOPPED:
            self._stop()
            raise self._worker.exception()
        return z


class Parmapper(Stream):
    def __init__(
        self,
        instream: Stream,
        func: Callable[[T], TT],
        *,
        executor: str = "thread",  # or 'process'
        concurrency: Optional[int] = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        **func_args,
    ):
        super().__init__(instream)

        if concurrency is None:
            if executor == "thread":
                concurrency = min(32, (os.cpu_count() or 1) + 4)
            else:
                concurrency = os.cpu_count() or 1
        else:
            assert concurrency > 0
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        if executor == "thread":
            self._executor = concurrent.futures.ThreadPoolExecutor(concurrency)
        else:
            assert executor == "process"
            self._stopped = MP_SPAWN_CTX.Event()
            self._executor = concurrent.futures.ProcessPoolExecutor(
                concurrency, mp_context=MP_SPAWN_CTX
            )
        self._tasks = SingleLane(concurrency)
        self._worker = Thread(
            target=self._start, args=(func,), kwargs=func_args, loud_exception=False
        )
        self._worker.start()
        self._finalize_func = Finalize(
            self,
            type(self)._finalize,
            (self._stopped, self._tasks, self._worker, self._executor),
            exitpriority=10,
        )

    def _start(self, func, **kwargs):
        tasks = self._tasks
        while True:
            try:
                x = next(self._instream)
            except StopIteration:
                tasks.put(FINISHED)
                break
            except Exception:
                tasks.put(STOPPED)
                raise
            if self._stopped.is_set():
                break
            t = self._executor.submit(func, x, **kwargs)
            # The size of the queue `tasks` regulates how many
            # concurrent calls to `func` there can be.
            tasks.put((x, t))

    @overrides
    def _stop(self):
        self._finalize_func()

    @staticmethod
    def _finalize(stopped, tasks, worker, executor):
        stopped.set()
        while not tasks.empty():
            _ = tasks.get()
        worker.join()
        executor.shutdown()

    @overrides
    def _get_next(self):
        if self._stopped.is_set():
            raise StopIteration
        z = self._tasks.get()
        if z == FINISHED:
            self._stop()
            raise StopIteration
        if z == STOPPED:
            self._stop()
            raise self._worker.exception()

        x, fut = z
        try:
            y = fut.result()
            if self._return_x:
                return x, y
            return y
        except Exception as e:
            if self._return_exceptions:
                # TODO: think about when `e` is a "remote exception".
                if self._return_x:
                    return x, e
                return e
            else:
                self._stop()
                raise
