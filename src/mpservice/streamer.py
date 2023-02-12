"""
The module ``mpservice.streamer`` provides utilities for stream processing with threading or multiprocessing concurrencies.

An input data stream goes through a series of operations.
The output from one operation becomes the input to the next operation.
One or more "primary" operations are so heavy
that they can benefit from concurrency via threading
(if they are I/O bound) or multiprocessing (if they are CPU bound).
The other operations are typically light weight, although important in their own right.
These operations perform batching, unbatching, buffering, mapping, filtering, grouping, logging, etc.
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
import functools
import os
import queue
import threading
import traceback
from collections import deque
from collections.abc import Iterable, Sequence
from multiprocessing.util import Finalize
from random import random
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
    TypeVar,
)

from deprecation import deprecated

from ._queues import SingleLane
from .util import (
    MP_SPAWN_CTX,
    Thread,
    get_remote_traceback,
    is_exception,
    is_remote_exception,
)

FINISHED = "8d906c4b-1161-40cc-b585-7cfb012bca26"
STOPPED = "ceccca5e-9bb2-46c3-a5ad-29b3ba00ad3e"
CRASHED = "57cf8a88-434e-4772-9bca-01086f6c45e9"
NOTSET = object()


T = TypeVar("T")  # indicates input data element
TT = TypeVar("TT")  # indicates output after an op on `T`


class Streamer(Iterable):
    """
    The class ``Streamer`` is the "entry-point" for the "streamer" utilities.
    User constructs a ``Streamer`` object
    by passing an `Iterable`_ to it, then calls its methods to use it.

    .. doctest::
        :hide:

        from mpservice.streamer import Streamer

    """

    def __init__(self, instream: Iterable, /):
        """
        Parameters
        ----------
        instream
            The input stream of elements. This is an `Iterable`_ or an `Iterator`_,
            which could be unlimited.
        """
        self.streamlets: list[Stream] = [Stream(instream)]

    @deprecated(
        deprecated_in="0.11.9",
        removed_in="0.12.2",
        details="Please use the object directly without context manager.",
    )
    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def __iter__(self):
        yield from self.streamlets[-1]

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

    def collect(self) -> list:
        """Return all the elements in a list.

        .. warning:: Do not call this method on "big data".
        """
        return list(self)

    def map(self, func: Callable[[T], Any], /, **kwargs):
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
        *kwargs
            Additional keyword arguments to ``func``, after the first argument, which
            is the data element.
        """
        self.streamlets.append(Mapper(self.streamlets[-1], func, **kwargs))
        return self

    def filter(self, func: Callable[[T], bool], /, **kwargs):
        """Select data elements to keep in the stream according to the predicate ``func``.

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
        *kwargs
            Additional keyword arguments to ``func``, after the first argument, which
            is the data element.
        """
        self.streamlets.append(Filter(self.streamlets[-1], func, **kwargs))
        return self

    def filter_exceptions(
        self,
        drop_exc_types: Optional[
            type[BaseException] | tuple[type[BaseException], ...]
        ] = None,
        keep_exc_types: Optional[
            type[BaseException] | tuple[type[BaseException], ...]
        ] = None,
    ):
        """
        If a call to :meth:`parmap` upstream has specified ``return_exceptions=True``,
        then its output stream may contain ``Exception`` objects.
        Other methods such as :meth:`map` may also deliberately capture and return
        Exception objects.

        ``filter_exceptions`` determines which ``Exception`` objects in the stream
        should be dropped, kept, or raised.

        While :meth:`parmap` and other operators can choose to continue processing the stream
        by returning rather than raising exceptions,
        a subsequent ``filter_exceptions`` drops known types of exception objects
        so that the next operator does not receive ``Exception`` objects as inputs.

        The default behavior (both ``drop_exc_types`` and ``keep_exc_types`` are ``None``)
        is to raise any Exception object that is encountered.

        A useful pattern is to specify one or a few known exception types to drop,
        and crash on any other unexpected exception.

        Parameters
        ----------
        drop_exc_types
            These types of exceptions are dropped from the stream.
            These should be one or a few carefully identified exception types that
            you know can be safely ignored.

            If ``None`` (the default) or ``()`` or ``[]``, no exception object is dropped.

            To drop all exceptions, use ``Exception`` or even ``BaseException``.
        keep_exc_types
            These types of exceptions are kept in the stream.

            If ``None`` (the default), no exception object is kept.

            To keep all exceptions, use ``Exception`` or even ``BaseException``.

            An exception object that is neither kept nor dropped will be raised.

            .. note:: The members in ``keep_exc_types`` and ``drop_exc_types`` should be distinct.
                If there is any common member, then it is kept because the ``keep_exc_types``
                condition is checked first.
        """

        def foo(x):
            if is_exception(x):
                if keep_exc_types is not None and isinstance(x, keep_exc_types):
                    return True
                if drop_exc_types is not None and isinstance(x, drop_exc_types):
                    return False
                raise x
            return True

        return self.filter(foo)

    def peek(
        self,
        *,
        print_func: Optional[Callable[[str], None]] = None,
        interval: int | float = 1000,
        exc_types: Optional[Sequence[type[BaseException]]] = BaseException,
        with_exc_tb: bool = True,
    ):
        """Take a peek at the data element *before* it continues in the stream.

        This is implemented by :meth:`map`, where the mapper function prints
        some info under certain conditions before returning the input value unchanged.
        If this does not do what you need, just create your own
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
            This is regardless of the ``interval`` value.

            If ``BaseException`` (the default), all exception objects are printed.

            To turn off the printouts by this condition, pass in ``None`` or ``()`` or ``[]``.
            In this case, ``interval`` is the only creterion for determining whether an element
            should be printed, the element being an ``Exception`` or not.
        with_exc_tb
            If ``True``, traceback, if available, will be printed when an ``Exception`` object is printed,
            the printing being determined by either ``interval`` or ``exc_types``.
        """
        if interval is not None:
            if isinstance(interval, float):
                assert 0 < interval < 1
            else:
                assert isinstance(interval, int)
                assert interval >= 1
        if exc_types is None:
            exc_types = ()
        elif type(exc_types) is type:  # a class
            exc_types = (exc_types,)

        class Peeker:
            def __init__(self):
                self._idx = 0
                self._print_func = print if print_func is None else print_func
                self._interval = interval
                self._exc_types = exc_types
                self._with_trace = with_exc_tb

            def __call__(self, x):
                self._idx += 1
                should_print = False
                if self._interval is not None:
                    if self._interval >= 1:
                        if self._idx % self._interval == 0:
                            should_print = True
                    else:
                        should_print = random() < self._interval
                if (
                    not should_print
                    and self._exc_types
                    and is_exception(x)
                    and isinstance(x, self._exc_types)
                ):
                    should_print = True
                if not should_print:
                    return x
                if not is_exception(x):
                    self._print_func("#%d:  %r" % (self._idx, x))
                    return x
                trace = ""
                if self._with_trace:
                    if is_remote_exception(x):
                        trace = get_remote_traceback(x)
                    else:
                        try:
                            trace = "".join(traceback.format_tb(x.__traceback__))
                        except AttributeError:
                            pass
                if trace:
                    self._print_func("#%d:  %r\n%s" % (self._idx, x, trace))
                else:
                    self._print_func(f"#{self._idx}:  {x!r}")
                return x

        return self.map(Peeker())

    @deprecated(
        deprecated_in="0.11.8", removed_in="0.12.2", details="Use ``peek`` instead."
    )
    def peek_every_nth(self, n: int):
        return self.peek(interval=n)

    def head(self, n: int):
        """
        Take the first ``n`` elements and ignore the rest.
        If the entire stream has less than ``n`` elements, just take all of them.

        This does not delegate to ``filter``, because ``filter``
        would need to walk through the entire stream,
        which is not needed for ``head``.
        """
        self.streamlets.append(Header(self.streamlets[-1], n))
        return self

    def tail(self, n: int):
        """
        Take the last ``n`` elements and ignore all the previous ones.
        If the entire stream has less than ``n`` elements, just take all of them.

        .. note:: ``n`` data elements need to be kept in memory, hence ``n`` should
            not be "too large" for the typical size of the data elements.
        """
        self.streamlets.append(Tailor(self.streamlets[-1], n))
        return self

    def groupby(self, func: Callable[[T], Any], /, **kwargs):
        """
        ``func`` takes a data element and outputs a value.
        **Consecutive** elements that have the same value of this output
        will be grouped into a list.

        Following this operator, every element in the output stream is a list of length 1 or more.

        The output of ``func`` is usually a str or int or a tuple of a few strings or ints.

        ``**kwargs`` are additional keyword arguments to ``func``.

        .. note:: A group will be kept in memory until it is concluded (i.e. the next element
            starts a new group). For this reason, the groups should not be too large
            for the typical size of the data element.

        Examples
        --------
        >>> data = ['atlas', 'apple', 'answer', 'bee', 'block', 'away', 'peter', 'question', 'plum', 'please']
        >>> print(Streamer(data).groupby(lambda x: x[0]).collect())
        [['atlas', 'apple', 'answer'], ['bee', 'block'], ['away'], ['peter'], ['question'], ['plum', 'please']]
        """
        self.streamlets.append(Grouper(self.streamlets[-1], func, **kwargs))
        return self

    def batch(self, batch_size: int):
        """
        Take elements from an input stream
        and bundle them up into batches up to a size limit,
        and produce the batches as lists.

        The output batches are all of the specified size,
        except possibly the final batch.
        There is no 'timeout' logic to proceed eagerly with a partial batch.
        For efficiency, this requires the input stream to have a steady supply.
        If that is a concern, having a :meth:`buffer` on the input stream
        prior to :meth:`batch` may help.

        Examples
        --------
        >>> ss = Streamer(range(10)).batch(3)
        >>> print(ss.collect())
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
        """
        self.streamlets.append(Batcher(self.streamlets[-1], batch_size))
        return self

    def unbatch(self):
        """
        Turn a stream of lists into a stream of individual elements.

        This is sometimes used to correspond with a previous
        :meth:`batch`, but that is by no means a requirement. The only requirement
        is that the input elements are lists.

        ``unbatch`` can be combined with :meth:`map` to implement "expanding" a stream, like this:

        >>> def explode(x):
        ...     if isinstance(x, int) and x > 0:
        ...         return [x] * x
        ...     return []
        >>> ss = Streamer([0, 1, 2, 'a', -1, 'b', 3, 4]).map(explode).unbatch()
        >>> print(ss.collect())
        [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]

        In fact, elements of the input stream do not have to be ``list``\\s.
        They can be any `Iterable`_. For example:

        >>> def expand(n):
        ...     for _ in range(n):
        ...         yield n
        >>>
        >>> stream = Streamer((1, 2, 4, 3, 0, 5)).map(expand).unbatch().collect()
        >>> print(stream)
        [1, 2, 2, 4, 4, 4, 4, 3, 3, 3, 5, 5, 5, 5, 5]
        """
        self.streamlets.append(Unbatcher(self.streamlets[-1]))
        return self

    def accumulate(
        self, func: Callable[[Any, T], Any], initializer: Any = NOTSET, **kwargs
    ):
        """
        This method is like "cumulative sum", but the operation is specified by ``func``, hence
        does not need to be "sum". If the last element in the output stream is ``x``
        and the upcoming element in the input stream is ``y``, then the next element in the output
        stream is

        ::

            func(x, y, **kwargs)

        If ``initializer`` is not provided, then the first element is output as is, and "accumulation" begins
        with the second element. Suppose the input stream is ``x0``, ``x1``, ``x2``, ..., then the output
        stream is

        ::

            x0, func(x0, x1, **kwargs), func(func(x0, x1, **kwargs), x2), ...

        If ``initializer`` is provided (any user-provided value, including ``None``), then the first element
        of the output stream is

        ::

            func(initializer, x0, **kwargs)

        hence "accumulation" begins with the first element.

        .. note:: In some other languages or libraries, ``accumulate`` takes a stream or sequence and returns
            a single value as the result. This method, in contrast, returns a value for each element in the stream.
            In fact, the implementation is a simple application of :meth:`map`.

        Examples
        --------
        >>> ss = Streamer(range(7))
        >>> print(ss.accumulate(lambda x, y: x + y, 3).collect())
        [3, 4, 6, 9, 13, 18, 24]
        """

        class Accumulator:
            def __init__(self):
                self._func = func
                self._initializer = initializer
                self._kwargs = kwargs

            def __call__(self, x):
                z = self._initializer
                if z is NOTSET:
                    z = x
                else:
                    z = self._func(z, x, **self._kwargs)
                self._initializer = z
                return z

        return self.map(Accumulator())

    def buffer(self, maxsize: int):
        """Buffer is used to stabilize and improve the speed of data flow.

        A buffer is useful following any operation that can not guarantee
        (almost) instant availability of output. A buffer allows its
        input to "pile up" when its downstream consumer is slow,
        so that data *is* available when the downstream does come to request
        data. The buffer evens out irregularities in the speeds of upstream
        production and downstream consumption.

        ``maxsize`` is the size of the internal buffer.
        """
        self.streamlets.append(Buffer(self.streamlets[-1], maxsize))
        return self

    @deprecated(
        deprecated_in="0.11.8", removed_in="0.12.2", details="Use ``parmap`` instead"
    )
    def transform(
        self,
        func,
        *,
        executor="thread",
        concurrency=None,
        return_x=False,
        return_exceptions=False,
        **func_args,
    ):
        return self.parmap(
            func,
            executor=executor,
            num_workers=concurrency,
            return_x=return_x,
            return_exceptions=return_exceptions,
            **func_args,
        )

    def parmap(
        self,
        func: Callable[[T], TT],
        /,
        executor: Literal["thread", "process"],
        *,
        num_workers: Optional[int] = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        **kwargs,
    ):
        """Parallel, or concurrent, counterpart of :meth:`map`.

        New threads or processes are created to execute ``func``.
        The function is applied on each element of the data stream and produces a new value,
        which forms the output stream.
        The entire input stream is *collectively* transformed by the multiple
        copies of ``func``.

        Elements in the output stream are in the order of the input elements.
        In other words, the order of data elements is preserved.

        The main difference between :meth:`parmap` and :meth:`map` is that the former
        creates a specified number of thread or processes to execute the function,
        whereas the latter executes a (simple) function in-line.

        Parameters
        ----------
        func
            A sync function that takes a single input item
            as the first positional argument and produces a result.
            Additional keyword args can be passed in via ``**kwargs``.

            The main point of ``func`` does not have to be its output.
            It could rather be some side effect. For example,
            saving data in a database. In that case, the output may be
            ``None``. Regardless, the output is yielded to be consumed by the next
            operator in the pipeline. A stream of ``None``\\s could be used
            in counting, for example.
        executor
            Either 'thread' or 'process'.
        num_workers
            Max number of threads (if ``executor='thread'``) or processes
            (if ``executor='process'``) created to run ``func``.
            This is also the max number of concurrent calls to ``func``
            that can be ongoing at any time.

            If ``None``, a default value is used depending on the value of ``executor``.
            If ``executor`` is ``'thread'``, as many as
            ``min(32, (os.cpu_count() or 1) + 4)`` threads are created.
            If ``executor`` is ``'process'``, as many as
            ``concurrency = os.cpu_count() or 1`` processes are created.
        return_x
            If ``True``, output stream will contain tuples ``(x, y)``;
            if ``False``, output stream will contain ``y`` only.
        return_exceptions
            If ``True``, exceptions raised by ``func`` will be
            in the output stream as if they were regular results; if ``False``,
            they will halt the operation and propagate.

            Note that a ``True`` value does not absorb exceptions
            raised by *previous* operators in the pipeline; it is concerned about
            exceptions raised by ``func`` only.
        **kwargs
            Passed on to :class:`Parmapper`.
        """
        self.streamlets.append(
            Parmapper(
                self.streamlets[-1],
                func,
                executor=executor,
                num_workers=num_workers,
                return_x=return_x,
                return_exceptions=return_exceptions,
                **kwargs,
            )
        )
        return self


class Stream(Iterable):
    def __init__(self, instream: Iterable, /):
        self._instream = instream

    def __iter__(self):
        yield from self._instream


class Mapper(Stream):
    def __init__(self, instream: Stream, func: Callable[[T], Any], **kwargs):
        super().__init__(instream)
        self.func = functools.partial(func, **kwargs) if kwargs else func

    def __iter__(self):
        func = self.func
        for v in self._instream:
            yield func(v)


class Filter(Stream):
    def __init__(self, instream: Stream, func: Callable[[T], bool], **kwargs):
        super().__init__(instream)
        self.func = functools.partial(func, **kwargs) if kwargs else func

    def __iter__(self):
        func = self.func
        for v in self._instream:
            if func(v):
                yield v


class Header(Stream):
    def __init__(self, instream: Stream, /, n: int):
        """
        Keeps the first ``n`` elements and ignores all the rest.
        """
        assert n > 0
        super().__init__(instream)
        self.n = n

    def __iter__(self):
        n = 0
        nn = self.n
        for v in self._instream:
            if n >= nn:
                break
            yield v
            n += 1


class Tailor(Stream):
    def __init__(self, instream: Stream, /, n: int):
        """
        Keeps the last ``n`` elements and ignores all the previous ones.
        If there are less than ``n`` data elements in total, then keep all of them.

        This object needs to walk through the entire input stream before
        starting to yield elements.

        .. note:: ``n`` data elements need to be kept in memory, hence ``n`` should
            not be "too large" for the typical size of the data elements.
        """
        super().__init__(instream)
        assert n > 0
        self.n = n

    def __iter__(self):
        data = deque(maxlen=self.n)
        for v in self._instream:
            data.append(v)
        yield from data


class Grouper(Stream):
    def __init__(self, instream: Stream, /, func: Callable[[T], Any], **kwargs):
        super().__init__(instream)
        self.func = functools.partial(func, **kwargs) if kwargs else func

    def __iter__(self):
        _z = object()
        group = None
        func = self.func
        for x in self._instream:
            z = func(x)
            if z == _z:
                group.append(x)
            else:
                if group is not None:
                    yield group
                group = [x]
                _z = z
        if group:
            yield group


class Batcher(Stream):
    def __init__(self, instream: Stream, /, batch_size: int):
        super().__init__(instream)
        assert batch_size > 0
        self._batch_size = batch_size

    def __iter__(self):
        batch_size = self._batch_size
        batch = []
        for x in self._instream:
            batch.append(x)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


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

    def __iter__(self):
        for x in self._instream:
            yield from x


class Buffer(Stream):
    def __init__(self, instream: Stream, /, maxsize: int):
        super().__init__(instream)
        assert 1 <= maxsize <= 10_000
        self.maxsize = maxsize
        self._stopped = threading.Event()
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
        try:
            for x in self._instream:
                if self._stopped.is_set():
                    return
                q.put(x)  # if `q` is full, will wait here
        except Exception:
            q.put(STOPPED)
            raise
        else:
            q.put(FINISHED)

    @staticmethod
    def _finalize(stopped, tasks, worker):
        stopped.set()
        while not tasks.empty():
            _ = tasks.get()
        # `tasks` is now empty. The thread needs to put at most one
        # more element into the queue, which is safe.
        worker.join()

    def __iter__(self):
        while True:
            try:
                z = self._tasks.get(timeout=1)
            except queue.Empty as e:
                if self._worker.is_alive():
                    continue
                if self._worker.exception():
                    raise self._worker.exception()
                raise RuntimeError("unknown situation occurred") from e
            else:
                if z == FINISHED:
                    break
                if z == STOPPED:
                    raise self._worker.exception()
                try:
                    yield z
                except GeneratorExit:
                    self._finalize(self._stopped, self._tasks, self._worker)
                    raise


class Parmapper(Stream):
    def __init__(
        self,
        instream: Stream,
        func: Callable[[T], TT],
        *,
        executor: Literal["thread", "process"] = "process",
        num_workers: Optional[int] = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        executor_initializer=None,
        executor_init_args=(),
        **kwargs,
    ):
        super().__init__(instream)

        if num_workers is None:
            if executor == "thread":
                num_workers = min(32, (os.cpu_count() or 1) + 4)
            else:
                num_workers = os.cpu_count() or 1
        else:
            assert num_workers > 0
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        if executor == "thread":
            self._stopped = threading.Event()
            self._executor = concurrent.futures.ThreadPoolExecutor(
                num_workers,
                initializer=executor_initializer,
                initargs=executor_init_args,
            )
        else:
            assert executor == "process"
            self._stopped = MP_SPAWN_CTX.Event()
            self._executor = concurrent.futures.ProcessPoolExecutor(
                num_workers,
                mp_context=MP_SPAWN_CTX,
                initializer=executor_initializer,
                initargs=executor_init_args,
            )
        self._tasks = SingleLane(num_workers)
        self._worker = Thread(
            target=self._start, args=(func,), kwargs=kwargs, loud_exception=False
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
        executor = self._executor
        try:
            for x in self._instream:
                if self._stopped.is_set():
                    return
                t = executor.submit(func, x, **kwargs)
                tasks.put((x, t))
                # The size of the queue `tasks` regulates how many
                # concurrent calls to `func` there can be.
        except Exception:
            tasks.put(STOPPED)
            raise
        else:
            tasks.put(FINISHED)

    @staticmethod
    def _finalize(stopped, tasks, worker, executor):
        stopped.set()
        while not tasks.empty():
            _ = tasks.get()
        worker.join()
        executor.shutdown()

    def __iter__(self):
        while True:
            z = self._tasks.get()
            if z == FINISHED:
                break
            if z == STOPPED:
                raise self._worker.exception()

            x, fut = z
            try:
                y = fut.result()
            except Exception as e:
                if self._return_exceptions:
                    # TODO: think about when `e` is a "remote exception".
                    try:
                        if self._return_x:
                            yield x, e
                        else:
                            yield e
                    except GeneratorExit:
                        self._finalize(
                            self._stopped, self._tasks, self._worker, self._executor
                        )
                        raise
                else:
                    self._finalize(
                        self._stopped, self._tasks, self._worker, self._executor
                    )
                    raise
            else:
                try:
                    if self._return_x:
                        yield x, y
                    else:
                        yield y
                except GeneratorExit:
                    self._finalize(
                        self._stopped, self._tasks, self._worker, self._executor
                    )
                    raise
