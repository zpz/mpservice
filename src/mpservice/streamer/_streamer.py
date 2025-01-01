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

from __future__ import annotations

# This is needed before 3.11 to allow `...` as the last parameter to `Concatenate`.
import asyncio
import concurrent.futures
import contextlib
import functools
import inspect
import itertools
import logging
import os
import queue
import random
import threading
import time
import traceback
from collections import deque
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    Sequence,
)
from typing import (
    Any,
    Awaitable,
    Concatenate,
    Literal,
    Optional,
    TypeVar,
)

from typing_extensions import Self  # In 3.11, import this from `typing`

from mpservice import multiprocessing
from mpservice._queues import SingleLane
from mpservice.concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
from mpservice.multiprocessing import remote_exception
from mpservice.threading import Thread

logger = logging.getLogger(__name__)

FINISHED = '8d906c4b-1161-40cc-b585-7cfb012bca26'
STOPPED = 'ceccca5e-9bb2-46c3-a5ad-29b3ba00ad3e'
NOTSET = object()


T = TypeVar('T')  # indicates input data element
TT = TypeVar('TT')  # indicates output after an op on `T`
Elem = TypeVar('Elem')


_NUM_THREADS = min(32, os.cpu_count() + 4)
_NUM_PROCESSES = os.cpu_count()


class Stream(Iterable[Elem]):
    """
    The class ``Stream`` is the "entry-point" for the "streamer" utilities.
    User constructs a ``Stream`` object
    by passing an `Iterable`_ to it, then calls its methods to use it.
    Most of the methods return the object itself, facilitating calls
    in a "chained" fashion, like this::

        s = Stream(...).map(...).filter(...).batch(...).parmap(...).ubatch(...)

    However, these methods modify the object in-place, hence the above is equivalent
    to calling the methods one by one::

        s = Stream(...)
        s.map(...)
        s.filter(...)
        s.batch(...)
        s.parmap(...)
        s.unbatch(...)
    """

    def __init__(self, instream: Iterable, /):
        """
        Parameters
        ----------
        instream
            The input stream of elements, possibly unlimited.
        """
        self.streamlets: list[Iterable] = [instream]

    def __iter__(self) -> Iterator[Elem]:
        return self.streamlets[-1].__iter__()

    def drain(self) -> int:
        """
        Drain off the stream and return the number of elements processed.

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

    def collect(self) -> list[Elem]:
        """
        Return all the elements in a list.

        .. warning:: Do not call this method on "big data".
        """
        return list(self)

    def map(self, func: Callable[[T], Any], /, **kwargs) -> Self:
        """
        Perform a simple transformation on each data element.

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

    def filter(self, func: Callable[[T], bool], /, **kwargs) -> Self:
        """
        Select data elements to keep in the stream according to the predicate ``func``.

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
    ) -> Self:
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
            if isinstance(x, BaseException):
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
        interval: int | float = 1,
        exc_types: Optional[Sequence[type[BaseException]]] = BaseException,
        with_exc_tb: bool = True,
        prefix: str = '',
        suffix: str = '',
    ) -> Self:
        """
        Take a peek at the data element *before* it continues in the stream.

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
        if prefix:
            if not prefix.endswith(' ') and not prefix.endswith('\n'):
                prefix = prefix + ' '
        if suffix:
            if not suffix.startswith(' ') and not suffix.startswith('\n'):
                suffix = ' ' + suffix

        class Peeker:
            def __init__(self):
                self._idx = 0
                self._print_func = print if print_func is None else print_func
                self._interval = interval
                self._exc_types = exc_types
                self._with_trace = with_exc_tb
                self._prefix = prefix
                self._suffix = suffix

            def __call__(self, x):
                self._idx += 1
                should_print = False
                if self._interval is not None:
                    if self._interval >= 1:
                        if self._idx % self._interval == 0:
                            should_print = True
                    else:
                        should_print = random.random() < self._interval
                if (
                    not should_print
                    and self._exc_types
                    and isinstance(x, BaseException)
                    and isinstance(x, self._exc_types)
                ):
                    should_print = True
                if not should_print:
                    return x
                if not isinstance(x, BaseException):
                    self._print_func(f'{self._prefix}#{self._idx}:')
                    self._print_func(f'{x}{self._suffix}')
                    return x
                trace = ''
                if self._with_trace:
                    if remote_exception.is_remote_exception(x):
                        trace = remote_exception.get_remote_traceback(x)
                    else:
                        try:
                            trace = ''.join(traceback.format_tb(x.__traceback__))
                        except AttributeError:
                            pass
                self._print_func(f'{self._prefix}#{self._idx}:')
                if trace:
                    self._print_func(f'{x}')
                    self._print_func(f'{trace}{self._suffix}')
                else:
                    self._print_func(f'{x}{self._suffix}')
                return x

        return self.map(Peeker())

    def shuffle(self, buffer_size: int = 1000) -> Self:
        """
        Shuffle the elements using a buffer as intermediate storage.

        If ``buffer_size`` is ``>=`` the number of the elements, then the shuffling is fully random.
        Otherwise, the shuffling is somewhat "localized".
        """
        self.streamlets.append(Shuffler(self.streamlets[-1], buffer_size=buffer_size))
        return self

    def head(self, n: int) -> Self:
        """
        Take the first ``n`` elements and ignore the rest.
        If the entire stream has less than ``n`` elements, just take all of them.

        This does not delegate to ``filter``, because ``filter``
        would need to walk through the entire stream,
        which is not needed for ``head``.
        """
        self.streamlets.append(Header(self.streamlets[-1], n))
        return self

    def tail(self, n: int) -> Self:
        """
        Take the last ``n`` elements and ignore all the previous ones.
        If the entire stream has less than ``n`` elements, just take all of them.

        .. note:: ``n`` data elements need to be kept in memory, hence ``n`` should
            not be "too large" for the typical size of the data elements.
        """
        self.streamlets.append(Tailer(self.streamlets[-1], n))
        return self

    def groupby(self, key: Callable[[T], Any], /, **kwargs) -> Self:
        """
        ``key`` takes a data element and outputs a value.
        **Consecutive** elements that have the same value of this output
        will be yielded as a generator.

        Following this operator, every element in the output stream is a tuple
        with the key and the values (as a generator).

        This function is similar to the standard
        `itertools.groupby <https://docs.python.org/3/library/itertools.html#itertools.groupby>`_.

        The output of ``key`` is usually a str or int or a tuple of a few strings or ints.

        ``**kwargs`` are additional keyword arguments to ``key``.

        Examples
        --------
        >>> data = ['atlas', 'apple', 'answer', 'bee', 'block', 'away', 'peter', 'question', 'plum', 'please']
        >>> print(Stream(data).groupby(lambda x: x[0]).map(lambda x: list(x[1])).collect())
        [['atlas', 'apple', 'answer'], ['bee', 'block'], ['away'], ['peter'], ['question'], ['plum', 'please']]
        """
        self.streamlets.append(Grouper(self.streamlets[-1], key, **kwargs))
        return self

    def batch(self, batch_size: int) -> Self:
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
        >>> ss = Stream(range(10)).batch(3)
        >>> print(ss.collect())
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
        """
        self.streamlets.append(Batcher(self.streamlets[-1], batch_size))
        return self

    def unbatch(self) -> Self:
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
        >>> ss = Stream([0, 1, 2, 'a', -1, 'b', 3, 4]).map(explode).unbatch()
        >>> print(ss.collect())
        [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]

        In fact, elements of the input stream do not have to be ``list``\\s.
        They can be any `Iterable`_. For example:

        >>> def expand(n):
        ...     for _ in range(n):
        ...         yield n
        >>>
        >>> stream = Stream((1, 2, 4, 3, 0, 5)).map(expand).unbatch().collect()
        >>> print(stream)
        [1, 2, 2, 4, 4, 4, 4, 3, 3, 3, 5, 5, 5, 5, 5]
        """
        self.streamlets.append(Unbatcher(self.streamlets[-1]))
        return self

    def accumulate(
        self, func: Callable[[Any, T], Any], initializer: Any = NOTSET, **kwargs
    ) -> Self:
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
        >>> ss = Stream(range(7))
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

    def buffer(self, maxsize: int) -> Self:
        """
        Buffer is used to stabilize and improve the speed of data flow.

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

    def parmap(
        self,
        func: Callable[[T], TT],
        /,
        *,
        concurrency: int = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        **kwargs,
    ) -> Self:
        """
        Parallel, or concurrent, counterpart of :meth:`map`.

        New threads or processes are created to execute ``func``.
        The function is applied on each element of the data stream and produces a new value,
        which forms the output stream.

        Elements in the output stream are in the order of the input elements.
        In other words, the order of data elements is preserved.

        The main difference between :meth:`parmap` and :meth:`map` is that the former
        executes the function concurrently in background threads or processes,
        whereas the latter executes a (simple) function in-line.

        Parameters
        ----------
        func
            A worker function that takes a single input item
            as the first positional argument and produces a result.
            Additional keyword args can be passed in via ``**kwargs``.

            The main point of ``func`` does not have to be its output.
            It could rather be some side effect. For example,
            saving data in a database. In that case, the output may be
            ``None``. Regardless, the output is yielded to be consumed by the next
            operator in the pipeline. A stream of ``None``\\s could be used
            in counting, for example.
        concurrency
            When ``func`` is sync, this is the
            max number of threads or processes created to run ``func``.
            This is also the max number of concurrent calls to ``func``
            that can be ongoing at any time.

            When ``func`` is async, this is the max number of concurrent
            (i.e. ongoing at the same time) calls to ``func``.
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
        """
        if inspect.iscoroutinefunction(func):
            # Usually this method is called within a sync environment.
            # The operator uses a worker thread to run the async ``func``.
            cls = ParmapperAsync
        else:
            cls = Parmapper

        self.streamlets.append(
            cls(
                self.streamlets[-1],
                func,
                concurrency=concurrency,
                return_x=return_x,
                return_exceptions=return_exceptions,
                **kwargs,
            )
        )
        return self


class Mapper(Iterable):
    def __init__(self, instream: Iterable, func: Callable[[T], Any], **kwargs):
        self._instream = instream
        self.func = functools.partial(func, **kwargs) if kwargs else func

    def __iter__(self):
        func = self.func
        for v in self._instream:
            yield func(v)


class Filter(Iterable):
    def __init__(self, instream: Iterable, func: Callable[[T], bool], **kwargs):
        self._instream = instream
        self.func = functools.partial(func, **kwargs) if kwargs else func

    def __iter__(self):
        func = self.func
        for v in self._instream:
            if func(v):
                yield v


class Header(Iterable):
    def __init__(self, instream: Iterable, /, n: int):
        """
        Keeps the first ``n`` elements and ignores all the rest.
        """
        assert n > 0
        self._instream = instream
        self.n = n

    def __iter__(self):
        n = 0
        nn = self.n
        for v in self._instream:
            if n >= nn:
                break
            yield v
            n += 1


class Tailer(Iterable):
    def __init__(self, instream: Iterable, /, n: int):
        """
        Keeps the last ``n`` elements and ignores all the previous ones.
        If there are less than ``n`` data elements in total, then keep all of them.

        This object needs to walk through the entire input stream before
        starting to yield elements.

        .. note:: ``n`` data elements need to be kept in memory, hence ``n`` should
            not be "too large" for the typical size of the data elements.
        """
        self._instream = instream
        assert n > 0
        self.n = n

    def __iter__(self):
        data = deque(maxlen=self.n)
        for v in self._instream:
            data.append(v)
        yield from data


class Grouper(Iterable):
    def __init__(self, instream: Iterable, /, key: Callable[[T], Any], **kwargs):
        self._instream = instream
        if kwargs:
            key = functools.partial(key, **kwargs)
        self.key = key

    def __iter__(self):
        yield from itertools.groupby(self._instream, self.key)

        # _z = object()
        # group = None
        # func = self.func
        # for x in self._instream:
        #     z = func(x)
        #     if z == _z:
        #         group.append(x)
        #     else:
        #         if group is not None:
        #             yield _z, group
        #         group = [x]
        #         _z = z
        # if group:
        #     yield _z, group


class Batcher(Iterable):
    """
    See :meth:`Stream.batch`.
    """

    def __init__(self, instream: Iterable, /, batch_size: int):
        self._instream = instream
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


class Unbatcher(Iterable):
    """
    See :meth:`Stream.unbatch`.
    This is comparable to the standard ``itertools.chain.from_iterable``.
    """

    def __init__(self, instream: Iterable, /):
        """
        The incoming stream consists of lists.
        This object "expands" or "flattens" the lists into a stream
        of individual elements. Usually, the output stream
        is "longer" than the input stream.

        This may correspond to a "Batcher" operator upstream,
        but that is by no means a requirement.
        """
        self._instream = instream

    def __iter__(self):
        for x in self._instream:
            yield from x


class EagerBatcher(Iterable):
    """
    ``EagerBatcher`` collects items from the incoming stream towards a target batch size and yields the batches.
    For each batch, after getting the first item, it will yield the batch either it has collected enough items
    or has reached ``timeout``. Note, the timer starts upon getting the first item, whereas getting the first item
    for a new batch may take however long.
    """

    def __init__(
        self,
        instream,
        /,
        batch_size: int,
        timeout: float = None,
        endmarker=None,
    ):
        # ``instream`` is a queue (thread or process) with the special value ``endmarker``
        # indicating the end of the stream.
        # ``timeout`` can be 0.
        self._instream = instream
        self._batch_size = batch_size
        if timeout is None:
            timeout = 3600 * 24  # effectively unlimited wait
        self._timeout = timeout
        self._endmarker = endmarker

    def __iter__(self):
        q_in = self._instream
        batchsize = self._batch_size
        timeout = self._timeout
        end = self._endmarker

        while True:
            z = q_in.get()  # wait as long as it takes to get one item.
            if end is None:
                if z is None:
                    break
            else:
                if z == end:
                    break

            batch = [z]
            n = 1
            deadline = time.perf_counter() + timeout
            # Timeout starts after the first item is obtained.

            while n < batchsize:
                t = deadline - time.perf_counter()
                try:
                    # If `t <= 0`, still get the next item
                    # if it's already available.
                    # In other words, if data elements are already here,
                    # get more towards the target batch-size
                    # even if it's already past the timeout deadline.
                    z = q_in.get(timeout=max(0, t))
                except queue.Empty:
                    break

                if end is None:
                    if z is None:
                        yield batch
                        return
                else:
                    if z == end:
                        yield batch
                        return

                batch.append(z)
                n += 1

            yield batch


class Shuffler(Iterable):
    def __init__(self, instream: Iterable, /, buffer_size: int = 1000):
        assert buffer_size > 0
        self._instream = instream
        self._buffersize = buffer_size

    def __iter__(self):
        buffer = []
        buffersize = self._buffersize
        randrange = random.randrange
        for x in self._instream:
            if len(buffer) < buffersize:
                buffer.append(x)
            else:
                idx = randrange(buffersize)
                y = buffer[idx]
                buffer[idx] = x
                yield y
        if buffer:
            random.shuffle(buffer)
            yield from buffer


class Buffer(Iterable):
    def __init__(
        self,
        instream: Iterable,
        /,
        maxsize: int,
        to_stop: threading.Event | multiprocessing.Event = None,
    ):
        self._instream = instream
        assert 1 <= maxsize <= 10_000
        self.maxsize = maxsize
        self._externally_stopped = to_stop

    def _start(self):
        self._stopped = threading.Event()
        self._tasks = SingleLane(self.maxsize)
        self._worker = Thread(target=self._run_worker, name='Buffer-worker-thread')
        self._worker.start()

    def _run_worker(self):
        q = self._tasks
        stopped = self._stopped
        extern_stopped = self._externally_stopped
        try:
            for x in self._instream:
                if stopped.is_set():
                    break
                if extern_stopped is not None and extern_stopped.is_set():
                    break
                q.put(x)  # if `q` is full, will wait here
            q.put(FINISHED)
        except Exception as e:
            q.put(STOPPED)
            q.put(e)
            # raise
            # Do not raise here. Otherwise it would print traceback,
            # while the same would be printed again in ``__iter__``.

    def _finalize(self):
        if self._stopped is None:
            return
        self._stopped.set()
        tasks = self._tasks
        while not tasks.empty():
            _ = tasks.get()
        # `tasks` is now empty. The thread needs to put at most one
        # more element into the queue, which is safe.
        self._worker.join()
        self._stopped = None

    def __iter__(self):
        self._start()
        tasks = self._tasks
        finished = FINISHED
        stopped = STOPPED
        try:
            while True:
                z = tasks.get()
                if z == finished:
                    break
                if z == stopped:
                    raise tasks.get()
                yield z
        finally:
            self._finalize()


def fifo_stream(
    instream: Iterable[T],
    func: Callable[Concatenate[T, ...], concurrent.futures.Future],
    *,
    name: str = 'fifo-stream-feeder-thread',
    capacity: int = 32,
    return_x: bool = False,
    return_exceptions: bool = False,
    preprocessor: Callable[[T], Any] = None,
    **kwargs,
) -> Iterator[TT | Exception] | Iterator[tuple[T, TT | Exception]]:
    """
    This is a helper function for preserving order of input/output elements during concurrent processing.

    Parameters
    ----------
    func
        A function that returns a ``concurrent.futures.Future`` object.
        This function is often a wrapper of the ``submit`` method of a ``concurrent.futures.ThreadPoolExecutor``
        or ``concurrent.futures.ProcessPoolExecutor``, but see
        ``mpservice.mpserver.Server.stream`` for a flexible example.

        ``func`` takes the data element of ``instream`` as the first positional argument, plus optional
        keyword args passed in via ``kwargs``.
    capacity
        The max number of elements that have been fetched from ``instream`` but have not been
        yielded out with results yet. This number may be larger than the max number of concurrent executions
        of (the function behind) ``func``.
        If ``func`` uses a ``ThreadPoolExecutor`` or ``ProcessPoolExecutor``, this capacity does not need
        to be much larger than the pool size.

        Although ``capacity`` has a default value, user is recommended
        to specify a value that is appropriate for their particular use case.
    preprocessor
        A function to be applied to each element of `instream`, the output of which
        is passed on to ``func``. If exception is raised, the exception object becomes
        the result of that input element (and the input element does not get called by ``func``).
        Whether the exception will propagate depends on the value of `return_exceptions`.

        One can imagine two use cases for ``preprocessor``.
        The first is a data validation "gate". The function returns the input unchanged
        if it's "valid", or raises an exception otherwise. This short-circuits bad data
        and prevents them from applied by ``func`` (which could involve multiprocessing and pickling).

        In the second use case, ``preprocessor`` extracts part of the data element as the
        real input to ``func``. This is supposed to be used along with ``return_x=True``;
        the original data element (rather than the output of ``preprocessor``) is returned.
        By this mechanism, only part of the data element is acted upon by ``func``,
        while the full element flows on to subsequent steps.

        The application of this callable happens in the current process/thread.
        It may be a lambda function.
        Usually this callable should be light weight.
        It should not modify its input.
    """

    def feed(instream, func, *, to_stop, q, preprocessor, **func_kwargs):
        try:
            for x in instream:
                if to_stop.is_set():
                    break
                if preprocessor is None:
                    fut = func(x, **func_kwargs)
                else:
                    try:
                        xx = preprocessor(x)
                    except Exception as e:
                        fut = concurrent.futures.Future()
                        fut.set_exception(e)
                    else:
                        fut = func(xx, **func_kwargs)
                q.put((x, fut))
                # The size of the queue `q` regulates how many
                # concurrent calls to `func` there can be.
        except Exception as e:
            q.put(e)
        else:
            q.put(None)

    tasks = SingleLane(capacity + 1)
    to_stop = threading.Event()
    feeder = Thread(
        target=feed,
        args=(instream, func),
        kwargs={'to_stop': to_stop, 'q': tasks, 'preprocessor': preprocessor, **kwargs},
        name=name,
    )
    feeder.start()

    try:
        while True:
            z = tasks.get()
            if z is None:
                break
            if isinstance(z, Exception):
                raise z

            x, fut = z

            try:
                y = fut.result()
            except Exception as e:
                if return_exceptions:
                    # TODO: think about when `e` is a "remote exception".
                    y = e
                else:
                    raise
            if return_x:
                yield x, y
            else:
                yield y
    except BaseException:  # in particular, include GeneratorExit
        to_stop.set()
        raise
    finally:
        while not tasks.empty():
            z = tasks.get()
            if z is None:
                break
            if isinstance(z, Exception):
                break
            _, t = z
            t.cancel()
        feeder.join()

    # Regarding clean-up of generators, see
    #   https://stackoverflow.com/a/30862344/6178706
    #   https://docs.python.org/3.6/reference/expressions.html#generator.close


async def async_fifo_stream(
    instream: AsyncIterable[T],
    func: Callable[Concatenate[T, ...], Awaitable[asyncio.Future]],
    *,
    name: str = 'async-fifo-stream-feeder-task',
    capacity: int = 128,
    return_x: bool = False,
    return_exceptions: bool = False,
    preprocessor: Callable[[T], Any] = None,
    **kwargs,
) -> AsyncIterator[TT | Exception] | Iterator[tuple[T, TT | Exception]]:
    """
    Analogous to :func:`fifo_stream` except for using an async worker function in an async context.
    """

    async def feed(instream, func, *, to_stop, tasks, preprocessor, **func_kwargs):
        try:
            async for x in instream:
                if to_stop.is_set():
                    break
                if preprocessor is None:
                    t = await func(x, **func_kwargs)
                else:
                    try:
                        xx = preprocessor(x)
                    except Exception as e:
                        fut = asyncio.Future()
                        fut.set_exception(e)
                    else:
                        t = await func(xx, **func_kwargs)
                await tasks.put((x, t))
                # The size of the queue `tasks` regulates how many
                # concurrent calls to `func` there can be.
        except Exception as e:
            await tasks.put(e)
        else:
            await tasks.put(None)

    to_stop = asyncio.Event()
    tasks = asyncio.Queue(capacity + 1)
    feeder = asyncio.create_task(
        feed(
            instream,
            func,
            to_stop=to_stop,
            tasks=tasks,
            preprocessor=preprocessor,
            **kwargs,
        ),
        name=name,
    )

    try:
        while True:
            z = await tasks.get()
            if z is None:
                break
            if isinstance(z, Exception):
                raise z

            x, t = z
            try:
                y = await t
            except Exception as e:
                if return_exceptions:
                    y = e
                else:
                    raise
            if return_x:
                yield x, y
            else:
                yield y
    except BaseException:
        to_stop.set()
        raise
    finally:
        cancelled_tasks = []
        while not tasks.empty():
            z = await tasks.get()
            if z is None:
                break
            if isinstance(z, Exception):
                break
            _, t = z
            t.cancel()
            cancelled_tasks.append(t)
        for t in cancelled_tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: S110
                pass
        try:
            await feeder
        except asyncio.CancelledError:
            pass

        # About termination of async generators, see
        #  https://docs.python.org/3.6/reference/expressions.html#asynchronous-generator-functions
        #
        # and
        #  https://peps.python.org/pep-0525/#finalization
        #  https://docs.python.org/3/library/sys.html#sys.set_asyncgen_hooks
        #  https://snarky.ca/unravelling-async-for-loops/
        #  https://github.com/python/cpython/blob/3.11/Lib/asyncio/base_events.py#L539
        #  https://stackoverflow.com/questions/60226557/how-to-forcefully-close-an-async-generator


class Parmapper(Iterable):
    # Environ is sync; worker func is sync.
    def __init__(
        self,
        instream: Iterable,
        func: Callable[[T], TT],
        *,
        executor: Literal['thread', 'process'],
        concurrency: int = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        preprocessor: Callable = None,
        executor_initializer=None,
        executor_init_args=(),
        parmapper_name='parmapper',
        **kwargs,
    ):
        """
        Parameters
        ----------
        executor
            Either 'thread' or 'process'.

            If ``executor`` is ``'process'``, then ``func`` must be pickle-able,
            for example, it can't be a lambda or a function defined within
            another function. The same caution applies to any parameter passed
            to ``func`` in ``kwargs``.
        kwargs
            Named arguments to ``func``, in addition to the first, positional
            argument, which is an element of ``instream``.
        """
        assert executor in ('thread', 'process')
        if executor_initializer is None:
            assert not executor_init_args
        self._instream = instream
        self._func = func
        self._func_kwargs = kwargs
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        self._preprocessor = preprocessor
        self._executor_type = executor
        if concurrency is None:
            concurrency = _NUM_THREADS if executor == 'thread' else _NUM_PROCESSES
        self._concurrency = concurrency
        self._executor_initializer = executor_initializer
        self._executor_init_args = executor_init_args
        self._name = parmapper_name
        self._fifo_capacity = (
            self._concurrency * 2
        )  # user might want to experiment with this value

    def __iter__(self):
        if self._executor_type == 'thread':
            executor = ThreadPoolExecutor(
                self._concurrency,
                initializer=self._executor_initializer,
                initargs=self._executor_init_args,
                thread_name_prefix=self._name + '-thread',
            )
        else:
            executor = ProcessPoolExecutor(
                self._concurrency,
                initializer=self._executor_initializer,
                initargs=self._executor_init_args,
            )
            # TODO: what about the process names?

        with executor:

            def _work(x, **kwargs):
                return executor.submit(self._func, x, loud_exception=False, **kwargs)

            yield from fifo_stream(
                self._instream,
                _work,
                name=self._name,
                capacity=self._fifo_capacity,
                return_x=self._return_x,
                return_exceptions=self._return_exceptions,
                preprocessor=self._preprocessor,
                **self._func_kwargs,
            )


class ParmapperAsync(Iterable):
    # Environ is sync; worker func is async.

    def __init__(
        self,
        instream: Iterable,
        func: Callable[[T], Awaitable[TT]],
        *,
        concurrency: int | None = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        preprocessor: Callable = None,
        parmapper_name='parmapper-sync-async',
        async_context: dict = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        async_context
            This dict contains *async* context managers that are arguments to ``func``,
            in addition to the arguments in ``kwargs``. These objects have been initialized
            (``__init__`` called), but have not entered their contexts yet (``__aenter__`` not called).
            The ``__aenter__`` and ``__aexit__`` of each of these objects will be called
            once  at the beginning and end in the worker thread (properly in an async environment).
            These objects will be passed to ``func`` as named arguments. Inside ``func``,
            these objects are used directly without ``async with``. In other words, each of
            these objects enters their context only once, then is used in any number of
            invocations of ``func``.

            One example: ``func`` conducts HTTP requests using the package ``httpx`` and uses
            a ``httpx.AsyncClient`` as a "session" object. You may do something like this::

                async def download_image(url, *, session: httpx.AsyncClient, **kwargs):
                    response = await session.get(url, **kwargs)
                    return response.content

                stream = Stream(urls)
                stream.parmap(download_image, async_context={'session': httpx.AsyncClient()}, **kwargs)
                for img in stream:
                    ...
        """
        self._instream = instream
        self._func = func
        self._func_kwargs = kwargs
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        self._preprocessor = preprocessor
        self._concurrency = concurrency or 128
        self._name = parmapper_name
        self._async_context = async_context or {}
        self._fifo_capacity = self._concurrency * 2

    def __iter__(self):
        def _do_async(to_stop, loop):
            async def main(to_stop):
                async with contextlib.AsyncExitStack() as stack:
                    for cm in self._async_context.values():
                        await stack.enter_async_context(cm)
                    while True:
                        if to_stop.is_set():
                            break
                        await asyncio.sleep(1)

            loop.run_until_complete(main(to_stop))

        loop = asyncio.new_event_loop()
        to_stop = threading.Event()
        worker = Thread(
            target=_do_async,
            args=(to_stop, loop),
            name=self._name,
        )
        worker.start()

        def func(x, **kwargs):
            return asyncio.run_coroutine_threadsafe(
                self._func(x, **kwargs),
                loop=loop,
            )

        try:
            yield from fifo_stream(
                self._instream,
                func,
                capacity=self._fifo_capacity,
                return_x=self._return_x,
                return_exceptions=self._return_exceptions,
                preprocessor=self._preprocessor,
                **self._func_kwargs,
                **self._async_context,
            )
        finally:
            to_stop.set()
            worker.join()
