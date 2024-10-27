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
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    Sequence,
)
from inspect import iscoroutinefunction
from types import SimpleNamespace
from typing import (
    Any,
    Awaitable,
    Concatenate,
    Generic,
    Literal,
    Optional,
    TypeVar,
)

import asyncstdlib.itertools
from typing_extensions import Self  # In 3.11, import this from `typing`

from . import multiprocessing
from ._common import StopRequested
from ._queues import SingleLane
from .concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
from .multiprocessing import remote_exception
from .threading import Thread

logger = logging.getLogger(__name__)

FINISHED = '8d906c4b-1161-40cc-b585-7cfb012bca26'
STOPPED = 'ceccca5e-9bb2-46c3-a5ad-29b3ba00ad3e'
NOTSET = object()


T = TypeVar('T')  # indicates input data element
TT = TypeVar('TT')  # indicates output after an op on `T`
Elem = TypeVar('Elem')


_NUM_THREADS = min(32, os.cpu_count() + 4)
_NUM_PROCESSES = os.cpu_count()


def isiterable(x):
    try:
        iter(x)
        return True
    except (TypeError, AttributeError):
        return False


def isasynciterable(x):
    try:
        aiter(x)
        return True
    except (TypeError, AttributeError):
        return False


class Stream(Generic[Elem]):
    """
    The class ``Stream`` is the "entry-point" for the "streamer" utilities.
    User constructs a ``Stream`` object
    by passing an `Iterable`_ or `AsyncIterable_` to it, then calls its methods to use it.
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

    def __init__(self, instream: Iterable | AsyncIterable, /):
        """
        Parameters
        ----------
        instream
            The input stream of elements, possibly unlimited.

            The sync-ness or async-ness of ``instream`` suggests to the ``Stream`` object
            whether the host environment is sync or async.
            In an async context, you may pass in a sync ``instream`` (hence creating
            a sync Stream), then turn it async like this::

                stream = Stream(range(1000)).to_async()
        """
        self.streamlets: list[Iterable | AsyncIterable] = [instream]

    def to_sync(self):
        """
        Make the stream "sync" iterable only.
        """
        if not isiterable(self):
            self.streamlets.append(SyncIter(self.streamlets[-1]))
        return self

    def to_async(self):
        """
        Make the stream "async" iterable only.
        """
        if not isasynciterable(self):
            self.streamlets.append(AsyncIter(self.streamlets[-1]))
        return self

    def __iter__(self) -> Iterator[Elem]:
        # Usually between ``__iter__`` and ``__aiter__`` only
        # one is available, dependending on the type of
        # ``self.streamlets[-1]``.
        return self.streamlets[-1].__iter__()

    def __aiter__(self) -> AsyncIterator[Elem]:
        return self.streamlets[-1].__aiter__()

    def drain(self) -> int:
        """Drain off the stream.

        If ``self`` is sync, return the number of elements processed.
        If ``self`` is async, return an awaitable that will do what the sync version does.

        This method is for the side effect: the entire stream has been processed
        by all the operations and results have been taken care of, for example,
        the final operation may have saved results in a database.

        If you need more info about the processing, such as inspecting exceptions
        as they happen (if ``return_expections`` to :meth:`parmap` is ``True``),
        then don't use this method. Instead, iterate the streamer yourself
        and do whatever you need to do.
        """
        if isiterable(self):
            n = 0
            for _ in self:
                n += 1
            return n

        async def main():
            n = 0
            async for _ in self:
                n += 1
            return n

        return main()

    def collect(self) -> list[Elem]:
        """If ``self`` is sync, return all the elements in a list.
        If ``self`` is async, return an awaitable that will do what the sync version does.

        .. warning:: Do not call this method on "big data".
        """
        if isiterable(self):
            return list(self)

        async def main():
            return [x async for x in self]

        return main()

    def _choose_by_mode(self, sync_choice, async_choice):
        if isiterable(self):
            return sync_choice
        return async_choice

    def map(self, func: Callable[[T], Any], /, **kwargs) -> Self:
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
        cls = self._choose_by_mode(Mapper, AsyncMapper)
        self.streamlets.append(cls(self.streamlets[-1], func, **kwargs))
        return self

    def filter(self, func: Callable[[T], bool], /, **kwargs) -> Self:
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
        cls = self._choose_by_mode(Filter, AsyncFilter)
        self.streamlets.append(cls(self.streamlets[-1], func, **kwargs))
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
        cls = self._choose_by_mode(Shuffler, AsyncShuffler)
        self.streamlets.append(cls(self.streamlets[-1], buffer_size=buffer_size))
        return self

    def head(self, n: int) -> Self:
        """
        Take the first ``n`` elements and ignore the rest.
        If the entire stream has less than ``n`` elements, just take all of them.

        This does not delegate to ``filter``, because ``filter``
        would need to walk through the entire stream,
        which is not needed for ``head``.
        """
        cls = self._choose_by_mode(Header, AsyncHeader)
        self.streamlets.append(cls(self.streamlets[-1], n))
        return self

    def tail(self, n: int) -> Self:
        """
        Take the last ``n`` elements and ignore all the previous ones.
        If the entire stream has less than ``n`` elements, just take all of them.

        .. note:: ``n`` data elements need to be kept in memory, hence ``n`` should
            not be "too large" for the typical size of the data elements.
        """
        cls = self._choose_by_mode(Tailor, AsyncTailor)
        self.streamlets.append(cls(self.streamlets[-1], n))
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
        cls = self._choose_by_mode(Grouper, AsyncGrouper)
        self.streamlets.append(cls(self.streamlets[-1], key, **kwargs))
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
        cls = self._choose_by_mode(Batcher, AsyncBatcher)
        self.streamlets.append(cls(self.streamlets[-1], batch_size))
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
        cls = self._choose_by_mode(Unbatcher, AsyncUnbatcher)
        self.streamlets.append(cls(self.streamlets[-1]))
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
        """Buffer is used to stabilize and improve the speed of data flow.

        A buffer is useful following any operation that can not guarantee
        (almost) instant availability of output. A buffer allows its
        input to "pile up" when its downstream consumer is slow,
        so that data *is* available when the downstream does come to request
        data. The buffer evens out irregularities in the speeds of upstream
        production and downstream consumption.

        ``maxsize`` is the size of the internal buffer.
        """
        cls = self._choose_by_mode(Buffer, AsyncBuffer)
        self.streamlets.append(cls(self.streamlets[-1], maxsize))
        return self

    def parmap(
        self,
        func: Callable[[T], TT],
        /,
        *,
        concurrency: int = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        _async: bool | None = None,
        **kwargs,
    ) -> Self:
        """Parallel, or concurrent, counterpart of :meth:`map`.

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
        if (_async is None and inspect.iscoroutinefunction(func)) or (_async is True):
            if isasynciterable(self):
                # This method is called within an async environment.
                # The operator runs in the current thread on the current asyncio event loop.
                cls = AsyncParmapperAsync
            else:
                # Usually this method is called within a sync environment.
                # The operator uses a worker thread to run the async ``func``.
                cls = ParmapperAsync
        else:
            if isasynciterable(self):
                cls = AsyncParmapper
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


class SyncIter(Iterable):
    def __init__(self, instream: AsyncIterable):
        self._instream = instream

    def _worker(self):
        async def main():
            q = self._q
            to_stop = self._stopped
            try:
                async for x in self._instream:
                    if to_stop.is_set():
                        while True:
                            try:
                                q.get_nowait()
                            except queue.Empty:
                                break
                        return
                    q.put(x)
                q.put(FINISHED)
            except Exception as e:
                q.put(STOPPED)
                q.put(e)

        asyncio.run(main())
        # Since `self._instream` is async iterable,
        # this program is running in an async environment,
        # hence an event loop is running.
        # `asyncio.run_coroutine_threadsafe` might be useful,
        # but I couldn't make it work.

    def _start(self):
        self._q = queue.Queue(2)
        self._stopped = threading.Event()
        self._worker_thread = Thread(target=self._worker)
        self._worker_thread.start()

    def _finalize(self):
        if self._stopped is None:
            return
        self._stopped.set()
        self._worker_thread.join()
        self._stopped = None

    def __iter__(self):
        if isiterable(self._instream):
            yield from self._instream
        else:
            self._start()
            q = self._q
            finished = FINISHED
            stopped = STOPPED
            try:
                while True:
                    x = q.get()
                    if x == finished:
                        break
                    if x == stopped:
                        raise q.get()
                    yield x
            finally:
                self._finalize()


class AsyncIter(AsyncIterable):
    def __init__(self, instream: Iterable):
        self._instream = instream

    async def __aiter__(self):
        if isasynciterable(self._instream):
            async for x in self._instream:
                yield x
        else:
            loop = asyncio.get_running_loop()
            instream = iter(self._instream)
            finished = FINISHED
            while True:
                # ``next(instream)`` could involve some waiting and sleeping,
                # hence doing it in another thread.
                x = await loop.run_in_executor(None, next, instream, finished)
                # `FINISHED` is returned if there's no more elements.
                # See https://stackoverflow.com/a/61774972
                if x == finished:  # `instream_` exhausted
                    break
                yield x


class Mapper(Iterable):
    def __init__(self, instream: Iterable, func: Callable[[T], Any], **kwargs):
        self._instream = instream
        self.func = functools.partial(func, **kwargs) if kwargs else func

    def __iter__(self):
        func = self.func
        for v in self._instream:
            yield func(v)


class AsyncMapper(AsyncIterable):
    def __init__(
        self,
        instream: AsyncIterable,
        func: Callable[[T], Any] | Callable[[T], Awaitable[T]],
        **kwargs,
    ):
        self._instream = instream
        if kwargs:
            func = functools.partial(func, **kwargs)
        self.func = func

    async def __aiter__(self):
        func = self.func
        if iscoroutinefunction(func):
            async for v in self._instream:
                yield await func(v)
        else:
            async for v in self._instream:
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


class AsyncFilter(AsyncIterable):
    def __init__(
        self,
        instream: AsyncIterable,
        func: Callable[[T], bool] | Callable[[T], Awaitable[bool]],
        **kwargs,
    ):
        self._instream = instream
        self.func = functools.partial(func, **kwargs) if kwargs else func

    async def __aiter__(self):
        func = self.func
        if iscoroutinefunction(func):
            async for v in self._instream:
                if await func(v):
                    yield v
        else:
            async for v in self._instream:
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


class AsyncHeader(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /, n: int):
        assert n > 0
        self._instream = instream
        self.n = n

    async def __aiter__(self):
        n = 0
        nn = self.n
        async for v in self._instream:
            if n >= nn:
                break
            yield v
            n += 1


class Tailor(Iterable):
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


class AsyncTailor(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /, n: int):
        self._instream = instream
        assert n > 0
        self.n = n

    async def __aiter__(self):
        data = deque(maxlen=self.n)
        async for v in self._instream:
            data.append(v)
        for x in data:
            yield x


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


class AsyncGrouper(AsyncIterable):
    def __init__(
        self,
        instream: AsyncIterable,
        /,
        key: Callable[[T], Any] | Callable[[T], Awaitable[Any]],
        **kwargs,
    ):
        self._instream = instream
        if kwargs:
            key = functools.partial(key, **kwargs)
        self.key = key

    async def __aiter__(self):
        async for v in asyncstdlib.itertools.groupby(self._instream, self.key):
            yield v

        # _z = object()
        # group = None
        # func = self.func
        # async for x in self._instream:
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


class AsyncBatcher(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /, batch_size: int):
        self._instream = instream
        assert batch_size > 0
        self._batch_size = batch_size

    async def __aiter__(self):
        batch_size = self._batch_size
        batch = []
        async for x in self._instream:
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


class AsyncUnbatcher(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /):
        self._instream = instream

    async def __aiter__(self):
        async for x in self._instream:
            # `x` must be iterable or async iterable.
            if isiterable(x):
                for y in x:
                    yield y
            else:
                async for y in x:
                    yield y


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
        # ``instream`` is a queue (thread or process) with the specicial value ``endmarker``
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


class AsyncShuffler(AsyncIterable):
    def __init__(self, instream: AsyncIterable, /, buffer_size: int = 1000):
        assert buffer_size > 0
        self._instream = instream
        self._buffersize = buffer_size

    async def __aiter__(self):
        buffer = []
        buffersize = self._buffersize
        randrange = random.randrange
        async for x in self._instream:
            if len(buffer) < buffersize:
                buffer.append(x)
            else:
                idx = randrange(buffersize)
                y = buffer[idx]
                buffer[idx] = x
                yield y
        if buffer:
            random.shuffle(buffer)
            for x in buffer:
                yield x


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


class AsyncBuffer(AsyncIterable):
    def __init__(
        self,
        instream: AsyncIterable,
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
        self._worker = Thread(target=self._run_worker, name='AsyncBuffer-worker-thread')
        self._worker.start()

    def _run_worker(self):
        async def main():
            q = self._tasks
            stopped = self._stopped
            extern_stopped = self._externally_stopped
            try:
                async for x in self._instream:
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

        asyncio.run(main())

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

    async def __aiter__(self):
        self._start()
        tasks = self._tasks
        finished = FINISHED
        stopped = STOPPED
        try:
            while True:
                try:
                    z = tasks.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.002)
                    # TODO: how to avoid this sleep?
                    continue
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
    name: str = 'fifo-stream',
    capacity: int = 32,
    return_x: bool = False,
    return_exceptions: bool = False,
    **kwargs,
) -> Iterator[TT | Exception] | Iterator[tuple[T, TT | Exception]]:
    """
    This is a helper function for preserving order of input/output elements during concurrent processing.

    ``func`` is a function that returns a ``concurrent.futures.Future`` object.
    This function is typically a wrapper of the ``submit`` method of a ``concurrent.futures.ThreadPoolExecutor``
    or ``concurrent.futures.ProcessPoolExecutor``.

    ``func`` takes the data element of ``instream`` as the first positional argument, plus optional
    keyword args passed in via ``kwargs``.

    ``capacity`` is the max number of elements that have fetched from ``instream`` but have not been
    yielded out with results yet. This number may be larger than the max number of concurrent executions
    of (the function behind) ``func``.
    If ``func`` uses a ``ThreadPoolExecutor`` or ``ProcessPoolExecutor``, this capacity does not need
    to be much larger than the pool size.

    Although ``capacity`` has a default value, user is recommended
    to specify a value that is appropriate for their particular use case.
    """

    def feed(instream, func, *, to_stop, q, **func_kwargs):
        try:
            for x in instream:
                if to_stop.is_set():
                    break
                fut = func(x, **func_kwargs)
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
        kwargs={'to_stop': to_stop, 'q': tasks, **kwargs},
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


async def fifo_astream(
    instream: AsyncIterable[T],
    func: Callable[Concatenate[T, ...], Awaitable[asyncio.Future]],
    *,
    name: str = 'fifo-astream-worker',
    capacity: int = 128,
    return_x: bool = False,
    return_exceptions: bool = False,
    **kwargs,
) -> AsyncIterator[TT | Exception] | Iterator[tuple[T, TT | Exception]]:
    """
    Analogous to :func:`fifo_stream` except for using an async worker function in an async context.
    """

    async def feed(instream, func, *, to_stop, tasks, **func_kwargs):
        try:
            async for x in instream:
                if to_stop.is_set():
                    break
                t = await func(x, **func_kwargs)
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
        feed(instream, func, to_stop=to_stop, tasks=tasks, **kwargs), name=name
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
        executor_initializer=None,
        executor_init_args=(),
        parmapper_name='parmapper-sync-sync',
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
        self._executor_type = executor
        if concurrency is None:
            concurrency = _NUM_THREADS if executor == 'thread' else _NUM_PROCESSES
        self._concurrency = concurrency
        self._executor_initializer = executor_initializer
        self._executor_init_args = executor_init_args
        self._name = parmapper_name

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
                capacity=self._concurrency * 2,
                return_x=self._return_x,
                return_exceptions=self._return_exceptions,
                **self._func_kwargs,
            )


class AsyncParmapper(AsyncIterable):
    # Environ is async; worker func is sync.
    def __init__(
        self,
        instream: AsyncIterable,
        func: Callable[[T], TT],
        *,
        executor: Literal['thread', 'process'],
        concurrency: int = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        executor_initializer=None,
        executor_init_args=(),
        parmapper_name='parmapper-async-sync',
        **kwargs,
    ):
        assert executor in ('thread', 'process')
        if executor_initializer is None:
            assert not executor_init_args
        self._instream = instream
        self._func = func
        self._func_kwargs = kwargs
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        self._executor_type = executor
        if concurrency is None:
            concurrency = _NUM_THREADS if executor == 'thread' else _NUM_PROCESSES
        self._concurrency = concurrency
        self._executor_initializer = executor_initializer
        self._executor_init_args = executor_init_args
        self._name = parmapper_name

    async def __aiter__(self):
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

            async def func(x, *, executor, loop, **kwargs):
                fut = executor.submit(self._func, x, **kwargs)
                return loop.run_in_executor(None, fut.result)

            loop = asyncio.get_running_loop()

            async for z in fifo_astream(
                self._instream,
                func,
                capacity=self._concurrency * 2,
                return_x=self._return_x,
                return_exceptions=self._return_exceptions,
                executor=executor,
                loop=loop,
                **self._func_kwargs,
            ):
                yield z


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
                stream.parmap_async(download_image, async_context={'session': httpx.AsyncClient()}, **kwargs)
                for img in stream:
                    ...
        """
        self._instream = instream
        self._func = func
        self._func_kwargs = kwargs
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        self._concurrency = concurrency or 128
        self._name = parmapper_name
        self._async_context = async_context or {}

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
                capacity=self._concurrency * 2,
                return_x=self._return_x,
                return_exceptions=self._return_exceptions,
                **self._func_kwargs,
                **self._async_context,
            )
        finally:
            to_stop.set()
            worker.join()


class AsyncParmapperAsync(AsyncIterable):
    # Environ is async; worker func is async.
    def __init__(
        self,
        instream: AsyncIterable,
        func: Callable[[T], Awaitable[TT]],
        *,
        concurrency: int | None = None,
        return_x: bool = False,
        return_exceptions: bool = False,
        parmapper_name='parmapper-async-async',
        **kwargs,
    ):
        self._instream = instream
        self._func = func
        self._func_kwargs = kwargs
        self._return_x = return_x
        self._return_exceptions = return_exceptions
        self._concurrency = concurrency or 128
        self._name = parmapper_name

    def __aiter__(self):
        async def func(x, loop, **kwargs):
            return loop.create_task(self._func(x, **kwargs))

        return fifo_astream(
            self._instream,
            func,
            name=self._name,
            capacity=self._concurrency * 2,
            return_x=self._return_x,
            return_exceptions=self._return_exceptions,
            loop=asyncio.get_running_loop(),
            **self._func_kwargs,
        )


class TeeX:
    # Tee element
    __slots__ = ('value', 'next', 'n', 'lock')
    # `n`` is count of the times this element has been "consumed".
    # Once `n` is equal to the number of forks in the tee, this
    # element can be discarded. In that situation, this element must
    # be at the head of the queue.

    def __init__(self, x, /):
        self.value = x
        self.next = None
        self.n = 0
        self.lock = threading.Lock()


class Fork:
    def __init__(
        self,
        instream: Iterator,
        n_forks: int,
        buffer: queue.Queue,
        head: SimpleNamespace,
        instream_lock: threading.Lock,
        fork_idx,
    ):
        self.instream = instream
        self.n_forks = n_forks
        self.buffer = buffer
        self.head = head
        self.instream_lock = instream_lock
        self.next: TeeX | None = None
        self._state = 0
        self._fork_idx = fork_idx

    def __iter__(self):
        return self

    def __next__(self):
        if self.next is None:
            if self.head.value is None:
                with self.instream_lock:
                    if self.head.value is None:
                        # Get the very first data element out of `instream`
                        # across all forks.
                        # If this raises `StopIteration`, meaning `instream`
                        # is empty, the exception will be propagated, halting
                        # this fork. All the other forks will also get to this
                        # point and exit the same way.
                        x = next(self.instream)
                        box = TeeX(x)
                        self.buffer.put(box)
                        self.head.value = box
                self.next = self.head.value
                return self.__next__()
            elif self._state == 0:
                # This fork is getting the first element.
                # Some fork (possibly this fork itself in the block above)
                # has obtained the element and assigned it to `self.head.value`.
                self.next = self.head.value
                return self.__next__()
            else:
                raise StopIteration
        else:
            while self.next.next is None:
                # During this loop while waiting on the `instream_lock`,
                # `self.next.next` may become not None thanks to another Fork's
                # actions.

                # Pre-fetch the next element because `self.next` points to
                # the final data element in the buffer.
                locked = self.instream_lock.acquire(timeout=0.1)
                if locked:
                    if self.next.next is None:
                        try:
                            x = next(self.instream)
                        except StopIteration:
                            # `instream` is exhausted.
                            # `self.next.next` remains `None`.
                            # The next call to `__next__` will land
                            # in the first branch and raise `StopIteration`.
                            pass
                        else:
                            box = TeeX(x)
                            self.next.next = box  # IMPORTANT: this line goes before the next to avoid race.
                            self.buffer.put(box)
                    self.instream_lock.release()
                    break

            # Check whether the buffer head should be popped:
            box = self.next
            with box.lock:
                box.n += 1
                if box.n == self.n_forks:
                    # No other fork would be accessing this TeeX "x" at this moment,
                    # and no other fork would be trying to pop the head of the buffer
                    # at this time, hence the it's OK to continue holding the lock.
                    self.buffer.get()

            self.next = box.next
            self._state = 1
            return box.value


def tee(
    instream: Iterable[Elem], n: int = 2, /, *, buffer_size: int = 256
) -> tuple[Stream[Elem], ...]:
    """
    ``tee`` produces multiple (default 2) "copies" of the input data stream,
    to be used in different ways.

    Suppose we have a data stream, on which we want to apply two different lines of operations, conceptually
    like this::

        data = [...]
        stream_1 = Stream(data).map(...).batch(...).parmap(...)...
        stream_2 = stream(data).buffer(...).parmap(...).map(...)...

    There are a few ways we can do this:

    1. Revise the code to apply multiple operations on each data element, that is, "merging"
       the two lines of operations. This will likely make the code more complex.

    2. Apply the first line of operations; then re-walk the data stream to apply the second
       line of operations. When feasible, this is simple and clean. However, this approach could
       be infeasible, for example, when walking the data stream is expensive.
       There are also stituations where it's not possible to get the data stream a second time.

    3. Use the current function::

        data = [...]
        stream_1, stream_2 = teee(data, 2)

       The two streams can be applied the :class:`Stream` operations freely and independently
       (after all, they are proper ``Stream`` objects), except for the final "trigger" operations
       :meth:`~Stream.__iter__`, :meth:`~Stream.collect`, and :meth:`~Stream.drain`::

        stream_1.map(...).batch(...).parmap(...)...
        stream_2.buffer(...).parmap(...).map(...)...

       Upon this setup, we need to start "consuming" the two streams at once, letting them run
       concurrently, and they will also finish around the same time. The concurrent "trigger"
       needs to be in different threads. Suppose ``stream_1`` is run for some side effect, hence
       we can simply ``drain`` it; for ``stream_2``, on the other hand, suppose we need
       its outcoming elements. We may do this::

        with concurrent.futures.ThreadPoolExecutor() as pool:
            t1 = pool.submit(stream_1.drain)
            t2 = pool.submit(stream_2.collect)
            n = t1.result()
            output = t2.result()

    The opposite of ``tee`` is the built-in ``zip``.

    Parameters
    ----------
    buffer_size
        Size of an internal buffer, in terms of the number of elements of ``instream`` it holds.
        Unless each element takes a lot of memory, it is recommended to use a large buffer size,
        such as the default 256.

        Think of the buffer as a "moving window" on ``instream``. The ``n`` "forks" produced by
        this function are iterating over ``instream`` independently subject to one constraint:
        at any moment, the next element to be obtained by each fork is an element in this window.
        Once the first element in the window has been obtained by all ``n`` forks, the window will
        advance by one element. (In detail, the oldest element in the window is dropped; the next
        element is obtained from ``instream`` and appended to the window as the newest element.)

        This moving window gives the "forks" wiggle room in their consumption of the data---they
        do not have to be processing the same element at the same time; their respective concurrent
        processing may be slow at different elements. A larger buffer window reduces the need
        for the forks to wait for their slower peers (if the slowness is on random elements rather
        than on every element).
    """
    assert buffer_size >= 2
    # In practice, use a reasonably large value that is feasible for the application.

    buffer = queue.Queue(buffer_size)

    if not hasattr(instream, '__next__'):
        instream = iter(instream)
    instream_lock = threading.Lock()

    head = SimpleNamespace()
    head.value = None
    # `head` holds the very first element of `instream`.
    # Once assigned, `head` will not change.

    forks = tuple(Fork(instream, n, buffer, head, instream_lock, i) for i in range(n))
    return tuple(Stream(f) for f in forks)


class IterableQueue(Iterator[T]):
    def __init__(
        self,
        q: queue.Queue
        | queue.SimpleQueue
        | multiprocessing.Queue
        | multiprocessing.SimpleQueue,
        *,
        num_suppliers: int = 1,
        to_stop: threading.Event | multiprocessing.Event = None,
    ):
        """
        `num_suppliers`: number of parties that will supply data elements to the queue by calling :meth:`put`.
            The parties are typically in different threads or processes.
            Each supplier should call :meth:`put_end` exactly once to indicate it is done adding data.
        `to_stop`: this is used by other parts of the application to tell this queue to exit (because
            some error has happened elsewhere), e.g. stop waiting on `get` or `put`.
            If the queue is to be passed between processes, `to_stop` should be a
            `mpservice.multiprocessing.Event`; otherwise, `to_stop` can be either `threading.Event`
            or `mpservice.multiprocessing.Event` (the latter may be required because the object `to_stop`
            needs to be passed between processes in other parts of the user application).

        `None` is used internally as a special indicator. It must not be a valid value in the user application.

        Typical use case:

        In each of the (one or more) supplier threads or processes, do

            q.put(x)
            q.put(y)
            ...
            q.put_end()

        In each of the (one or more) consumer threads or processes, do

            for z in q:
                use(z)

        The consumers collectively consume the data elements that have been put in the queue.
        """
        if isinstance(q, multiprocessing.SimpleQueue):
            if to_stop is not None:
                raise ValueError(
                    f'`to_stop` is not compatible with `q` of type {type(q).__name__}'
                )
                # Because `mpservice.multiprocessing.SimpleQueue.{get, put}` do not take argument `timeout`.
            self._can_timeout = False
        else:
            self._can_timeout = True
        self._q = q
        self._to_stop = to_stop
        self._wait_interval = 1.0
        # User may revise this value after the object is initiated.

        self._num_suppliers = num_suppliers
        if isinstance(q, (queue.Queue, queue.SimpleQueue)):
            self._spare_lids = queue.Queue(maxsize=num_suppliers)
            self._applied_lids = queue.Queue(maxsize=num_suppliers)
            self._used_lids = queue.Queue(maxsize=num_suppliers)
            # self._lock = threading.Lock()
        else:
            self._spare_lids = multiprocessing.Queue(maxsize=num_suppliers)
            self._applied_lids = multiprocessing.Queue(maxsize=num_suppliers)
            self._used_lids = multiprocessing.Queue(maxsize=num_suppliers)
            # self._lock = multiprocessing.Lock()
        for _ in range(num_suppliers):
            self._spare_lids.put(None)
        # User should not touch these internal helper queues.
        # TODO: the name 'lid' is not very good; something implying the "bottom" would be better.
        # TODO: do we need to use a lock to group the access to the helper queues?

    def __getstate__(self):
        # This will fail if the queues are not pickle-able. That would be a user mistake.
        return (
            self._q,
            self._to_stop,
            self._wait_interval,
            self._num_suppliers,
            self._spare_lids,
            self._applied_lids,
            self._used_lids,
            self._can_timeout,
        )

    def __setstate__(self, zz):
        (
            self._q,
            self._to_stop,
            self._wait_interval,
            self._num_suppliers,
            self._spare_lids,
            self._applied_lids,
            self._used_lids,
            self._can_timeout,
        ) = zz

    @property
    def maxsize(self) -> int:
        try:
            return self._q.maxsize
        except AttributeError:
            return self._q._maxsize
        # If you used a SimpleQueue for `__init__`, this would raise `AttributeError`.

    def qsize(self) -> int:
        return self._q.qsize()

    def put(self, x: T, *, timeout=None) -> None:
        """
        Use this method to put data elements in the queue.

        Once finished putting data in the queue (as far as one supplier,
        such as one thread or process, is concerned), call `put_end()` exactly once
        (per supplier).

        User should never call `put(None)`.
        That is reserved to be called by `put_end()` to indicate the end of
        one supplier's data input.
        """
        if not self._can_timeout:
            if timeout is not None:
                raise ValueError(
                    f'`timeout` is not supported for the type of queue used in this object: {type(self._q).__name__}'
                )
            self._q.put(x)
            return

        if timeout is not None:
            if timeout == 0:
                self._q.put_nowait(x)
                return
            if timeout <= self._wait_interval:
                self._q.put(x, timeout=timeout)
                return
            wait_interval = min(self._wait_interval, timeout / 5)
        else:
            wait_interval = self._wait_interval

        t0 = time.perf_counter()
        Full = queue.Full
        while True:
            try:
                self._q.put(x, timeout=wait_interval)
                return
            except Full:
                if self._to_stop is not None and self._to_stop.is_set():
                    raise StopRequested
                if timeout is not None and (time.perf_counter() - t0) >= timeout:
                    raise
                time.sleep(0)  # force a context switch

    def _get(self) -> T:
        """
        You should not call `_get` directly. Instead, use :meth:`__iter__` or :meth:`__next__`.
        """
        if not self._can_timeout:
            return self._q.get()

        Empty = queue.Empty
        while True:
            try:
                z = self._q.get(timeout=self._wait_interval)
                return z
            except Empty:
                if self._to_stop is not None and self._to_stop.is_set():
                    raise StopRequested
                time.sleep(0)  # force a context switch

    def put_end(self, *, wait_for_renew: bool = False) -> None:
        """
        Each "supplier" must call this method exactly once, after it is done putting
        data in the queue. Do not use `put(None)` for this purpose.

        Suppose in a certain use case a queue is populated by a supplier, which has called `put_end`;
        a consumer is designed to call `renew` after it finishes iterating the queue, allowing
        the next round of data population/consumption. Before the consumer calls `renew`,
        this object does not forbid the supplier from calling `put` to add new data elements
        to the queue (unless the user imposes such restriction themselves), although these
        new data elements are not accessible via `__next__` or `__iter__` until the consumer
        has called `renew`. Suppose the supplier gets to call `put_end` before the consumer
        calls `renew`, the object is in an unexpected state. If `wait_for_renew` is `True`,
        this situation is allowed, and `put_end` will wait to go through once `renew` is called.
        If `wait_for_renew` is `False` (the default), exception is raised in this situation.
        """
        # TODO: add an overall `timeout`?
        if wait_for_renew:
            while True:
                try:
                    z = self._spare_lids.get(timeout=self._wait_interval)
                    break
                except queue.Empty:
                    if self._to_stop is not None and self._to_stop.is_set():
                        raise StopRequested
        else:
            try:
                z = self._spare_lids.get(timeout=0.01)
            except queue.Empty:
                raise RuntimeError(
                    '`put_end` is called more than `num_suppliers` times'
                )

        self._applied_lids.put(z)
        self.put(None)
        # A `None` in the queue corresponds to a `None` in `self._applied_lids`.

    def __next__(self) -> T:
        if self._used_lids.full():
            raise StopIteration

        z = self._get()
        if z is None:
            if self._used_lids.full():
                # Other consumers have removed all the lids and confirmed
                # there's no more data to come from the queue.
                # There's no more `None` in `self._applied_lids`.
                self.put(None)
                # Let there always be an end marker so that other consumers
                # can still iterate over this queue and see it's finished.
                # `self._used_lids` remains full, hence the next call
                # to `__next__` will get here again.
                # This does not increase the number of `None`s in the queue
                # as it simply replaces the one that is just taken off the queue.
                raise StopIteration
            z = self._applied_lids.get()
            self._used_lids.put(z)
            if self._used_lids.full():
                # This is the first consumer who sees the queue is exhausted.
                # Put an extra `None` in the queue for other consumers to see.
                # This is needed because we don't assume nor limit the number
                # of consumers to the queue.
                # This is the only extra `None`: there is only one consumer
                # who is the first to see the bottom of the queue, and subsequent
                # consumers will get/put this `None` without increasing its count.
                self.put(None)
                raise StopIteration
            # The queue is not exhausted because all suppliers's end markers ("lids")
            # have not been collected yet.
            return self.__next__()
        return z

    def __iter__(self) -> Iterator[T]:
        while True:
            try:
                yield self.__next__()
            except StopIteration:
                break

    def renew(self):
        """
        This is for special use cases where the queue needs to be "reused" for
        more than one round of iterations.
        In those use cases, typically the consumer (or consuming side if there are
        more than one consumers) calls `renew` exactly once upon finishing iteration
        over the content of the queue. The suppliers can put more data into the queue
        and call `put_end` as usual once done; the consumer then iterates over the queue
        as if there were no previous rounds.

        This method can only be called after one round of consumption (by iteration) is complete.
        It can not be called at the beginning when no data has been placed in the queue,
        because the implementation does not provide a way to tell the object is in such "brand new"
        state.

        The application needs to ensure `renew` is called only once after one round of iteration.
        """
        if not self._used_lids.full():
            raise RuntimeError('the object is not in a renewable state')
        z = self._get()  # take out the extra `None`
        if z is not None:
            raise RuntimeError(f'expecting None, got {z}')

        for _ in range(self._num_suppliers):
            z = self._used_lids.get()
            self._spare_lids.put(z)


class ProcessRunnee(ABC):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    @abstractmethod
    def __call__(self, *args, **kwargs) -> Any:
        raise NotImplementedError


class ProcessRunner:
    """
    `ProcessRunner` creates a custom object (of a subclass of `ProcessRunnee`)
    in "background" process, and calls the object's `__call__` method as needed,
    any number of times, while keeping the background process alive until `join` is called.

    Some initial motivations for this class include:

    - The custom object may perform nontrivial setup once, and keep the setup in effect
      through the object's lifetime.

    - User may pass certain data to the custom object's `__init__` via `ProcessRunner.__init__`.
      A particular use case is to pass in a `mpservice.multiprocessing.Queue` or `mpservice.streamer.IterableQueue`,
      because these objects can't be passed to the custom object via `ProcessRunner.restart`.

      Note that a `mpservice.multiprocessing.Queue` can't be contained in another `mpservice.multiprocessing.Queue`,
      nor can it be passed to a `mpservice.multiprocessing.Manager` in a call to a "proxy method".
      If either is possible, the class `ProcessRunner` is not that needed.

    In some use cases, `ProcessRunner`, along with `IterableQueue` and its method `renew`, are useful in stream processing.
    """

    def __init__(
        self,
        *,
        target: type[ProcessRunnee],
        args=None,
        kwargs=None,
        name=None,
    ):
        self._instructions = multiprocessing.Queue()
        self._result = multiprocessing.Queue(maxsize=1)
        self._process = multiprocessing.Process(
            target=self._work,
            kwargs={
                'instructions': self._instructions,
                'result': self._result,
                'worker_cls': target,
                'args': args or (),
                'kwargs': kwargs or {},
            },
            name=name,
        )

    @staticmethod
    def _work(
        instructions,
        result,
        worker_cls: type[ProcessRunnee],
        args,
        kwargs,
    ):
        with worker_cls(*args, **kwargs) as worker:
            while True:
                zz = instructions.get()
                if zz is None:
                    break
                try:
                    z = worker(*zz[0], **zz[1])
                except Exception as e:
                    z = remote_exception.RemoteException(e)
                result.put(z)

    def start(self):
        self._process.start()

    def join(self, timeout=None):
        self._instructions.put(None)
        self._process.join(timeout=timeout)

    def restart(self, *args, **kwargs):
        self._instructions.put((args, kwargs))

    def rejoin(self):
        z = self._result.get()
        if isinstance(z, Exception):
            raise z
        return z

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.join()

    def __call__(self, *args, **kwargs):
        self.restart(*args, **kwargs)
        return self.rejoin()
