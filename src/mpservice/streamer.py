'''Utilities for processing a continuous stream of data in a synchronous context.

Please refer to the async counterpart in the module `mpservice.async_streamer`.
'''

import functools
import inspect
import logging
import multiprocessing
import random
from concurrent.futures import Future
from queue import Queue, Empty
from threading import Lock, Thread
from time import sleep
from typing import (
    Callable, TypeVar, Union, Optional,
    Iterable, Iterator,
    Tuple, List,
)


MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)

NO_MORE_DATA = object()

logger = logging.getLogger(__name__)

T = TypeVar('T')
TT = TypeVar('TT')


def _is_exc(e):
    return isinstance(e, Exception) or (
        inspect.isclass(e) and issubclass(e, Exception)
    )


class IterQueue(Queue):
    DEFAULT_MAXSIZE = 256

    def __init__(self, maxsize: int = None, q_err: Queue = None):
        super().__init__(maxsize or self.DEFAULT_MAXSIZE)
        self._q_err = Queue() if q_err is None else q_err
        self._closed = False
        self._exhausted = False

    def put_end(self):
        assert not self._closed
        self.put(NO_MORE_DATA)
        self._closed = True

    def put_error(self, e):
        assert not self._closed
        self._q_err.put(e)

    def put(self, x, **kwargs):
        assert not self._closed
        super().put(x, **kwargs)

    def put_nowait(self, x):
        assert not self._closed
        super().put_nowait(x)

    def get(self, **kwargs):
        if self._exhausted:
            return NO_MORE_DATA
        z = super().get(**kwargs)
        if z is NO_MORE_DATA:
            self._exhausted = True
        return z

    def get_nowait(self):
        if not self._q_err.empty():
            raise self._q_err.get()
        if self._exhausted:
            return NO_MORE_DATA
        z = super().get_nowait()
        if z is NO_MORE_DATA:
            self._exhausted = True
        return z

    def __next__(self):
        while True:
            try:
                z = self.get_nowait()
                if z is NO_MORE_DATA:
                    raise StopIteration
                return z
            except Empty:
                sleep(0.005)

    def __iter__(self):
        return self


def stream(x: Iterable, maxsize: int = None) -> IterQueue:
    if isinstance(x, IterQueue):
        return x

    if not hasattr(x, '__iter__'):
        if hasattr(x, '__next__'):
            def f(x):
                while True:
                    try:
                        yield x.__next__()
                    except StopIteration:
                        break
            x = f(x)
        else:
            raise TypeError('`x` is not iterable')

    def enqueue(q_in, q_out):
        try:
            for v in q_in:
                q_out.put(v)
        except Exception as e:
            q_out.put_error(e)
        q_out.put_end()

    q = IterQueue(maxsize)
    t = Thread(target=enqueue, args=(x, q))
    t.start()

    return q


def collect(in_stream: Iterable[T]) -> List[T]:
    return list(in_stream)


def batch(q_in: IterQueue, q_out: IterQueue, batch_size: int) -> None:
    '''Take elements from an input stream,
    and bundle them up into batches up to a size limit,
    and produce the batches in an iterable.

    The output batches are all of the specified size,
    except possibly the final batch.
    There is no 'timeout' logic to produce a smaller batch.
    For efficiency, this requires the input stream to have a steady supply.
    If that is a concern, having a `buffer` on the input stream may help.
    '''
    assert 0 < batch_size <= 10000
    batch_ = []
    n = 0
    try:
        for x in q_in:
            batch_.append(x)
            n += 1
            if n >= batch_size:
                q_out.put(batch_)
                batch_ = []
                n = 0
        if n:
            q_out.put(batch_)
    except Exception as e:
        q_out.put_error(e)
    q_out.put_end()


def unbatch(q_in: IterQueue, q_out: IterQueue) -> None:
    '''Reverse of "batch", turning a stream of batches into
    a stream of individual elements.
    '''
    try:
        for batch in q_in:
            for x in batch:
                q_out.put(x)
    except Exception as e:
        q_out.put_error(e)
    q_out.put_end()


def buffer(q_in: IterQueue, q_out: IterQueue) -> None:
    '''Buffer is used to stabilize and improve the speed of data flow.

    A buffer is useful after any operation that can not guarantee
    (almost) instant availability of output. A buffer allows its
    output to "pile up" when the downstream consumer is slow in requests,
    so that data *is* available when the downstream does come to request
    data. The buffer evens out unstabilities in the speeds of upstream
    production and downstream consumption.
    '''
    for x in q_in:
        q_out.put(x)
    q_out.put_end()


def drop_if(q_in: IterQueue, q_out: IterQueue,
            func: Callable[[int, T], bool]) -> None:
    n = 0
    try:
        for x in q_in:
            if func(n, x):
                n += 1
                continue
            q_out.put(x)
            n += 1
    except Exception as e:
        q_out.put_error(e)
    q_out.put_end()


def drop_exceptions(q_in, q_out):
    return drop_if(q_in, q_out, lambda i, x: _is_exc(x))


def drop_first_n(q_in, q_out, n: int):
    assert n >= 0
    return drop_if(q_in, q_out, lambda i, x: i < n)


def keep_if(q_in: IterQueue,
            q_out: IterQueue,
            func: Callable[[int, T], bool]) -> None:
    n = 0
    try:
        for x in q_in:
            if func(n, x):
                q_out.put(x)
            n += 1
    except Exception as e:
        q_out.put_error(e)
    q_out.put_end()


def keep_every_nth(q_in, q_out, nth: int):
    assert nth > 0
    return keep_if(q_in, q_out, lambda i, x: i % nth == 0)


def keep_random(q_in, q_out, frac: float):
    assert 0 < frac <= 1
    rand = random.random
    return keep_if(q_in, q_out, lambda i, x: rand() < frac)


def keep_first_n(q_in, q_out, n: int):
    assert n > 0
    k = 0
    for x in q_in:
        k += 1
        if k > n:
            break
        q_out.put(x)
    q_out.put_end()


def peek_if(q_in: IterQueue,
            q_out: IterQueue,
            condition_func: Callable[[int, T], bool],
            peek_func: Callable[[int, T], None] = None,
            ) -> None:
    '''Take a peek at the data elements that statisfy the specified condition.

    `peek_func` usually prints out info of the data element,
    but can save it to a file or does other things. This happens *before*
    the element is sent downstream.

    The peek function usually should not modify the data element.
    '''
    if peek_func is None:
        def peek_func(i, x):
            print('')
            print('#', i)
            print(x)

    n = 0
    try:
        for x in q_in:
            if condition_func(n, x):
                peek_func(n, x)
            q_out.put(x)
            n += 1
    except Exception as e:
        q_out.put_error(e)
    q_out.put_end()


def peek_every_nth(q_in, q_out, nth: int, peek_func=None):
    return peek_if(q_in, q_out, lambda i, x: i % nth == 0, peek_func)


def peek_random(q_in, q_out, frac: float, peek_func=None):
    assert 0 < frac <= 1
    rand = random.random
    return peek_if(q_in, q_out, lambda i, x: rand() < frac, peek_func)


def log_every_nth(q_in, q_out, nth: int):
    def peek_func(i, x):
        logger.info('data item #%d:  %s', i, x)

    return peek_every_nth(q_in, q_out, nth, peek_func)


def log_exceptions(q_in, q_out, level: str = 'error'):
    flog = getattr(logger, level)
    return peek_if(
        q_in, q_out,
        lambda i, x: _is_exc(x),
        lambda i, x: flog(x)
    )


def transform(q_in: IterQueue,
              q_out: IterQueue,
              func: Callable[[T], TT],
              *,
              workers: Optional[Union[int, str]] = None,
              return_exceptions: bool = False,
              **func_args,
              ) -> None:
    '''Apply a transformation on each element of the data stream,
    producing a stream of corresponding results.

    `func`: a function that takes a single input item
    as the first positional argument and produces a result.
    Additional keywargs can be passed in via the keyward arguments
    `func_args`.

    The outputs are in the order of the input elements.

    `workers`: max number of concurrent calls to `func`. By default
    this is 1, i.e. there is no concurrency.
    '''
    if workers is None:
        workers = 1
    elif isinstance(workers, str):
        assert workers == 'max'
        workers = MAX_THREADS
    else:
        workers > 0

    if workers == 1:
        for x in q_in:
            try:
                z = func(x, **func_args)
                q_out.put(z)
            except Exception as e:
                if return_exceptions:
                    q_out.put(e)
                else:
                    q_out.put_error(e)
                    break  # No need to process subsequent data.
        q_out.put_end()
        return

    finished = False

    def _process(in_stream, out_stream, func, lock, **kwargs):
        nonlocal finished
        while not finished:
            with lock:
                if finished:
                    return
                try:
                    x = in_stream.__next__()
                    fut = Future()
                    out_stream.put(fut)
                except StopIteration:
                    finished = True
                    out_stream.put(NO_MORE_DATA)
                    return
                except Exception as e:
                    fut = Future()
                    out_stream.put(fut)
                    fut.set_exception(e)
                    continue

            try:
                y = func(x, **kwargs)
                fut.set_result(y)
            except Exception as e:
                fut.set_exception(e)

    out_stream = IterQueue(max(q_in.maxsize, workers * 8))
    lock = Lock()

    t_workers = [
        Thread(target=_process,
               args=(
                   q_in,
                   out_stream,
                   func,
                   lock,
               ),
               kwargs=func_args)
        for _ in range(workers)
    ]
    for t in t_workers:
        t.start()

    for fut in out_stream:
        try:
            z = fut.result()
            q_out.put(z)
        except Exception as e:
            if return_exceptions:
                q_out.put(e)
            else:
                q_out.put_error(e)
                finished = True
                break  # No need to process subsequent data.
    q_out.put_end()
    for t in t_workers:
        t.join()


def drain(q_in: IterQueue, log_nth: int = 0) -> Union[int, Tuple[int, int]]:
    '''Drain off the stream.

    Return the number of elements processed.
    '''
    if log_nth > 0:
        q_out = IterQueue(q_in.maxsize, q_in._q_err)
        t = Thread(target=log_every_nth, args=(q_in, q_out, log_nth))
        t.start()
    else:
        q_out = q_in
    n = 0
    nexc = 0
    for v in q_out:
        n += 1
        if _is_exc(v):
            nexc += 1
    if log_nth > 0:
        t.join()
    if nexc:
        return n, nexc
    return n


class Stream:
    @classmethod
    def registerapi(cls,
                    func: Callable[..., None],
                    *,
                    name: str = None,
                    maxsize: bool = False,
                    ) -> None:
        '''
        `func` expects the input and output streams (both of type IterQueue)
        as the first two positional arguments. It may take additional
        positional and keyword arguments. See the functions `batch`, `drop_if`,
        `transform`, etc for examples.

        The created method accepts the extra positional and keyword
        args after the first two positional args. The input and output
        streams are not args of the method, because the first is
        provided by the host object, whereas the second is constructed
        during this registration.

        If `maxsize` is `True`, the created method also takes keyword
        arg `maxsize`, which is passed to the constructor of the
        output stream object, which is of type IterQueue.

        User can use this method to register other functions so that they
        can be used as methods of a `Stream` object, just like `batch`,
        `drop_if`, etc.
        '''
        if not name:
            name = func.__name__

        if maxsize:
            @functools.wraps(func)
            def wrapped(self, *args, maxsize: int = None, **kwargs):
                if maxsize is None:
                    maxsize = self.in_stream.maxsize
                q_out = IterQueue(maxsize, self.in_stream._q_err)
                t = Thread(target=func,
                           args=(self.in_stream, q_out, *args),
                           kwargs=kwargs)
                t.start()
                return cls(q_out)
        else:
            @functools.wraps(func)
            def wrapped(self, *args, **kwargs):
                q_out = IterQueue(self.in_stream.maxsize,
                                  self.in_stream._q_err)
                t = Thread(target=func,
                           args=(self.in_stream, q_out, *args),
                           kwargs=kwargs)
                t.start()
                return cls(q_out)

        setattr(cls, name, wrapped)

    def __init__(self,
                 in_stream: Union[Iterable, Iterator, IterQueue],
                 maxsize: int = None):
        self.in_stream = stream(in_stream, maxsize)

    def __next__(self):
        return self.in_stream.__next__()

    def __iter__(self):
        return self.in_stream.__iter__()

    def collect(self):
        return collect(self.in_stream)

    def drain(self, log_nth: int = 0):
        return drain(self.in_stream, log_nth)

    def buffer(self, buffer_size: int = None):
        if buffer_size is None:
            buffer_size = self.in_stream.maxsize * 4
        q = IterQueue(buffer_size, self.in_stream._q_err)
        t = Thread(target=buffer, args=(self.in_stream, q))
        t.start()
        return self.__class__(q)


Stream.registerapi(batch, maxsize=True)
Stream.registerapi(unbatch, maxsize=True)
Stream.registerapi(drop_if)
Stream.registerapi(drop_exceptions)
Stream.registerapi(drop_first_n)
Stream.registerapi(keep_if)
Stream.registerapi(keep_every_nth)
Stream.registerapi(keep_random)
Stream.registerapi(keep_first_n)
Stream.registerapi(peek_if)
Stream.registerapi(peek_every_nth)
Stream.registerapi(peek_random)
Stream.registerapi(log_every_nth)
Stream.registerapi(log_exceptions)
Stream.registerapi(transform)
