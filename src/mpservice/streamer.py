import functools
import inspect
import logging
import multiprocessing
import queue
import random
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from time import sleep
from typing import (
    Callable, TypeVar, Union, Optional,
    Iterable, Iterator,
    List, Tuple,
)


NO_MORE_DATA = object()
MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)
logger = logging.getLogger(__name__)
T = TypeVar('T')
TT = TypeVar('TT')


def _is_exc(e):
    return isinstance(e, Exception) or (
        inspect.isclass(e) and issubclass(e, Exception)
    )


def stream(x: Union[Iterable[T], Iterator[T]]) -> Iterator[T]:
    def f1(data):
        for v in data:
            yield v

    def f2(data):
        while True:
            try:
                yield data.__next__()
            except StopIteration:
                break

    if hasattr(x, '__iter__'):
        if hasattr(x, '__next__'):
            return x
        return f1(x)
    if hasattr(x, '__next__'):
        return f2(x)

    raise TypeError("`x` is not iterable")


def collect(in_stream: Iterable[T]) -> List[T]:
    return list(in_stream)


def batch(in_stream: Iterable[T],
          batch_size: int) -> Iterator[List[T]]:
    '''Take elements from an input stream,
    and bundle them up into batches up to a size limit,
    and produce the batches in an iterable.

    The output batches are all of the specified size, except possibly the final batch.
    There is no 'timeout' logic to produce a smaller batch.
    For efficiency, this requires the input stream to have a steady supply.
    If that is a concern, having a `buffer` on the input stream may help.
    '''
    assert 0 < batch_size <= 10000
    batch_ = []
    n = 0
    for x in in_stream:
        batch_.append(x)
        n += 1
        if n >= batch_size:
            yield batch_
            batch_ = []
            n = 0
    if n:
        yield batch_


def unbatch(in_stream: Iterable[Iterable[T]]) -> Iterator[T]:
    '''Reverse of "batch", turning a stream of batches into
    a stream of individual elements.
    '''
    for batch in in_stream:
        for x in batch:
            yield x


def buffer(in_stream: Iterable[T],
           buffer_size: int = None) -> Iterator[T]:
    '''Buffer is used to stabilize and improve the speed of data flow.

    A buffer is useful after any operation that can not guarantee
    (almost) instant availability of output. A buffer allows its
    output to "pile up" when the downstream consumer is slow in requests,
    so that data *is* available when the downstream does come to request
    data. The buffer evens out unstabilities in the speeds of upstream
    production and downstream consumption.
    '''
    out_stream = queue.Queue(maxsize=buffer_size or 256)

    def buff(in_stream, out_stream):
        for x in in_stream:
            out_stream.put(x)
        out_stream.put(NO_MORE_DATA)

    with ThreadPoolExecutor(1) as pool:
        t = pool.submit(buff, in_stream, out_stream)

        while True:
            if t.done() and t.exception() is not None:
                raise t.exception()
            try:
                x = out_stream.get_nowait()
                if x is NO_MORE_DATA:
                    break
                yield x
            except queue.Empty:
                sleep(0.008)

        _ = t.result()


def drop_if(in_stream: Iterable[T],
            func: Callable[[int, T], bool]) -> Iterator[T]:
    n = 0
    for x in in_stream:
        if func(n, x):
            n += 1
            continue
        yield x
        n += 1


def drop_exceptions(in_stream: Iterable[T]) -> Iterator[T]:
    return drop_if(in_stream, lambda i, x: _is_exc(x))


def drop_first_n(in_stream, n: int):
    assert n >= 0
    if n == 0:
        return in_stream
    return drop_if(in_stream, lambda i, x: i < n)


def keep_if(in_stream: Iterable[T],
            func: Callable[[int, T], bool]) -> Iterator[T]:
    n = 0
    for x in in_stream:
        if func(n, x):
            yield x
        n += 1


def keep_every_nth(in_stream, nth: int):
    assert nth > 0
    return keep_if(in_stream, lambda i, x: i % nth == 0)


def keep_random(in_stream, frac: float):
    assert 0 < frac <= 1
    rand = random.random
    return keep_if(in_stream, lambda i, x: rand() < frac)


def keep_first_n(in_stream, n: int):
    assert n > 0
    k = 0
    for x in in_stream:
        yield x
        k += 1
        if k >= n:
            break


def _default_peek_func(i, x):
    print('')
    print('#', i)
    print(x)


def peek_if(in_stream: Iterable[T],
            condition_func: Callable[[int, T], bool],
            peek_func: Callable[[int, T], None] = _default_peek_func,
            ) -> Iterator[T]:
    '''Take a peek at the data elements that statisfy the specified condition.

    `peek_func` usually prints out info of the data element,
    but can save it to a file or does other things. This happens *before*
    the element is sent downstream.

    The peek function usually should not modify the data element.
    '''
    n = 0
    for x in in_stream:
        if condition_func(n, x):
            peek_func(n, x)
        yield x
        n += 1


def peek_every_nth(in_stream, nth: int, peek_func=_default_peek_func):
    return peek_if(in_stream, lambda i, x: i % nth == 0, peek_func)


def peek_random(in_stream, frac: float, peek_func=_default_peek_func):
    assert 0 < frac <= 1
    rand = random.random
    return peek_if(in_stream, lambda i, x: rand() < frac, peek_func)


def log_every_nth(in_stream, nth: int):
    def peek_func(i, x):
        logger.info('data item #%d:  %s', i, x)

    return peek_every_nth(in_stream, nth, peek_func)


def log_exceptions(in_stream, level: str = 'error'):
    flog = getattr(logger, level)
    return peek_if(in_stream,
                   lambda i, x: _is_exc(x),
                   lambda i, x: flog(x))


def transform(
    in_stream: Iterator[T],
    func: Callable[[T], TT],
    *,
    workers: Optional[Union[int, str]] = None,
    return_exceptions: bool = False,
    **func_args,
) -> Iterator[T]:
    '''Apply a transformation on each element of the data stream,
    producing a stream of corresponding results.

    `func`: a function that takes a single input item
    as the first positional argument and produces a result.
    Additional keywargs can be passed in via the keyward arguments
    `func_args`.

    The outputs are in the order of the input elements in `in_stream`.

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
        for x in in_stream:
            try:
                z = func(x, **func_args)
                yield z
            except Exception as e:
                if return_exceptions:
                    yield e
                else:
                    raise e
        return

    finished = False

    def _process(in_stream, lock, out_stream, func, **kwargs):
        nonlocal finished
        while not finished:
            with lock:
                if finished:
                    return
                try:
                    x = in_stream.__anext__()
                    fut = asyncio.Future()
                    out_stream.put(fut)
                except StopIteration:
                    finished = True
                    out_stream.put(NO_MORE_DATA)
                    return
                except Exception as e:
                    fut = asyncio.Future()
                    out_stream.put(fut)
                    fut.set_exception(e)
                    continue

            try:
                y = func(x, **kwargs)
                fut.set_result(y)
            except Exception as e:
                fut.set_exception(e)

    out_buffer_size = workers * 8
    out_stream = queue.Queue(out_buffer_size)
    lock = Lock()

    with ThreadPoolExecutor(workers) as pool:
        t_workers = [
            pool.submit(_process,
                        in_stream,
                        lock,
                        out_stream,
                        func,
                        **func_args,
                        )
            for _ in range(workers)
        ]

        while True:
            try:
                fut = out_stream.get_nowait()
                if fut is NO_MORE_DATA:
                    break
            except queue.Empty:
                sleep(0.007)
                continue
            try:
                z = fut.result()
                yield z
            except Exception as e:
                if return_exceptions:
                    yield e
                else:
                    raise e

        for t in t_workers:
            _ = t.result()


def drain(in_stream: Iterable,
          log_nth: int = 0) -> Union[int, Tuple[int, int]]:
    '''Drain off the stream and the number of elements processed.
    '''
    if log_nth:
        in_stream = log_every_nth(in_stream, log_nth)
    n = 0
    nexc = 0
    for v in in_stream:
        n += 1
        if _is_exc(v):
            nexc += 1
    if nexc:
        return n, nexc
    return n


class Stream:
    @classmethod
    def registerapi(cls,
                    func: Callable[..., Iterator],
                    name: str = None) -> None:
        '''
        `func` expects the data stream, an Iterable or Iterator,
        as the first positional argument. It may take additional positional
        and keyword arguments. See the functions `batch`, `drop_if`,
        `transform`, etc for examples.

        User can use this method to register other functions so that they
        can be used as methods of a `Stream` object, just like `batch`,
        `drop_if`, etc.
        '''
        if not name:
            name = func.__name__

        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            return cls(func(self.in_stream, *args, **kwargs))

        setattr(cls, name, wrapped)

    def __init__(self, in_stream: Union[Iterable, Iterator]):
        self.in_stream = stream(in_stream)

    def __anext__(self):
        return self.in_stream.__anext__()

    def __aiter__(self):
        return self.in_stream.__aiter__()

    def collect(self):
        return collect(self.in_stream)

    def drain(self, log_nth=0):
        return drain(self.in_stream, log_nth)


Stream.registerapi(batch)
Stream.registerapi(unbatch)
Stream.registerapi(buffer)
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
