import queue
import threading
from collections.abc import Iterable, Iterator
from types import SimpleNamespace

from ._streamer import Elem, Stream


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
