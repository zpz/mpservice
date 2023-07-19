import multiprocessing
from collections.abc import Iterator
from typing import Generic, TypeVar

from mpservice.experimental.queue import FINISHED, Finished
from mpservice.multiprocessing import MP_SPAWN_CTX

Elem = TypeVar('Elem')


class IterableQueue(multiprocessing.queues.Queue, Generic[Elem]):
    # Refer to ``mpservice.experimental.queue.IterableQueue``.

    def __init__(self, maxsize=0, *, ctx=None):
        if ctx is None:
            ctx = MP_SPAWN_CTX
        super().__init__(maxsize, ctx=ctx)
        self._finished_ = ctx.Event()
        # Note: the parent class has an attribute
        # ``_closed`` in its implementation, otherwise
        # we might have used the name '_closed' in place
        # of '_finished_'.

    def __getstate__(self):
        return (super().__getstate__(), self._finished_)

    def __setstate__(self, data):
        a, b = data
        super().__setstate__(a)
        self._finished_ = b

    def finish(self, *, timeout=None):
        if self._finished_.is_set():
            return
        self._finished_.set()
        super().put(FINISHED, timeout=timeout)

    def finished(self) -> bool:
        # TODO: not reliable if there are multiple concurrent users to this object.
        return self._finished_.is_set() and self.qsize() <= 1

    def put(self, *args, **kwargs):
        if self._finished_.is_set():
            raise Finished
        return super().put(*args, **kwargs)

    def get(self, *args, **kwargs):
        z = super().get(*args, **kwargs)
        if z == FINISHED:
            super().put(z)
            raise Finished
        return z

    def __iter__(self) -> Iterator[Elem]:
        # This method is process-safe, meaning multiple processes can
        # collectively iter over an ``IterableProcessQueue``, each getting some
        # elements from it.
        while True:
            try:
                x = self.get()
            except Finished:
                break
            yield x
