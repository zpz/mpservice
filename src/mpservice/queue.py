import queue
from collections.abc import Iterator
from typing import Generic, TypeVar

Elem = TypeVar('Elem')
FINISHED = "8d906c4b-1161-40cc-b585-7cfb012bca26"


class Finished(Exception):
    pass


class IterableQueue(queue.Queue, Generic[Elem]):
    # In the implementations of ``queue.Queue`` and ``multiprocessing.queues.Queue``,
    # ``put_nowait`` and ``get_nowait`` simply call ``put`` and ``get``.
    # In the implementation of ``asyncio.queues.Queue``, however,
    # ``put`` calls ``put_nowait``, and ``get`` calls ``get_nowait``.

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._finished_ = False

    def finish(self):
        '''
        This is on the "putting" side, indicating all elements
        have been placed in the queue and there will be no more.

        The "getting" side can still get elements out of this
        queue until the end, that is, until the element retrieved
        is the special indicator that is put in the queue
        by this method.

        Calling :meth:`put` after this raises :class:`Finished`.

        Calling this method multiple times is OK.

        Another possible name of this method could be "close",
        but the multiprocessing Queue class has a method called
        "close", hence we can't use it.
        '''
        if self._finished_:
            return
        self._finished_ = True
        # NOTE: this must preceed the next line to prevent another call to `self.put`.
        # In effect, after one call to ``self.finish``, no more ``self.put``
        # will go through; more calls to ``self.finish`` will go through.
        # See :meth:`get`.
        super().put(FINISHED)

    def finished(self) -> bool:
        # TODO: not reliable if there are multiple concurrent users to this object.
        return self._finished_ and self.qsize() <= 1

    def put(self, *args, **kwargs):
        if self._finished_:
            raise Finished
        return super().put(*args, **kwargs)

    def get(self, *args, **kwargs):
        z = super().get(*args, **kwargs)
        if z == FINISHED:
            # The queue either is empty now or has more ``FINISHED`` indicators.
            # If another thread is trying to ``get`` now, it will either get
            # a remaining ``FINISHED`` or (if the queue is empty) wait for
            # the following ``self.finish()`` to put a ``FINISHED`` in the queue.
            super().put(z)
            # Put another indicator in the queue so that
            # the closing mark is always present.
            raise Finished
        return z

    def __iter__(self) -> Iterator[Elem]:
        # This method is thread-safe, meaning multiple threads can
        # collectively iter over an ``IterableQueue``, each getting some
        # elements from it.
        while True:
            try:
                x = self.get()
            except Finished:
                break
            yield x
