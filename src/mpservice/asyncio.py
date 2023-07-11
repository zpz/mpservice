import asyncio
from collections.abc import AsyncIterator
from typing import Generic, TypeVar

Elem = TypeVar('Elem')
FINISHED = "8d906c4b-1161-40cc-b585-7cfb012bca26"


class QueueFinished(Exception):
    pass


class IterableQueue(asyncio.Queue, Generic[Elem]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._finished_ = False

    async def finish(self):
        if self._finished_:
            return
        self._finished_ = True
        await self._put_finished_marker()

    async def _put_finished_marker(self):
        while True:
            try:
                super().put_nowait(FINISHED)
                break
            except asyncio.QueueFull:
                await asyncio.sleep(0.001)
        # can't use ``super().put(FINISHED)``, which would call ``self.put_nowait``
        # and raise :class:``QueueFinished``.

    def finish_nowait(self):
        if self._finished_:
            return
        self._finished_ = True
        super().put_nowait(FINISHED)

    def finished(self) -> bool:
        return self._finished_ and self.qsize() <= 1

    def put_nowait(self, *args, **kwargs):
        # `put` uses `put_nowait`.
        if self._finished_:
            raise QueueFinished
        return super().put_nowait(*args, **kwargs)

    def get_nowait(self, *args, **kwargs):
        # `get` uses `get_nowait`.
        z = super().get_nowait(*args, **kwargs)
        if z == FINISHED:
            super().put_nowait(z)
            raise QueueFinished
        return z

    async def __aiter__(self) -> AsyncIterator[Elem]:
        # Multiple async tasks can
        # collectively iter over an ``AsyncIterableQueue``, each getting some
        # elements from it.
        while True:
            try:
                x = await self.get()
            except QueueFinished:
                break
            yield x
