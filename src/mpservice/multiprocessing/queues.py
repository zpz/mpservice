import multiprocessing
from typing import Generic, TypeVar

from .context import MP_SPAWN_CTX

Elem = TypeVar('Elem')


class Queue(multiprocessing.queues.Queue, Generic[Elem]):
    def __init__(self, maxsize=0, *, ctx=None):
        super().__init__(maxsize, ctx=ctx or MP_SPAWN_CTX)

    @property
    def maxsize(self):
        # `queue.Queue` has attribute `maxsize`.
        # `multiprocessing.queues.Queue` has attribute `_maxsize`.
        return self._maxsize


class JoinableQueue(multiprocessing.queues.JoinableQueue, Generic[Elem]):
    def __init__(self, maxsize=0, *, ctx=None):
        super().__init__(maxsize, ctx=ctx or MP_SPAWN_CTX)


class SimpleQueue(multiprocessing.queues.SimpleQueue, Generic[Elem]):
    def __init__(self, *, ctx=None):
        super().__init__(ctx=ctx or MP_SPAWN_CTX)
