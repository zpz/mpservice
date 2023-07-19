import multiprocessing
from typing import Generic, TypeVar

Elem = TypeVar('Elem')


class Queue(multiprocessing.queues.Queue, Generic[Elem]):
    pass


class JoinableQueue(multiprocessing.queues.JoinableQueue, Generic[Elem]):
    pass


class SimpleQueue(multiprocessing.queues.SimpleQueue, Generic[Elem]):
    pass
