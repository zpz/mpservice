import queue
from typing import Generic, TypeVar

Elem = TypeVar('Elem')


class Queue(queue.Queue, Generic[Elem]):
    pass


class SimpleQueue(queue.SimpleQueue, Generic[Elem]):
    pass
