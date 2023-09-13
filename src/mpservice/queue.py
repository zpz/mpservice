import queue
from typing import Generic, TypeVar

Elem = TypeVar('Elem')


class Queue(queue.Queue, Generic[Elem]):
    pass


class SimpleQueue(queue.SimpleQueue, Generic[Elem]):
    pass


class LifoQueue(queue.LifoQueue, Generic[Elem]):
    pass


class PriorityQueue(queue.PriorityQueue, Generic[Elem]):
    pass


Empty = queue.Empty
Full = queue.Full
