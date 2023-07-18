import asyncio
from typing import Generic, TypeVar

Elem = TypeVar('Elem')


class Queue(asyncio.Queue, Generic[Elem]):
    pass
