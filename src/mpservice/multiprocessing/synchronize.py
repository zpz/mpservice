import multiprocessing

from .context import MP_SPAWN_CTX


class Lock(multiprocessing.synchronize.Lock):
    def __init__(self, *, ctx=None):
        super().__init__(ctx=ctx or MP_SPAWN_CTX)


class RLock(multiprocessing.synchronize.RLock):
    def __init__(self, *, ctx=None):
        super().__init__(ctx=ctx or MP_SPAWN_CTX)


class Condition(multiprocessing.synchronize.Condition):
    def __init__(self, lock=None, *, ctx=None):
        super().__init__(lock=lock, ctx=ctx or MP_SPAWN_CTX)


class Semaphore(multiprocessing.synchronize.Semaphore):
    def __init__(self, value=1, *, ctx=None):
        super().__init__(value=value, ctx=ctx or MP_SPAWN_CTX)


class BoundedSemaphore(multiprocessing.synchronize.BoundedSemaphore):
    def __init__(self, value=1, *, ctx=None):
        super().__init__(value=value, ctx=ctx or MP_SPAWN_CTX)


class Event(multiprocessing.synchronize.Event):
    def __init__(self, *, ctx=None):
        super().__init__(ctx=ctx or MP_SPAWN_CTX)


class Barrier(multiprocessing.synchronize.Barrier):
    def __init__(self, *args, ctx=None, **kwargs):
        super().__init__(*args, ctx=ctx or MP_SPAWN_CTX, **kwargs)
