import multiprocessing

from .context import MP_SPAWN_CTX


class Pool(multiprocessing.pool.Pool):
    def __init__(self, *args, context=None, **kwargs):
        super().__init__(*args, context=context or MP_SPAWN_CTX, **kwargs)
