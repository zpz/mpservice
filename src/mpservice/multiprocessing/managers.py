import multiprocessing

from .context import MP_SPAWN_CTX


class SyncManager(multiprocessing.managers.SyncManager):
    # Use this in its context manager.
    def __init__(
        self,
        *args,
        ctx=None,
        **kwargs,
    ):
        super().__init__(*args, ctx=ctx or MP_SPAWN_CTX, **kwargs)
