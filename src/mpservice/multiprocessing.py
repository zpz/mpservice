from multiprocessing import queues, context
from pickle import dumps as pickle_dumps, loads as pickle_loads
from queue import Empty, Full
from time import monotonic


class FastQueue(queues.SimpleQueue):
    def __init__(self, *, ctx):
        # if ctx is None:
        #    ctx = context._default_context
        super().__init__(ctx=ctx)
        self._closed = ctx.Event()

    def __getstate__(self):
        z = super().__getstate__()
        return (*z, self._closed)

    def __setstate__(self, state):
        self._closed = state[-1]
        super().__setstate__(state[:-1])

    def get(self, block=True, timeout=None):
        try:
            if block and timeout is None:
                return super().get()
            if block:
                deadline = monotonic() + timeout
            if not self._rlock.acquire(block, timeout):
                raise Empty
            try:
                if block:
                    timeout = deadline - monotonic()
                    if not self._poll(timeout):
                        raise Empty
                elif not self._poll():
                    raise Empty
                res = self._reader.recv_bytes()
            finally:
                self._rlock.release()
        except OSError as e:
            if str(e) == 'handle is closed':
                # `.close()` has been called in the same process.
                raise BrokenPipeError(f"Queue {self!r} is closed") from e
            raise
        except Empty:
            if self.closed():
                # `.close()` has been called in another process.
                raise BrokenPipeError(f"Queue {self!r} is closed")
            raise

        return pickle_loads(res)

    def get_nowait(self):
        return self.get(False)

    def put(self, obj, block=True, timeout=None):
        obj = pickle_dumps(obj, protocol=5)
        try:
            if self._wlock is None:
                self._writer.send_bytes(obj)
            else:
                if not self._wlock.acquire(block, timeout):
                    raise Full
                try:
                    self._writer.send_bytes(obj)
                finally:
                    self._wlock.release()
        except OSError as e:
            if str(e) == 'handle is closed':
                # `.close()` has been called in the same process.
                raise BrokenPipeError(f"Queue {self!r} is closed") from e
            raise
        except Full:
            if self.closed():
                # `.close()` has been called in another process.
                raise BrokenPipeError(f"Queue {self!r} is closed")
            raise

    def put_nowait(self, obj):
        self.put(obj, False)

    def close(self):
        # Based on very limited testing, after calling `close()`,
        # in the same process (on the same object), `get` and `put`
        # will raise OSError; in another process, however, `get`
        # and `put` will block forever unless `timeout` is used
        # (upon timeout, closed-ness is checked).
        self._reader.close()
        self._writer.close()
        self._closed.set()

    def closed(self):
        return self._closed.is_set()


def _FastQueue(self):
    return FastQueue(ctx=self.get_context())


context.BaseContext.FastQueue = _FastQueue
