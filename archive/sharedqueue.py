'''
A multiprocessing queue using shared memory.
I hoped it to be faster than the standard queue, but it turned out to
be slower, like 30%.
'''

import ctypes
import multiprocessing
from multiprocessing import shared_memory, context as mpcontext
from queue import Empty, Full
from time import monotonic
from typing import Union


_ForkingPickler = mpcontext.reduction.ForkingPickler


class Queue:
    # This class uses a shared-memory block as a "circular buffer" to hold
    # data in bytes, plus a few meta-info values (head, tail, qsize, etc).
    #
    # Each record (data element) in the buffer is laid out in two parts,
    #
    #    - 4 bytes to hold an int indicating the number of bytes for the element value,
    #    - followed by that many bytes holding the element value
    #
    # 4-byte int represent values up to 2 billion, 2G bytes, enough for this purpose.

    def __init__(self, maxsize_bytes: int = 1_000_000, *, ctx=None):
        '''
        `maxsize_bytes`: buffer size in bytes; default is 1 MB.
        '''
        if ctx is None:
            ctx = multiprocessing.get_context()

        assert maxsize_bytes >=10
        self._buffer = shared_memory.SharedMemory(create=True, size=maxsize_bytes)
        self._buf = self._buffer.buf
        self._buffer_size = maxsize_bytes
        self._head = ctx.Value(ctypes.c_long, lock=False)
        self._head.value = 0
        # Index of the first byte of the first element.
        # When buffer is empty, this is 0, which is not "correct" and not used.
        self._tail = ctx.Value(ctypes.c_long, lock=False)
        self._tail.value = 0
        # Index of the first byte of the next element to be added;
        # i.e., the last byte plus 1 of the last existing element.

        self._rlock = ctx.RLock()
        self._wlock = ctx.RLock()
        mutex = ctx.Lock()
        self._notempty = ctx.Condition(lock=mutex)
        self._notfull = ctx.Condition(lock=mutex)
        self._qsize = ctx.Value(ctypes.c_long, lock=False)
        self._qsize.value = 0
        # Number of elements in buffer

        self._closed = False
        self._origin = True

        # DEBUG: fill will '-' for visualization
        # for k in range(self._buffer_size):
        #     self._buf[k:k+1] = b'-'


    def __del__(self):
        self._buffer.close()
        if self._origin:
            # this is in the process that created the object.
            self._buffer.unlink()

    def __getstate__(self):
        mpcontext.assert_spawning(self)
        return (self._buffer.name, self._buffer_size, self._qsize, self._head, self._tail,
                self._rlock, self._wlock, self._notempty, self._notfull)

    def __setstate__(self, state):
        (buffername, self._buffer_size, self._qsize, self._head, self._tail,
                self._rlock, self._wlock, self._notempty, self._notfull) = state
        self._buffer = shared_memory.SharedMemory(create=False, name=buffername)
        self._buf = self._buffer.buf
        self._closed = False
        self._origin = False

    def close(self):
        if self._closed:
            return
        self._closed = True

    def qsize(self):
        return self._qsize.value

    def empty(self):
        return self._qsize.value == 0

    def _vacancy(self):
        head = self._head.value
        tail = self._tail.value
        if head < tail:
            #   |-------h=======t----|
            return self._buffer_size - tail + head
        if head > tail:
            #   |======t-----h=======|
            return head - tail
        return self._buffer_size

    def full(self):
        return self._vacancy() <= 5

    def _put_slice(self, data: Union[bytes, memoryview], tail=None):
        # Write data starting at byte index `tail`.
        # Return index of the next byte to be written.
        buf = self._buf
        head = self._head.value
        if tail is None:
            tail = self._tail.value
        nn = self._buffer_size
        n = len(data)
        if head <= tail:
            if tail + n < nn:
                buf[tail : tail + n] = data
                return tail + n
            if tail + n == nn:
                buf[tail : ] = data
                return 0
            k = nn - tail
            buf[tail :] = data[: k]
            buf[: n - k] = data[k :]
            return n - k
        buf[tail : tail + n] = data
        return tail + n

    def _get_slice(self, n: int, head=None):
        # Read n bytes starting at location `head`.
        # Return slice data and index of the next byte to be read.
        buf = self._buf
        nn = self._buffer_size
        if head is None:
            head = self._head.value
        if head + n < nn:
            return buf[head : head + n], head + n
        if head + n == nn:
            return buf[head : head + n], 0
        v1 = buf[head :]
        v2 = buf[: n - len(v1)]
        return bytes(v1) + bytes(v2), len(v2)

    def put_bytes(self, data: memoryview, block=True, timeout=None):
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        datasize = len(data)
        if self._buffer_size < 4 + datasize + 1:
            raise ValueError(f"data size ({datasize} + 5 bytes) exceeds buffer capacity ({self._buffer_size} bytes)")
        header = datasize.to_bytes(4, 'little')
        if timeout is not None:
            deadline = monotonic() + max(0, timeout)
        with self._wlock:
            if self._vacancy() < 4 + datasize + 1:
                # Require at least one byte extra space, so that
                # when the buffer is most full, `tail == head - 1`.
                # This distinguishes the case of an empty buffer, when
                # `tail == head`.
                if not block or (timeout is not None and timeout <= 0):
                    raise Full
                while self._vacancy() < 4 + datasize + 1:
                    if timeout is None:
                        with self._notfull:
                            if not self._notfull.wait(None):
                                raise Full
                    else:
                        t = max(0, deadline - monotonic())
                        with self._notfull:
                            if not self._notfull.wait(t):
                                raise Full

            idx_1 = self._put_slice(header)
            if datasize > 0:
                idx_2 = self._put_slice(data, idx_1)
                self._tail.value = idx_2
            else:
                self._tail.value = idx_1

            with self._notempty:
                self._qsize.value += 1
                self._notempty.notify()

    def put_bytes_nowait(self, data):
        self.put_bytes(data, False)

    def put(self, obj, block=True, timeout=None):
        x = _ForkingPickler.dumps(obj)
        self.put_bytes(x, block=block, timeout=timeout)

    def put_nowait(self, obj):
        self.put(obj, False)

    def get_bytes(self, block=True, timeout=None) -> bytes:
        if self._closed:
            raise ValueError(f"{self!r} is closed")
        with self._rlock:
            if self._qsize.value == 0:
                if not block:
                    raise Empty
                with self._notempty:
                    if not self._notempty.wait(timeout):
                        raise Empty
            header, idx_1 = self._get_slice(4)
            datasize = int.from_bytes(header, 'little')
            if datasize > 0:
                data, idx_2 = self._get_slice(datasize, idx_1)
                data = bytes(data)
                # this must be done before updating `_head`,
                # otherwise the data in these several bytes could be changed before use.
            else:
                data, idx_2 = b'', idx_1

            # DEBUG: fill will '-' for visualization
            # if idx_2 > self._head.value:
            #     for k in range(self._head.value, idx_2):
            #         self._buf[k:k+1] = b'-'
            # else:
            #     for k in range(self._head.value, self._buffer_size):
            #         self._buf[k:k+1] = b'-'
            #     for k in range(idx_2):
            #         self._buf[k:k+1] = b'-'

            self._head.value = idx_2
            # Index of the first byte of the new first element in the buffer.
            # The element could be nonexistent.

            with self._notfull:
                self._qsize.value -= 1
                self._notfull.notify()

        return data

    def get_bytes_nowait(self):
        return self.get_bytes(False)

    def get(self, block=True, timeout=None):
        z = self.get_bytes(block=block, timeout=timeout)
        zz = _ForkingPickler.loads(z)
        return zz

    def get_nowait(self):
        return self.get(False)
