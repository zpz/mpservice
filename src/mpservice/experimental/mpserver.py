import queue
import threading
from collections.abc import Iterable, Iterator
from typing import final

from mpservice.mpserver import (
    MP_SPAWN_CTX,
    NOMOREDATA,
    RemoteException,
    Servlet,
    Thread,
    _init_server,
    _SimpleProcessQueue,
    _SimpleThreadQueue,
)


class StreamServer:
    '''
    This class is experimental.

    This class is an alternative to :meth:`Server.stream`.
    It has a simpler implementation and more focused (narrower but likely enough)
    use case. This was created in hope of bringing a speedup, but surprisingly
    it has not demonstrated speed (or throughput) benefits. Until then,
    there is no plan to deprecate :meth:`Server.stream`.
    '''

    @final
    @classmethod
    def get_mp_context(cls):
        return MP_SPAWN_CTX

    def __init__(
        self,
        servlet: Servlet,
        *,
        capacity: int = 256,
    ):
        _init_server(self, servlet=servlet, capacity=capacity)

    @property
    def capacity(self) -> int:
        return self._capacity

    def __enter__(self):
        self._q_in = (
            _SimpleThreadQueue()
            if self.servlet.input_queue_type == 'thread'
            else _SimpleProcessQueue()
        )
        self._q_out = (
            _SimpleThreadQueue()
            if self.servlet.output_queue_type == 'thread'
            else _SimpleProcessQueue()
        )
        self.servlet.start(self._q_in, self._q_out)
        return self

    def __exit__(self, *args):
        self.servlet.stop()




    def stream(
        self,
        data_stream: Iterable,
        /,
        *,
        return_x: bool = False,
        return_exceptions: bool = False,
    ) -> Iterator:
        """
        Use this method for high-throughput processing of a long stream of
        data elements. In theory, this method achieves the throughput upper-bound
        of the server, as it saturates the pipeline.

        The order of elements in the stream is preserved, i.e.,
        elements in the output stream correspond to elements
        in the input stream in the same order.

        This method is NOT thread-safe, that is, there can not be multiple users
        calling this method concurrently from multiple threads.

        Parameters
        ----------
        data_stream
            An (possibly unlimited) iterable of input data elements.
        return_x
            If ``True``, each output element is a length-two tuple
            containing the input data and the result.
            If ``False``, each output element is just the result.
        return_exceptions
            If ``True``, any Exception object will be produced in the output stream
            in place of the would-be regular result.
            If ``False``, exceptions will be propagated right away, crashing the program.
        """

        def _enqueue(data_stream, uid_x, nomoredata, stopped):
            q_in = self._q_in
            try:
                for x in data_stream:
                    if stopped.is_set():
                        break
                    uid = id(x)
                    uid_x.put((uid, x))
                    q_in.put((uid, x))
            finally:
                uid_x.put(nomoredata)
            # TODO: the workers do not know a stream has concluded.
            # They may be waiting for some extra time for batching.
            # This is not a big problem for long jobs, but for short ones
            # this can be a user experience issue.

        nomoredata = NOMOREDATA
        uid_x = queue.Queue(self._capacity)
        stopped = threading.Event()

        worker = Thread(
            target=_enqueue,
            args=(data_stream, uid_x, nomoredata, stopped),
            name=f"{self.__class__.__name__}.stream._enqueue",
        )
        worker.start()

        uid_y = {}
        null = object()
        q_out = self._q_out
        n = 0
        try:
            while True:
                z = uid_x.get()
                # Get one input item from ``uid_x``.
                # Get its corresponding result, either already in cache ``uid_y``,
                # or to be obtained out of ``q_out``.
                if z == nomoredata:
                    break
                uid, x = z
                y = uid_y.pop(uid, null)
                while y is null:
                    uid_, y_ = q_out.get()
                    if uid_ == uid:
                        y = y_
                    else:
                        uid_y[uid_] = y_

                # Exception could happen in the following block,
                # either by `raise y`, or due to `ExitGenerator`.
                if isinstance(y, RemoteException):
                    y = y.exc
                if isinstance(y, Exception) and not return_exceptions:
                    raise y
                if return_x:
                    yield x, y
                else:
                    yield y
        finally:
            stopped.set()
            n = 0  # number of input items that have not been accounted for
            while z != nomoredata:
                z = uid_x.get()
                n += 1
            n -= 1
            n -= len(uid_y)
            # Now need to get count ``n`` results out of ``q_out``.
            # No need to pair them with input; just discard.
            for _ in range(n):
                q_out.get()
            worker.join()
