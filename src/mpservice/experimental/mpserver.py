import queue
import threading
from collections.abc import Iterable, Iterator
from typing import final
import concurrent.futures

from mpservice.mpserver import (
    MP_SPAWN_CTX,
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

        def _enqueue(data_stream, x_fut, uid_fut, stopped):
            q_in = self._q_in
            Future = concurrent.futures.Future
            try:
                for x in data_stream:
                    if stopped.is_set():
                        break
                    uid = id(x)
                    fut = Future()
                    x_fut.put((x, fut))
                    uid_fut[uid] = fut
                    q_in.put((uid, x))
            except Exception as e:
                x_fut.put(e)
            else:
                x_fut.put(None)
            finally:
                q_in.put(None)
            # TODO: the workers do not know a stream has concluded.
            # They may be waiting for some extra time for batching.
            # This is not a big problem for long jobs, but for short ones
            # this can be a user experience issue.

        def _collect(uid_fut):
            q_out = self._q_out
            while True:
                z = q_out.get()
                if z is None:
                    break
                uid, y = z
                fut = uid_fut.pop(uid)
                if isinstance(y, RemoteException):
                    y = y.exc
                if isinstance(y, Exception):
                    fut.set_exception(y)
                else:
                    fut.set_result(y)


        x_fut = queue.Queue(self._capacity)
        uid_fut = {}
        stopped = threading.Event()

        feeder = Thread(
            target=_enqueue,
            args=(data_stream, x_fut, uid_fut, stopped),
            name=f"{self.__class__.__name__}.stream._enqueue",
        )
        feeder.start()

        collector = Thread(
            target=_collect,
            args=(uid_fut,),
            name=f"{self.__class__.__name__}.stream._collect",
        )
        collector.start()

        try:
            while True:
                z = x_fut.get()
                if z is None:
                    break
                try:
                    x, fut = z
                except TypeError:
                    raise z
                try:
                    y = fut.result()
                except Exception as e:
                    if not return_exceptions:
                        raise
                    y = e
                if return_x:
                    yield x, y
                else:
                    yield y
        finally:
            stopped.set()
            while True:
                try:
                    x_fut.get_nowait()
                except queue.Empty:
                    break
            collector.join()
            feeder.join()
