__all__ = ['ProcessRunner', 'ProcessRunnee']

from abc import ABC, abstractmethod
from typing import Any

from . import Process, Queue, remote_exception


class ProcessRunnee(ABC):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    @abstractmethod
    def __call__(self, *args, **kwargs) -> Any:
        raise NotImplementedError


class ProcessRunner:
    """
    `ProcessRunner` creates a custom object (of a subclass of `ProcessRunnee`)
    in a "background" process, and calls the object's `__call__` method as needed,
    any number of times, while keeping the background process alive until `join` is called.

    Some initial motivations for this class include:

    - The custom object may perform nontrivial setup once, and keep the setup in effect
      through the object's lifetime.

    - User may pass certain data to the custom object's `__init__` via `ProcessRunner.__init__`.
      A particular use case is to pass in a `mpservice.multiprocessing.Queue` or `mpservice.streamer.IterableQueue`,
      because these objects can't be passed to the custom object via `ProcessRunner.restart`.

      Note that a `mpservice.multiprocessing.Queue` can't be contained in another `mpservice.multiprocessing.Queue`,
      nor can it be passed to a `mpservice.multiprocessing.Manager` in a call to a "proxy method".
      If either is possible, the class `ProcessRunner` is not that needed.

    In some use cases, `ProcessRunner`, along with `IterableQueue` and its method `renew`, are useful in stream processing.
    """

    def __init__(
        self,
        *,
        target: type[ProcessRunnee],
        args=None,
        kwargs=None,
        name=None,
    ):
        self._instructions = Queue()
        self._result = Queue(maxsize=1)
        self._process = Process(
            target=self._work,
            kwargs={
                'instructions': self._instructions,
                'result': self._result,
                'worker_cls': target,
                'args': args or (),
                'kwargs': kwargs or {},
            },
            name=name,
        )

    @staticmethod
    def _work(
        instructions,
        result,
        worker_cls: type[ProcessRunnee],
        args,
        kwargs,
    ):
        with worker_cls(*args, **kwargs) as worker:
            while True:
                zz = instructions.get()
                if zz is None:
                    break
                try:
                    z = worker(*zz[0], **zz[1])
                except Exception as e:
                    z = remote_exception.RemoteException(e)
                result.put(z)

    def start(self):
        self._process.start()

    def join(self, timeout=None):
        self._instructions.put(None)
        self._process.join(timeout=timeout)

    def restart(self, *args, **kwargs):
        self._instructions.put((args, kwargs))

    def rejoin(self):
        z = self._result.get()
        if isinstance(z, Exception):
            raise z
        return z

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.join()

    def __call__(self, *args, **kwargs):
        self.restart(*args, **kwargs)
        return self.rejoin()
