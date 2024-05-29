import logging
import os
import signal
import socket
import sys
import threading
from typing import Any, TypeVar
from weakref import WeakValueDictionary

import uvicorn
from uvicorn.main import STARTUP_FAILURE

from mpservice.multiprocessing import Event, SpawnProcess

logger = logging.getLogger(__name__)


# See `uvicorn`.
class Server(uvicorn.Server):  # pragma: no cover
    def run(
        self,
        *,
        sockets: list[socket.socket] | None = None,
        worker_context: Any = None,
        server_id: str,
        stop_event: Event = None,
    ) -> None:
        # When there are multiple worker processes, this method is the
        # `target` function that runs in a new process.
        config = self.config
        if not config.loaded:
            config.load()

        app = config.loaded_app
        while hasattr(app, 'app'):
            app = app.app
        assert not hasattr(app, 'worker_context')
        app.worker_context = worker_context
        # `app.worker_context` is designed to be used in the "lifespan"
        # of `app`. See Starlette documentation on "lifespan".

        global _servers
        if _servers is None:
            _servers = WeakValueDictionary()
        assert server_id not in _servers
        if stop_event is None:
            _servers[server_id] = self
        else:
            _servers[server_id] = stop_event
        self._stop_event = stop_event

        return super().run(sockets)

    async def on_tick(self, *args, **kwargs):
        return (await super().on_tick(*args, **kwargs)) or (
            self._stop_event is not None and self._stop_event.is_set()
        )


class Process(uvicorn.supervisors.multiprocess.Process):  # pragma: no cover
    def __init__(
        self, config, target, sockets, *, worker_context, server_id, stop_event
    ):
        super().__init__(config, target, sockets)

        self._worker_context = worker_context
        self._server_id = server_id
        self._stop_event = stop_event

        target, args, kwargs = (
            self.process._target,
            self.process._args,
            self.process._kwargs,
        )
        self.process = SpawnProcess(target=target, args=args, kwargs=kwargs)

    def target(self, sockets: list[socket.socket] | None = None) -> Any:
        if os.name == 'nt':  # pragma: py-not-win32
            # Windows doesn't support SIGTERM, so we use SIGBREAK instead.
            # And then we raise SIGTERM when SIGBREAK is received.
            # https://learn.microsoft.com/zh-cn/cpp/c-runtime-library/reference/signal?view=msvc-170
            signal.signal(
                signal.SIGBREAK,  # type: ignore[attr-defined]
                lambda sig, frame: signal.raise_signal(signal.SIGTERM),
            )

        threading.Thread(target=self.always_pong, daemon=True).start()
        return self.real_target(
            sockets=sockets,
            worker_context=self._worker_context,
            server_id=self._server_id,
            stop_event=self._stop_event,
        )


# See `uvicorn`.
class Multiprocess(uvicorn.supervisors.Multiprocess):  # pragma: no cover
    def __init__(
        self, config, target, sockets, *, worker_contexts, server_id, stop_event
    ):
        super().__init__(config=config, target=target, sockets=sockets)
        self._worker_contexts = worker_contexts
        self._server_id = server_id
        self.should_exit = stop_event  # override the parent one

    def init_processes(self):
        for _idx in range(self.processes_num):
            process = Process(
                self.config,
                self.target,
                self.sockets,
                worker_context=self._worker_contexts[_idx],
                server_id=self._server_id,
                stop_event=self.should_exit,
            )
            process.start()
            self.processes.append(process)

    def terminate_all(self):
        # Do not force terminate
        return


async def stop_server(server_id: str = '0'):
    """
    This function is to be called in a web service endpoint to request termination
    of the service.
    """
    # If `start_server` was called with `workers=1`, then the `Server` object runs in the same process/thread
    # where `start_server` was called. This function is involked in the same process/thread, and we set the
    # simple `should_exit` flag to stop the server.
    # If `start_server` was called with `workers > 1`, then multiple `Server` objects run in their own processes.
    # This function is invoked in the process of one `Server`. If this function is called only once,
    # then only one `Server` (in the said process) will be stopped. To avoid asking client to call this multiple times,
    # (and the client wouldn't know how many times they should call,)
    # We use a multiprocessing Event object shared between the server processes to get the signal across.

    s = _servers.get(server_id)
    if s is None:
        return
    if isinstance(s, Server):
        s.should_exit = True
    else:
        s.set()  # this is "propagated" to other worker processes as well as the ``Multiprocess`` object.


_servers: WeakValueDictionary[str, Server | Event] = None


ASGIApplication = TypeVar('ASGIApplication')


def start_server(
    app: ASGIApplication | str,
    *,
    host: str = '0.0.0.0',
    port: int = 8000,
    access_log: bool = False,
    backlog: int = 128,
    workers: int = 1,
    worker_contexts: list = None,
    log_config=None,
    server_id: str = '0',
    **kwargs,
):
    """
    This function is intended for launching a service that uses :mod:`mpservice.mpserver`.

    This function is adapted from ``uvicorn.main.run``.

    Parameters
    ----------
    app
        Usually `starlette.applications.Starlette <https://www.starlette.io/>`_ instance or the import string for such
        an instance, like ``'mypackage.mymodule:app'``. The module as named, `'mypackage.mymodule'`, must be able
        to be imported. If `app` is defined in a script (say `example.py`) that is in the current working directory,
        that is, if you type

            python example.py

        in the directory that contains `example.py`, then Python will be able to import that script.
        In that case, `app` can be written as `'example:app'`.

        If `workers > 1`, this must be a str, because a `Starlette` object cannot be pickled and sent
        to other processes.
    host
        The default ``'0.0.0.0'`` is suitable if the code runs within a Docker container.
    port
        Port.
    access_log
        Whether to let ``uvicorn`` emit access logs.
    backlog
        The default should be adequate. Don't make this large unless you know what you're doing.

        `uvicorn.Server.startup <https://github.com/encode/uvicorn/blob/master/uvicorn/server.py>`_
        passes this to AsyncIO ``loop.create_server`` (in
        `asyncio.base_events <https://github.com/python/cpython/blob/main/Lib/asyncio/base_events.py>`_,
        where default is 100), and in turn to
        `socket.socket.listen <https://docs.python.org/3/library/socket.html#socket.socket.listen>`_.

        ``uvicorn`` uses a large default value (2048). That might be misguided.

        See: https://bugs.python.org/issue38699, https://stackoverflow.com/a/2444491, https://stackoverflow.com/a/2444483,
    workers
        If ``1``, the server is launched in the current process/thread. If ``> 1``, this many processes
        are spawned, each running one copy of ``app`` independently.

        Note that worker processes are not dynamically created and killed according to need---this fixed
        number of processes are started and they remain active---this is in contrast to Gunicorn.
        This also makes the parameter ``worker_contexts`` meaningful.

        Since ``mpservice.mpserver`` handles sophisticated multiprocessing servers "natively",
        usually you should use ``workers=1``, that is, let ``uvicorn`` run a simple "in-process"
        worker (implemented by a ``mpservice.mpserver.AsyncServer`` object) and let ``AsyncServer``
        handle multiple worker processes.

        There *may* be cases where ``workers > 1`` has some speed advantage. One possible reason
        for such advantage might come from saving on pickling overhead. You need to bencharmk
        you particular use case to decide.
    worker_contexts
        If provided, this must be a list or tuple with as many as ``workers`` elements.
        The elements of ``worker_contexts`` will become the attribute ``worker_context`` of ``app``
        in the corresponding worker process (or the current process if ``workers=1``).

        This is mainly designed for passing config-style data to worker processes when ``workers > 1``.
        The ``app`` object is available to the "lifespan" of a ``Starlette`` object; user can obtain
        ``app.worker_context`` there and decide how to use it to initialize things, save it, as well
        as make it available to endpoint functions via the lifespan's "state". See tests for example usage.
    server_id
        If you run multiple services in the same program,
        you need to use `server_id` to distinguish them and provide the ID to both `start_server` and `stop_server`.

        However, that use case seems extrememly unlikely.
    **kwargs
        Passed to ``uvicorn.Config``.
    """
    assert log_config is None
    # `log_config` is ignored since 0.14.1.

    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        access_log=access_log,
        workers=workers,
        backlog=backlog,
        log_config=None,
        **kwargs,
    )

    if not worker_contexts:
        worker_contexts = [None] * workers

    server = Server(config=config)
    try:
        if workers == 1:
            server.run(worker_context=worker_contexts[0], server_id=server_id)
        else:
            sock = config.bind_socket()
            mult = Multiprocess(
                config,
                target=server.run,
                sockets=[sock],
                worker_contexts=worker_contexts,
                server_id=server_id,
                stop_event=Event(),
            )
            mult.run()
    finally:
        if config.uds and os.path.exists(config.uds):
            os.remove(config.uds)  # pragma: py-win32

    if not server.started and not config.should_reload and config.workers == 1:
        sys.exit(STARTUP_FAILURE)
