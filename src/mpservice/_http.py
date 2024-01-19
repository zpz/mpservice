import asyncio
import logging
import os
import signal
import socket
import sys
import time
import warnings
from typing import Any, Callable, List, Optional, TypeVar

import click
import uvicorn

from mpservice.multiprocessing import Event, Process

logger = logging.getLogger(__name__)


# See `uvicorn`.
class Server(uvicorn.Server):
    def run(
        self,
        *,
        stop_requested: Event,
        sockets: list[socket.socket] | None = None,
        worker_context: Any = None,
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

        global _stop_requested
        assert _stop_requested is None, f'{_stop_requested} is None'
        _stop_requested = stop_requested
        self._stop_requested = stop_requested  # to be used in `on_tick`.

        return super().run(sockets)

    async def on_tick(self, *args, **kwargs):
        return (await super().on_tick(*args, **kwargs)) or self._stop_requested.is_set()


# See `uvicorn`.
class Multiprocess(uvicorn.supervisors.Multiprocess):
    def run(
        self,
        stop_requested: Event,
        worker_contexts: list[Any],
    ):
        self.startup(stop_requested=stop_requested, worker_contexts=worker_contexts)
        while True:
            if self.should_exit.is_set() or stop_requested.is_set():
                break
            time.sleep(0.15)
        self.shutdown()

    def startup(
        self,
        stop_requested: Event,
        worker_contexts: list[Any],
    ) -> None:
        message = 'Started parent process [{}]'.format(str(self.pid))
        color_message = 'Started parent process [{}]'.format(
            click.style(str(self.pid), fg='cyan', bold=True)
        )
        logger.info(message, extra={'color_message': color_message})

        for sig in uvicorn.supervisors.multiprocess.HANDLED_SIGNALS:
            signal.signal(sig, self.signal_handler)

        for _idx in range(self.config.workers):
            process = get_subprocess(
                config=self.config,
                target=self.target,
                sockets=self.sockets,
                worker_context=worker_contexts[_idx],
                stop_requested=stop_requested,
                # Customization: pass in `worker_context` and `stop_requested`.
            )
            process.start()
            self.processes.append(process)


# See `uvicorn`.
def get_subprocess(
    config: uvicorn.Config,
    target: Callable[..., None],
    sockets: List[socket.socket],
    worker_context: Any,
    stop_requested: Event,
) -> Process:
    """
    Called in the parent process, to instantiate a new child process instance.
    The child is not yet started at this point.

    * config - The Uvicorn configuration instance.
    * target - A callable that accepts a list of sockets. In practice this will
               be the `Server.run()` method.
    * sockets - A list of sockets to pass to the server. Sockets are bound once
                by the parent process, and then passed to the child processes.
    """
    # We pass across the stdin fileno, and reopen it in the child process.
    # This is required for some debugging environments.

    # There are three customizations to this function:
    #   - take ``worker_idx``
    #   - use ``MP_SPAWN_CTX``
    #   - use ``stop_requested``

    stdin_fileno: Optional[int]
    try:
        stdin_fileno = sys.stdin.fileno()
    except OSError:
        stdin_fileno = None

    kwargs = {
        'config': config,
        'target': target,
        'sockets': sockets,
        'stdin_fileno': stdin_fileno,
        'worker_context': worker_context,
        'stop_requested': stop_requested,
    }

    return Process(target=subprocess_started, kwargs=kwargs)


# See `uvicorn`.
def subprocess_started(
    config: uvicorn.Config,
    target: Callable[..., None],
    sockets: List[socket.socket],
    stdin_fileno: Optional[int],
    worker_context: Any,
    stop_requested: Event,
) -> None:
    """
    Called when the child process starts.

    * config - The Uvicorn configuration instance.
    * target - A callable that accepts a list of sockets. In practice this will
               be the `Server.run()` method.
    * sockets - A list of sockets to pass to the server. Sockets are bound once
                by the parent process, and then passed to the child processes.
    * stdin_fileno - The file number of sys.stdin, so that it can be reattached
                     to the child process.
    """
    # There is one customization to this function:
    #   - take and use ``worker_idx``

    # Re-open stdin.
    if stdin_fileno is not None:
        sys.stdin = os.fdopen(stdin_fileno)

    # Logging needs to be setup again for each child.
    config.configure_logging()

    # Now we can call into `Server.run(sockets=sockets)`
    target(
        stop_requested=stop_requested, sockets=sockets, worker_context=worker_context
    )


_stop_requested: Event = None
# When a new process is spawned, this variable will be `None`
# in that process upon importing this module.
# This variable is initialized in `start_server` in the "main" process
# and re-assigned by `Server.run` in the main and child processes.


async def stop_server():
    """
    This function is to be called in a web service endpoint to request termination
    of the service.
    """
    _stop_requested.set()
    await asyncio.sleep(0.2)
    # TODO:
    # wait and verify things have stopped?


UNSET = object()

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
    log_config=UNSET,
    **kwargs,
):
    """
    This function is intended for lauching a service that uses :mod:`mpservice.mpserver`.

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
    **kwargs
        Passed to ``uvicorn.Config``.
    """
    if log_config is not UNSET:
        warnings.warn(
            '`log_config` is ignored since 0.14.1 and will be an error in 0.15.0',
            DeprecationWarning,
            stacklevel=2,
        )

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
    stop_requested = Event()
    if workers == 1:
        server.run(stop_requested=stop_requested, worker_context=worker_contexts[0])
    else:
        sock = config.bind_socket()
        Multiprocess(config, target=server.run, sockets=[sock]).run(
            stop_requested=stop_requested, worker_contexts=worker_contexts
        )

    if config.uds and os.path.exists(config.uds):
        os.remove(config.uds)  # pragma: py-win32
