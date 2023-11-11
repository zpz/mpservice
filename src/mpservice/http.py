"""
The module ``mpservice.http`` provides simple utilities for serving :class:`mpservice.mpserver.AsyncServer` over HTTP
using `uvicorn <https://www.uvicorn.org/>`_ and `starlette <https://www.starlette.io/>`_.

This utility code is not directly connected to :class:`~mpservice.mpserver.AsyncServer`, because AsyncServer simply
provides the method :meth:`~mpservice.mpserver.AsyncServer.call` that can be called from an HTTP
request handler function. The example below shows one way to connect things:

::

    # "example.py"

    import asyncio
    import contextlib
    import os
    from starlette.applications import Starlette
    from starlette.routing import Route
    from starlette.responses import PlainTextResponse, JSONResponse
    from mpservice.mpserver import AsyncServer
    from mpservice.http import start_server, stop_server


    async def handle_request(request):
        ...
        ...
        result = await request.state.model.call(...)
        ...
        return JSONResponse(...)


    async def stop(request):
        await stop_server()
        return PlainTextResponse("server shutdown as requested", status_code=200)


    @contextlib.asynccontextmanager
    async def lifespan(app):
        async with AsyncServer(...) as model:
            yield {'model': model}  # available in endpoint functions via `request.state`


    app = Starlette(lifespan=lifespan,
                    routes=[
                        Route('/', handle_request),
                        Route('/stop', stop, methods=['POST']),
                    ])

    if __name__ == '__main__':
        start_server('example:app')

As demonstrated, you can set up (async) context managers and other things
in ``lifespan`` and make things available via the lifespan's "state" as needed.
The ``app`` received by ``lifespan`` has an attribute ``worker_context``
(which is added by our customization) that can be used to pass config-style
data from the main process (where ``start_server`` is called) to the worker processes.

The use of ``starlette`` is very lightweight: it just handles HTTP
routing and request acceptance/response.
"""
from __future__ import annotations

import logging

import uvicorn
from deprecation import deprecated

from ._http import ASGIApplication, start_server, stop_server

__all__ = ['start_server', 'stop_server']


logger = logging.getLogger(__name__)


# About server shutdown:
#  https://github.com/encode/uvicorn/issues/742
#  https://stackoverflow.com/questions/58133694/graceful-shutdown-of-uvicorn-starlette-app-with-websockets

# See tests for examples of server shutdown.

# About socket 'backlog':
# https://stackoverflow.com/a/12340078
# https://stackoverflow.com/questions/36594400/what-is-backlog-in-tcp-connections
# http://veithen.io/2014/01/01/how-tcp-backlog-works-in-linux.html


@deprecated(
    deprecated_in='0.14.1', removed_in='0.15.0', details='use ``start_server`` instead'
)
def make_server(
    app: str | ASGIApplication,
    *,
    host: str = '0.0.0.0',
    port: int = 8000,
    access_log: bool = False,
    backlog: int = 128,
    **kwargs,
) -> uvicorn.Server:
    """
    This function is *deprecated* in favor of ``start_server`` and ``stop_server``.

    This function is comparable to ``start_server`` with ``workers=1``.

    NOTE: this function does not work with ``stop_server``; instead, set ``server.should_stop=True`` for that effect,
    where ``server`` is the output of this function.
    """
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        access_log=access_log,
        workers=1,
        backlog=backlog,
        **kwargs,
    )

    server = uvicorn.Server(config=config)

    return server
