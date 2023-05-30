"""
The module ``mpservice.http`` provides simple utilities for serving :class:`mpservice.mpserver.AsyncServer` over HTTP
using `uvicorn <https://www.uvicorn.org/>`_ and `starlette <https://www.starlette.io/>`_.

In practice, you may choose any other Python HTTP server library as long as it implements
the `ASGI specification <https://asgi.readthedocs.io/en/latest/>`_.

This utility code is not directly connected to :class:`~mpservice.mpserver.AsyncServer`, because AsyncServer simply
provides the method :meth:`~mpservice.mpserver.AsyncServer.call` that can be called from an HTTP
request handler function. There needs to be no particular ties between the request handler and ``uvicorn.Server``.

Below is one way to structure it.
In this example, we use a global ``context`` object to arrange some connections.

::

    import asyncio
    from types import SimpleNamespace
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse, JSONResponse
    from mpservice.mpserver import AsyncServer
    from mpservice.http import make_server


    async def handle_request(request):
        ...
        ...
        result = await context.model.call(...)
        ...
        return JSONResponse(...)


    async def stop(request):
        context.server.should_exit = True
        return PlainTextResponse("server shutdown as requested", status_code=200)


    context = SimpleNamespace()


    async def main():
        app = Starlette()
        app.add_route('/', handle_request, ['GET'])
        app.add_route('/stop', stop, ['POST])
        server = make_server(app)

        context.app = app
        context.server = server

        async with AsyncServer(...) as model:
            context.model = model

            # Start infinite loop
            await server.serve()

    if __name__ == '__main__':
        asyncio.run(main())

There is no async function calls exception for ``await server.serve()``,
but the async ``main`` allows user to call other async functions such as
setting up async context managers..

If you want to use ``uvloop`` to start ``main``, check out the 
`uvloop documentation <https://github.com/MagicStack/uvloop#using-uvloop>`_.
"""
from __future__ import annotations

import logging

import uvicorn
from asgiref.typing import ASGIApplication  # such as `starlette.applications.Starlette`

__all__ = ['make_server']


logger = logging.getLogger(__name__)


# About server shutdown:
#  https://github.com/encode/uvicorn/issues/742
#  https://stackoverflow.com/questions/58133694/graceful-shutdown-of-uvicorn-starlette-app-with-websockets

# See tests for examples of server shutdown.

# About socket 'backlog':
# https://stackoverflow.com/a/12340078
# https://stackoverflow.com/questions/36594400/what-is-backlog-in-tcp-connections
# http://veithen.io/2014/01/01/how-tcp-backlog-works-in-linux.html


def make_server(
    app: str | ASGIApplication,
    *,
    host: str = "0.0.0.0",
    port: int = 8000,
    access_log: bool = False,
    backlog: int = 128,
    **kwargs,
) -> uvicorn.Server:
    """
    This function is specifically for use with :mod:`mpservice.mpserver`.

    This function is adapted from ``uvicorn.main.run``.

    Parameters
    ----------
    app
        A `starlette.application.Starlette <https://www.starlette.io/>`_ instance or the import string for such
        an instance, like ``'mypackage.mymodule:app'``.
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
    **kwargs
        Passed to ``uvicorn.Config``.
        If user has their own ways to config logging, then pass in
        ``log_config=None`` through ``**kwargs``.
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
