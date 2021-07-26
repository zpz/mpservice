import asyncio
import contextlib
import logging
import sys
from typing import Union

import uvicorn  # type: ignore
from starlette.responses import PlainTextResponse
from starlette.applications import Starlette


logger = logging.getLogger(__name__)

SHUTDOWN_STATUS = 267
SHUTDOWN_MSG = 'CLIENT REQUESTED SHUTDOWN'
SHUTDOWN_RESPONSE = PlainTextResponse(SHUTDOWN_MSG, SHUTDOWN_STATUS)
# Returning this response to a request will signal the server
# to shutdown.


class ShutdownMiddleware:
    def __init__(self, app, server):
        self.app = app
        self.server = server

    async def __call__(self, scope, receive, send):
        async def inner_send(message):
            if (message['type'] == 'http.response.start'
                    and message['status'] == SHUTDOWN_STATUS
                    and message['headers'][0] == (
                        b'content-length',
                        str(len(SHUTDOWN_MSG.encode())).encode()
            )):
                message['status'] = 200
                self.server.should_exit = True
                await asyncio.sleep(2.2)  # wait for shutdown
            await send(message)

        try:
            await self.app(scope, receive, inner_send)
        except (BaseException, Exception) as e:
            logger.error('shutting down on error %r', e)
            self.server.should_exit = True
            await asyncio.sleep(2.2)
            raise


# About server shutdown:
#  https://github.com/encode/uvicorn/issues/742
#  https://stackoverflow.com/questions/58133694/graceful-shutdown-of-uvicorn-starlette-app-with-websockets


async def stop_app(request):
    return SHUTDOWN_RESPONSE


# Adapted from `uvicorn.main.run`.
def make_server(
        app: Union[str, Starlette],
        *,
        port: int,
        backlog: int = 512,
        log_level: str = None,
        debug: bool = None,
        access_log: bool = None,
        loop='none',
        shutdown_path='/stop',
        **kwargs,
):
    '''
    `app`: a `Starlette` instance or the import string for such
        an instance, like 'mymodule:app'.

    `loop`: usually, leave it at 'none', esp if you need to use
        the event loop before calling this function. Otherwise,
        `uvicorn` has some inconsistent behavior between `asyncio`
        and `uvloop`.

        If you don't need to use the eventloop at all before
        calling this function, then it's OK to pass in
        `loop='auto'`. In that case, `uvicorn` will use `uvloop`
        if that package is installed (w/o creating a new loop);
        otherwise it will create a new `asyncio` native event loop
        and set it as the default loop.
    '''
    if log_level is None:
        log_level = logging.getLevelName(logger.getEffectiveLevel()).lower()
    assert log_level in ('debug', 'info', 'warning')

    if debug is None:
        debug = log_level == 'debug'
    else:
        debug = bool(debug)

    if access_log is None:
        access_log = debug
    else:
        access_log = bool(access_log)

    config = uvicorn.Config(
        app,
        host='0.0.0.0',
        port=port,
        backlog=backlog,
        access_log=access_log,
        debug=debug,
        log_level=log_level,
        loop=loop,
        workers=1,  # Fix at 1 for the `mpservice` usage.
        reload=debug and isinstance(app, str),
        **kwargs)
    server = uvicorn.Server(config=config)

    if not config.loaded:
        config.load()

    config.loaded_app = ShutdownMiddleware(config.loaded_app, server)

    if shutdown_path is not None:
        # Add the `stop` endpoint.
        a = config.loaded_app
        while not isinstance(a, Starlette):
            a = a.app
        for r in a.router.routes:
            if r.path == shutdown_path:
                raise Exception(f"path '{shutdown_path}' is alreayd used")
        a.add_route(shutdown_path, stop_app, ['GET', 'POST'])

    if (config.reload or config.workers > 1) and not isinstance(app, str):
        logging.getLogger('uvicorn.error').warning(
            'You must pass the application as an import string to enable '
            '"reload" or "workers"'
        )
        sys.exit(1)

    # `workers > 1` is not used in my use case and
    # is likely broken.

    # This part is not tested.
    if config.should_reload:
        sock = config.bind_socket()
        supervisor = uvicorn.supervisors.ChangeReload(
            config, target=server.run, sockets=[sock])
        return supervisor

    # if config.workers > 1:
    #     sock = config.bind_socket()
    #     supervisor = uvicorn.supervisors.Multiprocess(
    #         config, target=server.run, sockets=[sock])
    #     return supervisor

    return server


def run_app(app, **kwargs):
    # For tests, run this in a subprocess and call the service
    # from the main or another process using server address
    # 'http://127.0.0.1:<port>'.

    server = make_server(app, **kwargs)
    server.config.setup_event_loop()
    loop = asyncio.get_event_loop()
    if loop.is_running():
        import nest_asyncio
        nest_asyncio.apply()
        # Prevent the "this event loop is alreayd running" problem
        # encountered when using this in a `multiprocessing.Process`.
    loop.run_until_complete(server.serve(sockets=None))


@contextlib.asynccontextmanager
async def run_local_app(app, **kwargs):
    # Run the server in the same thread in an async context.
    # Call the service by other aysnc functions using server address
    # 'http://127.0.0.1:<port>'.
    # Refer to tests in `uvicorn`.
    server = make_server(app, **kwargs)
    handle = asyncio.ensure_future(server.serve(sockets=None))

    await asyncio.sleep(0.1)
    # This fixes an issue but I didn't understand it.
    # This is also found in `uvicorn.tests.utils.run_server`.

    try:
        yield server
    finally:
        await server.shutdown()
        handle.cancel()
