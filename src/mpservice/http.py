import logging
from typing import Union

import uvicorn
from asgiref.typing import ASGIApplication  # such as `starlette.applications.Starlette`


logger = logging.getLogger(__name__)


# About server shutdown:
#  https://github.com/encode/uvicorn/issues/742
#  https://stackoverflow.com/questions/58133694/graceful-shutdown-of-uvicorn-starlette-app-with-websockets

# See tests for examples of server shutdown.

# About socket 'backlog':
# https://stackoverflow.com/a/12340078
# https://stackoverflow.com/questions/36594400/what-is-backlog-in-tcp-connections
# http://veithen.io/2014/01/01/how-tcp-backlog-works-in-linux.html

# Adapted from `uvicorn.main.run`.
def make_server(
    app: Union[str, ASGIApplication],
    *,
    host="0.0.0.0",
    port: int = 8000,
    access_log: bool = False,
    loop="auto",
    backlog: int = 64,
    **kwargs,
):
    """
    This function is specifically for use with `mpservice.mpserver`.
    The argument `workers` is fixed to 1.

    `app`: a `Starlette` instance or the import string for such
        an instance, like 'mymodule:app'.

    `loop`: if you encounter errors, esp if you need to use
        the event loop before calling this function, use 'none'.

        If you don't need to use the eventloop at all before
        calling this function, then it's OK to pass in
        `loop='auto'`. In that case, `uvicorn` will use `uvloop`
        if that package is installed (w/o creating a new loop);
        otherwise it will create a new `asyncio` native event loop
        and set it as the default loop.

    If user has their own ways to config logging, then pass in
    `log_config=None` in `kwargs`.

    `backlog`: this is passed to asyncio `loop.create_server` (in `asyncio.base_events`,
    where default is 100), and in-turn to `socket.listen`. Don't make this large
    unless you know what you're doing.
    """
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        access_log=access_log,
        loop=loop,
        workers=1,
        backlog=backlog,
        **kwargs,
    )

    server = uvicorn.Server(config=config)

    return server


def run_app(app, **kwargs):
    server = make_server(app, **kwargs)
    return server.run()
