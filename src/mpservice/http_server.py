import logging
from typing import Union

import uvicorn  # type: ignore
from asgiref.typing import ASGIApplication  # such as `starlette.applications.Starlette`


logger = logging.getLogger(__name__)


# About server shutdown:
#  https://github.com/encode/uvicorn/issues/742
#  https://stackoverflow.com/questions/58133694/graceful-shutdown-of-uvicorn-starlette-app-with-websockets

# See tests for examples of server shutdown.


# Adapted from `uvicorn.main.run`.
def make_server(
        app: Union[str, ASGIApplication],
        *,
        host='0.0.0.0',
        port: int = 8000,
        log_level: str = None,
        debug: bool = None,
        access_log: bool = None,
        loop='auto',
        workers=1,
        **kwargs,
):
    '''
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

    `workers`: when used for `mpservice.mpserver.MPServer`, this should be 1.
    '''
    if log_level is None:
        log_level = logging.getLevelName(logger.getEffectiveLevel()).lower()
    assert log_level in ('debug', 'info', 'warning', 'error')

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
        host=host,
        port=port,
        access_log=access_log,
        debug=debug,
        log_level=log_level,
        loop=loop,
        workers=workers,
        reload=debug and isinstance(app, str),
        **kwargs)
    server = uvicorn.Server(config=config)

    return server


def run_app(app, **kwargs):
    server = make_server(app, **kwargs)
    return server.run()
