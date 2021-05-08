"""
Configure logging, mainly the format.

A call to the function ``config_logger`` in a launching script is all that is needed to set up the logging format.
Usually the 'level' argument is the only argument one needs to customize::

  config_logger(level='info')

If `level` is not specified, environment variable `LOGLEVEL` is used;
if that is not set, a default level (currently 'info') is used.

Do not call this in library modules.
Library modules should have ::

   logger = logging.getLogger(__name__)

and then just use ``logger`` to write logs without concern about formatting,
destination of the log message, etc.
"""
from datetime import datetime
import logging
from logging import Formatter
import time
from typing import Union, Dict
import warnings

import pytz

#import os
# raiseExceptions = os.environ.get('ENVIRONMENT_TYPE', None) in ('test', 'dev')
# When exceptions are raised during logging, then,
# The default implementation of handleError() in Handler checks to see if a module-level variable,
# raiseExceptions, is set.
# If set, a traceback is printed to sys.stderr.
# If not set, the exception is swallowed.
#
# If no logging configuration is provided, then for Python 2.x,
#   If logging.raiseExceptions is False (production mode), the event is silently dropped.
#   If logging.raiseExceptions is True (development mode), a message 'No handlers could be found for logger X.Y.Z' is printed once.

# logging.logThreads = 0
# logging.logProcesses = 0


def log_level_to_str(level):
    '''
    `level`: `logging.DEBUG`, `logging.INFO`, etc.
    '''
    return logging.getLevelName(level)
    # Return uppercase 'DEBUG', 'INFO', etc.


def log_level_from_str(level):
    '''
    `level`: 'debug', 'info', etc.
    '''
    return getattr(logging, level.upper())


def _make_config(
        *,
        level: Union[str, int] = 'info',
        with_process_name: bool = False,
        with_thread_name: bool = False,
        timezone: str = 'US/Pacific',
        rich: bool = True,
        **kwargs) -> Dict:
    # 'level' is string form of the logging levels: 'debug', 'info', 'warning', 'error', 'critical'.
    if level not in (logging.DEBUG, logging.INFO, logging.WARNING,
                     logging.ERROR, logging.CRITICAL):
        level = getattr(logging, level.upper())  # type: ignore

    # TODO: looks like the following formatter is not used.
    if timezone.lower() == 'UTC':
        Formatter.converter = time.gmtime
    elif timezone.lower() == 'local':
        Formatter.converter = time.localtime
    else:
        def custom_time(*args):
            utc_dt = pytz.utc.localize(datetime.utcnow())
            my_tz = pytz.timezone(timezone)
            converted = utc_dt.astimezone(my_tz)
            return converted.timetuple()
        Formatter.converter = custom_time

    datefmt = '%Y-%m-%d %H:%M:%S'

    msg = '[%(asctime)s.%(msecs)03d ' + timezone + \
        '; %(levelname)s; %(name)s, %(funcName)s, %(lineno)d]'
    msg += '  '

    if with_process_name:
        if with_thread_name:
            fmt = f'{msg}[%(processName) %(threadName)s]  %(message)s'
        else:
            fmt = f'{msg}[%(processName)s  %(message)s]'
    elif with_thread_name:
        fmt = f'{msg}[%(threadName)s]  %(message)s'
    else:
        fmt = f'{msg}%(message)s'

    return dict(format=fmt, datefmt=datefmt, level=level, **kwargs)


def config_logger(**kwargs) -> None:
    kw = _make_config(**kwargs)

    rootlogger = logging.getLogger()
    if rootlogger.hasHandlers():
        rootlogger.handlers = []

    logging.basicConfig(**kw)

    logging.captureWarnings(True)
    warnings.filterwarnings('default', category=ResourceWarning)
    warnings.filterwarnings('default', category=DeprecationWarning)

    # This is how to turn on asyncio debugging.
    # Don't turn it on in this function.
    # asyncio.get_event_loop().set_debug(True)
