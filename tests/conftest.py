import asyncio
import pytest


# Redefine the `event_loop` fixture to properly close async generators
# that are prematurelly abandoned. See `test_streamer.py::test_async_parmap`.
#
# See https://github.com/pytest-dev/pytest-asyncio/issues/222
# https://github.com/pytest-dev/pytest-asyncio/pull/309
#
# The following code is for Python 3.10.
# In 3.11, the `asyncio.runners.Runner` code is cleaner.
@pytest.fixture(scope='function')
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    try:
        yield loop
    finally:
        try:
            asyncio.runners._cancel_all_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            loop.close()
