import logging
from time import sleep
import pytest
from mpservice.util import Thread, TimeoutError, SpawnProcess
from mpservice.remote_exception import RemoteException


logger = logging.getLogger(__name__)


def delay_double(x, delay=2):
    sleep(delay)
    if x < 10:
        return x * 2
    raise ValueError(x)


def _test_thread_process(cls):
    t = cls(target=delay_double, args=(3,))
    t.start()
    logger.info('to sleep')
    sleep(0.1)
    assert not t.done()
    assert t.is_alive()
    with pytest.raises(TimeoutError):
        y = t.result(0.1)
    with pytest.raises(TimeoutError):
        y = t.exception(0.1)
    assert t.result() == 6
    assert t.exception() is None
    t.join()

    t = cls(target=delay_double, args=(12,))
    t.start()
    with pytest.raises(TimeoutError):
        y = t.result(0.2)

    with pytest.raises(ValueError):
        y = t.result()

    e = t.exception()
    assert type(e) is ValueError

    t.join()


def test_thread():
    _test_thread_process(Thread)


def _process_func(x):
    print('in worker process')
    return x


def test_process():
    _test_thread_process(SpawnProcess)