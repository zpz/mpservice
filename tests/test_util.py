import concurrent.futures
import logging
import multiprocessing
import pickle
import sys
import traceback
from time import sleep
from types import TracebackType
import pytest
from mpservice.util import Thread, TimeoutError, SpawnProcess, MP_SPAWN_CTX
from mpservice.util import RemoteException, is_remote_exception, get_remote_traceback



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


def test_process():
    _test_thread_process(SpawnProcess)


def goo(x, q):
    try:
        if x < 10:
            q.put(x)
        else:
            raise ValueError('wrong value!')
    except Exception as e:
        q.put(RemoteException(e))


def test_exception():
    mp = multiprocessing.get_context('spawn')
    q = mp.Queue()
    p = mp.Process(target=goo, args=(20, q))
    p.start()
    p.join()

    y = q.get()
    assert isinstance(y, ValueError)
    assert str(y) == 'wrong value!'


def gee(x):
    raise ValueError(x)


# Note: the pytest plug-in 'pytest-parallel' would make this test fail.
# I don't understand why it changes the behavior of pickling Exception objects.
def test_remote_exception():
    print('')
    try:
        gee(10)
    except Exception as e:
        sysinfo = sys.exc_info()
        print('sys.exc_info:', sysinfo)
        print(e.__traceback__)
        assert e.__traceback__ is sysinfo[2]
        assert isinstance(e.__traceback__, TracebackType)
        ee = pickle.loads(pickle.dumps(e))
        assert ee.__traceback__ is None

        xx = pickle.loads(pickle.dumps(RemoteException(e)))
        assert is_remote_exception(xx)
        print('tracback:\n', get_remote_traceback(xx))
        print('cause:\n', xx.__cause__)
        assert xx.__traceback__ is None
        xxx = pickle.loads(pickle.dumps(xx))
        assert xxx.__traceback__ is None
        assert xxx.__cause__ is None

        err = RemoteException(e)
    
    err = pickle.loads(pickle.dumps(err))
    # Now, not in an exception handling context.
    # raise err
    tb = get_remote_traceback(err)

    # Need to pickle it (e.g. passing to another process), so put it in `RemoteException` again:
    e1 = pickle.loads(pickle.dumps(RemoteException(err)))
    assert is_remote_exception(e1)
    assert get_remote_traceback(e1) == tb
    # raise e1

    try:
        raise err
    except Exception as e:
        # a "remote" exc was raised. Wrap it again if we need to pickle it again
        err = RemoteException(e)

    err = pickle.loads(pickle.dumps(err))
    # raise err
    assert get_remote_traceback(err) != tb

    # An exception object w/o `except` handler context
    x = ValueError(38)
    assert x.__traceback__ is None
    with pytest.raises(ValueError):
        err = RemoteException(x)


def cfworker():
    logging.getLogger('worker').error('worker error')
    logging.getLogger('worker.warn').warning('worker warning')

    logging.getLogger('worker.info').info('worker info')
    logging.getLogger('worker.debug').debug('worker debug')


def test_concurrent_futures_executor():
    # Observe the printout of logs in the worker processes
    logging.basicConfig(
        format='[%(asctime)s.%(msecs)02d; %(levelname)s; %(name)s; %(funcName)s, %(lineno)d] [%(processName)s]  %(message)s',
        level=logging.DEBUG,
    )
    # TODO: this setting my interfere with other test. How to do it better?
    # TODO: the 'debug' level did not take effect due to pytext setting.
    # A separate script will be better to test this.
    logger.error('main error')
    logger.info('main info')
    with concurrent.futures.ProcessPoolExecutor(mp_context=MP_SPAWN_CTX) as pool:
        t = pool.submit(cfworker)
        t.result()
    logger.warning('main warning')
    logger.debug('main debug')
