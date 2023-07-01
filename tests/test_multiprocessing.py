import logging
import multiprocessing
import pickle
import sys
from time import sleep
from types import TracebackType

import pytest
from mpservice import TimeoutError
from mpservice.multiprocessing import (
    FIRST_EXCEPTION,
    Process,
    RemoteException,
    as_completed,
    get_remote_traceback,
    is_remote_exception,
    wait,
)
from mpservice.threading import Thread

logger = logging.getLogger(__name__)


def delay_double(x, delay=2):
    sleep(delay)
    if x < 10:
        return x * 2
    if x < 100:
        raise ValueError(x)
    if x < 1000:
        raise KeyboardInterrupt
    raise SystemExit()


def _test_thread_process(cls):
    # No exception
    t = cls(target=delay_double, args=(3,))
    t.start()
    logger.info('to sleep')
    sleep(0.1)
    assert not t.done()
    assert t.is_alive()
    print('TimeoutError:', TimeoutError)
    with pytest.raises(TimeoutError):
        t.result(0.1)
    with pytest.raises(TimeoutError):
        t.exception(0.1)
    assert t.result() == 6
    assert t.exception() is None
    t.join()

    # Exception
    t = cls(target=delay_double, args=(12,))
    t.start()
    with pytest.raises(ValueError):
        t.join()

    t = cls(target=delay_double, args=(13,))
    t.start()

    with pytest.raises(TimeoutError):
        t.result(0.2)

    with pytest.raises(ValueError):
        t.result()

    e = t.exception()
    assert type(e) is ValueError

    with pytest.raises(ValueError):
        t.join()

    # BaseException
    t = cls(target=delay_double, args=(200,))
    t.start()
    with pytest.raises(KeyboardInterrupt):
        t.join()
    with pytest.raises(KeyboardInterrupt):
        t.result()
    assert isinstance(t.exception(), KeyboardInterrupt)

    # SystemExit
    t = cls(target=delay_double, args=(2000,))
    t.start()
    t.join()


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_thread():
    _test_thread_process(Thread)


def test_process():
    _test_thread_process(Process)


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


def sleeper(sleep_seconds):
    if sleep_seconds > 10:
        sleep(sleep_seconds * 0.1)
        raise ValueError(sleep_seconds)
    sleep(sleep_seconds)
    return sleep_seconds


def test_terminate():
    p = Process(target=sleeper, args=(9,))
    p.start()
    sleep(1)
    p.terminate()
    p.join()


def test_wait():
    workers = [
        Process(target=sleeper, args=(2,)),
        Process(target=sleeper, args=(3,)),
        Thread(target=sleeper, args=(4,)),
    ]
    for t in workers:
        t.start()
    done, notdone = wait(workers, timeout=2.2)
    assert len(done) == 1
    assert done.pop() is workers[0]


def test_wait_exc():
    workers = [Process(target=sleeper, args=(x,)) for x in (3, 2, 13)]
    for t in workers:
        t.start()
    done, notdone = wait(workers, return_when=FIRST_EXCEPTION)
    assert len(done) == 1
    assert done.pop() is workers[2]

    workers = [Process(target=sleeper, args=(x,)) for x in (3, 2)] + [
        Thread(target=sleeper, args=(25,))
    ]
    for t in workers:
        t.start()
    done, notdone = wait(workers, return_when=FIRST_EXCEPTION)
    assert len(done) == 2
    assert notdone.pop() is workers[0]


def test_as_completed():
    workers = [Process(target=sleeper, args=(3,))] + [
        Process(target=sleeper, args=(x,)) for x in (2, 4)
    ]
    for t in workers:
        t.start()
    k = 0
    for t in as_completed(workers):
        if k == 0:
            assert t is workers[1]
        elif k == 1:
            assert t is workers[0]
        else:
            assert t is workers[2]
        k += 1
