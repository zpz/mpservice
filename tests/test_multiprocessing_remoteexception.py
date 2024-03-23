import logging
import multiprocessing
import pickle
import sys
from types import TracebackType

import pytest
from mpservice.multiprocessing.remote_exception import (
    RemoteException,
    get_remote_traceback,
    is_remote_exception,
)

logger = logging.getLogger(__name__)


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
