import multiprocessing

from mpservice.remote_exception import RemoteException


def goo(x, q):
    try:
        if x < 10:
            q.put(x)
        else:
            raise ValueError('wrong value!')
    except:
        q.put(RemoteException())


def test_exception():
    mp = multiprocessing.get_context('spawn')
    q = mp.Queue()
    p = mp.Process(target=goo, args=(20, q))
    p.start()
    p.join()

    y = q.get()
    assert isinstance(y, RemoteException)
    assert str(y) == 'ValueError: wrong value!'

    print('')
    y.print()
