from multiprocessing import Process, Queue

from mpservice._mperror import MPError


def goo(x, q):
    try:
        if x < 10:
            q.put(x)
        else:
            raise ValueError('wrong value!')
    except Exception as e:
        q.put(MPError(e))


def test_exception():
    q = Queue()
    p = Process(target=goo, args=(20, q))
    p.start()
    p.join()

    y = q.get()
    assert isinstance(y, MPError)
    assert y.message == 'wrong value!'
