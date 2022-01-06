from multiprocessing import Process, Queue

from mpservice.mperror import MPError


def goo(x, q):
    try:
        if x < 10:
            q.put(x)
        else:
            raise ValueError('wrong value!')
    except:
        q.put(MPError())


def test_exception():
    q = Queue()
    p = Process(target=goo, args=(20, q))
    p.start()
    p.join()

    y = q.get()
    assert isinstance(y, MPError)
    assert str(y) == 'wrong value!'

    print('')
    y.print()
    print('')

    raise y


test_exception()
