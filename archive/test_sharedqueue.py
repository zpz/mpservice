import multiprocessing
from queue import Empty, Full

import pytest
from mpservice._fastqueue import Queue


@pytest.fixture(params=[None, 'spawn'])
def mp(request):
    return multiprocessing.get_context(request.param)


def test_number():
    for x in range(1000000):
        assert int.from_bytes(x.to_bytes(4, 'little'), 'little') == x


def test_basic(mp):
    q = Queue(ctx=mp)
    q.put_bytes(b'123abc')
    q.get_bytes() == b'123abc'
    q.put({'a': 3, 'b': [23, 25.6]})
    q.get() == {'a': 3, 'b': [23, 25.6]}

    q.close()
    with pytest.raises(ValueError):
        q.put(23)

    x = [[1, 2, 'a', ['badf', 98.3]], {382: ['a', 'x', 6], 'xyz': (5, -3)}, 'yes man!', 432]
    q = Queue(ctx=mp, maxsize_bytes=100)
    for v in x * 1000:
        q.put(v)
        assert q.get() == v
    q.close()

    q = Queue(ctx=mp, maxsize_bytes=100)
    for v in range(10000):
        q.put(v)
        assert q.get() == v


def test_boundary(mp):
    q = Queue(ctx=mp, maxsize_bytes=20)
    q.put_bytes(b'1234')
    assert q._head.value == 0
    assert q._tail.value == 8
    q.put_bytes(b'abcd')
    assert q._tail.value == 16
    with pytest.raises(Full):
        q.put_bytes_nowait(b'12')
    assert q.get_bytes() == b'1234'
    assert q._head.value == 8
    assert q._tail.value == 16
    assert not q.empty()
    assert not q.full()
    assert q.qsize() == 1
    q.put_bytes(b'123')
    assert q._tail.value == 3
    assert q.get_bytes() == b'abcd'
    assert q.get_bytes() == b'123'
    assert q.empty()
    assert q._head.value == 3
    assert q._tail.value == 3
    q.close()

    q = Queue(ctx=mp, maxsize_bytes=10)
    q.put_bytes(b'12345')
    assert q._tail.value == 9
    assert q.get_bytes() == b'12345'
    assert q._head.value == 9
    assert q._tail.value == 9
    q.put_bytes(b'123')
    assert q._head.value == 9
    assert q._tail.value == 6
    assert q.get_bytes() == b'123'
    assert q.empty()


def simple_put(q, n):
    for k in range(n):
        q.put(k)
    q.put(None)


def simple_get(q):
    n = 0
    while True:
        z = q.get()
        if z is None:
            break
        n += 1
    q.put(n)


def test_simple_mp(mp):
    print('')
    q = Queue(ctx=mp, maxsize_bytes=100)
    n = 12345
    pput = mp.Process(target=simple_get, args=(q, ))
    pget = mp.Process(target=simple_put, args=(q, n))
    pput.start()
    pget.start()
    pput.join()
    pget.join()
    assert q.get() == n


