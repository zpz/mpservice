import queue
from time import sleep

from mpservice.experimental.streamer import EagerBatcher
from mpservice.threading import Thread


def test_eager_batcher():
    def stuff(q):
        sleep(0.2)
        q.put('OK')
        q.put(1)
        q.put(2)
        sleep(0.1)
        q.put(3)
        q.put(4)
        sleep(0.05)
        q.put(5)
        sleep(0.4)
        q.put(6)
        sleep(0.3)
        q.put(7)
        sleep(0.25)
        q.put(None)

    q = queue.Queue()
    stuffer = Thread(target=stuff, args=(q,))
    stuffer.start()
    walker = EagerBatcher(q, batch_size=3, timeout=0.2)
    q.get()
    zz = list(walker)
    print(zz)
    assert zz == [[1, 2, 3], [4, 5], [6], [7]]
