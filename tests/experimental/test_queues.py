import pytest
from mpservice.experimental.asyncio import IterableQueue as AsyncIterableQueue, QueueFinished
from mpservice.multiprocessing import Process
from mpservice.experimental.multiprocessing.queues import IterableQueue as IterableProcessQueue
from mpservice.experimental.queue import Finished, IterableQueue
from mpservice.threading import Thread


def _iterqueue_put(q: IterableQueue[int] | IterableProcessQueue[int]):
    q.put(100)
    return 100


def _iterqueue_get(q):
    return q.get()


def test_iterqueue():
    for cls in (IterableQueue, IterableProcessQueue):
        print(cls)
        q = cls()
        for x in range(30):
            q.put(x)
        q.finish()
        with pytest.raises(Finished):
            q.put(3)
        assert list(q) == list(range(30))
        with pytest.raises(Finished):
            q.put(4)
        with pytest.raises(Finished):
            _ = q.get()

        if cls is IterableQueue:
            Pool = Thread
        else:
            Pool = Process

        w = Pool(target=_iterqueue_get, args=(q,))
        with pytest.raises(Finished):
            w.start()
            w.join()
            print(w.result())

        w = Pool(target=_iterqueue_put, args=(q,))
        with pytest.raises(Finished):
            w.start()
            w.join()
            print(w.result())


@pytest.mark.asyncio
async def test_asynciterqueue():
    q = AsyncIterableQueue()
    for x in range(30):
        await q.put(x)
    await q.finish()

    assert [x async for x in q] == list(range(30))

    with pytest.raises(QueueFinished):
        await q.put(8)

    with pytest.raises(QueueFinished):
        await q.get()
