import asyncio
import random
import time
from threading import Thread

import pytest

from mpservice.rate_limiter import AsyncRateLimiter, Empty, Full, RateLimiter, Ring


def test_ring():
    ring = Ring(4)
    assert ring.empty()
    assert not ring.full()
    assert len(ring) == 0

    ring.push(1)
    ring.push(2)
    assert len(ring) == 2

    assert ring.head() == 1
    assert ring.tail() == 2

    assert ring.pop() == 1
    assert len(ring) == 1

    ring.push(3)
    ring.push(4)
    assert not ring.empty()
    assert not ring.full()

    ring.push(6)
    assert ring.full()

    with pytest.raises(Full):
        ring.push(7)

    assert ring.pop() == 2
    assert len(ring) == 3
    assert ring.head() == 3
    assert ring.tail() == 6

    assert ring.pop() == 3
    assert len(ring) == 2

    ring.push(7)
    ring.push(8)
    assert ring.full()

    assert ring.pop() == 4
    assert ring.pop() == 6
    assert len(ring) == 2
    assert ring.head() == 7
    assert ring.tail() == 8

    assert ring.pop() == 7
    assert ring.pop() == 8
    assert ring.empty()

    with pytest.raises(Empty):
        ring.pop()


def test_rate_limiter():
    limiter = RateLimiter(3)
    t0 = time.perf_counter()
    for x in range(20):
        limiter.wait()
    t1 = time.perf_counter()
    print('time taken:', t1 - t0)
    assert 6 < t1 - t0 < 7


def test_rate_limiter_threads():
    def worker(n, limiter):
        for x in range(n):
            limiter.wait()
            print(x)
            time.sleep(random.uniform(0.01, 0.03))

    limiter = RateLimiter(5, time_window_in_seconds=2)
    workers = [Thread(target=worker, args=(n, limiter)) for n in (4, 5, 7, 9, 12)]
    t0 = time.perf_counter()
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    t1 = time.perf_counter()
    print('time taken:', t1 - t0)

    mult, rem = divmod(4 + 5 + 7 + 9 + 12, 5)
    print(mult * 2, rem)
    assert mult * 2 <= t1 - t0 < mult * 2 + 1.0


@pytest.mark.asyncio
async def test_async_rate_limiter():
    limiter = AsyncRateLimiter(3)
    t0 = time.perf_counter()
    for x in range(20):
        await limiter.wait()
    t1 = time.perf_counter()
    print('time taken:', t1 - t0)
    assert 6 < t1 - t0 < 7


@pytest.mark.asyncio
async def test_async_rate_limiter_tasks():
    async def worker(n, limiter):
        for x in range(n):
            await limiter.wait()
            print(x)
            await asyncio.sleep(random.uniform(0.01, 0.03))

    limiter = AsyncRateLimiter(5, time_window_in_seconds=2)
    tasks = [worker(n, limiter) for n in (4, 5, 7, 9, 12)]
    t0 = time.perf_counter()
    await asyncio.gather(*tasks)
    t1 = time.perf_counter()
    print('time taken:', t1 - t0)

    mult, rem = divmod(4 + 5 + 7 + 9 + 12, 5)
    print(mult * 2, rem)
    assert mult * 2 <= t1 - t0 < mult * 2 + 1.0
