import asyncio
import time
from mpservice.streamer import AsyncStream as Stream

NX = 100


async def inc(x):
    await asyncio.sleep(1)
    return x + 1


async def dec(x):
    await asyncio.sleep(0.1)
    return x - 1


def sdec(x):
    time.sleep(0.01)
    return x - 1


def sinc(x):
    time.sleep(1)
    return x + 1


def data():
    for i in range(NX):
        yield i


async def plain():
    result = []
    t0 = time.perf_counter()
    for x in data():
        result.append(await inc(x))
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)
    assert result == list(range(1, NX+1))


async def streamed(workers, func):
    t0 = time.perf_counter()
    s = Stream(data()).transform(func, workers=workers)

    result = await s.collect()
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)
    assert result == list(range(1, NX+1))


async def chained(workers, f1, f2):
    t0 = time.perf_counter()
    s = (
        Stream(data())
        .transform(f1, workers=workers)
        .buffer(30)
        .transform(f2, workers=workers)
    )

    result = await s.collect()
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)

    assert result == list(range(NX))


print('streamed')
asyncio.run(streamed(100, inc))
# 1.0094 seconds
# 4-core Linux machine

print('')
print('streamed sync')
asyncio.run(streamed(100, sinc))
# 1.0175 seconds

print('')
print('chained async sync')
asyncio.run(chained(100, inc, sdec))
# 1.0297 seconds

print('')
print('chained sync async')
asyncio.run(chained(100, sinc, dec))
# 1.1272 seconds

print('')
print('10-streamed')
asyncio.run(streamed(10, inc))
# 10.0201 seconds

print('')
print('10-streamed sync')
asyncio.run(streamed(10, sinc))
# 10.0198 seconds

print('')
print('10-chained async sync')
asyncio.run(chained(10, inc, sdec))
# 10.0266 seconds

print('')
print('10-chained sync async')
asyncio.run(chained(10, sinc, dec))
# 10.1243 seconds

print('')
print('unistreamed')
asyncio.run(streamed(1, inc))
# 100.1345 seconds

print('')
print('unistreamed sync')
asyncio.run(streamed(1, sinc))
# 100.1037 seconds

print('')
print('uni-chained async sync')
asyncio.run(chained(1, inc, sdec))
# 100.0990 seconds

print('')
print('uni-chained sync async')
asyncio.run(chained(1, sinc, dec))
# 100.2087 seconds

print('')
print('plain')
asyncio.run(plain())
# 100.1271 seconds.
