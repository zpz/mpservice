import asyncio
import time
from mpservice.async_streamer import Stream

NX = 100


async def inc(x):
    await asyncio.sleep(1)
    return x + 1


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
    print(result)


async def streamed(workers, func):
    t0 = time.perf_counter()
    s = Stream(data()).transform(func, workers=workers)

    result = await s.collect()
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)
    print(result)


print('streamed')
asyncio.run(streamed(100, inc))
# This took 1.0082 seconds on my 4-core Linux machine,
# compared to the perfect value 1.0000.

print('streamed sync')
asyncio.run(streamed(100, sinc))
# 1.0251 seconds

print('')
print('10-streamed')
asyncio.run(streamed(10, inc))
# 10.0162 seconds, compared to 10.0000.

print('')
print('10-streamed sync')
asyncio.run(streamed(10, sinc))
# 10.0234 seconds.

print('')
print('unistreamed')
asyncio.run(streamed(1, inc))
# 100.1277 seconds, compared to 100.0000.

print('')
print('unistreamed sync')
asyncio.run(streamed(1, sinc))
# 100.1160 seconds.

print('')
print('plain')
asyncio.run(plain())
# 100.1271 seconds.
