import asyncio
import time
from mpservice.async_streamer import Stream

NX = 100


async def inc(x):
    await asyncio.sleep(1)
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


async def streamed(workers):
    t0 = time.perf_counter()
    s = Stream(data()).transform(inc, workers=workers)

    result = await s.collect()
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)
    print(result)


print('streamed')
asyncio.run(streamed(workers=100))
# This took 1.0075 seconds on my 4-core Linux machine,
# compared to the perfect value 1.0000.

print('')
print('10-streamed')
asyncio.run(streamed(workers=10))
# This took 10.0181 seconds on my 4-core Linux machine,
# compared to the perfect value 10.0000.

print('')
print('unistreamed')
asyncio.run(streamed(workers=1))
# This took 100.1306 seconds on my 4-core Linux machine,
# compared to the perfect value 100.0000.

print('')
print('plain')
asyncio.run(plain())
# This took 100.1310 seconds on my 4-core Linux machine.
