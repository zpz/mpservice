import asyncio
import time
from mpservice.streamer import transform, stream, buffer


async def inc(x):
    await asyncio.sleep(1)
    return x + 1


def data():
    for i in range(100):
        yield i


async def plain():
    result = []
    t0 = time.perf_counter()
    for x in data():
        result.append(await inc(x))
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)
    print(result)


async def streamed():
    t0 = time.perf_counter()
    z = transform(stream(data()), inc, workers=100)
    z = buffer(z, 20)

    result = [x async for x in z]
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)
    print(result)


print('streamed')
asyncio.run(streamed())
# This took 1.0164 seconds on my 4-core Linux machine.

print('')
print('plain')
asyncio.run(plain())
# This took 100.1098 seconds on my 4-core Linux machine.
