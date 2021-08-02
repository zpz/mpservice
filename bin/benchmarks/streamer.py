import time
from mpservice.streamer import Stream

NX = 100


def inc(x):
    time.sleep(1)
    return x + 1


def data():
    for i in range(NX):
        yield i


def plain():
    result = []
    t0 = time.perf_counter()
    for x in data():
        result.append(inc(x))
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)
    print(result)


def streamed(workers):
    t0 = time.perf_counter()
    s = Stream(data()).transform(inc, workers=workers)

    result = s.collect()
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)
    print(result)


print('streamed')
streamed(workers=100)
# This took 1.0183 seconds on my 4-core Linux machine,
# compared to the perfect value 1.0000.

print('')
print('10-streamed')
streamed(workers=10)
# This took 10.0186 seconds on my 4-core Linux machine,
# compared to the perfect value 10.0000.

print('')
print('unistreamed')
streamed(workers=1)
# This took 100.1112 seconds on my 4-core Linux machine,
# compared to the perfect value 100.0000.

print('')
print('plain')
plain()
# This took 100.1073 seconds on my 4-core Linux machine.
