import time
from mpservice.streamer import Stream

NX = 100


def inc(x):
    time.sleep(1)
    return x + 1


def dec(x):
    time.sleep(0.1)
    return x - 1


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

    assert result == list(range(1, NX+1))


def streamed(workers):
    t0 = time.perf_counter()
    s = Stream(data()).transform(inc, workers=workers)

    result = s.collect()
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)
    assert result == list(range(1, NX+1))


def chained(workers):
    t0 = time.perf_counter()
    s = (
        Stream(data())
        .transform(inc, workers=workers)
        .buffer(maxsize=30)
        .transform(dec, workers=workers)
    )

    result = s.collect()
    t1 = time.perf_counter()

    print('time elapsed:', t1 - t0)

    assert result == list(range(NX))


print('streamed')
streamed(workers=100)
# 1.0138 seconds
# 4-core Linux machine.

print('')
print('chained')
chained(workers=100)
# 1.1398 seconds

print('')
print('10-streamed')
streamed(workers=10)
# 10.0170 seconds

print('')
print('10-chained')
chained(workers=10)
# 10.1298 seconds

print('')
print('unistreamed')
streamed(workers=1)
# 100.1035 seconds

print('')
print('uni-chained')
chained(workers=1)
# 100.2106 seconds

print('')
print('plain')
plain()
# 100.1073 seconds
