from time import sleep, perf_counter
from random import uniform, seed
from mpservice.mpserver import SimpleServer
from zpz.logging import config_logger

seed(100)

def double(x):
    sleep(uniform(0.1, 0.3))
    if isinstance(x, list):
        return [v * 2 for v in x]
    return x * 2


def main():
    config_logger(with_process_name=True)
    model = SimpleServer(double, batch_size=1000, batch_wait_time=0.1, max_queue_size=1000)

    data = range(100000)
    with model:
        t0 = perf_counter()
        s = model.stream(data)
        n = 0
        for _ in enumerate(s):
            n += 1
            pass
        t1 = perf_counter()
        print('finished', n, 'items in', t1 - t0, 'seconds')


if __name__ == '__main__':
    main()








