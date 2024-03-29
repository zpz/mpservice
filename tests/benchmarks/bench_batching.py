from random import seed, uniform
from time import perf_counter, sleep

from mpservice.mpserver import ProcessWorker, Server, Servlet
from zpz.logging import config_logger


class Double(ProcessWorker):
    def call(self, x):
        sleep(uniform(0.05, 0.2))
        if isinstance(x, list):
            return [v * 2 for v in x]
        return x * 2


def main(model):
    seed(100)
    data = range(100000)
    t0 = perf_counter()
    s = model.stream(data)
    n = 0
    for _ in enumerate(s):
        n += 1
        pass
    t1 = perf_counter()
    print('finished', n, 'items in', t1 - t0, 'seconds')


if __name__ == '__main__':
    print('')
    seed(100)
    config_logger(with_process_name=True)
    s = Servlet(Double, batch_size=1000, batch_wait_time=0.01)
    with Server(s) as model:
        main(model)
