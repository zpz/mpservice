from time import sleep, monotonic
from random import uniform, seed
import mpservice.mpserver
from mpservice.mpserver import SimpleServer
from zpz.logging import config_logger

def double(x):
    sleep(uniform(0.05, 0.2))
    if isinstance(x, list):
        return [v * 2 for v in x]
    return x * 2


def main(model):
    seed(100)
    data = range(100000)
    t0 = monotonic()
    s = model.stream(data)
    n = 0
    for _ in enumerate(s):
        n += 1
        pass
    t1 = monotonic()
    print('finished', n, 'items in', t1 - t0, 'seconds')


if __name__ == '__main__':
    for qtype in ('BasicQueue', 'FastQueue', 'UniQueue'):
        print('')
        print('using', qtype)
        seed(100)
        config_logger(with_process_name=True)
        model = SimpleServer(double, batch_size=1000, batch_wait_time=0.01, queue_type=qtype)
        with model:
            main(model)

