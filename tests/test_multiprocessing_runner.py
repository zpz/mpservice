import mpservice
from mpservice.streamer import IterableQueue
from mpservice.multiprocessing.runner import ProcessRunner, ProcessRunnee


class MyProcessRunnee(ProcessRunnee):
    def __init__(self, in_queue, out_queue, factor):
        self._in_queue = in_queue
        self._out_queue = out_queue
        self._factor = factor

    def __call__(self, multiplier):
        for x in self._in_queue:
            self._out_queue.put(x * multiplier)
        self._out_queue.put_end()
        return multiplier * self._factor


def test_process_runner():
    q_in = IterableQueue(mpservice.multiprocessing.Queue())
    q_out = IterableQueue(mpservice.multiprocessing.Queue())

    runner = ProcessRunner(
        target=MyProcessRunnee,
        args=(q_in, q_out, 3),
    )
    with runner:
        q_in.put(1)
        q_in.put(2)
        q_in.put(3)
        q_in.put(4)
        q_in.put_end()

        runner.restart(multiplier=2)
        z = runner.rejoin()
        assert z == 6

        assert list(q_out) == [2, 4, 6, 8]
        q_in.renew()
        q_out.renew()

        q_in.put(9)
        q_in.put(12)
        q_in.put(13)
        q_in.put(8)
        q_in.put_end()
        z = runner(multiplier=3)
        assert z == 9
        assert list(q_out) == [27, 36, 39, 24]
        q_in.renew()
        q_out.renew()
