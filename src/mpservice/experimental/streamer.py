import queue
from collections.abc import Iterable
from time import perf_counter


class EagerBatcher(Iterable):
    '''
    ``EagerBatcher`` collects items from the incoming stream towards a target batch size and yields the batches.
    For each batch, after getting the first item, it will yield the batch either it has collected enough items
    or has reached ``timeout``. Note, the timer starts upon getting the first item, whereas getting the first item
    for a new batch may take however long.
    '''

    def __init__(
        self,
        instream,
        /,
        batch_size: int,
        timeout: float = None,
        endmarker=None,
    ):
        # ``instream`` is a queue (thread or process) with the specicial value ``endmarker``
        # indicating the end of the stream.
        # ``timeout`` can be 0.
        self._instream = instream
        self._batch_size = batch_size
        if timeout is None:
            timeout = 3600 * 24  # effectively unlimited wait
        self._timeout = timeout
        self._endmarker = endmarker

    def __iter__(self):
        q_in = self._instream
        batchsize = self._batch_size
        timeout = self._timeout
        end = self._endmarker

        while True:
            z = q_in.get()  # wait as long as it takes to get one item.
            if (z is None if end is None else z == end):
                break

            batch = [z]
            n = 1
            deadline = perf_counter() + timeout
            # Timeout starts after the first item is obtained.

            while n < batchsize:
                t = deadline - perf_counter()
                try:
                    # If `t <= 0`, still get the next item
                    # if it's already available.
                    # In other words, if data elements are already here,
                    # get more towards the target batch-size
                    # even if it's already past the timeout deadline.
                    z = q_in.get(timeout=max(0, t))
                except queue.Empty:
                    break
                else:
                    if z is None if end is None else z == end:
                        break
                    batch.append(z)
                    n += 1

            yield batch
