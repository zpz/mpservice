import logging
import logging.handlers
import multiprocessing
import multiprocessing.queues
import threading



class ProcessLogger:
    '''
    Logging messages produced in worker processes are tricky.
    First, some settings should be concerned in the main process only,
    including log formatting, log-level control, log handler (destination), etc.
    Specifically, these should be settled in the "launching script", and definitely
    should not be concerned in worker processes.
    Second, the terminal printout of loggings in multiple processes tends to be
    intermingled and mis-ordered.

    This class uses a queue to transmit all logging messages that are produced
    in a worker process to the main process/thread, to be handled there.

    Usage:

        1. In main process, create a `ProcessLogger` instance and start it:

                pl = ProcessLogger(ctx=...).start()

        2. Pass this object to other processes. (Yes, this object is picklable.)

        3. In the other process, start it. Suppose the object is also called `pl`,
           then do

                pl.start()

    Remember to call the `stop` method in both main and the other process.
    For this reason, you may want to use the context manager.
    '''
    def __init__(self, *, ctx: multiprocessing.context.BaseContext):
        # assert ctx.get_start_method() == 'spawn'
        self._ctx = ctx
        self._t = None
        self._creator = True

    def __getstate__(self):
        assert self._creator
        assert self._t is not None
        return self._q

    def __setstate__(self, state):
        # In another process.
        self._q = state
        self._creator = False

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    def start(self):
        if self._creator:
            self._start_in_main_process()
        else:
            self._start_in_other_process()
        return self

    def stop(self):
        if self._creator:
            assert self._t is not None
            self._q.put(None)
            self._t.join()
        else:
            self._q.close()

    def _start_in_main_process(self):
        assert self._t is None
        self._q = self._ctx.Queue()

        self._t = threading.Thread(target=self._logger_thread, args=(self._q, ))
        self._t.start()

    def _start_in_other_process(self):
        '''
        In a Process (created using the "spawn" method),
        run this function at the beginning to set up putting all log messages
        ever produced in that process into the queue that will be consumed
        in the main process by `self._logger_thread`.

        During the execution of the process, logging should not be configured.
        Logging config should happen in the main process/thread.
        '''
        root = logging.getLogger()
        if root.handlers:
            print('root logger has handlers: {}; deleted'.format(root.handlers))
        root.setLevel(logging.DEBUG)
        qh = logging.handlers.QueueHandler(self._q)
        root.addHandler(qh)

    @staticmethod
    def _logger_thread(q: multiprocessing.queues.Queue):
        threading.current_thread().name = 'logger_thread'
        while True:
            record = q.get()
            if record is None:
                # This is put in the queue by `self.stop()`.
                break
            logger = logging.getLogger(record.name)
            if record.levelno >= logger.getEffectiveLevel():
                logger.handle(record)
