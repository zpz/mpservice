import logging
import logging.handlers
import warnings


def logger_thread(q):
    '''
    In main thread, start another thread with this function as `target`.
    '''
    while True:
        record = q.get()
        if record is None:
            # User should put a `None` in `q` to indicate stop.
            break
        logger = logging.getLogger(record.name)
        if record.levelno >= logger.getEffectiveLevel():
            logger.handle(record)


def forward_logs(q):
    '''
    In a Process, run this function at the start to put all log messages
    ever produced in that process in the queue that is consumed by
    `logger_thread`.

    During the execution of the process, logging should not be configured.
    Logging config should happen in the main process/thread.
    '''
    root = logging.getLogger()
    if root.handlers:
        warnings.warn('root logger has handlers: {}; deleted'.format(root.handlers))
        root.handlers = []
    root.setLevel(logging.DEBUG)
    qh = logging.handlers.QueueHandler(q)
    root.addHandler(qh)
