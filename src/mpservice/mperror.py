import sys
from traceback import format_exc

# Using `concurrent.futures.ProcessPoolExecutor`
# or `asyncio.get_event_loop().run_in_executor`
# will handle remote exceptions properly, hence
# using those in place of a raw Process is recommended
# when possible.


def full_class_name(cls):
    if not isinstance(cls, type):
        cls = cls.__class__
    mod = cls.__module__
    if mod is None or mod == 'builtins':
        return cls.__name__
    return mod + '.' + cls.__name__


# TODO
# check out
#   https://github.com/ionelmc/python-tblib
#   https://stackoverflow.com/questions/6126007/python-getting-a-traceback-from-a-multiprocessing-process
# about pickling Exceptions with tracebacks.


class MPError(Exception):
    # An object of this class can be properly pickled
    # to survice transmission in `multiprocessing.Queue`.
    def __init__(self, e: Exception):
        # This instance must be created right after the exception `e`
        # was raised, before any other exception was raised,
        # for otherwise, we can't get the traceback for `e`.
        self.name = e.__class__.__name__
        self.qualname = full_class_name(e.__class__)
        self.trace_back = format_exc()
        self._args = e.args
        self._repr_e = repr(e)
        self._str_e = str(e)

    @property
    def args(self):
        return self._args

    def __repr__(self):
        return self._repr_e

    def __str__(self):
        return self._str_e


_excepthook_ = sys.excepthook


def _my_excepthook(type_, value, tb):
    if type_ is MPError:
        # With this hook, the printout upon
        #   `raise SubProcessError(ValueError('wrong value'))`
        # is like what happens upon
        #   `raise ValueError('wrong value')`
        # in the sub process.
        # The word `SubProcessError` does not appear in the printout.
        # The traceback is what's relevant in the subprocess where the error happened,
        # instead of what's relevant with respect to where `raise SubProcessError(...)` is called.
        print(value.trace_back, file=sys.stderr)
    else:
        _excepthook_(type, value, tb)


sys.excepthook = _my_excepthook
