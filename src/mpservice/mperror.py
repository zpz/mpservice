'''
An Exception subclass that carries traceback info to be transported
in multiprocessing code, surviving pickling.
'''

import sys
import traceback

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

# See also: boltons.tbutils
# See also: package `eliot`: https://github.com/itamarst/eliot/blob/master/eliot/_traceback.py


class MPError(Exception):
    '''
    An object of this class should be created in a `except` block
    before any other exception was raised; otherwise, we can't get
    relevant traceback info.

    Example:
        try:
            a = 0 / 1
        except:
            e = MPError()

        # ... pass `e` around.

    A MPError object can be properly pickled.

    Following this example, one could use a `traceback.TracebackException`
    object directly to get most of the benefits.
    '''

    def __init__(self):
        ei = sys.exc_info()
        self.exc_type = ei[0]   # the exception class object
        self.exc_value = ei[1]  # the exception instance
        self.tb_exc_value = traceback.TracebackException(*ei)

    @property
    def args(self):
        return self.exc_value.args

    def __repr__(self):
        return self.exc_value.__repr__()

    def __str__(self):
        return self.exc_value.__str__()

    @property
    def stack(self):
        # Return a `traceback.StackSummary` object
        return self.tb_exc_value.stack

    def format(self, *, chain=True):
        '''
        The returned string is like the output of
        `traceback.format_exc()`.
        If one needs further control over the format of printing,
        one may want to use `self.tb_exc_value.format` directly.
        '''
        return ''.join(self.tb_exc_value.format(chain=chain))

    def print(self, *, chain=True):
        print(self.format(chain=chain), file=sys.stderr)

    def format_exc(self):
        return ''.join(self.tb_exc_value.format_exception_only())

    def print_exc(self):
        print(self.format_exc(), file=sys.stderr)


_excepthook_ = sys.excepthook


def _my_excepthook(type_, value, tb):
    if type_ is MPError:
        # With this hook, the printout upon
        #   `raise MPError(ValueError('wrong value'))`
        # is like what happens upon
        #   `raise ValueError('wrong value')`
        # in the originating process.
        # The word `MPError` does not appear in the printout.
        # The traceback is what's relevant in the process where the error happened,
        # instead of what's relevant with respect to where `raise MPError(...)` is called.
        value.print()
    else:
        _excepthook_(type_, value, tb)


sys.excepthook = _my_excepthook
