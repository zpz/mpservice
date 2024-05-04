import importlib
import sys
import traceback
import warnings

# TODO
# check out
#   https://github.com/ionelmc/python-tblib
#   https://stackoverflow.com/questions/6126007/python-getting-a-traceback-from-a-multiprocessing-process
# about pickling Exceptions with tracebacks.

# See also: boltons.tbutils
# See also: package `eliot`: https://github.com/itamarst/eliot/blob/master/eliot/_traceback.py
# See also: package `pebble`

# TODO:
# checkout `concurrent.futures.process._ExceptionWithTraceback`.


def full_class_name(cls):
    if not isinstance(cls, type):
        cls = cls.__class__
    mod = cls.__module__
    if mod is None or mod == "builtins":
        return cls.__name__
    return mod + "." + cls.__name__
