import warnings

warnings.warn(
    "'mpservice.multiprocessing.context' became 'mpservice.multiprocessing._context' in 0.14.3. Please import its (exposed) content from 'mpservice.multiprocessing'.",
    DeprecationWarning,
    stacklevel=2,
)

from ._context import *  # noqa
