import warnings

from .multiprocessing import Manager

__all__ = ['Manager']

warnings.warn(
    "``mpservice.server_process`` is deprecated in 0.12.4 and will be removed after 0.13.0; use ``mpservice.multiprocessing`` instead.",
    warnings.DeprecationWarning,
    stacklevel=2,
)
