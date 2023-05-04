import warnings

warnings.warn(
    "``mpservice.server_process`` is deprecated in 0.12.4 and will be removed after 0.13.0; use ``mpservice.multiprocessing`` instead.",
    DeprecationWarning,
    stacklevel=2,
)

from .multiprocessing import ServerProcess as Manager

__all__ = ['Manager']
