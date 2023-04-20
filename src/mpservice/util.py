import warnings

# The following imports are provided for back compat, and will be removed at a later time.
# Please import from the corresponding modules.
from .concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    get_shared_process_pool,
    get_shared_thread_pool,
)
from .multiprocessing import (
    MP_SPAWN_CTX,
    Process,
    RemoteException,
    RemoteTraceback,
    SpawnProcess,
    get_remote_traceback,
    is_remote_exception,
)
from .socket import get_docker_host_ip, is_async
from .threading import MAX_THREADS, Thread

SpawnProcessPoolExecutor = ProcessPoolExecutor

warnings.warn(
    "``mpservice.util`` is deprecated in 0.12.4 and will be removed after 0.13.0.",
    warnings.DeprecationWarning,
    stacklevel=2,
)

# This function is no longer used in this package but can be useful.
# It will be removed eventually.
def full_class_name(cls):
    if not isinstance(cls, type):
        cls = cls.__class__
    mod = cls.__module__
    if mod is None or mod == "builtins":
        return cls.__name__
    return mod + "." + cls.__name__
