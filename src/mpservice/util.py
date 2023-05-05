import warnings
from importlib import import_module


# This function is no longer used in this package but can be useful.
# It will be removed eventually.
def full_class_name(cls):
    if not isinstance(cls, type):
        cls = cls.__class__
    mod = cls.__module__
    if mod is None or mod == "builtins":
        return cls.__name__
    return mod + "." + cls.__name__


def __getattr__(name):
    mname = None
    if name in (
        'ProcessPoolExecutor',
        'ThreadPoolExecutor',
        'get_shared_process_pool',
        'get_shared_thread_pool',
    ):
        mname = 'mpservice.concurrent.futures'
    elif name in (
        'MP_SPAWN_CTX',
        'Process',
        'RemoteException',
        'RemoteTraceback',
        'SpawnProcess',
        'get_remote_traceback',
        'is_remote_exception',
    ):
        mname = 'mpservice.multiprocessing'
    elif name in ('get_docker_host_ip', 'is_async'):
        mname = 'mpservice.socket'
    elif name in ('MAX_THREADS', 'Thread'):
        mname = 'mpservice.threading'
    elif name == 'SpawnProcessPoolExecutor':
        from mpservice.concurrent.futures import ProcessPoolExecutor

        warnings.warn(
            "'mpservice.util.SpawnProcessPoolExecutor' is deprecated in 0.12.7 and will be removed in 0.14.0. Use 'mpservice.concurrent.futures.ProcessPoolExecutor' instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return ProcessPoolExecutor
    else:
        raise AttributeError(f"module 'mpservice.util' has no attribute '{name}'")

    m = import_module(mname)
    o = getattr(m, name)
    warnings.warn(
        f"'mpservice.util.{name}' is deprecated in 0.12.7 and will be removed in 0.14.0. Use '{mname}.{name}' instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return o
