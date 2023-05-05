# noqa: I001, F401, F801

import pytest


def test_deprecated():
    from mpservice.util import (
        ProcessPoolExecutor, ThreadPoolExecutor, get_shared_process_pool, get_shared_thread_pool,
        MP_SPAWN_CTX, Process, RemoteException, RemoteTraceback, SpawnProcess, get_remote_traceback, is_remote_exception,
        get_docker_host_ip, is_async,
        MAX_THREADS, Thread,
        SpawnProcessPoolExecutor,
    )

    with pytest.warns(DeprecationWarning):
        from mpservice.server_process import Manager
        assert True