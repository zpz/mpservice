from __future__ import annotations

import functools
import inspect
import subprocess


def get_docker_host_ip():
    """
    From within a Docker container, this function finds the IP address
    of the host machine.
    """
    # INTERNAL_HOST_IP=$(ip route show default | awk '/default/ {print $3})')
    # another idea:
    # ip -4 route list match 0/0 | cut -d' ' -f3
    #
    # Usually the result is '172.17.0.1'
    #
    # The command `ip` is provided by the Linux package `iproute2`.

    z = subprocess.check_output(["ip", "-4", "route", "list", "match", "0/0"])
    z = z.decode()[len("default via ") :]
    return z[: z.find(" ")]


def is_async(func):
    while isinstance(func, functools.partial):
        func = func.func
    return inspect.iscoroutinefunction(func) or (
        not inspect.isfunction(func)
        and hasattr(func, "__call__")
        and inspect.iscoroutinefunction(func.__call__)
    )


def full_class_name(cls):
    if not isinstance(cls, type):
        cls = cls.__class__
    mod = cls.__module__
    if mod is None or mod == "builtins":
        return cls.__name__
    return mod + "." + cls.__name__
