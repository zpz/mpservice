"""The module ``mpservice.pipe`` provides tools to use a "named pipe" to communicate between
two Python processes on the same machine.

Usually the two Python processes are two separately started programs.
If they are two processes created by `multiprocessing`_ in a single program,
then you would directly use `multiprocessing.Pipe <https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Pipe>`_ instead of this module.

To start, create a :class:`Server` object in one process and a :class:`Client` object in
the other process, providing the same ``path`` argument.
The two objects can be created in any order, and their roles are symmetric.
The different names simply remind the user to create one of each
in the two processes.

Two uni-directional pipes are created between the two processes, represented
by the files named ``f"{path}.1"`` and ``f"{path}.2"``.
In each process, you :meth:`~_Pipe.send` to one pipe and :meth:`~_Pipe.recv` from the other pipe.
The ``send`` and ``recv`` functions take and return picklable Python objects.
While ``send`` does not block as long as system buffer has space,
``recv`` blocks until one data item is read.

It's up to the application to design handshaking values understood by both sides.

The roles of the two pipes in the two processes are flipped;
this role assignment is take care of internally.
To prevent glitches, make sure the two files are non-existent before server and client
are created.
"""

import os
import stat
from multiprocessing.connection import Connection


def _mkfifo(path: str):
    if os.path.exists(path):
        assert stat.S_ISFIFO(
            os.stat(path).st_mode
        ), f"file '{path}' exists but is not a FIFO"
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            os.mkfifo(path)
        except FileExistsError:
            # The file may have been created by the other side of the connection
            # in another process since the above `exists` check.
            assert stat.S_ISFIFO(
                os.stat(path).st_mode
            ), f"file '{path}' exists but is not a FIFO"


class _Pipe:
    """
    See `multiprocessing.connection.Connection <https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Connection>`_ for documentation on the
    methods :meth:`send`, :meth:`recv`, :meth:`send_bytes`, :meth:`recv_bytes`, :meth:`recv_bytes_into`.
    """

    def __init__(self, rpath: str, wpath: str):
        self._rpath = os.path.abspath(rpath)
        self._wpath = os.path.abspath(wpath)
        _mkfifo(self._rpath)
        _mkfifo(self._wpath)

        hw = os.open(self._wpath, os.O_SYNC | os.O_CREAT | os.O_RDWR)
        self._writer = Connection(hw, readable=False)
        self._reader = None

    def send_bytes(self, buf, offset=0, size=None):
        self._writer.send_bytes(buf, offset=offset, size=size)

    def send(self, obj):
        self._writer.send(obj)

    def _get_reader(self):
        if self._reader is None:
            # Open for reading will block until the other end
            # has opened the same path for writing.
            # That's why we don't open this in `__init__`.
            # In contrast, open for writing does not block.
            hr = os.open(self._rpath, os.O_RDONLY)
            self._reader = Connection(hr, writable=False)
        return self._reader

    def recv_bytes(self, maxlength=None):
        return self._get_reader().recv_bytes(maxlength)

    def recv_bytes_into(self, buf, offset=0):
        return self._get_reader().recv_bytes_into(buf, offset)

    def recv(self):
        return self._get_reader().recv()


class Server(_Pipe):
    def __init__(self, path: str):
        super().__init__(path + ".1", path + ".2")


class Client(_Pipe):
    def __init__(self, path: str):
        super().__init__(path + ".2", path + ".1")
