"""
The package `mpservice <https://github.com/zpz/mpservice>`_ provides utilities for Python concurrency, including

- Serving with multiprocessing to make full use of multiple cores,
  and batching to take advantage of vectorized computation if some
  components of the service have that capability.

  One use case is machine learning model serving, although the code is generic
  and not restricted to this particular use case.

- Stream processing, i.e. processing a long, possibly infinite stream
  of input data, with multiple operators in the pipeline. A main use case
  is that one or more of the operators is I/O bound or CPU bound,
  hence can benefit from concurrency.

The serving and streaming utilities can be combined because a ``mpservice.mpserver.Server`` instance,
while doing heavy-lifting in other processes, acts as an I/O bound operator in the main process.
Indeed, ``mpservice.mpserver.Server`` provides method ``stream`` to process data streams
using the server largely running in other processes.

A ``Server`` object could be used either in "embedded" mode or to back a service.
Utilities are provided in ``mpservice.http``, ``mpservice.socket``, and ``mpservice.pipe``
for the latter use case.

The package also contains some other related utilities.
"""

__version__ = "0.11.8b3"
