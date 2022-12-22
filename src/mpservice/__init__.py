"""
The package `mpservice <https://github.com/zpz/mpservice>`_ provides utilities for Python concurrency, including most notably

1. Serving with multiprocessing to make full use of multiple cores,
   and batching to take advantage of vectorized computation if some
   components of the service have that capability.

   One use case is machine learning model serving, although the code is generic
   and not restricted to this particular use case.

   A :class:`mpservice.mpserver.Server` object could be used either in "embedded" mode or to back a service.
   Utilities are provided in :mod:`mpservice.http`, :mod:`mpservice.socket`, and :mod:`mpservice.pipe`
   for the latter use case.
2. Stream processing, i.e. processing a long, possibly infinite stream
   of input data, with multiple operators in the pipeline. A main use case
   is that one or more of the operators is I/O bound or CPU bound,
   hence can benefit from concurrency.
"""

__version__ = "0.11.8"
