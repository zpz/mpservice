# mpservice

Utilities for Python concurrency, including

- Serving with multiprocessing to make full use of multiple cores,
  and batching to take advantage of vectorized computation if some
  components of the service have that capability.

  One use case is machine learning model serving, although the code is generic and not restricted to this particular use case.

- Stream processing, i.e. processing a long, possibly infinite stream
  of input data, with multiple operators in the pipeline. A main use case
  is that one or more of the operators is I/O bound (think: calling an external
  service), hence can benefit from concurrency. Both sync and async interfaces
  are provided.
  
The serving and streaming utilities can be combined because a `mpservice.mpserver.MPServer` instance, while doing heavy-lifting in other processes, acts as an
I/O bound operator in the main process. Indeed, `mpservice.mpserver.MPServer` provides methods `stream` and `async_stream` for using the server to process data streams.

A `MPServer` object could be used either in "embedded" mode or to back a HTTP service. In the latter case, the `starlette` package is a viable choice for providing async service. A few utilities in `mpservice.http_server` assist with this use case.

The package also contains some other related utilities.

To install, do `pip install mpservice`.
