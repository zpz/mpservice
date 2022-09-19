# Changelog

## Release 0.11.5 (upcoming)

- `RemoteException` is re-written with much simplifications; the class is moved from `mpservice.remote_exception` to `mpservice.util`; the module `mpservice.remote_exception` is removed.
- Enhancements to `SpawnProcess`.
- Improvements to `mpservice.util.ProcessLogger`.
- The new constant `mpservice.util.MP_SPAWN_CTX` is a customization to the standard spawn
  context that uses `SpawnProcess` instead of `Process`.
- Use spawn method or `SpawnProcess` exclusively or by default in various places in the code.
- `Streamer.transform` gets new parameter `executor` to support multiprocessing for CPU-bound operators.
- New module `mpservice.managers` (experimental; intention is to eventually replace `server_process`).


## Release 0.11.4

- `mpservice.util.ProcessLogger` gets context manager methods.
- New class `mpservice.util.SpawnProcess`.



## Release 0.11.3

- Add dependency `asgiref`.
  Previously we've relied on getting `asgiref` from `uvicorn` dependency,
  which is a bad idea. Recently, `uvicorn` removed its dependency on `asgiref`.
- Reduce the default frequency of resource utilization logs.


## Release 0.11.2

- Refinement and simplification to `mpservice.streamer`.
- Refinement to `mpservice.server_process`.


## Release 0.11.1

- Added `mpserver.ThreadWorker` and `ThreadServlet`.
- Simplified `mpserver` parameter for CPU pinning spec.
- Added log on worker process CPU/memory utilization in `mpserver`.


## Release 0.11.0

- Refactor to `mpservice.mpserver` with API changes.
  New design allows flexible composition of sequential and ensemble setups,
  leading to considerable enhancements in capability and flexibility.
  There are also considerable improvements to the implementation
  (in terms of simplicity, elegance, robustness).
- Replaced all uses of `time.monotonic` by `time.perf_counter`, which has much
  higher resolution.
- Added module `mpservice.named_pipe`.


## Release 0.10.9

- Added (or brought back) parameter `backlog` to `MPServer`.
- Implimentation improvements: simplified utitlity queues; removed error pipe of MPServer.


## Release 0.10.8

- Finetune to `EnsembleServer`: ensemble elements could be `RemoteException`
  objects, i.e. failure of one ensemble component will not halt or invalidate
  the other components.


## Release 0.10.7

- By default, `MPServer` uses the new, custom queue type `Unique` for faster
  buildup of batches for servlets. Removed `ZeroQueue` and `BasicQueue`.
- Simplified timeout in `mpserver`. `EnqueueTimeout` is gone; `TotalTimeout` is renamed
  `Timeout`. The parameters `enqueue_timeout` and `total_timeout` are combined
  into `timeout`.
- `ServerProcess` gets new parameter `ctx` for multiprocessing context.


## Release 0.10.6

- Added alternative multiprocessing queues, namely `BasicQueue`, `FastQueue` and `ZeroQueue`,
  in an attempt to improve service speed, expecially batching.
  This topic is still under experimentation. `ZeroQueue` is not ready for use.
- Changed socket encoder from 'orjson' to 'pickle'.
- Removed `max_queue_size` parameter of `MPServer`; use unbounded queues.
- `MPServer` parameter `cpus` renamed to `main_cpu` with default value `0`.


## Release 0.10.5

- Minor fine-tuning and documentation touch-ups.


## Release 0.10.4

- Rewrote `mpservice.socket` to be fully based on `asyncio.streams`.
- Refactored socket server side to make its usage similar to a web app.
- Refactored `mpservice._streamer` for much improved robustness and simplicity.
- Streamer API changes, mainly:
  1. use context manager;
  2. operations modify the hosting object in-place, hence eliminating the need to assign
     intermediate transforms to new names.
- `Streamer.transform` now accepts both sync and async functions as the "operator".
- Improved `MPServer.stream` and `SocketClient.stream`; removed `MPServer.stream2`.
- Rewrote `RemoteException`, especially for pickle-safety.
- `SocketServer` shutdown handling.
- Removed `MPServer.{start, stop}`; use `__enter__/__exit__`.
- Removed some contents of `mpservice.util`.


## Release 0.10.3.post1, post2, post3, post4, post5

- Add async-based `MPServer.stream2`.
- Improve printing of RemoteException in `__exit__`.
- SocketServer creates directory path as needed at startup.
- Small improvements.


## Release 0.10.3

- Removed async streamer.
- Simplified implementation of the `transform` method of streamer.


## Release 0.10.2

- Simplify `MPServer.stream`; remove `MPServer.async_stream`; the functionality
  of `async_stream` is provided by `stream`.
- Make more utility functions available in `mpservice.util`.
- Simplify `mpservice._async_streamer`.


## Release 0.10.1

- `mpservice.http_server` was renamed to `mpservice.http`.
- Initial implementation of socket client/server.


## Release 0.10.0

- `mpserver` fine tune, esp about graceful shutdown.
- Use `overrides` to help keep sanity checks on class inheritance.
- Bug fixes in streamer.


## Release 0.9.9.post1

- Handle logging in multiprocessing.


## Release 0.9.9

- Use 'spawn' method for process creation.
- Refactored and simplified test/build process.
- Removed `mpservice.http_server.run_local_app`.
- Minor bug fixes.


## Release 0.9.8.post2

- Improvements to the utilities in `mpservice.http_server`, esp regarding service shutdown.


## Release 0.9.8.post1

- Requirement on `uvicorn` changes to `uvicorn[standard]`, which uses `uvloop`.


## Release 0.9.8

- Reworked error propagation in streamer; fixed bugs therein.
- Renamed `mpservice.exception` to `mpservice.remote_exception`.


## Release 0.9.8b1

- Corner-case bug fixes in `MPServer`.
- Increase queue size in `ConcurrentTransformer`.


## Release 0.9.7

- Refactor the `MPError` class, with a renaming to `RemoteException`.


## Release 0.9.6

- Refactor the `MPError` class.


## Release 0.9.5.post3

- Relax version requirements on dependencies.


## Release 0.9.5.post2

- `BackgroundTask` refinements, esp about support for asyncio.


## Release 0.9.5.post1

- `BackgroundTask` bug fix.


## Release 0.9.5

- `BackgroundTask` refactor.


## Release 0.9.4

- `MPServer.start` starts the servlets sequentially by default.


## Release 0.9.3

- Bug fix in `streamer.{Stream, AsyncStream}.batch`.
- Change Python version requirement from 3.7 to 3.8, due to the use of
  parameter `name` in `asyncio.create_task`.


## Release 0.9.2

- Revise `background_task` API.


## Release 0.9.1

- Rewrite `streamer` and `async_streamer` to avoid queues in simple "pass-through" ops, such as `drop_if`, `log_exceptions`.
- Minor improvements to `http_server`.
- Possible bug fix related to `total_timeout`.
- Added new module `background_task`.


## Release 0.9.0

- Bug fix.
- `mpservice.mpserver.Servlet.__call__` is renamed to `call`.


## Release 0.8.9

- Add `mpserver.EnsembleServer` to implement ensembles; rename `Server` to `SequentialServer`.
- Add `mpserver.SimpleServer`.
- Revise `cpu` specification in `mpserver`.


## Release 0.8.8

- `mpserver.Server` gets a sync API, in addition to the existing async API.
- `mpserver.Server` gets sync and async stream methods.


## Release 0.8.7

Streamer API fine-tuning and bug fixes.


## Release 0.8.6

Added sync streamer.


## Release 0.8.5

Refactor the async streamer API and tests.


## Release 0.8.4

Fine tuning on `streamer`.


## Release 0.8.3

Added:

- `mpservice.streamer.{transform, unordered_transform}` get new parameter `return_exceptions`.
  Similarly, `drain` gets new parameter `ignore_exceptions`.


## Release 0.8.2

Added:

- `mpservice.streamer`


## Release 0.8.1

Changed:

- `Servlet.process` is renamed to `Servlet.__call__`.
- `mpservice._http` renamed to `mpservice.http_server` with enhancements.
- `mpservice._server_process` renamed to `mpservice.server_process`.


## Release 0.8.0

Changed:

- Replaced machine learning terminologies ('model', 'predict', etc) in namings
  by generic names. This broke public APIs.
- Guarantee worker initiation (running in other processes) before service startup
  finishes.


## Release 0.7.3

Added:

- `ModelService.a_predict` gains new parameters `enqueue_timeout` and `total_timeout`,
  with default values 10 and 100 seconds, respectively.
