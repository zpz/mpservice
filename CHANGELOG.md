# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## [0.12.2] - in progress

### Changed

- ``SpawnProcess`` does not forward logs to the main process if the root logger has any handler configured.

### Fixed

- ``SpawnProcess`` and ``Thread`` in "loud_exception" mode do not print exception info if the exception
  is ``SystemExit(0)``. This is the case when a "server process" exits.


## [0.12.1] - 2023-03-26

### Added

- ``EnsembleServlet`` gets new parameter ``fail_fast`` to control behavior when ensemble members return exceptions.
- New exception class ``EnsembleError``.
- Added ``mpservice.util.Process``, which is an alias to ``mpservice.util.SpawnProcess``.
- Refinements to ``mpservice.util.SpawnProcessPoolExecutor``.
- Added ``mpservice.util.ProcessPoolExecutor``, which is an alias to ``mpservice.util.SpawnProcessPoolExecutor``.
- New class ``mpservice.util.ThreadPoolExecutor``.
- New class ``mpservice.mpserver.SwitchServlet``.

### Fixed

- ``mpservice.util.{Thread, SpawnProcess}`` print out tracback upon exception, making errors in concurrent code more
  discoverable. This functionality was there previously but it was buggy.
- Fixed a deadlock situation during the shutdown of streamer ``parmap``.


## [0.12.0] - 2023-03-10

### Bug fixes

- Bug in `mpserver.EnsembleServlet`.


## [0.11.9] - 2022-02-25

### Removed

- Deprecated context manager on `Streamer`. Instead, use the object directly.
- Deprecated function `mpservice.util.is_exception`.

### Changed

- `Streamer.peek` parameter `interval`: default changed to 1 from 1000.
- Class `Streamer` is renamed `Stream`; the old class `Stream` was removed.

### Added or enhanced

- `mpservice.streamer.Parmapper.__init__` takes two new arguments `executor_initializer`
  and `executor_init_args`.
- Simplifications to the implementation of `streamer.py`, making use of `GeneratorExit` and removing class `Stream`.
- New utility functions `mpservice.util.get_shared_thread_pool`, `mpservice.util.get_shared_process_pool`.


## [0.11.8] - 2022-12-21

The two largest efforts of this release are documentation and "streamer" refactor.

### Removed

- `mpservice.streamer.Streamer.{drop_first_n, peek_random, drop_if, keep_if}`, and corresponding classes
  `Dropper`.
- `mpservice.streamer.Streamer.drop_exceptions`.

### Changed

- `mpservice.streamer.Streamer.transform`: parameter `concurrency` used to default to 1 (i.e. no concurrency), now defaults to higher numbers (i.e. with concurrency).
- `mpservice.mpserver.{Sequential, Ensemble}` were renamed to `SequentialServlet` and `EnsembleServlet` respectively.
- `mpservice.streamer.Streamer.drain`: return count of elements processed, instead of the tuple of element count and exception count.
- `mpservice.streamer.Streamer.peek` was refactored.
- `mpservice.streamer.Streamer.transform` was renamed to `parmap`.
- Relaxed the requirement for using context manager with `Streamer`.
- `Streamer.parmap` uses processes by default, instead of threads.

### Added or enhanced

- Enhanced documentation. Started to host generated doc on Read the Docs.
- New class `mpservice.mpserver.CpuAffinity`.
- New method on `mpservice.streamer.Streamer` and corresponding classes:
  `filter` and `Filter`, `tail` and `Tailor`, `map` and `Mapper`, `groupby` and `Grouper`.
- New method `mpservice.streamer.Streamer.filter_exceptions`, `mpservice.streamer.Streamer.accumulate`.


## [0.11.7.post1] - 2022-10-21

- Upgrade for a breaking change in `uvicorn` 0.19.0.


## [0.11.7] - 2022-10-15

- `Streamer` implementation finetune, mainly about worker thread/process finalization.
- `Streamer` removes methods that are trivial (so user can implement them if needed) and unnecessary or not very needed: `collect`, `drop_nones`, `keep_every_nth`, `keep_random`, `log_every_nth`.
- `Streamer.log_exceptions` was renamed `peek_exceptions` with minor changes.
- Parameter `shed_load` to `mpserver.Server.async_call` is renamed to `backpressure`.


## [0.11.6] - 2022-10-07

- `mpserver` wait-time fine tunning.
- `mpserver.Server.async_call` gets new parameter `shed_load` with default `True`.
- New exception class `PipelineFull` in `mpserver`.


## [0.11.5] - 2022-09-22

- `RemoteException` is re-written with much simplifications; the class is moved from `mpservice.remote_exception` to `mpservice.util`; the module `mpservice.remote_exception` is removed.
- Enhancements to `SpawnProcess`.
- Improvements to `mpservice.util.ProcessLogger`.
- The new constant `mpservice.util.MP_SPAWN_CTX` is a customization to the standard spawn
  context that uses `SpawnProcess` instead of `Process`.
- Use spawn method or `SpawnProcess` exclusively or by default in various places in the code.
- `Streamer.transform` gets new parameter `executor` to support multiprocessing for CPU-bound operators.
- The module `mpservice.server_process` is re-written.
- The module `named_pipe` is renamed `pipe`.


## [0.11.4] - 2022-09-01

- `mpservice.util.ProcessLogger` gets context manager methods.
- New class `mpservice.util.SpawnProcess`.


## [0.11.3] - 2022-06-24

- Add dependency `asgiref`.
  Previously we've relied on getting `asgiref` from `uvicorn` dependency,
  which is a bad idea. Recently, `uvicorn` removed its dependency on `asgiref`.
- Reduce the default frequency of resource utilization logs.


## [0.11.2] - 2022-06-05

- Refinement and simplification to `mpservice.streamer`.
- Refinement to `mpservice.server_process`.


## [0.11.1] - 2022-05-31

- Added `mpserver.ThreadWorker` and `ThreadServlet`.
- Simplified `mpserver` parameter for CPU pinning spec.
- Added log on worker process CPU/memory utilization in `mpserver`.


## [0.11.0] - 2022-05-27

- Refactor to `mpservice.mpserver` with API changes.
  New design allows flexible composition of sequential and ensemble setups,
  leading to considerable enhancements in capability and flexibility.
  There are also considerable improvements to the implementation
  (in terms of simplicity, elegance, robustness).
- Replaced all uses of `time.monotonic` by `time.perf_counter`, which has much
  higher resolution.
- Added module `mpservice.named_pipe`.


## [0.10.9] - 2022-05-21

- Added (or brought back) parameter `backlog` to `MPServer`.
- Implimentation improvements: simplified utitlity queues; removed error pipe of MPServer.


## [0.10.8] - 2022-05-18

- Finetune to `EnsembleServer`: ensemble elements could be `RemoteException`
  objects, i.e. failure of one ensemble component will not halt or invalidate
  the other components.


## [0.10.7] - 2022-05-15

- By default, `MPServer` uses the new, custom queue type `Unique` for faster
  buildup of batches for servlets. Removed `ZeroQueue` and `BasicQueue`.
- Simplified timeout in `mpserver`. `EnqueueTimeout` is gone; `TotalTimeout` is renamed
  `Timeout`. The parameters `enqueue_timeout` and `total_timeout` are combined
  into `timeout`.
- `ServerProcess` gets new parameter `ctx` for multiprocessing context.


## [0.10.6] - 2022-04-29

- Added alternative multiprocessing queues, namely `BasicQueue`, `FastQueue` and `ZeroQueue`,
  in an attempt to improve service speed, expecially batching.
  This topic is still under experimentation. `ZeroQueue` is not ready for use.
- Changed socket encoder from 'orjson' to 'pickle'.
- Removed `max_queue_size` parameter of `MPServer`; use unbounded queues.
- `MPServer` parameter `cpus` renamed to `main_cpu` with default value `0`.


## [0.10.5] - 2022-04-08

- Minor fine-tuning and documentation touch-ups.


## [0.10.4] - 2022-04-03

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


## [0.10.3.post1, post2, post3, post4, post5] - 2022-03-25

- Add async-based `MPServer.stream2`.
- Improve printing of RemoteException in `__exit__`.
- SocketServer creates directory path as needed at startup.
- Small improvements.


## [0.10.3] - 2022-03-19

- Removed async streamer.
- Simplified implementation of the `transform` method of streamer.


## [0.10.2] - 2022-03-19

- Simplify `MPServer.stream`; remove `MPServer.async_stream`; the functionality
  of `async_stream` is provided by `stream`.
- Make more utility functions available in `mpservice.util`.
- Simplify `mpservice._async_streamer`.


## [0.10.1] - 2022-03-17

- `mpservice.http_server` was renamed to `mpservice.http`.
- Initial implementation of socket client/server.


## [0.10.0] - 2022-03-13

- `mpserver` fine tune, esp about graceful shutdown.
- Use `overrides` to help keep sanity checks on class inheritance.
- Bug fixes in streamer.


## [0.9.9.post1] - 2022-03-11

- Handle logging in multiprocessing.


## [0.9.9] - 2022-03-10

- Use 'spawn' method for process creation.
- Refactored and simplified test/build process.
- Removed `mpservice.http_server.run_local_app`.
- Minor bug fixes.


## [0.9.8.post2] - 2022-01-31

- Improvements to the utilities in `mpservice.http_server`, esp regarding service shutdown.


## [0.9.8.post1] - 2022-01-31

- Requirement on `uvicorn` changes to `uvicorn[standard]`, which uses `uvloop`.


## [0.9.8] - 2022-01-30

- Reworked error propagation in streamer; fixed bugs therein.
- Renamed `mpservice.exception` to `mpservice.remote_exception`.
- Corner-case bug fixes in `MPServer`.
- Increase queue size in `ConcurrentTransformer`.


## [0.9.7] - 2022-01-16

- Refactor the `MPError` class, with a renaming to `RemoteException`.


## [0.9.6] - 2022-01-09

- Refactor the `MPError` class.


## [0.9.5.post3] - 2021-12-31

- Relax version requirements on dependencies.


## [0.9.5.post2] - 2021-12-05

- `BackgroundTask` refinements, esp about support for asyncio.


## [0.9.5.post1] - 2021-12-05 [YANKED]

- `BackgroundTask` bug fix.


## [0.9.5] - 2021-12-05 [YANKED]

- `BackgroundTask` refactor.


## [0.9.4] - 2021-12-05

- `MPServer.start` starts the servlets sequentially by default.


## [0.9.3] - 2021-11-16

- Bug fix in `streamer.{Stream, AsyncStream}.batch`.
- Change Python version requirement from 3.7 to 3.8, due to the use of
  parameter `name` in `asyncio.create_task`.


## [0.9.2] - 2021-10-31

- Revise `background_task` API.


## [0.9.1] - 2021-10-31

- Rewrite `streamer` and `async_streamer` to avoid queues in simple "pass-through" ops, such as `drop_if`, `log_exceptions`.
- Minor improvements to `http_server`.
- Possible bug fix related to `total_timeout`.
- Added new module `background_task`.


## [0.9.0] - 2021-08-28

- Bug fix.
- `mpservice.mpserver.Servlet.__call__` is renamed to `call`.


## [0.8.9] - 2021-08-16

- Add `mpserver.EnsembleServer` to implement ensembles; rename `Server` to `SequentialServer`.
- Add `mpserver.SimpleServer`.
- Revise `cpu` specification in `mpserver`.


## [0.8.8] - 2021-08-10

- `mpserver.Server` gets a sync API, in addition to the existing async API.
- `mpserver.Server` gets sync and async stream methods.


## [0.8.7] - 2021-08-05

Streamer API fine-tuning and bug fixes.


## [0.8.6] - 2021-08-02

Added sync streamer.


## [0.8.5] - 2021-08-01

Refactor the async streamer API and tests.


## [0.8.4] - 2021-07-28

Fine tuning on `streamer`.


## [0.8.3] - 2021-07-27

Added:

- `mpservice.streamer.{transform, unordered_transform}` get new parameter `return_exceptions`.
  Similarly, `drain` gets new parameter `ignore_exceptions`.


## [0.8.2] - 2021-07-25

Added:

- `mpservice.streamer`


## [0.8.1] - 2021-07-24

Changed:

- `Servlet.process` is renamed to `Servlet.__call__`.
- `mpservice._http` renamed to `mpservice.http_server` with enhancements.
- `mpservice._server_process` renamed to `mpservice.server_process`.


## [0.8.0] - 2021-05-19

Changed:

- Replaced machine learning terminologies ('model', 'predict', etc) in namings
  by generic names. This broke public APIs.
- Guarantee worker initiation (running in other processes) before service startup
  finishes.


## [0.7.3] - 2021-05-09

Added:

- `ModelService.a_predict` gains new parameters `enqueue_timeout` and `total_timeout`,
  with default values 10 and 100 seconds, respectively.
