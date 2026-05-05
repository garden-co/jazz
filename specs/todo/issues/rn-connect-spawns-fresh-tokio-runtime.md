# RN connect() spawns a fresh OS thread and Tokio runtime per call

## What

`crates/jazz-rn/rust/src/lib.rs:847-853` spawns a new OS thread and builds a new `tokio::runtime::Builder::new_current_thread().enable_all()` runtime on every `connect()` call. A rustls-enabled Tokio runtime is not a cheap object. Explicit `disconnect()` + `connect()` cycles (server URL change, manual reconnect) thrash runtimes; first connect on cold start adds a thread-spawn + runtime-build to the critical path.

## Priority

medium

## Notes

- Lazy-initialise a shared runtime on the RN module and `handle.spawn(manager.run())` into it, mirroring the NAPI `Handle::try_current()` pattern.
- Also consider whether to expose the handle for test injection.
