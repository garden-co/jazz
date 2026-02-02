# Batched Tick Orchestration

> **Status: COMPLETE ✓**

## Overview

This document describes how RuntimeCore orchestrates storage I/O and sync messaging through a batched tick architecture. The design separates _what_ needs to happen (storage requests, sync messages) from _when/how_ it happens (IoHandler implementation).

## Core Concepts

### RuntimeCore<H: IoHandler>

RuntimeCore is generic over an IoHandler, which abstracts platform-specific I/O:

- **TokioIoHandler** (groove-tokio): Uses RocksDB synchronously, spawns batched_tick via tokio
- **WasmIoHandler** (groove-wasm): Fires requests to JS callbacks, uses spawn_local

### Two Tick Methods

1. **`immediate_tick()`** - Synchronous processing of in-memory state:
   - Picks up object updates from ObjectManager
   - Settles subscriptions and query graphs
   - Generates storage requests (but doesn't send them)
   - Called after mutations, after parking responses, etc.

2. **`batched_tick()`** - Handles all I/O:
   - Collects storage requests from QueryManager (objects + indices)
   - Sends them via IoHandler
   - Drains pending responses (for sync drivers like RocksDB)
   - Applies parked storage responses
   - Applies parked sync messages
   - Calls `immediate_tick()` after each step

### Storage Request Flow

All storage requests flow through a single path:

```
insert()/update()/delete()
    → ObjectManager queues CreateObject/AppendCommit
    → Index marks pages dirty

immediate_tick()
    → Processes in-memory state
    → Does NOT send storage requests

batched_tick()
    → QM::take_storage_requests() collects:
        - ObjectManager requests (CreateObject, AppendCommit, etc.)
        - Index requests (StoreIndexPage, StoreIndexMeta, etc.)
    → IoHandler::send_storage_request() for each
    → For sync drivers: take_pending_responses() + apply
```

**Key invariant**: Only `batched_tick()` via the IoHandler sends storage requests. There is no other path.

### Cold Start Loading

On cold start (reopening a database):

1. **`load_indices_via_handler()`** - Loads index metadata and pages from storage
2. **Query execution** - If objects aren't in memory, query returns `Pending`
3. **`batched_tick()`** - Sends `LoadObjectBranch` requests, applies responses
4. **Query retry** - Now objects are loaded, query succeeds

## Platform Implementations

### TokioIoHandler (groove-tokio)

```rust
impl IoHandler for TokioIoHandler {
    fn send_storage_request(&mut self, request: StorageRequest) {
        // Process synchronously through RocksDB driver
        let responses = self.driver.process(vec![request]);
        // Park responses for later processing
        self.pending_responses.extend(responses);
    }

    fn schedule_batched_tick(&self) {
        // Spawn task to call batched_tick() asynchronously
        tokio::spawn(async move { ... });
    }

    fn take_pending_responses(&mut self) -> Vec<StorageResponse> {
        std::mem::take(&mut self.pending_responses)
    }
}
```

For synchronous drivers like RocksDB, `TokioRuntime` calls `batched_tick()` synchronously after CRUD operations to ensure data is persisted before returning.

### WasmIoHandler (groove-wasm)

```rust
impl IoHandler for WasmIoHandler {
    fn send_storage_request(&mut self, request: StorageRequest) {
        // Fire-and-forget to JS callback
        self.storage_callback.call1(&JsValue::NULL, &to_js(request));
    }

    fn schedule_batched_tick(&self) {
        // Use spawn_local for single-threaded WASM
        wasm_bindgen_futures::spawn_local(async move { ... });
    }
}
```

JS driver calls back with responses, which get parked and processed in `batched_tick()`.

## Testing

Tests use two approaches:

1. **TestDriver with `process_storage_with_driver()`** - For QueryManager-level tests that need real persistence behavior without the full runtime stack.

2. **`drain_storage_noop()`** - For tests that don't need real persistence. Generates success responses for all requests.

## Files

- `groove/src/runtime_core.rs` - RuntimeCore with immediate_tick/batched_tick
- `groove/src/io_handler.rs` - IoHandler trait and NullIoHandler
- `groove-tokio/src/lib.rs` - TokioIoHandler and TokioRuntime
- `groove-wasm/src/runtime.rs` - WasmIoHandler and WasmRuntime
