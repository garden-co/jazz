# Native-First JazzClient Design

## Status

Approved for implementation planning.

## Context

Today the TypeScript `JazzClient` is more than a binding wrapper. It owns client behavior such as
write context encoding, batch and transaction helpers, write waiting, mutation error flushing,
query option defaults, subscription scheduling, schema-order alignment, and some transport/auth
glue. The Rust WASM, NAPI, and React Native runtimes already own the database engine, but each
binding still has to participate in client behavior.

This design moves the real JazzClient contract into Rust so WASM, NAPI, React Native, and future
bindings share the same behavior.

## Goals

- Make Rust the source of truth for `JazzClient` behavior.
- Keep bindings thin and mostly mechanical.
- Allow the public `JazzClient` API shape to change when that produces a cleaner native-first API.
- Reduce duplicated behavior across WASM, NAPI, React Native, and future bindings.
- Keep migration staged so existing tests can prove parity as behavior moves.

## Non-Goals

- Redesigning `Db`.
- Deciding optional native module loading or packaging.
- Changing the typed table/query builder surface.
- Reworking the browser worker bridge beyond what is needed to call the new native client shape.

## Architecture

Add a Rust-owned `JazzClientCore` above `RuntimeCore`.

`JazzClientCore` owns the behavior that should be identical across bindings:

- client and session state
- write context creation
- direct batches and transactions
- write handles and batch settlement waiting
- query option defaults
- subscription lifecycle
- schema-order alignment
- transport connect, disconnect, and auth updates
- normalized client errors

Each host binding exposes a thin adapter:

- WASM converts JavaScript values, promises, and callbacks to Rust calls.
- NAPI converts Node values, promises, and threadsafe callbacks to Rust calls.
- React Native routes generated bridge calls to the same Rust contract.
- Future bindings bind to the same Rust-facing API instead of rebuilding client behavior.

TypeScript `JazzClient` can remain as an ergonomic wrapper, but it should not define core behavior.

## Components

### `JazzClientCore`

Owns the runtime, declared schema, default session, transport state, subscription registry, and
batch waiters.

It should expose methods for:

- creating a client from native config
- creating session-scoped clients or per-call session contexts
- direct writes
- direct batches
- transactions
- queries
- subscriptions
- local batch record reads
- rejected batch acknowledgement
- transport lifecycle
- shutdown

### `ClientConfig`

Native config for:

- `app_id`
- schema envelope
- environment
- user branch
- storage mode
- optional server URL
- auth credentials
- default durability tier
- binding/runtime mode

The schema envelope should preserve the current loaded-policy-bundle bit.

### `ClientRuntimeHost`

Small interface for host-specific behavior that should stay outside core client logic.

It covers:

- callback delivery
- host cleanup hooks
- async/promise bridging
- platform-specific scheduling when a binding requires it

The core should depend on this interface only where it needs to notify the host. It should not let
host details leak into write, query, batch, transaction, or transport semantics.

### `WriteHandleCore`

Native handle for a write batch.

It owns:

- `batch_id`
- `wait(tier)`
- local batch record reload
- rejection handling

Bindings return host-native promise/async wrappers around this handle.

### `DirectBatchCore` and `TransactionCore`

Native batch builders.

Rust generates the batch id and target branch, stages writes through `RuntimeCore`, and seals or
commits the batch in Rust. TypeScript should no longer encode batch mode, batch id, or target branch
itself.

### `SubscriptionCore`

Native subscription handle.

Rust owns create, execute, unsubscribe, query state, and schema alignment. Bindings only provide the
callback sink and host cleanup behavior.

### `ClientError`

One Rust error enum used by all bindings. Bindings may adapt it to host error types, but the stable
code and core message come from Rust.

Suggested codes:

- `InvalidConfig`
- `InvalidSchema`
- `InvalidQuery`
- `WriteRejected`
- `BatchRejected`
- `UnsupportedRuntimeFeature`
- `TransportError`
- `AuthFailure`
- `StorageError`

## Behavior Moving From TypeScript

Move these out of TypeScript `JazzClient`:

- `encodeWriteContext`
- batch context creation
- `beginBatch` and `beginTransaction` internals
- `sealBatch`
- rejected batch draining
- mutation error flushing
- write `wait()` polling and settlement logic
- default durability tier resolution
- query and subscription option parsing
- schema alignment fallback logic
- session resolution when native has enough auth/session data

Keep these in TypeScript:

- TypeScript types and overloads
- conversion from typed query builders to runtime query JSON
- browser worker bridge coordination for now
- framework integration
- host-specific packaging and loading

## Data Flow

### Startup

1. Host code passes config to the binding.
2. The binding forwards the schema envelope and auth config to Rust.
3. Rust builds `JazzClientCore`.
4. Rust opens the selected runtime and storage.
5. Rust installs transport when the config includes a server URL.
6. The binding receives a native client handle.

### Writes

1. Host calls `insert`, `update`, `delete`, a direct batch method, or a transaction method.
2. Rust resolves session, attribution, timestamp, batch mode, batch id, and target branch.
3. Rust writes through `RuntimeCore`.
4. Rust returns row data plus `WriteHandleCore`, or only `WriteHandleCore`.
5. `wait(tier)` runs in Rust against retained batch records and settlements.

### Queries And Subscriptions

1. TypeScript may translate typed query builders into runtime query JSON.
2. The binding passes query JSON plus session and options to Rust.
3. Rust parses options, applies defaults, queries or subscribes, aligns row order, and returns rows
   or deltas.
4. The binding converts the result into host-friendly values.

### Transport And Auth

1. `connect`, `disconnect`, and `updateAuth` are Rust client methods.
2. Auth failures become native auth events and `ClientError::AuthFailure`.
3. Host wrappers only subscribe to auth events and update UI/framework state.

## Binding Shape

Bindings should expose the same conceptual methods even if the exact host signatures differ:

- `createClient(config)`
- `client.insert(...)`
- `client.update(...)`
- `client.delete(...)`
- `client.beginBatch()`
- `client.beginTransaction()`
- `client.query(...)`
- `client.subscribe(...)`
- `client.localBatchRecord(batchId)`
- `client.localBatchRecords()`
- `client.acknowledgeRejectedBatch(batchId)`
- `client.connect(...)`
- `client.disconnect()`
- `client.updateAuth(...)`
- `client.close()`

Host wrappers can add ergonomic aliases, but the shared method set should stay small and stable.

## Error Handling

All core errors should map to `ClientError`.

Bindings should preserve:

- stable error code
- human-readable message
- optional batch id
- optional table name
- optional object id
- optional source error detail

Bindings should not reinterpret error meaning. For example, a rejected batch should remain
`BatchRejected` in WASM, NAPI, React Native, and future bindings.

## Testing Strategy

Add Rust contract tests for `JazzClientCore`:

- direct insert, update, and delete
- direct batch commit
- transaction commit
- batch rejection and acknowledgement
- durability wait behavior
- query option defaults
- subscription lifecycle
- schema alignment
- session-scoped writes
- transport auth update events, using a fake transport where practical

Keep binding tests thin:

- WASM, NAPI, and React Native can call the same core operations.
- binding errors preserve Rust error codes.
- callbacks and subscriptions cross the host boundary correctly.
- bindings do not reimplement batch, session, or wait behavior.

## Migration Plan

1. Add `JazzClientCore` behind the existing APIs.
2. Move write context, batch, transaction, and wait behavior into Rust.
3. Move query and subscription option defaults into Rust.
4. Move schema alignment into Rust for all binding paths.
5. Update WASM and NAPI bindings to expose the native client shape.
6. Update TypeScript `JazzClient` to wrap the native client shape.
7. Move the React Native adapter to the same contract.
8. Remove old duplicated TypeScript behavior after parity tests pass.

This order keeps the change reviewable while still ending with Rust as the source of truth.

## Deferred Decisions

Optional native module loading, including JSON Schema packaging, is intentionally left for a later
design. This spec only defines the native-first `JazzClient` contract.
