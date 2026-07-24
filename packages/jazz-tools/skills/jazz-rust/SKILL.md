---
name: jazz-rust
description: Build and troubleshoot Jazz clients, services, and integration tests in Rust. Use for the jazz-tools Rust crate, JazzClient, AppContext, SchemaBuilder, TableSchema, QueryBuilder, Value decoding, persistent or memory storage, session-scoped writes, Axum authentication, durability waits, subscriptions, transactions, JazzServer test topology, deterministic convergence tests, and Rust-side row permissions or sync behavior.
---

# Jazz Rust

Implement against the installed Rust crate's public API. Do not translate TypeScript table handles
or framework conventions into Rust names that do not exist.

## Start from the crate

1. Read the workspace `Cargo.toml`, the installed `jazz-tools` crate version and enabled features,
   nearby Rust examples, and the public exports in `lib.rs`.
2. Read `crates/jazz-tools/TESTING_GUIDELINES.md` in full before writing a Rust test in this
   repository.
3. Determine whether the process is an embedded client, trusted backend, authenticated HTTP
   service, sync server, or test participant.
4. Locate schema construction, `AppContext`, client ownership, request auth, storage path, waits,
   subscriptions, and graceful shutdown.
5. Read the reference that matches the task:
   - [client-api.md](references/client-api.md) for crate features, schema/query/write APIs, typed
     application codecs, sessions, durability, transactions, and lifecycle.
   - [services-and-testing.md](references/services-and-testing.md) for authenticated services,
     backend/request authority, HTTP error boundaries, public black-box tests, and convergence.

## Preserve the actual Rust boundary

- Build schemas and permissions with `SchemaBuilder`, `TableSchema::builder`, and public policy
  builders. Use `row_input!` for inserts. Do not author JSON-like test fixtures.
- Treat `JazzClient::query(...)` as dynamically typed at the row boundary: it returns
  `Vec<(ObjectId, Vec<Value>)>`. Define a strict application struct and fallible positional decoder
  for repeated use.
- Centralize each query projection with its decoder. Reject wrong lengths, unexpected `Value`
  variants, and invalid nullability rather than silently coercing schema drift.
- Use `QueryBuilder`, `insert`, `update`, `delete`, `subscribe`, and transaction methods from the
  installed public API. Do not invent generated Rust table handles.

## Own client authority and lifetime

- Create one long-lived `JazzClient` per process role and reuse cheap scoped clones. Do not connect
  once per HTTP request.
- Use `ClientStorage::Persistent` with a stable `data_dir` for durable service state; use memory only
  when process-lifetime data is intentional or an upstream participant is authoritative.
- Use `client.for_session(session)` when caller row policies and authorship must apply. Keep backend
  and admin credentials separate from untrusted request authentication.
- Use `wait_for_batch(batch_id, DurabilityTier::EdgeServer)` or `GlobalServer` when the operation's
  contract requires authority-visible settlement. Add an application timeout around unbounded
  network waits.
- Use the installed subscription cleanup API and call `shutdown()` on the owning client during
  graceful termination. If the installed `subscribe` surface does not expose its internal handle,
  report that limitation rather than inventing one.

## Build services through explicit boundaries

- Authenticate the HTTP caller and derive a stable `Session` before creating a session-scoped
  client.
- Use the scoped client for both reads and writes when hidden rows must remain hidden and authorship
  must match the caller.
- Requery at the required tier before returning an authoritative representation when optimistic
  local state is insufficient.
- Map missing/filtered rows, auth failure, validation failure, authoritative rejection, timeout,
  and internal schema-decoding errors to distinct application errors. Do not expose raw secrets or
  token diagnostics.

## Test observable behavior

1. Use `JazzClient::test_client` for one-runtime behavior when sufficient.
2. Use a real `JazzServer` and independent clients when permissions, transport, durability, or
   convergence is under test.
3. Build schemas, permissions, queries, and rows through public Rust builders.
4. Assert through query results, subscription deltas, write settlement, and visible row state.
5. Use `wait_for_query` or bounded retry helpers rather than fixed sleeps.
6. Use message blocking when a test must prove a genuine concurrent frontier.
7. Shut down all clients, servers, issuers, listeners, and every subscription the public API lets
   the test own explicitly.

## Cross into adjacent work deliberately

- Load `jazz-sync` for merge semantics, replay risk, or convergence design that spans Rust and
  TypeScript clients.
- Load `jazz-core` only when the requested change also includes a TypeScript client surface.
- Do not load `jazz-backend` or `jazz-testing` for a Rust-only task; those skills target TypeScript.

## Avoid these failure modes

- Do not claim the current Rust row API is generated or compile-time typed.
- Do not decode positional values ad hoc in every handler.
- Do not use a trusted backend client for caller-scoped operations merely to bypass policy failures.
- Do not return success before the promised durability tier settles.
- Do not duplicate production route implementations inside integration tests.
- Do not inspect internal history or transport state when public behavior can prove the result.
