# Durable Object integration tests

Local workerd coverage for the production Jazz DO host and Worker backend.
These tests never deploy Cloudflare resources or require account credentials.
Worker fixtures import `jazz-tools/backend` using the package's `workerd` condition,
without entry-point aliases. Bundle checks reject native dependencies; the
remote-only fixture also excludes the deployable host artifact.
`tsconfig.worker-backend.json` checks the matching Worker API types.

```sh
pnpm --filter jazz-tools test:durable-objects
# With current artifacts already built:
pnpm --filter jazz-tools exec vitest run --config vitest.config.durable-objects.ts
```

The preparation script builds the production Worker package, the fixture WASM,
the native sync-test client and backend JavaScript. Requires the wasm32 Rust
target, wasm-pack, the repository's NAPI artifacts and loopback sockets. Tests
run sequentially in isolated Node processes because they reconstruct workerd
and deliberately interrupt sockets/processes. The SIGKILL test requires POSIX.
Use `RUSTC_WRAPPER= CC=cc CXX=c++` if local compiler wrappers are unavailable.

## Coverage

- `test-create-jazz`, `test-worker-backend`, `test-accounts`: publication,
  admission, policy-scoped reads/writes, account/guest/revocation, Worker sockets.
- `test-worker-provider`: the public Worker provisioning provider uploads the
  exact packaged host through a local Cloudflare API double, then publishes,
  reads/writes and reuses the real local DO core across Worker invocations.
- `test-thin-parts`: fragmented requests, malformed/interleaved parts, per-session
  and aggregate budgets, disconnect cleanup and the production inactivity timeout.
- `test-lifecycle`, `test-crash`: local reconstruction, lost uploads/acknowledgements,
  retry/idempotency and atomicity after process termination.
- `test-catalogue`, `test-scopes`, `test-values`, `test-server-core`: catalogue
  rollback, SQL callback scopes, numeric types and native replica sync, independent
  storage owners and shared core scheduling/cleanup.

`runtime.mjs` and `server-runtime.mjs` configure local Miniflare. `worker.mjs`,
`catalogue-worker.mjs` and the fixture crate expose test-only controls; release
packaging uses none of them. The production workspace lives in
`crates/jazz-wasm/durable-object`, the fixture workspace in
`crates/jazz-wasm/durable-object-tests`. No separate test server engine is maintained.

## Known limits

Open transactions (thin-client, backend and Rust callback transactions) run
on managed storage without holding a SQL scope between engine calls: each
stage or read replays the batch in one `transactionSync` scope and rolls it
back, and commit applies it in one scope. Cost is a replay per stage or read,
so it grows with batch size. Chunked large values are read through the DO's
own SQL connection with a bounded cache, so they survive restarts. The
`scopes` test runs again. `crash` stays skipped: its transaction now commits
on the DO, but after the SIGKILL the thin writer raises an uncaught
`ThinConnectionLostError` and the harness exits.

An incremental transaction overlay (no per-operation replay), chunk storage
in R2 and hibernation remain deferred;
see the [existing plan](../../../../crates/jazz-wasm/durable-object/PLAN.md).
DO integers are limited to ±(2^53−1); REAL values retain IEEE-754 behavior.
Thin requests may span frames, within 8 MiB per session and 16 MiB aggregate
DO ingress reservations. Incomplete requests expire after 90 seconds without a
part; over-budget input is refused. Worker applications
use `jazz-tools/backend` with `nodejs_compat`, request-owned clients and
WebSockets; their HTTP-stream fallback and text-anchor helpers are unsupported.
Keep deployed behavior and performance claims separate from local results.

### Wire compatibility: thin-backend-scope-v1

This is an additive wire-v6 capability, feature bit 12. It appends ThinOp tag 18
without changing tags 0–17 or durable storage bytes. The envelope contains
length-prefixed JSON context bytes and length-prefixed encoded ThinRequest
bytes. The inner request must have the same id; nested envelopes and scoped
schema declarations are rejected. Only authenticated SYSTEM sessions can use
it. This is an authorization assertion from a trusted backend, never a public
client capability. Scope is not inferred from the peer's advertised role.

New backend clients require the feature in Hello negotiation and fail explicitly
against older servers. Ordinary thin clients continue to use the existing
frames. The Rust `backend_scope_v1_bytes` and TypeScript wire tests pin the same
bytes: `04 07 12 02 7b 7d 03 04 07 0d` (request 7, empty backend context, inner
capture-snapshot request). No stored or sync-record encoding changes.
