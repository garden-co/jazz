# Jazz Rivet actor

The `jazz-rivet` executable registers a `JazzActor` with RivetKit. The actor hosts
Jazz's embedded server and supplies its public `ctx.sql()` database to Jazz's
async storage interface. The TypeScript `createJazz({ store: rivet(...) })`
backend continues to authenticate users and publish schema/permission bundles.

See [the todo example](../../examples/rivet-todos/README.md) for deployment.

## Integration boundary

Production code depends on `rivetkit` and its public actor, SQL, transaction,
HTTP and WebSocket APIs. RivetKit manages SQLite workers, durable pages, actor
generations, sleep and shutdown. Jazz does not implement the Envoy or page
protocol and does not patch Rivet. The Cargo manifest permits compatible SDK
updates; the lockfile currently resolves RivetKit 3.1.1 with `sqlite-local`.

Jazz's outer savepoint acquires a RivetKit transaction. Nested savepoints run
through that same transaction handle, so Rivet's own SQL cannot interleave with
a Jazz transaction. Complete admitted Jazz operations still belong to the
server owner thread, not to request futures. On shutdown, Jazz drains those
operations and releases its transaction; RivetKit performs the final database
close. WebSocket hibernation is disabled.

The public API does not expose SQLite's autocommit flag. An SQL error therefore
rolls back the outstanding transaction, poisons that Jazz adapter, and requests
an actor restart. An uncertain commit is never retried automatically. This is
conservative: even a recoverable SQL statement error can restart the actor.
Binding validation errors are rejected before SQL and do not poison storage.
RivetKit’s default transaction timeout (60 seconds) applies.

## Shared database and validation

RivetKit metadata and Jazz live in the same SQLite file. Jazz discovers its
application tables from its persisted schema catalogue, including historical
schemas, and leaves unrelated tables alone. No provider-specific exclusions are
needed. New Jazz tables cannot adopt existing unrelated SQLite objects. Existing
Jazz format stamps are still validated, and unstamped Jazz metadata is rejected. No storage or sync
encoding changes: the Jazz format remains 1 and `JazzRivetAppIdV1` continues to
map an actor ID deterministically to the same app ID.

Schema deployment validation uses a fresh, bounded, in-memory SQLite database.
Its small `rusqlite` adapter is only for disposable validation; it never opens
actor storage. Its dependency must be compatible with the SQLite library linked
by RivetKit, which is why this crate remains an independent Cargo workspace.
Other Jazz providers keep their existing native SQLite dependency and execution.

## Checks

The integration tests use actual RivetKit actor databases backed by a local
Rivet engine. Supply an installed engine binary:

```sh
RIVET_ENGINE_BINARY_PATH=/absolute/path/to/rivet-engine sh crates/jazz-rivet/check.sh
```

The check script uses Rust 1.99.1 and the SQLite atomic-write compile options.
Tests cover scalar fidelity, nested transactions and exclusion of unrelated
actor SQL, automatic rollback, Jazz replica sync, permissions, large values,
request cancellation, reopening stores, and gateway/lifecycle handling.
Commit failures are injected at Jazz's provider boundary; these tests do not
claim to simulate Rivet's internal distributed commit protocol or Cloud outages.
