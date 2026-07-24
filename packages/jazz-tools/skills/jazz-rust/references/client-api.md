# Rust client API

Confirm names and features against the installed crate; the Rust surface is evolving independently
from the TypeScript package.

## Cargo features

The workspace crate currently exposes feature groups including:

- `client` for `JazzClient`, WebSocket transport, Tokio runtime, and client errors;
- `server` for `JazzServer`, Axum routes, JWT/JWKS middleware, and server runtime;
- `sqlite` or `rocksdb` for storage implementations;
- `test-utils` for server, client, SQLite, and test support.

Enable only what the process role needs. Use the repository's published crate name and version when
working outside the workspace.

## Schema and rows

Construct schemas with public builders:

```rust
let schema = SchemaBuilder::new()
    .table(
        TableSchema::builder("todos")
            .column("title", ColumnType::Text)
            .column("done", ColumnType::Boolean)
            .nullable_column("description", ColumnType::Text),
    )
    .build();
```

Use `row_input!` for insert values:

```rust
let (id, values, batch_id) = client.insert(
    "todos",
    row_input!("title" => "Ship", "done" => false, "description" => Value::Null),
)?;
```

`query(...)` returns object IDs plus positional `Value` vectors. Define one projection and decoder:

```rust
struct TodoRow {
    id: ObjectId,
    title: String,
    done: bool,
    description: Option<String>,
}

fn decode_todo(id: ObjectId, values: &[Value]) -> Result<TodoRow, AppError> {
    // Validate the exact projection length and every Value variant.
    // Accept Value::Null only where the schema is nullable.
    # todo!()
}
```

Select magic provenance columns explicitly when the handler or test needs them. Keep projection
order and decoder order together so a schema change cannot silently shuffle meaning.

## Configuration and lifecycle

`AppContext` carries app ID, schema, server URL, data directory, storage mode, and one of the
applicable authentication credentials. Create a client once:

```rust
let client = JazzClient::connect(context).await?;
```

`ClientStorage::Persistent` stores state under `data_dir`. `ClientStorage::Memory` lasts only for the
process. A WebSocket handshake establishes transport but does not prove a particular schema query is
ready; perform a bounded readiness query when startup requires one.

Call `shutdown()` on the owning client. It disconnects, flushes the runtime and storage, and closes
the storage handle.

## Queries, writes, and settlement

Build queries with `QueryBuilder` and inspect the installed builder for filters, selection, ordering,
includes, and branch requirements. Do not substitute TypeScript `where(...)` objects.

Writes return a `BatchId`:

```rust
let batch_id = client.update(id, vec![("done".into(), Value::Boolean(true))])?;

tokio::time::timeout(
    deadline,
    client.wait_for_batch(batch_id, DurabilityTier::EdgeServer),
)
.await??;
```

Use `EdgeServer` for nearest-server acknowledgement and `GlobalServer` when the core authority must
confirm the batch. A direct one-row write already has batch identity.

Use `begin_transaction()` when multiple mutations must remain staged until authority accepts the
whole group. Commit or roll back explicitly, then await the transaction batch at the required tier.

## Sessions and subscriptions

`client.for_session(session)` creates a cheap scoped client whose policy evaluation and write
context use that session. A server-connected client needs the appropriate backend credential before
trusted code can scope arbitrary sessions.

`subscribe(query)` returns a delta stream. Consume deltas through that public stream and prefer a
one-shot `query` when live updates are not part of the feature. Inspect the installed cleanup shape:
the current API tracks an internal `SubscriptionHandle` but does not return it from `subscribe`,
despite exposing `unsubscribe(handle)`. Do not fabricate a handle; drop the stream, shut down its
owning client, and surface the API limitation when explicit per-subscription disposal is required.
