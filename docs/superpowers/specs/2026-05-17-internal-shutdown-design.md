# Internal Server Shutdown Design

## Goal

Add an internal server shutdown API that lets infrastructure request a controlled process shutdown before a container is terminated.

The first trigger is `POST /internal/shutdown`, authenticated with the existing admin secret. The shutdown mechanism itself should be independent of HTTP so a later `SIGTERM` handler can reuse the same lifecycle.

## Non-Goals

- Add a polling status endpoint.
- Wait for `/internal/shutdown` to complete all shutdown work before responding.
- Guarantee upstream replication before exit. The required durability guarantee is local RocksDB persistence.
- Introduce a second administrative secret or a separate auth system.

## Existing Context

The production `jazz-tools server` command currently serves Axum directly and has no signal or shutdown hook. Test and dev hosted servers already perform the important storage sequence:

1. `runtime.flush().await`
2. stop the Axum server task
3. `storage.flush()`
4. `storage.flush_wal()`
5. `storage.close()`

RocksDB storage already implements `flush`, `flush_wal(true)`, and `close`. WebSocket handlers already register active connections in `ServerState.connections` and `ConnectionEventHub`, which gives us a natural place to reject new connections and close existing ones.

## External Contract

### `POST /internal/shutdown`

Authentication:

- Requires `X-Jazz-Admin-Secret`.
- Uses the existing `validate_admin_secret` behavior.
- If no admin secret is configured, returns `403`.
- If the header is missing or wrong, returns the existing `401` admin-auth shape.

Response:

- Returns `202 Accepted` immediately after the shutdown request is accepted.
- Does not wait for WebSocket drain, runtime flush, RocksDB flush, or process exit.
- Is idempotent. A repeated valid call while shutdown is already in progress returns `202 Accepted` with an `already_shutting_down` status.

Response body:

```json
{
  "status": "shutting_down"
}
```

Repeated valid call response body:

```json
{
  "status": "already_shutting_down"
}
```

### `GET /health`

`/health` remains the external shutdown observation mechanism.

- Before shutdown: `200 OK`, `{ "status": "healthy" }`
- After shutdown is accepted and while work is still in progress: `503 Service Unavailable`, with `status: "shutting_down"` and a `phase` value such as `draining_connections`, `flushing_runtime`, or `closing_storage`.
- After storage is closed, the server asks Axum to stop. Pollers then observe connection failure once the listener exits.

This lets infrastructure call `POST /internal/shutdown`, then poll `/health`. `503` means shutdown is in progress. Connection failure means shutdown finalization reached the point where the listener has been stopped.

## Lifecycle State

Add a shared shutdown controller owned by `ServerState`.

State phases:

- `Running`
- `ShuttingDown`
- `DrainingConnections`
- `FlushingRuntime`
- `ClosingStorage`
- `StorageClosed`

Required controller behavior:

- Phase changes are observable by handlers through a cheap read.
- WebSocket handlers can subscribe to a shutdown notification.
- The server command can wait for shutdown completion before stopping Axum.
- The first caller wins. Later callers see the already-started state and do not spawn duplicate shutdown tasks.

The default graceful budget is `30s`, configurable with `JAZZ_SHUTDOWN_TIMEOUT_SECS` and a `--shutdown-timeout-secs` CLI option.

## Shutdown Sequence

When `/internal/shutdown` accepts a valid request:

1. Atomically transition from `Running` to `ShuttingDown`.
2. Return `202 Accepted` to the caller.
3. Mark `/health` as unhealthy.
4. Reject new app-scoped HTTP and WebSocket requests with `503 Service Unavailable`.
5. Notify active WebSocket handlers to close.
6. Active WebSocket handlers send a close frame with a service-restart reason and then run normal cleanup.
7. If this server has an upstream WebSocket transport, disconnect it so no new upstream traffic is accepted during local shutdown.
8. Wait for active app requests and WebSocket handlers to drain.
9. Run `runtime.flush().await` to drain scheduled runtime work.
10. Run storage finalization through `runtime.with_storage`: `flush()`, `flush_wal()`, then `close()`.
11. Transition to `StorageClosed`.
12. Notify the server command to stop Axum gracefully.

The configured graceful budget applies to network drain. WebSocket handlers are expected to close locally without waiting for client acknowledgement. If app-scoped HTTP handlers are still active when the budget expires, the server logs the active counts and keeps `/health` at `503`; it does not close storage underneath active handlers. The container's external termination grace period remains the final backstop for a genuinely stuck handler.

## Request Gating

After shutdown starts:

- `/health` remains available and reports unhealthy progress.
- `POST /internal/shutdown` remains available and idempotent.
- App-scoped routes under `/apps/:app_id/...` return `503`.
- New WebSocket handshakes are rejected even if they slip past route-level gating.

The app-scoped gate should also track active app HTTP requests so shutdown can wait for short in-flight admin/schema requests before closing storage. WebSocket handlers are tracked separately through the existing connection state and shutdown notification.

## WebSocket Behavior

Existing connections should not be allowed to keep the process alive indefinitely.

The WebSocket loop adds a shutdown branch to its `tokio::select!`:

- on shutdown notification, send a close frame best-effort
- break the loop
- run existing `ws_cleanup`

The close reason should be stable and human-readable, for example `server shutting down`. Use WebSocket close code `1012` (service restart).

Inbound frames received after shutdown starts should not be processed. The shutdown branch wins by exiting the loop.

## Storage Durability

The durability sequence is:

1. `runtime.flush().await`
2. `storage.flush()`
3. `storage.flush_wal()`
4. `storage.close()`

For RocksDB this forces memtable flush, synchronous WAL flush, and release of DB resources. `close()` is idempotent today and should remain safe if tests call it after shutdown.

Storage finalization errors should be logged and surfaced in the shutdown phase state. The server should still stop serving after a failed close attempt, because retrying indefinitely inside a terminating container is less useful than making the failure visible to logs and orchestrator restart behavior.

## Server Command Integration

`commands::server::run` should switch from plain `axum::serve(listener, built.app).await?` to a controlled lifecycle:

1. build server state and router
2. serve Axum normally
3. wait for the shutdown controller's completion signal
4. stop Axum with `with_graceful_shutdown`
5. return from `run`, letting the process exit normally

The shutdown finalization task should be owned by the server command, not by a request handler. The handler only requests shutdown. This keeps shutdown work alive even after the HTTP response is returned.

## Testing Strategy

Use TDD and prefer integration-style server tests.

Tests to cover:

- `POST /internal/shutdown` without admin secret configured returns `403`.
- `POST /internal/shutdown` without a header returns `401`.
- `POST /internal/shutdown` with a wrong admin secret returns `401`.
- `POST /internal/shutdown` with a valid admin secret returns `202`.
- A repeated valid shutdown call returns `202` and does not start a second finalization task.
- `/health` returns `503` after shutdown starts.
- New app-scoped HTTP requests return `503` after shutdown starts.
- A WebSocket connected before shutdown receives a close and is cleaned up.
- Runtime flush and storage flush/close are called in order by using a test storage backend or test-only instrumentation.
- `jazz-tools server` stops after `/internal/shutdown` and the health endpoint eventually becomes unavailable.

## Future Extension

Add `SIGTERM` handling as another caller of the same shutdown controller. It should follow the same phases and durability path, with no separate signal-only implementation.
