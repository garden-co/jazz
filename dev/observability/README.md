# Local Observability Stack

OTel Collector + Grafana/Tempo/Prometheus/Loki for viewing traces and metrics locally.

```
jazz-server ──OTLP gRPC:4317──→ OTel Collector ──OTLP:4317──→ grafana/otel-lgtm
                                                                 ├── Tempo (traces)
                                                                 ├── Prometheus (metrics)
                                                                 ├── Loki (logs)
                                                                 └── Grafana UI (:3000)
```

## Prerequisites

- Docker

## Start

```sh
cd dev/observability
docker compose up -d
```

The Grafana dashboard "Jazz Cloud Server" is auto-provisioned from `grafana/jazz-cloud-server-dashboard.json`.

## Build and run the cloud server with OTel

```sh
JAZZ_OTEL=1 \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_METRIC_EXPORT_INTERVAL=10000 \
cargo run -p jazz-cloud-server --features otel -- \
  --internal-api-secret test-secret \
  --secret-hash-key test-hash-key \
  --worker-threads 2
```

## Build and run the single-app server with OTel

```sh
JAZZ_OTEL=1 \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
RUST_LOG=jazz_tools=debug,groove=debug \
cargo run -p jazz-tools --features otel -- server <APP_ID>
```

## View

- **Dashboard**: http://localhost:3000 → Dashboards → Jazz Cloud Server
- **Traces**: http://localhost:3000 → Explore → Tempo → Search
- **Metrics**: http://localhost:3000 → Explore → Prometheus

## Stop

```sh
docker compose down      # stop containers
docker compose down -v   # stop + wipe data
```

## Environment variables

| Variable                      | Purpose                                | Default         |
| ----------------------------- | -------------------------------------- | --------------- |
| `JAZZ_OTEL`                   | Enable the OTel layer (`1` to enable)  | off             |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector endpoint                     | stdout fallback |
| `OTEL_SERVICE_NAME`           | Service name in traces/metrics         | `jazz-server`   |
| `OTEL_SERVICE_INSTANCE_ID`    | Unique instance identifier             | random UUID     |
| `OTEL_METRIC_EXPORT_INTERVAL` | Metric export interval in milliseconds | `60000`         |
| `RUST_LOG`                    | Log filter for `tracing` subscriber    | —               |

## Metric catalog

All metrics are behind the `otel` feature flag. Dots in metric names become underscores in Prometheus (e.g. `jazz.sync.connections.active` → `jazz_sync_connections_active`).

### Connection metrics

| Metric                         | Type          | Description                                                                                             | Dimensions                |
| ------------------------------ | ------------- | ------------------------------------------------------------------------------------------------------- | ------------------------- |
| `jazz.sync.connections.active` | UpDownCounter | Currently open event streams. Incremented on stream open, decremented on stream close (via Drop guard). | `app_id`, `env`, `worker` |
| `jazz.sync.connections.total`  | Counter       | Cumulative connections opened.                                                                          | `app_id`, `env`, `worker` |

### Message throughput

| Metric                        | Type    | Description                                                                              | Dimensions                                            |
| ----------------------------- | ------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `jazz.sync.messages.received` | Counter | Inbound sync payloads received via `POST /sync`. One increment per payload in the batch. | `app_id`, `env`, `payload_type`, `direction=inbound`  |
| `jazz.sync.messages.sent`     | Counter | Outbound frames sent on event streams.                                                   | `app_id`, `env`, `payload_type`, `direction=outbound` |

`payload_type` values: `ObjectUpdated`, `ObjectTruncated`, `QuerySubscription`, `QueryUnsubscription`, `PersistenceAck`, `QuerySettled`, `SchemaWarning`, `Error`.

### Message size

| Metric                         | Type      | Description                                                                                                                                                                               | Dimensions                                                   |
| ------------------------------ | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| `jazz.sync.message.size_bytes` | Histogram | Payload size in bytes. Inbound: `Content-Length` of the sync batch HTTP request body (recorded once per batch, not per payload). Outbound: wire frame size (4-byte length prefix + JSON). | `app_id`, `env`, `payload_type` (outbound only), `direction` |

### Sync handler latency

| Metric                          | Type      | Description                                                                                  | Dimensions      |
| ------------------------------- | --------- | -------------------------------------------------------------------------------------------- | --------------- |
| `jazz.sync.handler.duration_ms` | Histogram | End-to-end time for a `POST /sync` batch, including worker dispatch and response collection. | `app_id`, `env` |

### Worker command latency

| Metric                            | Type      | Description                                                 | Dimensions                         |
| --------------------------------- | --------- | ----------------------------------------------------------- | ---------------------------------- |
| `jazz.worker.command.duration_ms` | Histogram | Time to process a single worker command in the worker loop. | `app_id`, `command_type`, `worker` |

`command_type` values: `CreateRuntime`, `EnsureClientWithSession`, `EnsureClientAsBackend`, `SyncAsSession`, `SyncAsBackend`, `SyncAsAdmin`, `GetCatalogueSchema`, `PublishSchema`, `PublishPermissions`, `GetPermissionsHead`, `GetSchemaHashes`, `GetCatalogueStateHash`.

### HTTP server metrics

| Metric                         | Type          | Description                                                  | Dimensions                                                       |
| ------------------------------ | ------------- | ------------------------------------------------------------ | ---------------------------------------------------------------- |
| `http.server.request.duration` | Histogram     | HTTP request duration in seconds (OTel semantic convention). | `http.request.method`, `http.route`, `http.response.status_code` |
| `http.server.active_requests`  | UpDownCounter | Concurrent in-flight HTTP requests.                          | `http.request.method`, `http.route`                              |

`http.route` is the axum matched path (e.g. `/apps/:app_id/sync`). Falls back to `unknown` for unmatched routes.

### Query subscription metrics

| Metric                          | Type    | Description                       | Dimensions      |
| ------------------------------- | ------- | --------------------------------- | --------------- |
| `jazz.sync.subscriptions.total` | Counter | Cumulative subscriptions created. | `app_id`, `env` |

**Why no `subscriptions.active` gauge?** An UpDownCounter for active subscriptions would need to decrement when a client disconnects without explicitly unsubscribing (e.g. network drop, app killed). Since `QueryUnsubscription` is never sent in that case, the server would need per-connection subscription tracking to know how much to subtract on disconnect. Without that, the gauge drifts permanently upward and becomes meaningless. We opted to remove it rather than add per-connection state tracking. Use `rate(jazz_sync_subscriptions_total)` for subscription activity instead.

### Persistence and durability metrics

| Metric                             | Type    | Description                                     | Dimensions              |
| ---------------------------------- | ------- | ----------------------------------------------- | ----------------------- |
| `jazz.sync.persistence_acks.total` | Counter | Persistence acknowledgements sent to clients.   | `app_id`, `env`, `tier` |
| `jazz.sync.query_settled.total`    | Counter | Query settlement notifications sent to clients. | `app_id`, `env`, `tier` |

`tier` values: `EdgeServer`, `GlobalServer` (Debug-formatted from `DurabilityTier` enum).

### Error and warning metrics

| Metric                            | Type    | Description                                      | Dimensions                   |
| --------------------------------- | ------- | ------------------------------------------------ | ---------------------------- |
| `jazz.sync.errors.total`          | Counter | Sync error payloads (both inbound and outbound). | `app_id`, `env`, `direction` |
| `jazz.sync.schema_warnings.total` | Counter | Schema warning events.                           | `app_id`, `env`              |

### Broadcast metrics

| Metric                           | Type    | Description                                                                           | Dimensions         |
| -------------------------------- | ------- | ------------------------------------------------------------------------------------- | ------------------ |
| `jazz.sync.broadcast.lag_events` | Counter | Number of times a connection lagged behind the broadcast channel and missed messages. | `app_id`, `worker` |

### Worker metrics

| Metric                       | Type          | Description                                                                                             | Dimensions               |
| ---------------------------- | ------------- | ------------------------------------------------------------------------------------------------------- | ------------------------ |
| `jazz.worker.commands.total` | Counter       | Cumulative commands processed per worker.                                                               | `command_type`, `worker` |
| `jazz.worker.apps.active`    | UpDownCounter | Number of active app runtimes per worker. Only increments (runtimes are never removed in current code). | `worker`                 |
| `jazz.worker.queue.depth`    | Gauge         | Current pending commands in the worker's `FairAppQueue`, sampled after each batch.                      | `worker`                 |

### App lifecycle metrics

| Metric                     | Type    | Description              | Dimensions         |
| -------------------------- | ------- | ------------------------ | ------------------ |
| `jazz.app.runtime.created` | Counter | Runtime creation events. | `app_id`, `worker` |

### Storage metrics

| Metric                           | Type      | Description                                               | Dimensions |
| -------------------------------- | --------- | --------------------------------------------------------- | ---------- |
| `jazz.storage.flush.total`       | Counter   | Number of `FjallStorage::flush()` calls.                  | —          |
| `jazz.storage.flush.duration_ms` | Histogram | Time taken for `db.persist(SyncData)` in each flush call. | —          |

Note: Storage metrics are currently dimensionless because `FjallStorage` doesn't know its app context. The `FjallStorage` also exposes `disk_space()` and `journal_count()` methods for future periodic sampling, but no periodic task is wired yet.

## Known gaps

- **No integration tests for metric emission.** Production code emits 18+ OTel metrics but no test verifies they fire correctly. A test that boots a `TestingServer` with `--features otel`, connects a client, performs sync operations, and asserts metric values via `TestMeterProvider` would catch regressions.
- **`ConnectionMetricsGuard` drop behavior is untested.** There's no test proving `jazz.sync.connections.active` returns to zero after a client disconnects. A test should open a connection, drop it, and assert the gauge decremented.
