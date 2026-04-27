# Dev Sync Payload OTel Telemetry Design

## Goal

Add opt-in sync-message telemetry for local dev sessions started by the Jazz dev plugins. When enabled, telemetry captures structured sync-message diagnostics for both browser-internal worker bridge traffic and client/server WebSocket traffic, then exports everything through one local server-side OpenTelemetry exporter as OpenTelemetry log records.

Telemetry payloads must never be printed to console or stdout.

## Entry Points

The telemetry option is configured on the dev server setup surface:

```ts
jazzPlugin({
  server: {
    telemetry: {
      collectorUrl: "http://localhost:4317",
    },
  },
});
```

The same `server.telemetry` option is supported by plugin setup APIs:

- `jazzPlugin(...)`
- `withJazz(...)`
- `jazzSvelteKit(...)`

The lower-level server API uses the flat equivalent:

```ts
startLocalJazzServer({
  telemetry: {
    collectorUrl: "http://localhost:4317",
  },
});
```

`collectorUrl` defaults to `http://localhost:4317` when `telemetry` is enabled and no URL is provided.

## Architecture

The local dev server owns the OpenTelemetry exporter. Browser runtime code never exports to OTel directly and never talks to the collector.

Browser-side worker bridge traffic is decoded in the TypeScript runtime layer, converted to compact structured telemetry observations, and sent to the local dev server with an app-scoped telemetry ingest request:

```text
POST /apps/:app_id/dev/sync-payload-telemetry
```

Client/server WebSocket traffic is observed inside the embedded Rust dev server. The server records both inbound client frames and outbound server frames, decodes the sync payloads, and exports the same telemetry observation shape directly.

All telemetry data leaves through one path:

```text
browser main thread <-> worker
        -> dev-server telemetry ingest
        -> dev-server OTel exporter
        -> configured OTel collector

worker/client <-> dev-server WebSocket
        -> dev-server OTel exporter
        -> configured OTel collector
```

## OTel Signal

Each sync observation is exported as an OpenTelemetry log record, not as a span or span event.

Sync messages are discrete observations rather than operations with start/end lifetimes, and the browser-ingested worker-bridge records will not share span context with the Rust WebSocket server. Logs let the implementation export searchable structured attributes without inventing artificial span parenting.

The local observability stack must include an OTLP logs pipeline when sync payload telemetry is enabled.

## Telemetry Records

Each decoded message produces one log record with:

- `scope`: `worker_bridge` or `websocket`
- `direction`: `main_to_worker`, `worker_to_main`, `client_to_server`, or `server_to_client`
- `app_id`
- `client_id`, when known
- `connection_id`, when known
- `sequence`, when known
- `payload_variant`
- `message_bytes`
- `message_encoding`: `binary` or `utf8`
- `recorded_at`
- `source_frame_id`, when the observation came from a frame or bridge message that carried multiple payloads
- `source_payload_index`, when the source carried multiple payloads
- `source_payload_count`, when the source carried multiple payloads
- `source_frame_bytes`, when different from `message_bytes`

The record also includes searchable fields extracted or derived from the decoded payload:

- `row_id`
- `table_name`
- `branch_name`
- `batch_id`
- `query_id`
- `schema_hash`
- `durability_tier`
- `error_variant`
- `error_code`, when the `SyncError` variant carries a `code` field
- `member_index`, when a sync payload expands into multiple per-member records
- `member_count`, when a sync payload expands into multiple per-member records

Decoded sync error/failure payloads are the exception to the structured-only body rule. They still export searchable attributes such as `payload_variant`, `error_variant`, and `error_code`, and they also put the full parsed payload JSON in the OTel log body/content so the failure can be inspected locally. The full parsed payload is not emitted as top-level searchable attributes.

### Derived Fields

`table_name` and `schema_hash` are required top-level searchable fields in v1 whenever a sync payload record refers to row data, query data, schema diagnostics, or catalogue/schema data.

Some payloads carry these fields directly, such as `SchemaWarning`, `ConnectionSchemaDiagnostics`, query payloads, and catalogue/schema payloads. Row-oriented payloads such as `RowBatchCreated`, `RowBatchNeeded`, `RowBatchStateChanged`, `BatchSettlement`, and `SealBatch` require derivation from the row id, branch, metadata, schema catalogue, storage, or connection schema context available to the local runtime/server.

For multi-member payloads, the implementation emits one log record per member rather than collapsing multiple tables or schema hashes into one arbitrary top-level value. Each expanded record includes `member_index` and `member_count`. If derivation fails for one member, only that member record gets the corresponding `table_name_error` or `schema_hash_error`.

If the implementation cannot derive a required field for a payload that should have it, it must still export the message and set a derivation error field, for example `table_name_error` or `schema_hash_error`. Missing derivation must be visible in OTel, not silently omitted.

Decode failures are exported too, but they do not include a payload body because no parsed payload exists. A decode-failure record includes scope, direction, app id, client or connection identifiers when available, message byte size, message encoding, and decode error. It must not include raw payload bytes, base64-encoded payload bytes, or the original string payload.

## No Stdout Fallback

Sync payload telemetry must not use `console.log`, `println!`, `tracing_subscriber::fmt`, or `opentelemetry-stdout` for telemetry payloads.

If `telemetry` is enabled and exporter setup fails, dev server startup fails with a clear error. The implementation must not silently fall back to stdout.

The existing CLI-oriented OTel behavior can remain separate, but the dev sync payload telemetry path must always use the configured collector exporter.

## Components

### TypeScript Dev Plugin Options

Extend the dev server option types with:

```ts
type TelemetryOptions =
  | boolean
  | {
      collectorUrl?: string;
    };
```

Normalize `true` to `{ collectorUrl: "http://localhost:4317" }`.

Forward the normalized option through `ManagedDevRuntime`, `startLocalJazzServer`, and `DevServer.start`.

Update the generated NAPI TypeScript surface so `crates/jazz-napi/index.d.ts` exposes the same `telemetry` shape on `DevServer.start(...)`.

The dev plugin also exposes browser-readable runtime configuration for worker bridge telemetry:

- `*_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL`

The browser runtime mirrors the server option through `DbConfig` and `WorkerBridgeOptions`. Worker bridge telemetry is disabled unless an ingest URL is present.

`startLocalJazzServer(...)` returns the telemetry ingest URL on its server handle so framework plugins can inject the public env value.

### Rust NAPI Dev Server

Parse the new `telemetry` option in `DevServerStartOptions`.

When enabled, initialize a dev-server OTel exporter against the configured collector URL before starting the hosted server. Store the exporter/provider with the dev server lifetime and shut it down when the server stops.

### Telemetry Ingest Route

Add an app-scoped dev-only ingest route for browser worker-bridge observations:

```text
POST /apps/:app_id/dev/sync-payload-telemetry
```

The route is available only when sync payload telemetry is enabled. When sync payload telemetry is disabled, the route returns `404` and performs no export.

Browser-ingested observations do not need to include `app_id` in the request body. The route attaches the `:app_id` path value server-side. If the body does include `app_id`, the handler must validate that it matches `:app_id` and reject mismatches with `400`.

The ingest route is unauthenticated in v1. This is an explicit dev-only tradeoff: sync payload telemetry is opt-in, local-only, structured-only, and the route is unavailable when telemetry is disabled. Any local origin that can reach the dev server and knows the app id can post arbitrary structured telemetry records while telemetry is enabled.

Browser telemetry delivery is best-effort and lossy in v1. Failed ingest requests must not log to console and must not affect sync. Retry buffers are out of scope for the first version.

### TypeScript Sync Payload Decoder

Add a TypeScript sync-payload decoder for browser-side instrumentation. This is new runtime infrastructure: today worker bridge payloads can be `Uint8Array` or string values and there is no shared TypeScript decoder at that layer.

The decoder must turn the worker-bridge `Uint8Array` or string payload into structured telemetry fields that match the Rust `SyncPayload` extractors, derive `payload_variant`, derive searchable fields including `table_name` and `schema_hash`, and return a structured decode-failure result when decoding fails. `message_bytes` is the byte length of the binary payload or the UTF-8 byte length of the string payload. It must not materialize or return the decoded payload body for normal successful sync payloads. For decoded sync error/failure payloads, it must return the full parsed payload JSON for use as the OTel log body/content.

### Worker Bridge Instrumentation

Instrument the browser worker bridge in both directions:

- main runtime outgoing sync payloads before they are sent to the worker
- worker sync payloads before they are delivered to the main runtime

Decode each payload with the new TypeScript sync-payload decoder. Send the telemetry observation to the dev-server ingest endpoint using `fetch` and without logging failures to the console. Failed telemetry delivery must not break sync.

### WebSocket Instrumentation

Instrument the embedded Rust WebSocket server in both directions:

- client sync payload received by the dev server
- server sync payload sent to a client

Decode the typed sync payload and export the same telemetry observation fields used by browser-ingested events.

Inbound WebSocket frames can contain multiple sync payloads. The server emits one observation per inner sync payload, or one observation per member when an inner payload expands into multiple row/member records. `message_bytes` is the size of the inner payload when available; `source_frame_bytes` carries the entire frame size. `source_frame_id`, `source_payload_index`, and `source_payload_count` correlate records back to source order.

## Testing

Implementation should be test-driven. The first tests should cover:

- `jazzPlugin`, `withJazz`, and `jazzSvelteKit` accept `server.telemetry`; `startLocalJazzServer` accepts flat `telemetry`; all forward `collectorUrl` and generated browser ingest URL configuration.
- `DevServer.start` parses `telemetry` and rejects invalid shapes.
- The generated NAPI TypeScript declaration includes `telemetry`.
- The dev OTel exporter path emits OTel logs through the configured collector URL and never configures stdout export for sync payload telemetry.
- The TypeScript sync-payload decoder returns `payload_variant`, `table_name`, `schema_hash`, other searchable fields, and decode-failure records without payload bodies or raw bytes.
- Decoded sync error/failure payloads include the full parsed payload JSON in the OTel log body/content.
- The worker bridge emits decoded telemetry observations for `main_to_worker` and `worker_to_main` only when ingest URL is configured.
- The telemetry ingest route attaches or validates `app_id`, accepts decoded observations without printing them, and rejects body/path app id mismatches.
- The Rust WebSocket server emits decoded observations for `client_to_server` and `server_to_client`, including derived `table_name` and `schema_hash` for row-oriented sync payloads.
- Multi-payload WebSocket frames include frame correlation fields, and multi-member sync payloads emit per-member records.
- Decode failures produce explicit decode-error observations without payload bodies or raw bytes.

## Out Of Scope

- Production telemetry.
- Direct browser-to-collector export.
- Console or stdout telemetry output.
- Reliable browser telemetry delivery, retry queues, or ring buffers.
- Sampling, redaction, or payload-size truncation controls.
