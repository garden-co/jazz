# Dev Sync OTel Tracing Design

## Goal

Add opt-in sync-message tracing for local dev sessions started by the Jazz dev plugins. When enabled, tracing captures structured sync-message diagnostics for both browser-internal worker bridge traffic and client/server WebSocket traffic, then exports everything through one local server-side OpenTelemetry exporter as OpenTelemetry log records.

Trace payloads must never be printed to console or stdout.

## Entry Points

The tracing option is configured on the dev server setup surface:

```ts
jazzPlugin({
  server: {
    syncTracing: {
      collectorUrl: "http://localhost:4317",
      payload: "structured",
    },
  },
});
```

The same `server.syncTracing` option is supported by:

- `jazzPlugin(...)`
- `withJazz(...)`
- `jazzSvelteKit(...)`
- `startLocalJazzServer(...)`

`collectorUrl` defaults to `http://localhost:4317` when `syncTracing` is enabled and no URL is provided. `payload` defaults to `"structured"` and can be set to `"full"` for deeper local debugging.

## Architecture

The local dev server owns the OpenTelemetry exporter. Browser runtime code never exports to OTel directly and never talks to the collector.

Browser-side worker bridge traffic is decoded in the TypeScript runtime layer, converted to compact structured trace observations, and sent to the local dev server with an app-scoped trace ingest request:

```text
POST /apps/:app_id/dev/sync-traces
```

Client/server WebSocket traffic is observed inside the embedded Rust dev server. The server records both inbound client frames and outbound server frames, decodes the sync payloads, and exports the same trace observation shape directly.

All trace data leaves through one path:

```text
browser main thread <-> worker
        -> dev-server trace ingest
        -> dev-server OTel exporter
        -> configured OTel collector

worker/client <-> dev-server WebSocket
        -> dev-server OTel exporter
        -> configured OTel collector
```

## OTel Signal

Each sync observation is exported as an OpenTelemetry log record, not as a span or span event.

Sync messages are discrete observations rather than operations with start/end lifetimes, and the browser-ingested worker-bridge records will not share span context with the Rust WebSocket server. Logs let the implementation export searchable structured attributes without inventing artificial span parenting.

The local observability stack must include an OTLP logs pipeline when sync tracing is enabled.

## Trace Records

Each successfully decoded message produces one log record with:

- `scope`: `worker_bridge` or `websocket`
- `direction`: `main_to_worker`, `worker_to_main`, `client_to_server`, or `server_to_client`
- `app_id`
- `client_id`, when known
- `connection_id`, when known
- `sequence`, when known
- `payload_variant`
- `message_bytes`
- `recorded_at`

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

When `payload: "full"` is explicitly enabled, successful records also include:

- `payload_json`: the full decoded sync payload JSON

`payload_json` is not emitted in the default `"structured"` mode. The default mode should avoid serializing or exporting the full decoded payload when the structured fields are sufficient.

### Derived Fields

`table_name` and `schema_hash` are required top-level searchable fields in v1 whenever a sync payload refers to row data, query data, schema diagnostics, or catalogue/schema data.

Some payloads carry these fields directly, such as `SchemaWarning`, `ConnectionSchemaDiagnostics`, query payloads, and catalogue/schema payloads. Row-oriented payloads such as `RowBatchCreated`, `RowBatchNeeded`, `RowBatchStateChanged`, `BatchSettlement`, and `SealBatch` require derivation from the row id, branch, metadata, schema catalogue, storage, or connection schema context available to the local runtime/server.

If the implementation cannot derive a required field for a payload that should have it, it must still export the message and set a derivation error field, for example `table_name_error` or `schema_hash_error`. Missing derivation must be visible in OTel, not silently omitted.

Decode failures are traced too. A decode-failure record includes scope, direction, app id, client or connection identifiers when available, message byte size, decode error, and a capped `message_base64` containing the raw undecoded message bytes. It does not silently degrade to an opaque byte-only success record. The cap is implementation-defined in v1 but must be documented near the option and applied before export.

## No Stdout Fallback

Sync tracing must not use `console.log`, `println!`, `tracing_subscriber::fmt`, or `opentelemetry-stdout` for trace payloads.

If `syncTracing` is enabled and exporter setup fails, dev server startup fails with a clear error. The implementation must not silently fall back to stdout.

The existing CLI-oriented OTel behavior can remain separate, but the dev sync tracing path must always use the configured collector exporter.

## Components

### TypeScript Dev Plugin Options

Extend the dev server option types with:

```ts
type SyncTracingOptions =
  | boolean
  | {
      collectorUrl?: string;
      payload?: "structured" | "full";
    };
```

Normalize `true` to `{ collectorUrl: "http://localhost:4317", payload: "structured" }`.

Forward the normalized option through `ManagedDevRuntime`, `startLocalJazzServer`, and `DevServer.start`.

Update the generated NAPI TypeScript surface so `crates/jazz-napi/index.d.ts` exposes the same `syncTracing` shape on `DevServer.start(...)`.

The dev plugin also exposes browser-readable runtime configuration for worker bridge tracing:

- `*_JAZZ_SYNC_TRACE_INGEST_URL`
- `*_JAZZ_SYNC_TRACE_PAYLOAD`

The browser runtime mirrors the server option. When the mode is `"structured"`, it must not request or emit `payload_json`.

### Rust NAPI Dev Server

Parse the new `syncTracing` option in `DevServerStartOptions`.

When enabled, initialize a dev-server OTel exporter against the configured collector URL before starting the hosted server. Store the exporter/provider with the dev server lifetime and shut it down when the server stops.

### Trace Ingest Route

Add an app-scoped dev-only ingest route for browser worker-bridge observations:

```text
POST /apps/:app_id/dev/sync-traces
```

The route is available only when sync tracing is enabled. When sync tracing is disabled, the route returns `404` and performs no export.

Browser-ingested observations do not need to include `app_id` in the request body. The route attaches the `:app_id` path value server-side. If the body does include `app_id`, the handler must validate that it matches `:app_id` and reject mismatches with `400`.

Browser trace delivery is best-effort and lossy in v1. Failed ingest requests must not log to console and must not affect sync. Retry buffers are out of scope for the first version.

### TypeScript Sync Payload Decoder

Add a TypeScript sync-payload decoder for browser-side instrumentation. This is new runtime infrastructure: today worker bridge payloads are `Uint8Array` values and there is no shared TypeScript decoder at that layer.

The decoder must turn the worker-bridge payload bytes into structured trace fields that match the Rust `SyncPayload` extractors, derive `payload_variant`, derive searchable fields including `table_name` and `schema_hash`, and return a decode-failure result with capped `message_base64` when decoding fails. It only materializes and returns full decoded JSON when the tracing config requests `payload: "full"`.

### Worker Bridge Instrumentation

Instrument the browser worker bridge in both directions:

- main runtime outgoing sync payloads before they are sent to the worker
- worker sync payloads before they are delivered to the main runtime

Decode each payload with the new TypeScript sync-payload decoder. Send the trace observation to the dev-server ingest endpoint using `fetch` without logging failures to the console. Failed trace delivery must not break sync.

### WebSocket Instrumentation

Instrument the embedded Rust WebSocket server in both directions:

- client sync payload received by the dev server
- server sync payload sent to a client

Decode the typed sync payload and export the same trace observation fields used by browser-ingested events.

## Testing

Implementation should be test-driven. The first tests should cover:

- `jazzPlugin`, `withJazz`, `jazzSvelteKit`, and `startLocalJazzServer` accept and forward `syncTracing.collectorUrl`.
- `DevServer.start` parses `syncTracing` and rejects invalid shapes.
- The generated NAPI TypeScript declaration includes `syncTracing`.
- The dev OTel exporter path emits OTel logs through the configured collector URL and never configures stdout export for sync tracing.
- The TypeScript sync-payload decoder returns `payload_variant`, `table_name`, `schema_hash`, other searchable fields, optional decoded JSON only when `payload: "full"`, and decode-failure records with capped `message_base64`.
- The worker bridge emits decoded trace observations for `main_to_worker` and `worker_to_main`.
- The trace ingest route attaches or validates `app_id`, accepts decoded observations without printing them, and rejects body/path app id mismatches.
- The Rust WebSocket server emits decoded observations for `client_to_server` and `server_to_client`, including derived `table_name` and `schema_hash` for row-oriented sync payloads.
- Decode failures produce explicit decode-error observations with raw bytes encoded as base64.

## Out Of Scope

- Production tracing.
- Direct browser-to-collector export.
- Console or stdout trace output.
- Reliable browser trace delivery, retry queues, or ring buffers.
- Sampling or redaction controls beyond the default structured-only payload mode.
