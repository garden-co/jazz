# Dev Sync Payload OTel Telemetry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in dev sync payload telemetry that captures structured worker-bridge and WebSocket sync diagnostics, including `table_name` and `schema_hash`, and exports them as OTel log records through the local dev server. Normal non-error payloads stay structured-only; decoded error/failure payloads include the full parsed payload JSON in the OTel log body/content.

**Architecture:** The dev plugin forwards `server.telemetry` into the NAPI dev server. Rust owns the OTel log exporter and the dev-only ingest route. Browser worker-bridge instrumentation is enabled only when the plugin/runtime provides an ingest URL; it uses a TS helper backed by WASM/Rust decoding to build telemetry records, then posts them to the local server. WebSocket instrumentation records typed Rust `SyncPayload`s directly.

**Tech Stack:** TypeScript/Vitest, Rust/Tokio/Axum/NAPI, jazz-wasm bindings, OpenTelemetry OTLP logs, lefthook/oxfmt.

---

## File Map

- Modify `packages/jazz-tools/src/dev/vite.ts`: add `TelemetryOptions` to plugin server options.
- Modify `packages/jazz-tools/src/dev/next.ts`: preserve inherited server option type for `withJazz`.
- Modify `packages/jazz-tools/src/dev/sveltekit.ts`: preserve inherited server option type for `jazzSvelteKit`.
- Modify `packages/jazz-tools/src/dev/managed-runtime.ts`: normalize and forward `telemetry`.
- Modify `packages/jazz-tools/src/dev/dev-server.ts`: forward `telemetry` to NAPI.
- Modify `packages/jazz-tools/src/dev/*.test.ts`: verify option forwarding.
- Modify `crates/jazz-napi/src/lib.rs`: parse `telemetry` and pass it to server builder.
- Modify `crates/jazz-napi/index.d.ts`: expose `telemetry` on `DevServer.start`.
- Modify `crates/jazz-tools/Cargo.toml`: add OTel log dependencies/features usable without CLI stdout fallback.
- Create `crates/jazz-tools/src/sync_payload_telemetry.rs`: telemetry record types, field extraction, derivation errors, exporter trait.
- Modify `crates/jazz-tools/src/lib.rs`: export `sync_payload_telemetry`.
- Modify `crates/jazz-tools/src/server/mod.rs`: store optional sync payload telemetry sink/exporter in `ServerState`.
- Modify `crates/jazz-tools/src/server/builder.rs`: configure sync payload telemetry on hosted server builds.
- Modify `crates/jazz-tools/src/routes.rs`: add ingest route and WebSocket instrumentation.
- Modify `dev/observability/otel-collector.yml`: add OTLP logs pipeline.
- Modify `dev/observability/README.md`: document `telemetry`.
- Modify `crates/jazz-wasm/src/runtime.rs`: expose a telemetry decode helper for JS.
- Modify `packages/jazz-tools/src/types/jazz-wasm.d.ts`: type the new WASM helper.
- Create `packages/jazz-tools/src/runtime/sync-payload-telemetry.ts`: browser telemetry record helper and best-effort ingest client.
- Modify `packages/jazz-tools/src/runtime/db.ts`: pass sync payload telemetry ingest config into `WorkerBridgeOptions`.
- Modify `packages/jazz-tools/src/runtime/worker-bridge.ts`: telemetry main-to-worker and worker-to-main messages.
- Modify `packages/jazz-tools/src/worker/worker-protocol.ts`: carry telemetry ingest configuration into the worker.
- Modify `packages/jazz-tools/src/worker/jazz-worker.ts`: telemetry worker-side bridge messages.

---

### Task 1: TypeScript Dev Option Surface

**Files:**

- Modify: `packages/jazz-tools/src/dev/vite.ts`
- Modify: `packages/jazz-tools/src/dev/managed-runtime.ts`
- Modify: `packages/jazz-tools/src/dev/dev-server.ts`
- Test: `packages/jazz-tools/src/dev/dev-server.test.ts`
- Test: `packages/jazz-tools/src/dev/vite.test.ts`
- Test: `packages/jazz-tools/src/dev/next.test.ts`
- Test: `packages/jazz-tools/src/dev/sveltekit.test.ts`

- [ ] **Step 1: Write failing tests for forwarding `telemetry`**

Add a unit test in `packages/jazz-tools/src/dev/dev-server.test.ts` that mocks `jazz-napi` and asserts `startLocalJazzServer` forwards the normalized option:

```ts
it("forwards telemetry collectorUrl to DevServer.start", async () => {
  const calls: unknown[] = [];
  vi.doMock("jazz-napi", () => ({
    DevServer: {
      start: vi.fn(async (options: unknown) => {
        calls.push(options);
        return {
          appId: "app-1",
          port: 1234,
          url: "http://127.0.0.1:1234",
          dataDir: "",
          adminSecret: "admin",
          backendSecret: null,
          stop: vi.fn(async () => {}),
        };
      }),
    },
  }));
  const { startLocalJazzServer } = await import("./dev-server.js");

  const handle = await startLocalJazzServer({
    appId: "app-1",
    port: 1234,
    inMemory: true,
    adminSecret: "admin",
    telemetry: { collectorUrl: "http://127.0.0.1:4317" },
  });

  expect(calls[0]).toMatchObject({
    telemetry: { collectorUrl: "http://127.0.0.1:4317" },
  });
  await handle.stop();
});
```

Add one focused test in each plugin test file showing the option is accepted by TypeScript and reaches `ManagedDevRuntime.initialize(...)`:

```ts
jazzPlugin({
  server: {
    telemetry: { collectorUrl: "http://localhost:4317" },
  },
});
```

Keep this distinction explicit in tests: framework plugins accept `server.telemetry`, while `startLocalJazzServer(...)` accepts flat `telemetry`.

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
pnpm --filter jazz-tools test -- src/dev/dev-server.test.ts src/dev/vite.test.ts src/dev/next.test.ts src/dev/sveltekit.test.ts
```

Expected: TypeScript compile or assertion failures mentioning `telemetry`.

- [ ] **Step 3: Add option types and forwarding**

In `packages/jazz-tools/src/dev/vite.ts`, extend `JazzServerOptions`:

```ts
export type TelemetryOptions =
  | boolean
  | {
      collectorUrl?: string;
    };

export interface JazzServerOptions {
  port?: number;
  adminSecret?: string;
  appId?: string;
  allowLocalFirstAuth?: boolean;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  catalogueAuthority?: "local" | "forward";
  catalogueAuthorityUrl?: string;
  catalogueAuthorityAdminSecret?: string;
  telemetry?: TelemetryOptions;
}
```

In `packages/jazz-tools/src/dev/dev-server.ts`, add the same exported type and option:

```ts
export type TelemetryOptions =
  | boolean
  | {
      collectorUrl?: string;
    };

export interface StartLocalJazzServerOptions {
  appId?: string;
  port?: number;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  backendSecret?: string;
  adminSecret?: string;
  allowLocalFirstAuth?: boolean;
  catalogueAuthority?: "local" | "forward";
  catalogueAuthorityUrl?: string;
  catalogueAuthorityAdminSecret?: string;
  enableLogs?: boolean;
  telemetry?: TelemetryOptions;
}
```

Forward it in `DevServer.start({...})`:

```ts
telemetry: options.telemetry,
```

In `packages/jazz-tools/src/dev/managed-runtime.ts`, forward `telemetry` explicitly in the `startLocalJazzServer(...)` call:

```ts
telemetry: serverConfig.telemetry,
```

Keep `normalizeServerOption(...)` explicit: add `telemetry` as a known `JazzServerOptions` field and include only that known field, matching the existing `appId`, `port`, and auth option pattern.

- [ ] **Step 4: Run tests to verify green**

Run:

```bash
pnpm --filter jazz-tools test -- src/dev/dev-server.test.ts src/dev/vite.test.ts src/dev/next.test.ts src/dev/sveltekit.test.ts
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/dev
git commit -m "feat: thread sync payload telemetry dev options"
```

---

### Task 2: NAPI Option Parsing

**Files:**

- Modify: `crates/jazz-napi/src/lib.rs`
- Modify: `crates/jazz-napi/index.d.ts`
- Test: `crates/jazz-napi/src/lib.rs`

- [ ] **Step 1: Write failing Rust option parse tests**

Add tests near `parse_dev_server_start_options`:

```rust
#[cfg(test)]
mod dev_server_option_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_sync_payload_telemetry_collector_url() {
        let opts = parse_dev_server_start_options(json!({
            "appId": "app-1",
            "telemetry": { "collectorUrl": "http://127.0.0.1:4317" }
        }))
        .expect("parse options");

        assert_eq!(
            opts.sync_payload_telemetry.as_ref().and_then(|o| o.collector_url.as_deref()),
            Some("http://127.0.0.1:4317")
        );
    }

    #[test]
    fn parses_sync_payload_telemetry_true_as_enabled_default_url() {
        let opts = parse_dev_server_start_options(json!({
            "appId": "app-1",
            "telemetry": true
        }))
        .expect("parse options");

        assert_eq!(
            opts.sync_payload_telemetry.as_ref().and_then(|o| o.collector_url.as_deref()),
            Some("http://localhost:4317")
        );
    }
}
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cargo test -p jazz-napi dev_server_option_tests
```

Expected: FAIL because `sync_payload_telemetry` is not defined.

- [ ] **Step 3: Implement parsing**

Add the types:

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
enum DevServerTelemetryInput {
    Enabled(bool),
    Options(DevServerTelemetryOptions),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DevServerTelemetryOptions {
    collector_url: Option<String>,
}

fn normalize_sync_payload_telemetry(
    input: Option<DevServerTelemetryInput>,
) -> Result<Option<DevServerTelemetryOptions>, String> {
    match input {
        None | Some(DevServerTelemetryInput::Enabled(false)) => Ok(None),
        Some(DevServerTelemetryInput::Enabled(true)) => Ok(Some(DevServerTelemetryOptions {
            collector_url: Some("http://localhost:4317".to_string()),
        })),
        Some(DevServerTelemetryInput::Options(mut options)) => {
            if options.collector_url.is_none() {
                options.collector_url = Some("http://localhost:4317".to_string());
            }
            Ok(Some(options))
        }
    }
}
```

Parse with a raw struct, then normalize:

```rust
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawDevServerStartOptions {
    app_id: String,
    port: Option<u16>,
    data_dir: Option<String>,
    in_memory: Option<bool>,
    jwks_url: Option<String>,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
    allow_local_first_auth: Option<bool>,
    catalogue_authority: Option<String>,
    catalogue_authority_url: Option<String>,
    catalogue_authority_admin_secret: Option<String>,
    #[serde(rename = "telemetry")]
    sync_payload_telemetry: Option<DevServerTelemetryInput>,
}
```

Update `DevServerStartOptions` to include:

```rust
sync_payload_telemetry: Option<DevServerTelemetryOptions>,
```

When constructing `SyncPayloadTelemetryConfig`, store the collector URL and return the app-scoped sync payload telemetry ingest URL on the NAPI dev server handle so dev plugins can inject browser config.

- [ ] **Step 4: Update generated declaration**

In `crates/jazz-napi/index.d.ts`, add:

```ts
telemetry?: boolean | { collectorUrl?: string };
```

inside `DevServer.start(options: { ... })`, and add these optional fields to the returned dev server handle:

```ts
syncPayloadTelemetryIngestUrl?: string;
```

- [ ] **Step 5: Run tests to verify green**

Run:

```bash
cargo test -p jazz-napi dev_server_option_tests
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-napi/src/lib.rs crates/jazz-napi/index.d.ts
git commit -m "feat: parse dev sync payload telemetry options"
```

---

### Task 3: Rust Telemetry Record Model And Field Derivation

**Files:**

- Create: `crates/jazz-tools/src/sync_payload_telemetry.rs`
- Modify: `crates/jazz-tools/src/lib.rs`
- Test: `crates/jazz-tools/src/sync_payload_telemetry.rs`

- [ ] **Step 1: Write failing unit tests**

Create `crates/jazz-tools/src/sync_payload_telemetry.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_manager::{DurabilityTier, QueryId, SyncPayload};

    #[test]
    fn query_settled_extracts_query_and_tier() {
        let payload = SyncPayload::QuerySettled {
            query_id: QueryId(42),
            tier: DurabilityTier::GlobalServer,
            through_seq: 9,
        };

        let fields = SyncPayloadTelemetryFields::from_payload(&payload, FieldDerivation::default());

        assert_eq!(fields.payload_variant, "QuerySettled");
        assert_eq!(fields.query_id.as_deref(), Some("42"));
        assert_eq!(fields.durability_tier.as_deref(), Some("GlobalServer"));
    }

    #[test]
    fn row_payload_records_derivation_errors_when_table_and_schema_are_missing() {
        let payload = test_row_batch_created_payload();

        let fields = SyncPayloadTelemetryFields::from_payload(&payload, FieldDerivation::default());

        assert_eq!(fields.payload_variant, "RowBatchCreated");
        assert!(fields.row_id.is_some());
        assert_eq!(fields.table_name, None);
        assert_eq!(fields.schema_hash, None);
        assert_eq!(fields.table_name_error.as_deref(), Some("not_derived"));
        assert_eq!(fields.schema_hash_error.as_deref(), Some("not_derived"));
    }
}
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cargo test -p jazz-tools sync_payload_telemetry --features test
```

Expected: FAIL because `sync_payload_telemetry` does not exist.

- [ ] **Step 3: Implement record model**

Add:

```rust
use serde::{Deserialize, Serialize};

use crate::sync_manager::SyncPayload;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPayloadTelemetryScope {
    WorkerBridge,
    Websocket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPayloadTelemetryDirection {
    MainToWorker,
    WorkerToMain,
    ClientToServer,
    ServerToClient,
}

#[derive(Debug, Clone, Default)]
pub struct FieldDerivation {
    pub table_name: Option<String>,
    pub schema_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SyncPayloadTelemetryFields {
    pub payload_variant: String,
    pub row_id: Option<String>,
    pub table_name: Option<String>,
    pub table_name_error: Option<String>,
    pub branch_name: Option<String>,
    pub batch_id: Option<String>,
    pub query_id: Option<String>,
    pub schema_hash: Option<String>,
    pub schema_hash_error: Option<String>,
    pub durability_tier: Option<String>,
    pub error_variant: Option<String>,
    pub error_code: Option<String>,
    pub member_index: Option<u32>,
    pub member_count: Option<u32>,
}
```

Implement `SyncPayloadTelemetryFields::from_payload(&SyncPayload, FieldDerivation)` for single-record payloads using `payload.variant_name()`, `payload.object_id()`, `payload.branch_name()`, and explicit matches for batch/query/tier/error fields. For decoded sync error/failure payloads, export `payload_variant`, `error_variant`, and `error_code` when available, and set the OTel log body/content to the full parsed payload JSON. Do not emit that full parsed payload as top-level searchable attributes. For multi-member payloads such as `BatchSettlement` and `SealBatch`, add `SyncPayloadTelemetryFields::records_from_payload(...) -> Vec<SyncPayloadTelemetryFields>` and emit one field set per member with `member_index` and `member_count`. Do not collapse multiple tables or schema hashes into one arbitrary value. For row-oriented payloads, set `table_name_error` or `schema_hash_error` to `"not_derived"` when derivation is absent.

Export it in `crates/jazz-tools/src/lib.rs`:

```rust
pub mod sync_payload_telemetry;
```

- [ ] **Step 4: Run tests to verify green**

Run:

```bash
cargo test -p jazz-tools sync_payload_telemetry --features test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/sync_payload_telemetry.rs crates/jazz-tools/src/lib.rs
git commit -m "feat: add sync payload telemetry field extraction"
```

---

### Task 4: Rust OTel Log Exporter And Server Wiring

**Files:**

- Modify: `crates/jazz-tools/Cargo.toml`
- Create: `crates/jazz-tools/src/sync_payload_telemetry/exporter.rs`
- Modify: `crates/jazz-tools/src/sync_payload_telemetry.rs`
- Modify: `crates/jazz-tools/src/server/mod.rs`
- Modify: `crates/jazz-tools/src/server/builder.rs`
- Test: `crates/jazz-tools/src/server/builder.rs`

- [ ] **Step 1: Write failing builder tests**

Add a test showing sync payload telemetry config lands in `ServerState`:

```rust
#[tokio::test]
async fn server_builder_stores_sync_payload_telemetry_config() {
    let app_id = crate::schema_manager::AppId::from_name("telemetry-builder-test");
    let server = crate::server::builder::ServerBuilder::new(app_id)
        .with_in_memory_storage()
        .with_sync_payload_telemetry(crate::sync_payload_telemetry::SyncPayloadTelemetryConfig {
            collector_url: "http://127.0.0.1:4317".to_string(),
        })
        .build()
        .await
        .expect("build server");

    assert_eq!(
        server.state().sync_payload_telemetry_config().map(|c| c.collector_url.as_str()),
        Some("http://127.0.0.1:4317")
    );
}
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cargo test -p jazz-tools server_builder_stores_sync_payload_telemetry_config --features test
```

Expected: FAIL because `with_sync_payload_telemetry` is missing.

- [ ] **Step 3: Add config and sink**

In `sync_payload_telemetry.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncPayloadTelemetryConfig {
    pub collector_url: String,
}

pub trait SyncPayloadTelemetrySink: Send + Sync + 'static {
    fn emit(&self, record: SyncPayloadTelemetryRecord);
}
```

Define `SyncPayloadTelemetryRecord` as the exported/serialized shape and flatten `SyncPayloadTelemetryFields` into it. Do not duplicate field definitions in multiple structs.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPayloadTelemetryRecord {
    pub app_id: Option<String>,
    pub scope: SyncPayloadTelemetryScope,
    pub direction: SyncPayloadTelemetryDirection,
    pub client_id: Option<String>,
    pub connection_id: Option<String>,
    pub sequence: Option<u64>,
    pub source_frame_id: Option<String>,
    pub source_payload_index: Option<u32>,
    pub source_payload_count: Option<u32>,
    pub source_frame_bytes: Option<usize>,
    pub message_bytes: usize,
    pub message_encoding: SyncPayloadTelemetryMessageEncoding,
    pub recorded_at: u64,
    pub decode_error: Option<String>,
    pub log_body: Option<serde_json::Value>,
    #[serde(flatten)]
    pub fields: SyncPayloadTelemetryFields,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPayloadTelemetryMessageEncoding {
    Binary,
    Utf8,
}
```

Successful non-error records always set flattened `fields` and leave `log_body` empty. Decoded sync error/failure records set flattened `fields` and set `log_body` to the full parsed payload JSON; the OTel exporter maps `log_body` to the log body/content, not to searchable attributes. Decode-failure records set `decode_error`, `message_bytes`, and `message_encoding`, leave `fields` at `SyncPayloadTelemetryFields::default()`, and leave `log_body` empty because no parsed payload exists. Decode-failure records must not include raw payload bytes, base64-encoded payload bytes, or original string payloads.

In `server/mod.rs`, add to `ServerState`:

```rust
pub sync_payload_telemetry_config: Option<crate::sync_payload_telemetry::SyncPayloadTelemetryConfig>,
pub sync_payload_telemetry_sink: Option<std::sync::Arc<dyn crate::sync_payload_telemetry::SyncPayloadTelemetrySink>>,
```

Add accessors:

```rust
pub fn sync_payload_telemetry_config(&self) -> Option<&crate::sync_payload_telemetry::SyncPayloadTelemetryConfig> {
    self.sync_payload_telemetry_config.as_ref()
}
```

In `server/builder.rs`, add `sync_payload_telemetry: Option<SyncPayloadTelemetryConfig>` and:

```rust
pub fn with_sync_payload_telemetry(mut self, config: crate::sync_payload_telemetry::SyncPayloadTelemetryConfig) -> Self {
    self.sync_payload_telemetry = Some(config);
    self
}
```

- [ ] **Step 4: Add OTel logs exporter feature**

In `crates/jazz-tools/Cargo.toml`, add a non-CLI feature:

```toml
otel-logs = [
  "dep:opentelemetry",
  "dep:opentelemetry_sdk",
  "dep:opentelemetry-otlp",
]
```

Build a `SyncPayloadTelemetrySink` implementation behind `#[cfg(feature = "otel-logs")]` that uses OTLP logs and the configured collector URL. Do not use `opentelemetry-stdout`.

`SyncPayloadTelemetrySink::emit(...)` must be fire-and-forget. Implement the OTel sink as a bounded in-memory queue drained by a background task; if the queue has no capacity, drop the telemetry record and increment an internal dropped counter rather than blocking the WebSocket handler.

The `otel-logs` feature must be independent from the existing `otel` feature. Enabling sync payload telemetry must not transitively enable `opentelemetry-stdout`; if both features are enabled in a build, sync-payload-telemetry records still use only the OTel logs exporter path.

- [ ] **Step 5: Run tests to verify green**

Run:

```bash
cargo test -p jazz-tools server_builder_stores_sync_payload_telemetry_config --features test
cargo check -p jazz-tools --features otel-logs
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/Cargo.toml crates/jazz-tools/src/sync_payload_telemetry.rs crates/jazz-tools/src/sync_payload_telemetry crates/jazz-tools/src/server
git commit -m "feat: wire sync payload telemetry sink into server"
```

---

### Task 5: Telemetry Ingest Route

**Files:**

- Modify: `crates/jazz-tools/src/routes.rs`
- Test: `crates/jazz-tools/src/routes.rs`

- [ ] **Step 1: Write failing route tests**

Add tests near route tests:

```rust
#[tokio::test]
async fn sync_payload_telemetry_ingest_rejects_mismatched_app_id() {
    let state = test_state_with_sync_payload_telemetry_sink();
    let app = make_test_router(state.clone());
    let body = serde_json::json!({
        "appId": "wrong-app",
        "scope": "worker_bridge",
        "direction": "main_to_worker",
        "payloadVariant": "QuerySettled",
        "queryId": 1,
        "durabilityTier": "Local",
        "messageBytes": 12,
        "recordedAt": 1
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(test_app_route("/dev/sync-payload-telemetry"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
```

Add a second test that posts without `appId` and asserts the in-memory sink receives a record with `app_id` equal to the route app.

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cargo test -p jazz-tools sync_payload_telemetry_ingest --features test
```

Expected: FAIL because route is missing.

- [ ] **Step 3: Implement route**

In `create_router`, add:

```rust
.route("/dev/sync-payload-telemetry", post(sync_payload_telemetry_ingest_handler))
```

Implement:

```rust
async fn sync_payload_telemetry_ingest_handler(
    State(state): State<Arc<ServerState>>,
    Json(mut record): Json<crate::sync_payload_telemetry::SyncPayloadTelemetryRecord>,
) -> impl IntoResponse {
    if state.sync_payload_telemetry_config.is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let route_app_id = state.app_id.to_string();
    if let Some(body_app_id) = record.app_id.as_deref() {
        if body_app_id != route_app_id {
            return StatusCode::BAD_REQUEST.into_response();
        }
    }
    record.app_id = Some(route_app_id);
    let Some(sink) = state.sync_payload_telemetry_sink.as_ref() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    sink.emit(record);
    StatusCode::ACCEPTED.into_response()
}
```

The server builder must enforce the invariant that `sync_payload_telemetry_config` and `sync_payload_telemetry_sink` are either both `Some` or both `None`. If telemetry config is present and sink construction fails, server startup fails before routes are served.

The existing router already applies `CorsLayer::permissive()` to all routes in `create_router`. Keep `/dev/sync-payload-telemetry` inside that router so cross-origin POSTs from the host dev server origin are allowed. The route is unauthenticated in v1 by design: it is dev-only, opt-in, local, structured-only, and unavailable when telemetry is disabled. Add a route test with an `Origin` header and `OPTIONS` preflight or POST assertion that CORS headers are present for `/dev/sync-payload-telemetry`.

- [ ] **Step 4: Run tests to verify green**

Run:

```bash
cargo test -p jazz-tools sync_payload_telemetry_ingest --features test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/routes.rs crates/jazz-tools/src/sync_payload_telemetry.rs
git commit -m "feat: add sync payload telemetry ingest route"
```

---

### Task 6: WebSocket Sync Payload Telemetry Instrumentation

**Files:**

- Modify: `crates/jazz-tools/src/routes.rs`
- Modify: `crates/jazz-tools/src/server/mod.rs`
- Test: `crates/jazz-tools/src/routes.rs`

- [ ] **Step 1: Write failing WebSocket telemetry tests**

Add a route unit test that calls `process_ws_client_frame(...)` with a `SyncBatchRequest` containing multiple payloads and asserts the sink receives one record per inner payload with `direction = client_to_server`, `source_frame_id`, `source_payload_index`, `source_payload_count`, `source_frame_bytes`, `message_bytes`, and the expected structured fields.

Add a route unit test for a multi-member payload such as `BatchSettlement` or `SealBatch` and assert the sink receives one record per member with `member_index`, `member_count`, and per-member `table_name`/`schema_hash` or visible derivation error fields.

Add a test for server-to-client by exercising `handle_ws_connection(...)` far enough for its `sync_rx.recv()` branch to send a `ServerEvent::SyncUpdate`. Assert the sink receives `direction = server_to_client`, `payload_variant`, and the same structured fields.

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cargo test -p jazz-tools ws_sync_payload_telemetry --features test
```

Expected: FAIL because WebSocket telemetry emission is missing.

- [ ] **Step 3: Implement inbound telemetry emission**

In `process_ws_client_frame(...)`, after decoding each client payload and before applying it, call:

```rust
let source_frame_id = state.next_sync_payload_telemetry_frame_id();
let source_frame_bytes = inner.len();
let source_payload_count = request.payloads.len();
for (source_payload_index, payload) in request.payloads.iter().enumerate() {
    state.emit_sync_payload_telemetry(crate::sync_payload_telemetry::SyncPayloadTelemetryEnvelope {
        scope: SyncPayloadTelemetryScope::Websocket,
        direction: SyncPayloadTelemetryDirection::ClientToServer,
        client_id: Some(client_id.to_string()),
        connection_id: Some(connection_id.to_string()),
        sequence: None,
        source_frame_id: Some(source_frame_id.clone()),
        source_payload_index: Some(source_payload_index as u32),
        source_payload_count: Some(source_payload_count as u32),
        source_frame_bytes: Some(source_frame_bytes),
        payload,
        message_bytes: payload.encoded_len_for_telemetry().unwrap_or(source_frame_bytes),
        message_encoding: SyncPayloadTelemetryMessageEncoding::Binary,
    });
}
```

Add `encoded_len_for_telemetry(...)` or an equivalent helper beside the sync payload telemetry emission code; it returns the encoded byte length of the inner payload when the already-decoded payload still has enough source information, otherwise the caller falls back to `source_frame_bytes`.

Change `process_ws_client_frame(...)` to accept the active connection id:

```rust
state
    .process_ws_client_frame(connection_id, client_id, &inner)
    .await
```

and update unit tests that call it directly.

Add helper methods on `ServerState` to derive `table_name` and `schema_hash` from storage/schema context and to set `table_name_error` or `schema_hash_error` fields when derivation fails.

`emit_sync_payload_telemetry(...)` must emit structured fields without materializing the decoded payload body for normal non-error payloads. For decoded error/failure payloads, it must set `log_body` to the full parsed payload JSON.

- [ ] **Step 4: Implement outbound telemetry emission**

In the `update = sync_rx.recv()` branch of `handle_ws_connection`, emit before serializing `ServerEvent::SyncUpdate`:

```rust
state.emit_sync_payload_telemetry(crate::sync_payload_telemetry::SyncPayloadTelemetryEnvelope {
    scope: SyncPayloadTelemetryScope::Websocket,
    direction: SyncPayloadTelemetryDirection::ServerToClient,
    client_id: Some(client_id.to_string()),
    connection_id: Some(connection_id.to_string()),
    sequence: Some(u.seq),
    source_frame_id: Some(state.next_sync_payload_telemetry_frame_id()),
    source_payload_index: Some(0),
    source_payload_count: Some(1),
    source_frame_bytes: Some(bytes.len()),
    payload: &u.payload,
    message_bytes: bytes.len(),
    message_encoding: SyncPayloadTelemetryMessageEncoding::Binary,
});
```

- [ ] **Step 5: Run tests to verify green**

Run:

```bash
cargo test -p jazz-tools ws_sync_payload_telemetry --features test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/src/routes.rs crates/jazz-tools/src/server/mod.rs crates/jazz-tools/src/sync_payload_telemetry.rs
git commit -m "feat: emit websocket sync payload telemetry"
```

---

### Task 7: WASM/TypeScript Decode Helper

**Files:**

- Modify: `crates/jazz-wasm/src/runtime.rs`
- Modify: `packages/jazz-tools/src/types/jazz-wasm.d.ts`
- Create: `packages/jazz-tools/src/runtime/sync-payload-telemetry.ts`
- Test: `packages/jazz-tools/src/runtime/worker-bridge.test.ts`

- [ ] **Step 1: Write failing TS helper tests**

In `worker-bridge.test.ts`, add a helper-level test with a fake decoder:

```ts
it("builds structured telemetry records", () => {
  const record = buildSyncPayloadTelemetryRecord({
    scope: "worker_bridge",
    direction: "main_to_worker",
    appId: "app-1",
    payload: new Uint8Array([1, 2, 3]),
    decode: () => ({
      ok: true,
      payloadVariant: "QuerySettled",
      fields: { queryId: 1 },
    }),
  });

  expect(record).toMatchObject({
    scope: "worker_bridge",
    direction: "main_to_worker",
    appId: "app-1",
    payloadVariant: "QuerySettled",
    queryId: 1,
  });
});

it("puts decoded error payloads in the log body", () => {
  const parsedErrorPayload = {
    type: "SyncError",
    code: "schema_mismatch",
    message: "Client schema is behind",
    expectedSchemaHash: "schema-new",
    actualSchemaHash: "schema-old",
  };
  const record = buildSyncPayloadTelemetryRecord({
    scope: "worker_bridge",
    direction: "worker_to_main",
    appId: "app-1",
    payload: new Uint8Array([1, 2, 3]),
    decode: () => ({
      ok: true,
      payloadVariant: "SyncError",
      fields: { errorVariant: "SchemaMismatch", errorCode: "schema_mismatch" },
      logBody: parsedErrorPayload,
    }),
  });

  expect(record).toMatchObject({
    payloadVariant: "SyncError",
    errorVariant: "SchemaMismatch",
    errorCode: "schema_mismatch",
    logBody: parsedErrorPayload,
  });
});

it("builds decode-failure telemetry records without payload bytes", () => {
  const record = buildSyncPayloadTelemetryRecord({
    scope: "worker_bridge",
    direction: "main_to_worker",
    appId: "app-1",
    payload: new Uint8Array([1, 2, 3]),
    decode: () => ({ ok: false, error: "bad postcard" }),
  });

  expect(record).toMatchObject({
    scope: "worker_bridge",
    direction: "main_to_worker",
    appId: "app-1",
    messageBytes: 3,
    messageEncoding: "binary",
    decodeError: "bad postcard",
  });
  expect(record).not.toHaveProperty("messageBase64");
});

it("builds decode-failure telemetry records for string payloads without payload bytes", () => {
  const record = buildSyncPayloadTelemetryRecord({
    scope: "worker_bridge",
    direction: "worker_to_main",
    appId: "app-1",
    payload: '{"bad":true}',
    decode: () => ({ ok: false, error: "bad json payload" }),
  });

  expect(record).toMatchObject({
    messageBytes: 12,
    messageEncoding: "utf8",
    decodeError: "bad json payload",
  });
  expect(record).not.toHaveProperty("messageBase64");
});
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/worker-bridge.test.ts
```

Expected: FAIL because `buildSyncPayloadTelemetryRecord` is missing.

- [ ] **Step 3: Add WASM decode export**

In `crates/jazz-wasm/src/runtime.rs`, add:

```rust
#[wasm_bindgen(js_name = decodeSyncPayloadForTelemetry)]
pub fn decode_sync_payload_for_telemetry(
    &self,
    payload: JsValue,
) -> Result<JsValue, JsError> {
    let payload = self.parse_sync_payload(payload)?;
    let telemetry = crate::sync_payload_telemetry::SyncPayloadTelemetryDecoded::from_payload(&payload);
    let telemetry_json = serde_wasm_bindgen::to_value(&telemetry)
        .map_err(|e| JsError::new(&format!("Sync payload JSON conversion failed: {e}")))?;
    Ok(telemetry_json)
}
```

If `serde_wasm_bindgen` is not already available in `jazz-wasm`, add it to `crates/jazz-wasm/Cargo.toml`.

Update `packages/jazz-tools/src/types/jazz-wasm.d.ts`:

```ts
decodeSyncPayloadForTelemetry(payload: Uint8Array | string): unknown;
```

- [ ] **Step 4: Add TS telemetry helper**

Create `packages/jazz-tools/src/runtime/sync-payload-telemetry.ts`:

```ts
export type SyncPayloadTelemetryScope = "worker_bridge" | "websocket";
export type SyncPayloadTelemetryDirection =
  | "main_to_worker"
  | "worker_to_main"
  | "client_to_server"
  | "server_to_client";

export type DecodeResult =
  | { ok: true; payloadVariant: string; fields: Record<string, unknown>; logBody?: unknown }
  | { ok: false; error: string };

export function buildSyncPayloadTelemetryRecord(options: {
  scope: SyncPayloadTelemetryScope;
  direction: SyncPayloadTelemetryDirection;
  appId: string;
  clientId?: string;
  payload: Uint8Array | string;
  decode: (payload: Uint8Array | string) => DecodeResult;
}): Record<string, unknown> {
  const messageBytes =
    typeof options.payload === "string"
      ? new TextEncoder().encode(options.payload).byteLength
      : options.payload.byteLength;
  const decoded = options.decode(options.payload);
  if (!decoded.ok) {
    return {
      scope: options.scope,
      direction: options.direction,
      appId: options.appId,
      clientId: options.clientId,
      messageBytes,
      messageEncoding: typeof options.payload === "string" ? "utf8" : "binary",
      recordedAt: Date.now(),
      decodeError: decoded.error,
    };
  }
  const record: Record<string, unknown> = {
    scope: options.scope,
    direction: options.direction,
    appId: options.appId,
    clientId: options.clientId,
    payloadVariant: decoded.payloadVariant,
    messageBytes,
    messageEncoding: typeof options.payload === "string" ? "utf8" : "binary",
    recordedAt: Date.now(),
    ...decoded.fields,
  };
  if (decoded.logBody !== undefined) {
    record.logBody = decoded.logBody;
  }
  return record;
}
```

Implement `payloadVariantFromJson(...)` and direct field extraction for row id, branch, batch, query, tier, error variant, `table_name`, and `schema_hash`. For missing required derivations, include `tableNameError` or `schemaHashError`.

- [ ] **Step 5: Run tests to verify green**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/worker-bridge.test.ts
cargo check -p jazz-wasm
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-wasm packages/jazz-tools/src/runtime/sync-payload-telemetry.ts packages/jazz-tools/src/types/jazz-wasm.d.ts packages/jazz-tools/src/runtime/worker-bridge.test.ts
git commit -m "feat: add browser sync payload telemetry decoder"
```

---

### Task 8: Worker Bridge And Worker Ingest Instrumentation

**Files:**

- Modify: `packages/jazz-tools/src/runtime/worker-bridge.ts`
- Modify: `packages/jazz-tools/src/worker/worker-protocol.ts`
- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts`
- Modify: `packages/jazz-tools/src/runtime/sync-payload-telemetry.ts`
- Test: `packages/jazz-tools/src/runtime/worker-bridge.test.ts`
- Test: `packages/jazz-tools/src/worker/jazz-worker.test.ts`

- [ ] **Step 1: Write failing instrumentation tests**

In `worker-bridge.test.ts`, add:

```ts
it("posts main_to_worker telemetry records without blocking sync", async () => {
  const posted: unknown[] = [];
  const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

  const initPromise = bridge.init({
    schemaJson: "{}",
    appId: "app-1",
    env: "dev",
    userBranch: "main",
    dbName: "test-db",
    telemetry: {
      appId: "app-1",
      ingestUrl: "http://127.0.0.1:1234/apps/app-1/dev/sync-payload-telemetry",
      post: async (_url, body) => posted.push(body),
    },
  });
  worker.emitFromWorker({ type: "init-ok", clientId: "worker-client-123" });
  await initPromise;

  runtimeMock.emitSyncPayload(
    "server",
    "server-1",
    enc({ QuerySettled: { query_id: 1, tier: "Local", through_seq: 1 } }),
    false,
  );
  await Promise.resolve();

  expect(posted).toHaveLength(1);
  expect(posted[0]).toMatchObject({ direction: "main_to_worker", scope: "worker_bridge" });
});
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/worker-bridge.test.ts src/worker/jazz-worker.test.ts
```

Expected: FAIL because telemetry options are missing.

- [ ] **Step 3: Wire telemetry config through worker protocol**

In `worker-protocol.ts`, extend `InitMessage`:

```ts
telemetry?: {
  ingestUrl: string;
  appId: string;
};
```

In `WorkerBridgeOptions`, add the same `telemetry` shape.

- [ ] **Step 4: Instrument main-thread bridge**

In `worker-bridge.ts`, keep the constructor signature unchanged:

```ts
constructor(worker: Worker, runtime: Runtime)
```

Store telemetry config from `WorkerBridge.init(options.telemetry)`. The instrumentation point for main-to-worker messages is inside the private `enqueueSyncMessageForWorker(payload)` method before pushing to `pendingSyncPayloadsForWorker`. The instrumentation point for worker-to-main messages is in the `this.worker.onmessage` `"sync"` branch before `this.runtime.onSyncMessageReceived(payload)`.

Worker bridge telemetry is disabled unless `ingestUrl` and `appId` are present. `buildSyncPayloadTelemetryRecord(...)` must not emit decoded payload bodies for normal non-error payloads, but decoded error/failure payloads must set `logBody` to the full parsed payload JSON.

`postSyncPayloadTelemetryRecord` must catch and ignore fetch failures without console logging:

```ts
export async function postSyncPayloadTelemetryRecord(
  ingestUrl: string,
  record: Record<string, unknown>,
  post: typeof fetch = fetch,
): Promise<void> {
  try {
    await post(ingestUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(record),
    } as RequestInit);
  } catch {
    return;
  }
}
```

- [ ] **Step 5: Instrument worker side**

In `jazz-worker.ts`, when handling `sync` messages from main and when enqueueing sync for main, emit `worker_bridge` records with directions `main_to_worker` and `worker_to_main`.

- [ ] **Step 6: Run tests to verify green**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/worker-bridge.test.ts src/worker/jazz-worker.test.ts
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/jazz-tools/src/runtime/worker-bridge.ts packages/jazz-tools/src/runtime/sync-payload-telemetry.ts packages/jazz-tools/src/worker/worker-protocol.ts packages/jazz-tools/src/worker/jazz-worker.ts packages/jazz-tools/src/runtime/worker-bridge.test.ts packages/jazz-tools/src/worker/jazz-worker.test.ts
git commit -m "feat: emit worker bridge sync payload telemetry"
```

---

### Task 9: Plugin Runtime Configuration For Browser Ingest

**Files:**

- Modify: `packages/jazz-tools/src/dev/managed-runtime.ts`
- Modify: `packages/jazz-tools/src/dev/vite.ts`
- Modify: `packages/jazz-tools/src/dev/next.ts`
- Modify: `packages/jazz-tools/src/dev/sveltekit.ts`
- Modify: `packages/jazz-tools/src/runtime/db.ts`
- Test: `packages/jazz-tools/src/dev/vite.test.ts`
- Test: `packages/jazz-tools/src/dev/next.test.ts`
- Test: `packages/jazz-tools/src/dev/sveltekit.test.ts`
- Test: `packages/jazz-tools/src/runtime/db.worker-bootstrap.test.ts`

- [ ] **Step 1: Write failing config injection tests**

In Vite plugin test, after configure server with sync payload telemetry, assert:

```ts
expect(fakeViteServer.config.env.VITE_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL).toBe(
  `http://127.0.0.1:${port}/apps/${fakeViteServer.config.env.VITE_JAZZ_APP_ID}/dev/sync-payload-telemetry`,
);
```

Add equivalent assertions for:

```ts
NEXT_PUBLIC_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL;
PUBLIC_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL;
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
pnpm --filter jazz-tools test -- src/dev/vite.test.ts src/dev/next.test.ts src/dev/sveltekit.test.ts src/runtime/db.worker-bootstrap.test.ts
```

Expected: FAIL because env vars are missing.

- [ ] **Step 3: Return ingest URL from managed runtime and inject public env**

Extend `ManagedRuntime`:

```ts
syncPayloadTelemetryIngestUrl?: string;
```

When local server sync payload telemetry is enabled, set:

```ts
syncPayloadTelemetryIngestUrl = `${serverUrl}/apps/${appId}/dev/sync-payload-telemetry`;
```

Set the per-framework public env keys where app id and server URL are already injected:

```ts
viteServer.config.env.VITE_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL =
  managed.syncPayloadTelemetryIngestUrl;
```

```ts
[NEXT_PUBLIC_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL]: managed.syncPayloadTelemetryIngestUrl,
```

```ts
viteServer.config.env.PUBLIC_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL =
  managed.syncPayloadTelemetryIngestUrl;
```

- [ ] **Step 4: Connect runtime config to WorkerBridgeOptions**

Add a small resolver near `DbConfig` in `packages/jazz-tools/src/runtime/db.ts`:

```ts
function defaultSyncPayloadTelemetryIngestUrlFromEnv(): string | undefined {
  if (typeof import.meta !== "undefined") {
    const viteUrl = (import.meta as any).env?.VITE_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL;
    const svelteUrl = (import.meta as any).env?.PUBLIC_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL;
    if (typeof viteUrl === "string" && viteUrl.length > 0) return viteUrl;
    if (typeof svelteUrl === "string" && svelteUrl.length > 0) return svelteUrl;
  }
  if (typeof process !== "undefined" && process.env) {
    const nextUrl = process.env.NEXT_PUBLIC_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL;
    if (typeof nextUrl === "string" && nextUrl.length > 0) return nextUrl;
  }
  return undefined;
}
```

In `packages/jazz-tools/src/runtime/db.ts`, extend `DbConfig`:

```ts
/** Dev-only sync payload telemetry ingest endpoint injected by Jazz dev plugins. */
syncPayloadTelemetryIngestUrl?: string;
```

In `Db.buildWorkerBridgeOptions(schemaJson)`, compute:

```ts
const syncPayloadTelemetryIngestUrl =
  this.config.syncPayloadTelemetryIngestUrl ?? defaultSyncPayloadTelemetryIngestUrlFromEnv();
```

and include:

```ts
telemetry: syncPayloadTelemetryIngestUrl
  ? {
      appId: this.config.appId,
      ingestUrl: syncPayloadTelemetryIngestUrl,
    }
  : undefined,
```

Add a `db.worker-bootstrap.test.ts` assertion that `buildWorkerBridgeOptions("{}")` uses `NEXT_PUBLIC_JAZZ_SYNC_PAYLOAD_TELEMETRY_INGEST_URL` when explicit config is absent, mirroring the existing `NEXT_PUBLIC_JAZZ_WASM_URL` pattern.

- [ ] **Step 5: Run tests to verify green**

Run:

```bash
pnpm --filter jazz-tools test -- src/dev/vite.test.ts src/dev/next.test.ts src/dev/sveltekit.test.ts src/runtime/db.worker-bootstrap.test.ts
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/jazz-tools/src/dev packages/jazz-tools/src/runtime
git commit -m "feat: inject sync payload telemetry ingest config"
```

---

### Task 10: Observability Config And End-to-End Verification

**Files:**

- Modify: `dev/observability/otel-collector.yml`
- Modify: `dev/observability/README.md`
- Test/Run: package and Rust checks

- [ ] **Step 1: Update collector logs pipeline**

In `dev/observability/otel-collector.yml`, add logs:

```yaml
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/lgtm]
    logs:
      receivers: [otlp]
      exporters: [otlp/lgtm]
```

- [ ] **Step 2: Update README**

Document:

```ts
jazzPlugin({
  server: {
    telemetry: {
      collectorUrl: "http://localhost:4317",
    },
  },
});
```

State that sync payloads are exported as structured OTel logs, not stdout, and that normal non-error decoded payload bodies are not exported. Decoded error/failure payloads include the full parsed payload JSON in the OTel log body/content.

- [ ] **Step 3: Run focused verification**

Run:

```bash
pnpm --filter jazz-tools test -- src/dev/dev-server.test.ts src/dev/vite.test.ts src/dev/next.test.ts src/dev/sveltekit.test.ts src/runtime/worker-bridge.test.ts src/worker/jazz-worker.test.ts
cargo test -p jazz-tools sync_payload_telemetry --features test
cargo test -p jazz-tools sync_payload_telemetry_ingest --features test
cargo test -p jazz-tools ws_sync_payload_telemetry --features test
cargo test -p jazz-napi dev_server_option_tests
cargo check -p jazz-tools --features otel-logs
cargo check -p jazz-wasm
```

Expected: all commands pass.

- [ ] **Step 4: Run formatting**

Run:

```bash
pnpm format docs/superpowers/plans/2026-04-27-dev-sync-otel-tracing.md dev/observability/README.md dev/observability/otel-collector.yml packages/jazz-tools/src crates/jazz-napi/index.d.ts
cargo fmt
```

Expected: commands exit 0.

- [ ] **Step 5: Commit**

```bash
git add dev/observability docs/superpowers/plans/2026-04-27-dev-sync-otel-tracing.md
git commit -m "docs: document dev sync payload telemetry"
```

---

## Final Verification

- [ ] Run `git status --short` and confirm only intentional files are changed.
- [ ] Run `pnpm --filter jazz-tools test -- src/dev/dev-server.test.ts src/dev/vite.test.ts src/dev/next.test.ts src/dev/sveltekit.test.ts src/runtime/worker-bridge.test.ts src/worker/jazz-worker.test.ts`.
- [ ] Run `cargo test -p jazz-tools sync_payload_telemetry --features test`.
- [ ] Run `cargo test -p jazz-napi dev_server_option_tests`.
- [ ] Run `cargo check -p jazz-tools --features otel-logs`.
- [ ] Run `cargo check -p jazz-wasm`.
- [ ] Start `dev/observability/docker compose up -d`, run one example app with `server.telemetry`, perform a write, and confirm Grafana/Loki receives `jazz.sync` log records containing structured fields and either `table_name`/`schema_hash` or the corresponding `table_name_error`/`schema_hash_error` derivation field. Confirm normal non-error records do not include decoded payload bodies, and decoded error/failure records include the full parsed payload JSON in the log body/content.
