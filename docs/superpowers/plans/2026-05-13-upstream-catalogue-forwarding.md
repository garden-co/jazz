# Upstream Catalogue Forwarding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove explicit `catalogueAuthority*` configuration and derive catalogue HTTP forwarding from `upstreamUrl`.

**Architecture:** `ServerBuilder` computes both edge topology and an optional upstream HTTP base URL from `upstreamUrl`. Core servers handle catalogue routes locally; edge servers validate `X-Jazz-Admin-Secret` locally, then forward all catalogue GET and POST HTTP routes to the upstream using the same admin header.

**Tech Stack:** Rust `jazz-tools` server, Axum route tests, NAPI Rust bindings, TypeScript `jazz-tools` dev server options, Vitest, status-quo Markdown specs.

---

### Task 1: Add Builder URL and Edge Auth Tests

**Files:**

- Modify: `crates/jazz-tools/src/server/builder.rs`

- [ ] **Step 1: Write failing builder tests**

Add these tests inside `#[cfg(test)] mod tests` in `crates/jazz-tools/src/server/builder.rs`, near the existing `upstream_url_conversion_*` tests:

```rust
#[test]
fn upstream_http_url_conversion_maps_base_urls_to_app_routes() {
    let app_id =
        AppId::from_string("00000000-0000-0000-0000-000000000001").expect("parse app id");

    assert_eq!(
        upstream_http_url("https://core.example.com", app_id).expect("https conversion"),
        "https://core.example.com/"
    );
    assert_eq!(
        upstream_http_url("http://core.example.com/base/", app_id).expect("http conversion"),
        "http://core.example.com/base/"
    );
    assert_eq!(
        upstream_http_url("ws://core.example.com", app_id).expect("ws conversion"),
        "http://core.example.com/"
    );
    assert_eq!(
        upstream_http_url(
            "wss://core.example.com/apps/00000000-0000-0000-0000-000000000001/ws",
            app_id,
        )
        .expect("wss conversion"),
        "https://core.example.com/"
    );
}

#[test]
fn upstream_http_url_conversion_rejects_query_and_fragment_urls() {
    let app_id =
        AppId::from_string("00000000-0000-0000-0000-000000000001").expect("parse app id");

    assert!(upstream_http_url("https://core.example.com?token=abc", app_id).is_err());
    assert!(upstream_http_url("https://core.example.com#cluster-a", app_id).is_err());
}

#[tokio::test]
async fn builder_requires_admin_secret_in_edge_mode() {
    let auth_config = AuthConfig {
        peer_secret: Some("cluster-peer-secret".to_string()),
        allow_local_first_auth: true,
        ..Default::default()
    };

    let result = ServerBuilder::new(AppId::from_name("test-app"))
        .with_auth_config(auth_config)
        .with_storage(StorageBackend::InMemory)
        .with_upstream_url("ws://127.0.0.1:9")
        .build()
        .await;
    let error = result
        .err()
        .expect("edge mode without admin secret should fail");

    assert!(error.contains("--admin-secret"));
    assert!(error.contains("--upstream-url"));
}
```

- [ ] **Step 2: Run builder tests and verify red**

Run:

```bash
cargo test -p jazz-tools upstream_http_url_conversion_maps_base_urls_to_app_routes
cargo test -p jazz-tools upstream_http_url_conversion_rejects_query_and_fragment_urls
cargo test -p jazz-tools builder_requires_admin_secret_in_edge_mode
```

Expected:

- The first two commands fail because `upstream_http_url` is not defined.
- The third command fails because edge mode currently requires `peer_secret`, but not `admin_secret`.

### Task 2: Derive Catalogue Upstream from `upstreamUrl`

**Files:**

- Modify: `crates/jazz-tools/src/server/mod.rs`
- Modify: `crates/jazz-tools/src/server/builder.rs`
- Modify: `crates/jazz-tools/src/commands/server.rs`

- [ ] **Step 1: Remove `CatalogueAuthorityMode` from server state**

In `crates/jazz-tools/src/server/mod.rs`, delete `CatalogueAuthorityMode` and replace the `ServerState` catalogue field with an upstream HTTP URL:

```rust
/// Upstream HTTP base URL used by edge servers to forward catalogue HTTP requests.
pub upstream_http_url: Option<String>,
/// Whether this process is the core/global node or an edge syncing upstream.
pub topology: ServerTopology,
```

- [ ] **Step 2: Simplify `ServerBuilder` fields and construction**

In `crates/jazz-tools/src/server/builder.rs`, remove the import of `CatalogueAuthorityMode`, remove the `catalogue_authority` field, remove the default initialization, and delete `with_catalogue_authority`.

In `build`, compute both upstream URLs from the same source:

```rust
let topology = if self.upstream_url.is_some() {
    ServerTopology::Edge
} else {
    ServerTopology::Core
};
let upstream_ws_url = match self.upstream_url.as_deref() {
    Some(upstream_url) => Some(upstream_ws_url(upstream_url, self.app_id)?),
    None => None,
};
let upstream_http_url = match self.upstream_url.as_deref() {
    Some(upstream_url) => Some(upstream_http_url(upstream_url, self.app_id)?),
    None => None,
};
validate_server_config(&auth_config, topology)?;
let jwt_verifier = build_jwt_verifier(&auth_config).await?;
log_auth_config(&auth_config, topology);
```

In the `ServerState` construction, set:

```rust
upstream_http_url,
topology,
```

- [ ] **Step 3: Require admin secret in edge mode**

Replace `validate_server_config` with:

```rust
fn validate_server_config(auth_config: &AuthConfig, topology: ServerTopology) -> Result<(), String> {
    if !topology.is_edge() {
        return Ok(());
    }

    if auth_config.peer_secret.is_none() {
        return Err("edge mode requires --peer-secret / JAZZ_PEER_SECRET when --upstream-url / JAZZ_UPSTREAM_URL is set".to_string());
    }

    if auth_config.admin_secret.is_none() {
        return Err("edge mode requires --admin-secret / JAZZ_ADMIN_SECRET when --upstream-url / JAZZ_UPSTREAM_URL is set".to_string());
    }

    Ok(())
}
```

Replace `log_auth_config` with the same log fields except `catalogue_authority`:

```rust
fn log_auth_config(auth_config: &AuthConfig, topology: ServerTopology) {
    info!(
        "Auth configured: local_first={}, jwks={}, static_jwt_key={}, cookie={}, backend={}, admin={}, peer={}, topology={:?}",
        auth_config.allow_local_first_auth,
        auth_config.jwks_url.is_some(),
        auth_config.jwt_public_key.is_some(),
        auth_config.auth_cookie_name.is_some(),
        auth_config.backend_secret.is_some(),
        auth_config.admin_secret.is_some(),
        auth_config.peer_secret.is_some(),
        topology
    );
}
```

- [ ] **Step 4: Add `upstream_http_url` helper**

Add this function near `upstream_ws_url`:

```rust
pub fn upstream_http_url(base_url: &str, app_id: AppId) -> Result<String, String> {
    let mut url = reqwest::Url::parse(base_url)
        .map_err(|err| format!("invalid upstream URL '{base_url}': {err}"))?;

    if url.query().is_some() || url.fragment().is_some() {
        return Err("upstream URL must not include query parameters or a fragment".to_string());
    }

    let scheme = match url.scheme() {
        "http" => "http",
        "https" => "https",
        "ws" => "http",
        "wss" => "https",
        other => {
            return Err(format!(
                "unsupported upstream URL scheme '{other}'; expected http, https, ws, or wss"
            ));
        }
    };
    url.set_scheme(scheme)
        .map_err(|_| format!("failed to set upstream URL scheme to {scheme}"))?;

    let app_ws_path = format!("/apps/{app_id}/ws");
    let normalized_path = url.path().trim_end_matches('/');
    if normalized_path == app_ws_path.trim_end_matches('/') {
        url.set_path("");
    }

    Ok(url.to_string())
}
```

- [ ] **Step 5: Remove catalogue authority from command runner**

In `crates/jazz-tools/src/commands/server.rs`, remove `CatalogueAuthorityMode` from the import, remove the `catalogue_authority` parameter from `run`, and build the server with:

```rust
let builder = ServerBuilder::new(app_id).with_auth_config(auth_config);
let builder = match upstream_url {
    Some(upstream_url) => builder.with_upstream_url(upstream_url),
    None => builder,
};
```

- [ ] **Step 6: Update existing edge builder test**

In `builder_uses_edge_tier_with_upstream`, add `admin_secret` to the existing `AuthConfig`:

```rust
.with_auth_config(AuthConfig {
    admin_secret: Some("admin-secret".to_string()),
    peer_secret: Some("cluster-secret".to_string()),
    ..Default::default()
})
```

- [ ] **Step 7: Run builder tests and verify green**

Run:

```bash
cargo test -p jazz-tools upstream_http_url_conversion_maps_base_urls_to_app_routes
cargo test -p jazz-tools upstream_http_url_conversion_rejects_query_and_fragment_urls
cargo test -p jazz-tools builder_requires_admin_secret_in_edge_mode
cargo test -p jazz-tools builder_uses_edge_tier_with_upstream
```

Expected: all pass.

### Task 3: Convert Route Tests to Upstream-Based Forwarding

**Files:**

- Modify: `crates/jazz-tools/src/server/routes/mod.rs`

- [ ] **Step 1: Update route test imports and helper**

Remove `CatalogueAuthorityMode` from the test import:

```rust
use crate::server::{ServerBuilder, ServerState, StorageBackend};
```

Replace `make_state_with_schema_and_authority` with:

```rust
async fn make_edge_state_with_schema_and_upstream(
    schema: crate::query_manager::types::Schema,
    upstream_url: String,
) -> Arc<ServerState> {
    let auth_config = AuthConfig {
        admin_secret: Some("admin-secret".to_string()),
        peer_secret: Some("cluster-peer-secret".to_string()),
        allow_local_first_auth: true,
        ..Default::default()
    };

    ServerBuilder::new(AppId::from_name("test-app"))
        .with_auth_config(auth_config)
        .with_upstream_url(upstream_url)
        .with_storage(StorageBackend::InMemory)
        .with_schema(schema)
        .build()
        .await
        .expect("build edge state with upstream")
        .state
}
```

- [ ] **Step 2: Rewrite the forwarding test setup**

Rename `catalogue_authority_forwarding_proxies_schema_and_permissions_requests` to:

```rust
async fn edge_upstream_forwarding_proxies_schema_and_permissions_requests()
```

Replace the state construction in that test with:

```rust
let state = make_edge_state_with_schema_and_upstream(
    schema.clone(),
    format!("http://{authority_addr}/authority-prefix"),
)
.await;
```

Change the forwarded admin-secret assertion to expect the caller's admin secret:

```rust
assert!(
    forwarded
        .iter()
        .all(|request| request.admin_secret.as_deref() == Some("admin-secret")),
    "forwarded requests should reuse the validated caller admin secret: {forwarded:?}"
);
```

- [ ] **Step 3: Add local rejection test for invalid edge admin secret**

Add this route test near the forwarding test:

```rust
#[tokio::test]
async fn edge_catalogue_forwarding_rejects_invalid_admin_secret_before_upstream() {
    use std::sync::{Arc, Mutex};

    let forwarded = Arc::new(Mutex::new(Vec::<ForwardedAdminRequest>::new()));
    let forwarded_for_router = forwarded.clone();
    let authority_routes = axum::Router::new().route(
        &test_app_route("/schemas"),
        get(move |headers: HeaderMap| {
            let forwarded = forwarded_for_router.clone();
            async move {
                forwarded.lock().unwrap().push(ForwardedAdminRequest {
                    method: "GET".to_string(),
                    path: test_app_route("/schemas"),
                    admin_secret: headers
                        .get("X-Jazz-Admin-Secret")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string),
                    body: None,
                });
                Json(serde_json::json!({ "hashes": [] }))
            }
        }),
    );
    let authority_app = axum::Router::new().nest("/authority-prefix", authority_routes);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind authority listener");
    let authority_addr = listener.local_addr().expect("authority local addr");
    let authority_task = tokio::spawn(async move {
        axum::serve(listener, authority_app)
            .await
            .expect("serve authority app");
    });

    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();
    let state = make_edge_state_with_schema_and_upstream(
        schema,
        format!("http://{authority_addr}/authority-prefix"),
    )
    .await;
    let app = make_test_router(state);

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri(test_app_route("/schemas"))
                .header("X-Jazz-Admin-Secret", "wrong-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(forwarded.lock().unwrap().is_empty());
    authority_task.abort();
}
```

- [ ] **Step 4: Replace edge publish rejection test expectation**

Rename `edge_mode_rejects_local_admin_catalogue_publishing` to:

```rust
async fn edge_mode_forwards_admin_catalogue_publishing()
```

Rewrite it to create an upstream route for `POST /admin/schemas`, build edge state with `make_edge_state_with_schema_and_upstream`, send the same publish request, assert `StatusCode::CREATED`, and assert exactly one forwarded request:

```rust
assert_eq!(response.status(), StatusCode::CREATED);
let forwarded = forwarded.lock().unwrap().clone();
assert_eq!(forwarded.len(), 1);
assert_eq!(forwarded[0].method, "POST");
assert_eq!(forwarded[0].path, test_app_route("/admin/schemas"));
assert_eq!(forwarded[0].admin_secret.as_deref(), Some("admin-secret"));
```

- [ ] **Step 5: Run route tests and verify red**

Run:

```bash
cargo test -p jazz-tools edge_upstream_forwarding_proxies_schema_and_permissions_requests
cargo test -p jazz-tools edge_catalogue_forwarding_rejects_invalid_admin_secret_before_upstream
cargo test -p jazz-tools edge_mode_forwards_admin_catalogue_publishing
```

Expected:

- Forwarding tests fail because route handlers still depend on `CatalogueAuthorityMode`.
- The publish test fails because edge mode still rejects local catalogue publishing before forwarding.

### Task 4: Forward Catalogue Routes from Edge Topology

**Files:**

- Modify: `crates/jazz-tools/src/server/routes/http.rs`

- [ ] **Step 1: Remove catalogue authority import**

Change:

```rust
use crate::server::{CatalogueAuthorityMode, ServerState};
```

to:

```rust
use crate::server::ServerState;
```

- [ ] **Step 2: Update forwarding helper**

Replace `forward_catalogue_request` with:

```rust
async fn forward_catalogue_request(
    state: &Arc<ServerState>,
    method: reqwest::Method,
    path: &str,
    body: Option<Vec<u8>>,
    admin_secret: &str,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let base_url = state.upstream_http_url.as_deref().ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(
                "catalogue forwarding requested without a configured upstream".to_string(),
            )),
        )
    })?;

    let app_scoped_path = format!("/apps/{}/{}", state.app_id, path.trim_start_matches('/'));
    let authority_url = authority_endpoint_url(base_url, &app_scoped_path).map_err(|message| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(message)),
        )
    })?;

    let mut request = state
        .http_client
        .request(method, authority_url)
        .header("X-Jazz-Admin-Secret", admin_secret);
    if let Some(body) = body {
        request = request.header(CONTENT_TYPE, "application/json").body(body);
    }

    let response = request.send().await.map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::internal(format!(
                "failed to reach catalogue upstream: {err}"
            ))),
        )
    })?;

    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    let bytes = response.bytes().await.map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::internal(format!(
                "failed to read upstream catalogue response: {err}"
            ))),
        )
    })?;

    let mut response_builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        response_builder = response_builder.header(CONTENT_TYPE, content_type);
    }

    response_builder
        .body(axum::body::Body::from(bytes))
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to build forwarded response: {err}"
                ))),
            )
        })
}
```

- [ ] **Step 3: Delete publish rejection helper**

Delete `reject_edge_catalogue_publish`.

- [ ] **Step 4: Forward reads and writes when `state.topology.is_edge()`**

In each catalogue handler, after successful `validate_admin_secret`, replace the `CatalogueAuthorityMode::Forward` check with:

```rust
if state.topology.is_edge() {
    return match forward_catalogue_request(
        &state,
        reqwest::Method::GET,
        "/schemas",
        None,
        admin_secret.expect("validated admin secret"),
    )
    .await
    {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
}
```

Use the same pattern for each route, changing method/path/body:

- `schema_handler`: `GET`, `format!("/schema/{hash_text}")`, `None`
- `schema_hashes_handler`: `GET`, `"/schemas"`, `None`
- `schema_connectivity_handler`: `GET`, `format!("/admin/schema-connectivity?fromHash={}&toHash={}", params.from_hash, params.to_hash)`, `None`
- `publish_schema_handler`: `POST`, `"/admin/schemas"`, serialized request body
- `permissions_head_handler`: `GET`, `"/admin/permissions/head"`, `None`
- `permissions_handler`: `GET`, `"/admin/permissions"`, `None`
- `publish_permissions_handler`: `POST`, `"/admin/permissions"`, serialized request body
- `publish_migration_handler`: `POST`, `"/admin/migrations"`, serialized request body

Remove the early `reject_edge_catalogue_publish` calls from publish handlers so edge writes can forward.

- [ ] **Step 5: Run route tests and verify green**

Run:

```bash
cargo test -p jazz-tools edge_upstream_forwarding_proxies_schema_and_permissions_requests
cargo test -p jazz-tools edge_catalogue_forwarding_rejects_invalid_admin_secret_before_upstream
cargo test -p jazz-tools edge_mode_forwards_admin_catalogue_publishing
```

Expected: all pass.

### Task 5: Remove Public `catalogueAuthority*` Configuration

**Files:**

- Modify: `crates/jazz-tools/src/main.rs`
- Modify: `crates/jazz-napi/src/lib.rs`
- Modify: `crates/jazz-napi/index.d.ts`
- Modify: `packages/jazz-tools/src/dev/dev-server.ts`
- Modify: `packages/jazz-tools/src/dev/managed-runtime.ts`
- Modify: `packages/jazz-tools/src/dev/vite.ts`
- Modify: `packages/jazz-tools/src/dev/dev-server.test.ts`

- [ ] **Step 1: Add failing CLI validation test**

In `crates/jazz-tools/src/main.rs`, add this test beside `server_cli_validation_requires_peer_secret_in_edge_mode`:

```rust
#[test]
fn server_cli_validation_requires_admin_secret_in_edge_mode() {
    let cli = Cli::try_parse_from([
        "jazz-tools",
        "server",
        "00000000-0000-0000-0000-000000000001",
        "--upstream-url",
        "https://core.example.com",
        "--peer-secret",
        "cluster-secret",
    ])
    .expect("server command should parse");

    let error = validate_server_cli_options(&cli.command)
        .expect_err("edge mode without admin secret should fail validation");

    assert!(error.contains("--admin-secret"));
    assert!(error.contains("--upstream-url"));
}
```

Run:

```bash
cargo test -p jazz-tools server_cli_validation_requires_admin_secret_in_edge_mode
```

Expected: fail because CLI validation does not require `admin_secret` yet.

- [ ] **Step 2: Remove CLI catalogue authority options**

In `crates/jazz-tools/src/main.rs`:

- Delete `CatalogueAuthorityArg`.
- Remove `use jazz_tools::server::CatalogueAuthorityMode;`.
- Remove the `catalogue_authority`, `catalogue_authority_url`, and `catalogue_authority_admin_secret` fields from `Commands::Server`.
- Remove those three names from the `Commands::Server` match.
- Delete the `let catalogue_authority = match catalogue_authority { ... }` block.
- Call `commands::server::run` without a catalogue authority argument.
- Update `validate_server_cli_options` to destructure `admin_secret` and require it when `upstream_url.is_some()`.

Use this validation body:

```rust
if upstream_url.is_some() && peer_secret.is_none() {
    return Err("--peer-secret / JAZZ_PEER_SECRET is required when --upstream-url / JAZZ_UPSTREAM_URL is set".to_string());
}

if upstream_url.is_some() && admin_secret.is_none() {
    return Err("--admin-secret / JAZZ_ADMIN_SECRET is required when --upstream-url / JAZZ_UPSTREAM_URL is set".to_string());
}
```

- [ ] **Step 3: Remove NAPI catalogue authority parsing**

In `crates/jazz-napi/src/lib.rs`:

- Remove `CatalogueAuthorityMode` from the `use jazz_tools::server::{...}` list.
- Remove these fields from `DevServerStartOptions`:

```rust
catalogue_authority: Option<String>,
catalogue_authority_url: Option<String>,
catalogue_authority_admin_secret: Option<String>,
```

- Remove `catalogueAuthority`, `catalogueAuthorityUrl`, and `catalogueAuthorityAdminSecret` from the `ts_arg_type` string.
- Delete the `let catalogue_authority = match opts.catalogue_authority.as_deref() { ... };` block.
- Build with:

```rust
let mut server_builder = ServerBuilder::new(app_id).with_auth_config(auth_config);
```

- [ ] **Step 4: Update generated NAPI declaration**

In `crates/jazz-napi/index.d.ts`, remove the catalogue authority options from the `DevServer.start` options type so it contains:

```ts
{ appId: string; port?: number; dataDir?: string; inMemory?: boolean; jwksUrl?: string; allowLocalFirstAuth?: boolean; backendSecret?: string; adminSecret?: string; upstreamUrl?: string; peerSecret?: string; telemetryCollectorUrl?: string }
```

- [ ] **Step 5: Remove TypeScript dev server option pass-through**

In `packages/jazz-tools/src/dev/dev-server.ts`, remove these fields from `StartLocalJazzServerOptions` and from the object passed to `DevServer.start`:

```ts
catalogueAuthority?: "local" | "forward";
catalogueAuthorityUrl?: string;
catalogueAuthorityAdminSecret?: string;
```

In `packages/jazz-tools/src/dev/managed-runtime.ts`, remove these properties from the `startLocalJazzServer` call:

```ts
catalogueAuthority: serverConfig.catalogueAuthority,
catalogueAuthorityUrl: serverConfig.catalogueAuthorityUrl,
catalogueAuthorityAdminSecret: serverConfig.catalogueAuthorityAdminSecret,
```

In `packages/jazz-tools/src/dev/vite.ts`, remove these fields from `JazzServerOptions`:

```ts
catalogueAuthority?: "local" | "forward";
catalogueAuthorityUrl?: string;
catalogueAuthorityAdminSecret?: string;
```

- [ ] **Step 6: Update dev server edge test**

In `packages/jazz-tools/src/dev/dev-server.test.ts`, update `"passes edge upstream and peer secret options through DevServer"` to include admin secret:

```ts
handle = await startLocalJazzServer({
  port,
  upstreamUrl: "ws://127.0.0.1:9",
  peerSecret: "cluster-peer-secret",
  adminSecret: "admin-secret",
  inMemory: true,
});
```

- [ ] **Step 7: Run targeted config tests**

Run:

```bash
cargo test -p jazz-tools server_cli_validation_requires_admin_secret_in_edge_mode
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/dev-server.test.ts
```

Expected: both pass.

### Task 6: Update Status-Quo Specs

**Files:**

- Modify: `specs/status-quo/schema_manager.md`
- Modify: `specs/status-quo/http_transport.md`
- Modify: `specs/status-quo/sync_manager.md`

- [ ] **Step 1: Update schema manager wording**

In `specs/status-quo/schema_manager.md`, replace the edge/core catalogue paragraph with:

```markdown
In an edge/core deployment, catalogue authority is core-only. Schemas,
permissions, and migrations are published to the core server. Edge servers proxy
catalogue HTTP reads and writes to their configured upstream after validating the
admin secret locally. Edges also learn catalogue entries through server-to-server
sync and install them locally once they arrive, so runtime/query paths can use
local catalogue state.
```

- [ ] **Step 2: Update HTTP transport wording**

In `specs/status-quo/http_transport.md`, replace the catalogue admin paragraph with:

```markdown
Catalogue admin writes remain core-authoritative. Edge servers validate the
admin secret locally and proxy catalogue HTTP reads and writes to the configured
upstream core. They also learn schemas, permissions, and migrations through the
sync channel from core for local runtime/query use.
```

- [ ] **Step 3: Update sync manager wording**

In `specs/status-quo/sync_manager.md`, replace the catalogue authority lines with:

```markdown
Catalogue updates use the same sync payload lane, but publication authority is
core-only in edge deployments. Edges receive schema, migration, and permissions
catalogue entries from core; catalogue HTTP reads and writes received by an edge
are validated locally and proxied to the configured upstream core.
```

- [ ] **Step 4: Verify no stale status-quo rejection wording remains**

Run:

```bash
rg -n "reject local admin catalogue|reject writes|publish catalogue to the core|catalogueAuthority|catalogue_authority|CatalogueAuthority" specs/status-quo crates/jazz-tools/src crates/jazz-napi/src packages/jazz-tools/src crates/jazz-napi/index.d.ts
```

Expected: no matches for removed code symbols or stale rejection wording.

### Task 7: Full Verification and Commit

**Files:**

- Verify all files changed by Tasks 1-6.

- [ ] **Step 1: Format**

Run:

```bash
pnpm format
```

Expected: exits 0.

- [ ] **Step 2: Rust targeted tests**

Run:

```bash
cargo test -p jazz-tools upstream_http_url_conversion_maps_base_urls_to_app_routes
cargo test -p jazz-tools upstream_http_url_conversion_rejects_query_and_fragment_urls
cargo test -p jazz-tools builder_requires_admin_secret_in_edge_mode
cargo test -p jazz-tools builder_uses_edge_tier_with_upstream
cargo test -p jazz-tools server_cli_validation_requires_admin_secret_in_edge_mode
cargo test -p jazz-tools edge_upstream_forwarding_proxies_schema_and_permissions_requests
cargo test -p jazz-tools edge_catalogue_forwarding_rejects_invalid_admin_secret_before_upstream
cargo test -p jazz-tools edge_mode_forwards_admin_catalogue_publishing
```

Expected: all commands exit 0.

- [ ] **Step 3: TypeScript targeted test**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/dev-server.test.ts
```

Expected: exits 0.

- [ ] **Step 4: Build the affected public surfaces**

Run:

```bash
pnpm build
```

Expected: exits 0.

- [ ] **Step 5: Final stale-symbol scan**

Run:

```bash
rg -n "catalogueAuthority|catalogue_authority|CatalogueAuthority|JAZZ_CATALOGUE_AUTHORITY|catalogue-authority" crates/jazz-tools/src crates/jazz-napi/src crates/jazz-napi/index.d.ts packages/jazz-tools/src specs/status-quo
```

Expected: no matches.

- [ ] **Step 6: Review diff**

Run:

```bash
git diff --stat
git diff -- crates/jazz-tools/src/server/mod.rs crates/jazz-tools/src/server/builder.rs crates/jazz-tools/src/server/routes/http.rs crates/jazz-tools/src/server/routes/mod.rs crates/jazz-tools/src/main.rs crates/jazz-tools/src/commands/server.rs crates/jazz-napi/src/lib.rs crates/jazz-napi/index.d.ts packages/jazz-tools/src/dev/dev-server.ts packages/jazz-tools/src/dev/managed-runtime.ts packages/jazz-tools/src/dev/vite.ts packages/jazz-tools/src/dev/dev-server.test.ts specs/status-quo/schema_manager.md specs/status-quo/http_transport.md specs/status-quo/sync_manager.md
```

Expected: diff only implements upstream-derived catalogue forwarding and removes `catalogueAuthority*`.

- [ ] **Step 7: Commit**

Run:

```bash
git add crates/jazz-tools/src/server/mod.rs crates/jazz-tools/src/server/builder.rs crates/jazz-tools/src/server/routes/http.rs crates/jazz-tools/src/server/routes/mod.rs crates/jazz-tools/src/main.rs crates/jazz-tools/src/commands/server.rs crates/jazz-napi/src/lib.rs crates/jazz-napi/index.d.ts packages/jazz-tools/src/dev/dev-server.ts packages/jazz-tools/src/dev/managed-runtime.ts packages/jazz-tools/src/dev/vite.ts packages/jazz-tools/src/dev/dev-server.test.ts specs/status-quo/schema_manager.md specs/status-quo/http_transport.md specs/status-quo/sync_manager.md
git commit -m "refactor: derive catalogue forwarding from upstream"
```

Expected: commit succeeds. Do not stage unrelated existing work such as `examples/todo-client-localfirst-react/src/App.tsx` or `.codex/`.
