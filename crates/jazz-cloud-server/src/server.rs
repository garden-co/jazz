use std::collections::{HashMap, VecDeque, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    Router,
    extract::{Path as AxumPath, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
    },
    response::{Html, IntoResponse, Json},
    routing::{get, patch, post},
};
use base64::Engine;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use jazz_tools::jazz_transport::{
    ConnectionId, ErrorResponse, ServerEvent, SyncBatchRequest, SyncBatchResponse,
    SyncPayloadResult,
};
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::query::QueryBuilder;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{
    ColumnType, RowDescriptor, Schema, SchemaBuilder, SchemaHash, TableName, TablePolicies,
    TableSchema, Value,
};
use jazz_tools::runtime_core::ReadDurabilityOptions;
use jazz_tools::runtime_tokio::TokioRuntime;
use jazz_tools::schema_manager::manager::PermissionsHeadSummary;
use jazz_tools::schema_manager::{AppId, SchemaManager, rehydrate_schema_manager_from_manifest};
use jazz_tools::storage::RocksDBStorage;
use jazz_tools::sync_manager::{
    ClientId, Destination, DurabilityTier, InboxEntry, Source, SyncManager, SyncPayload,
};
use jsonwebtoken::jwk::{Jwk, JwkSet, KeyAlgorithm};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;
const DEFAULT_JWKS_CACHE_TTL_SECS: u64 = 300;
/// Minimum interval between forced JWKS refreshes. Prevents unauthenticated
/// callers from triggering unbounded outbound fetches by sending JWTs with
/// fabricated key IDs.
const JWKS_FORCED_REFRESH_COOLDOWN: Duration = Duration::from_secs(10);
/// Maximum time a stale keyset is served after TTL expiry.
const DEFAULT_JWKS_MAX_STALE_SECS: u64 = 300;
const WORKER_SYNC_QUEUE_CAPACITY: usize = 4096;
const WORKER_APP_QUANTUM: usize = 1;
const LOCAL_MODE_HEADER: &str = "X-Jazz-Local-Mode";
const LOCAL_TOKEN_HEADER: &str = "X-Jazz-Local-Token";
const MANAGEMENT_USERNAME: &str = "admin";
const MANAGEMENT_BASIC_AUTH_REALM: &str = "jazz-cloud-server-management";
const MANAGEMENT_PAGE_HTML: &str = r##"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Jazz Cloud Server Management</title>
    <style>
      :root {
        color-scheme: light;
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif;
      }
      body {
        margin: 0;
        padding: 1.5rem;
        background: #f6f8fa;
        color: #18202a;
      }
      main {
        max-width: 1000px;
        margin: 0 auto;
      }
      h1 {
        margin-top: 0;
      }
      .card {
        background: #fff;
        border: 1px solid #d0d7de;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 1rem;
      }
      form {
        display: grid;
        gap: 0.75rem;
      }
      .row {
        display: grid;
        gap: 0.75rem;
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      }
      label {
        display: grid;
        gap: 0.25rem;
        font-size: 0.9rem;
      }
      input[type="text"] {
        border: 1px solid #c6ccd2;
        border-radius: 6px;
        padding: 0.5rem;
        font-size: 0.95rem;
      }
      .checkboxes {
        display: flex;
        flex-wrap: wrap;
        gap: 0.75rem;
      }
      .checkboxes label {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
      }
      button {
        border: 1px solid #1f6feb;
        background: #1f6feb;
        color: #fff;
        border-radius: 6px;
        padding: 0.5rem 0.8rem;
        cursor: pointer;
      }
      button.secondary {
        border-color: #8c959f;
        background: #fff;
        color: #18202a;
      }
      button:disabled {
        opacity: 0.6;
        cursor: not-allowed;
      }
      table {
        width: 100%;
        border-collapse: collapse;
      }
      th, td {
        text-align: left;
        font-size: 0.9rem;
        border-bottom: 1px solid #e5e9ed;
        padding: 0.5rem 0.35rem;
        vertical-align: top;
      }
      th {
        font-weight: 600;
      }
      code, pre {
        background: #f6f8fa;
        border-radius: 6px;
      }
      code {
        padding: 0.1rem 0.25rem;
      }
      pre {
        border: 1px solid #d0d7de;
        padding: 0.75rem;
        overflow: auto;
      }
      #status {
        min-height: 1.25rem;
        font-size: 0.9rem;
      }
      .error {
        color: #b42318;
      }
      .muted {
        color: #57606a;
        font-size: 0.82rem;
      }
      .auth-editor {
        display: grid;
        gap: 0.45rem;
        margin-top: 0.45rem;
      }
      .auth-editor input[type="text"] {
        font-size: 0.85rem;
        padding: 0.35rem;
      }
      .auth-editor .checkboxes {
        gap: 0.5rem;
      }
      .auth-editor .checkboxes label {
        font-size: 0.82rem;
      }
      .secret-controls {
        display: flex;
        flex-wrap: wrap;
        gap: 0.35rem;
        margin-top: 0.45rem;
      }
    </style>
  </head>
  <body>
    <main>
      <h1>Jazz Cloud Server Management</h1>
      <p>Authenticated as basic user <code>admin</code>. Use this page for app provisioning, auth-mode edits, and admin secret management.</p>

      <section class="card">
        <h2>Create App</h2>
        <form id="create-form">
          <div class="row">
            <label>
              App name
              <input type="text" id="app-name" required />
            </label>
            <label>
              JWKS endpoint (optional)
              <input type="text" id="jwks-endpoint" placeholder="https://idp.example.com/.well-known/jwks.json" />
            </label>
          </div>
          <div class="row">
            <label>
              Backend secret (optional)
              <input type="text" id="backend-secret" placeholder="auto-generated if empty" />
            </label>
            <label>
              Admin secret (optional)
              <input type="text" id="admin-secret" placeholder="auto-generated if empty" />
            </label>
          </div>
          <div class="row">
            <label>
              JWKS cache TTL (seconds)
              <input type="number" id="jwks-cache-ttl-secs" min="0" step="1" placeholder="300" />
            </label>
            <label>
              JWKS max stale (seconds)
              <input type="number" id="jwks-max-stale-secs" min="0" step="1" placeholder="300" />
            </label>
          </div>
          <div class="checkboxes">
            <label><input type="checkbox" id="allow-anonymous" checked /> Allow anonymous local auth</label>
            <label><input type="checkbox" id="allow-demo" checked /> Allow demo local auth</label>
          </div>
          <div>
            <button type="submit">Create app</button>
          </div>
        </form>
        <p id="status"></p>
        <pre id="create-result" hidden></pre>
      </section>

      <section class="card">
        <h2>Apps</h2>
        <table>
          <thead>
            <tr>
              <th>App</th>
              <th>Status</th>
              <th>Auth config</th>
              <th>Admin secret</th>
              <th>Worker</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody id="apps-body"></tbody>
        </table>
      </section>
    </main>

    <script>
      const statusEl = document.getElementById("status");
      const appsBodyEl = document.getElementById("apps-body");
      const resultEl = document.getElementById("create-result");
      const revealedAdminSecrets = new Map();

      async function api(path, options = {}) {
        const config = { ...options };
        config.headers = { ...(options.headers || {}) };
        if (config.body && !config.headers["Content-Type"]) {
          config.headers["Content-Type"] = "application/json";
        }
        const response = await fetch(path, config);
        const text = await response.text();
        let payload = null;
        if (text.length > 0) {
          try {
            payload = JSON.parse(text);
          } catch (_) {
            payload = text;
          }
        }
        if (!response.ok) {
          const message =
            (payload && payload.error && payload.error.message) ||
            (payload && payload.message) ||
            (typeof payload === "string" ? payload : "") ||
            `Request failed (${response.status})`;
          throw new Error(message);
        }
        return payload;
      }

      function setStatus(message, isError = false) {
        statusEl.textContent = message || "";
        statusEl.className = isError ? "error" : "";
      }

      function setCreateResult(value) {
        if (!value) {
          resultEl.hidden = true;
          resultEl.textContent = "";
          return;
        }
        resultEl.hidden = false;
        resultEl.textContent = JSON.stringify(value, null, 2);
      }

      function maskSecret(secret) {
        return "*".repeat(Math.max(8, Math.min(secret.length, 24)));
      }

      function readOptionalSecondsValue(input, label) {
        const raw = input.value.trim();
        if (!raw) {
          return null;
        }

        const parsed = Number.parseInt(raw, 10);
        if (!Number.isInteger(parsed) || parsed < 0) {
          throw new Error(`${label} must be a non-negative integer.`);
        }

        return parsed;
      }

      async function loadApps() {
        const apps = await api("/manage/api/apps");
        appsBodyEl.textContent = "";
        if (!Array.isArray(apps) || apps.length === 0) {
          const emptyRow = document.createElement("tr");
          const emptyCell = document.createElement("td");
          emptyCell.colSpan = 6;
          emptyCell.textContent = "No apps created yet.";
          emptyRow.appendChild(emptyCell);
          appsBodyEl.appendChild(emptyRow);
          return;
        }

        for (const app of apps) {
          const tr = document.createElement("tr");

          const appCell = document.createElement("td");
          const name = document.createElement("div");
          name.textContent = app.app_name || "(unnamed)";
          const id = document.createElement("code");
          id.textContent = app.app_id;
          appCell.appendChild(name);
          appCell.appendChild(id);

          const statusCell = document.createElement("td");
          statusCell.textContent = app.status;

          const authCell = document.createElement("td");
          const authSummary = document.createElement("div");
          authSummary.className = "muted";
          authSummary.textContent = `${app.allow_anonymous ? "anonymous:on" : "anonymous:off"}, ${app.allow_demo ? "demo:on" : "demo:off"}, ttl:${app.jwks_cache_ttl_secs}s, max-stale:${app.jwks_max_stale_secs}s`;
          authCell.appendChild(authSummary);

          const authEditor = document.createElement("div");
          authEditor.className = "auth-editor";

          const flags = document.createElement("div");
          flags.className = "checkboxes";

          const anonymousLabel = document.createElement("label");
          const anonymousCheckbox = document.createElement("input");
          anonymousCheckbox.type = "checkbox";
          anonymousCheckbox.checked = Boolean(app.allow_anonymous);
          anonymousLabel.appendChild(anonymousCheckbox);
          anonymousLabel.appendChild(document.createTextNode("Allow anonymous"));

          const demoLabel = document.createElement("label");
          const demoCheckbox = document.createElement("input");
          demoCheckbox.type = "checkbox";
          demoCheckbox.checked = Boolean(app.allow_demo);
          demoLabel.appendChild(demoCheckbox);
          demoLabel.appendChild(document.createTextNode("Allow demo"));

          flags.appendChild(anonymousLabel);
          flags.appendChild(demoLabel);

          const jwksInput = document.createElement("input");
          jwksInput.type = "text";
          jwksInput.placeholder = "JWKS endpoint (blank disables JWT auth)";
          jwksInput.value = app.jwks_endpoint || "";

          const jwksCacheTtlInput = document.createElement("input");
          jwksCacheTtlInput.type = "number";
          jwksCacheTtlInput.min = "0";
          jwksCacheTtlInput.step = "1";
          jwksCacheTtlInput.placeholder = "JWKS cache TTL (seconds)";
          jwksCacheTtlInput.value = String(app.jwks_cache_ttl_secs ?? 300);

          const jwksMaxStaleInput = document.createElement("input");
          jwksMaxStaleInput.type = "number";
          jwksMaxStaleInput.min = "0";
          jwksMaxStaleInput.step = "1";
          jwksMaxStaleInput.placeholder = "JWKS max stale (seconds)";
          jwksMaxStaleInput.value = String(app.jwks_max_stale_secs ?? 300);

          const saveAuthButton = document.createElement("button");
          saveAuthButton.type = "button";
          saveAuthButton.className = "secondary";
          saveAuthButton.textContent = "Save auth";
          saveAuthButton.addEventListener("click", async () => {
            saveAuthButton.disabled = true;
            try {
              const payload = {
                allow_anonymous: anonymousCheckbox.checked,
                allow_demo: demoCheckbox.checked,
                jwks_endpoint: jwksInput.value.trim(),
              };
              const jwksCacheTtlSecs = readOptionalSecondsValue(
                jwksCacheTtlInput,
                "JWKS cache TTL"
              );
              const jwksMaxStaleSecs = readOptionalSecondsValue(
                jwksMaxStaleInput,
                "JWKS max stale"
              );
              if (jwksCacheTtlSecs !== null) {
                payload.jwks_cache_ttl_secs = jwksCacheTtlSecs;
              }
              if (jwksMaxStaleSecs !== null) {
                payload.jwks_max_stale_secs = jwksMaxStaleSecs;
              }
              await api(`/manage/api/apps/${encodeURIComponent(app.app_id)}/auth`, {
                method: "PATCH",
                body: JSON.stringify(payload),
              });
              setStatus(`Saved auth config for ${app.app_id}`);
              await loadApps();
            } catch (error) {
              setStatus(error.message || String(error), true);
            } finally {
              saveAuthButton.disabled = false;
            }
          });

          authEditor.appendChild(flags);
          authEditor.appendChild(jwksInput);
          authEditor.appendChild(jwksCacheTtlInput);
          authEditor.appendChild(jwksMaxStaleInput);
          authEditor.appendChild(saveAuthButton);
          authCell.appendChild(authEditor);

          const secretCell = document.createElement("td");
          const secretDisplay = document.createElement("code");
          const knownSecretState = revealedAdminSecrets.get(app.app_id);
          if (knownSecretState && knownSecretState.value) {
            secretDisplay.textContent = knownSecretState.visible
              ? knownSecretState.value
              : maskSecret(knownSecretState.value);
          } else {
            secretDisplay.textContent = "(hidden)";
          }
          secretCell.appendChild(secretDisplay);

          const secretControls = document.createElement("div");
          secretControls.className = "secret-controls";

          const revealButton = document.createElement("button");
          revealButton.type = "button";
          revealButton.className = "secondary";
          revealButton.textContent =
            knownSecretState && knownSecretState.visible ? "Hide" : "Reveal";
          revealButton.addEventListener("click", async () => {
            const currentState = revealedAdminSecrets.get(app.app_id);
            if (currentState && currentState.value) {
              currentState.visible = !currentState.visible;
              revealedAdminSecrets.set(app.app_id, currentState);
              await loadApps();
              return;
            }

            revealButton.disabled = true;
            try {
              const revealed = await api(
                `/manage/api/apps/${encodeURIComponent(app.app_id)}/admin-secret`
              );
              if (revealed && revealed.admin_secret) {
                revealedAdminSecrets.set(app.app_id, {
                  value: revealed.admin_secret,
                  visible: true,
                });
                setStatus(`Revealed admin secret for ${app.app_id}`);
                setCreateResult(revealed);
              } else {
                setStatus(
                  `No stored admin secret for ${app.app_id}. Rotate to generate a new one.`,
                  true
                );
              }
              await loadApps();
            } catch (error) {
              setStatus(error.message || String(error), true);
            } finally {
              revealButton.disabled = false;
            }
          });

          const rotateButton = document.createElement("button");
          rotateButton.type = "button";
          rotateButton.className = "secondary";
          rotateButton.textContent = "Rotate";
          rotateButton.addEventListener("click", async () => {
            rotateButton.disabled = true;
            try {
              const rotated = await api(
                `/manage/api/apps/${encodeURIComponent(app.app_id)}/admin-secret/rotate`,
                { method: "POST" }
              );
              if (rotated && rotated.admin_secret) {
                revealedAdminSecrets.set(app.app_id, {
                  value: rotated.admin_secret,
                  visible: true,
                });
                setCreateResult(rotated);
              }
              setStatus(`Rotated admin secret for ${app.app_id}`);
              await loadApps();
            } catch (error) {
              setStatus(error.message || String(error), true);
            } finally {
              rotateButton.disabled = false;
            }
          });

          secretControls.appendChild(revealButton);
          secretControls.appendChild(rotateButton);
          secretCell.appendChild(secretControls);

          const workerCell = document.createElement("td");
          workerCell.textContent = String(app.worker);

          const actionCell = document.createElement("td");
          const toggleButton = document.createElement("button");
          const nextStatus = app.status === "active" ? "disabled" : "active";
          toggleButton.textContent = app.status === "active" ? "Disable" : "Enable";
          toggleButton.className = "secondary";
          toggleButton.addEventListener("click", async () => {
            toggleButton.disabled = true;
            try {
              await api(`/manage/api/apps/${encodeURIComponent(app.app_id)}/status`, {
                method: "POST",
                body: JSON.stringify({ status: nextStatus }),
              });
              setStatus(`Updated ${app.app_id} -> ${nextStatus}`);
              await loadApps();
            } catch (error) {
              setStatus(error.message || String(error), true);
            } finally {
              toggleButton.disabled = false;
            }
          });
          actionCell.appendChild(toggleButton);

          tr.appendChild(appCell);
          tr.appendChild(statusCell);
          tr.appendChild(authCell);
          tr.appendChild(secretCell);
          tr.appendChild(workerCell);
          tr.appendChild(actionCell);
          appsBodyEl.appendChild(tr);
        }
      }

      document.getElementById("create-form").addEventListener("submit", async (event) => {
        event.preventDefault();
        setStatus("");
        setCreateResult(null);

        const payload = {
          app_name: document.getElementById("app-name").value.trim(),
          jwks_endpoint: document.getElementById("jwks-endpoint").value.trim(),
          allow_anonymous: document.getElementById("allow-anonymous").checked,
          allow_demo: document.getElementById("allow-demo").checked,
        };

        const backendSecret = document.getElementById("backend-secret").value.trim();
        const adminSecret = document.getElementById("admin-secret").value.trim();
        if (backendSecret.length > 0) {
          payload.backend_secret = backendSecret;
        }
        if (adminSecret.length > 0) {
          payload.admin_secret = adminSecret;
        }

        try {
          const jwksCacheTtlSecs = readOptionalSecondsValue(
            document.getElementById("jwks-cache-ttl-secs"),
            "JWKS cache TTL"
          );
          const jwksMaxStaleSecs = readOptionalSecondsValue(
            document.getElementById("jwks-max-stale-secs"),
            "JWKS max stale"
          );
          if (jwksCacheTtlSecs !== null) {
            payload.jwks_cache_ttl_secs = jwksCacheTtlSecs;
          }
          if (jwksMaxStaleSecs !== null) {
            payload.jwks_max_stale_secs = jwksMaxStaleSecs;
          }
        } catch (error) {
          setStatus(error.message || String(error), true);
          return;
        }

        if (!payload.app_name) {
          setStatus("App name is required.", true);
          return;
        }

        try {
          const created = await api("/manage/api/apps", {
            method: "POST",
            body: JSON.stringify(payload),
          });
          if (created && created.app_id && created.admin_secret) {
            revealedAdminSecrets.set(created.app_id, {
              value: created.admin_secret,
              visible: true,
            });
          }
          setCreateResult(created);
          setStatus("App created.");
          event.target.reset();
          document.getElementById("allow-anonymous").checked = true;
          document.getElementById("allow-demo").checked = true;
          await loadApps();
        } catch (error) {
          setStatus(error.message || String(error), true);
        }
      });

      loadApps().catch((error) => {
        setStatus(error.message || String(error), true);
      });
    </script>
  </body>
</html>
"##;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CatalogueAuthorityMode {
    #[default]
    Local,
    Forward,
}
type ClientSyncUpdate = (ClientId, u64, SyncPayload);
type ClientSendSeqMap = Arc<Mutex<HashMap<ClientId, u64>>>;

fn parse_test_delay_ms(raw: &str) -> Option<Duration> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some((min_raw, max_raw)) = trimmed.split_once('-') {
        let min = min_raw.trim().parse::<u64>().ok()?;
        let max = max_raw.trim().parse::<u64>().ok()?;
        if min > max {
            return None;
        }
        return Some(Duration::from_millis(min + ((max - min) / 2)));
    }

    trimmed.parse::<u64>().ok().map(Duration::from_millis)
}

fn test_delay_server_send_object_updated(payload: &SyncPayload) -> Option<Duration> {
    if !matches!(payload, SyncPayload::ObjectUpdated { .. }) {
        return None;
    }

    let delay =
        parse_test_delay_ms(&std::env::var("JAZZ_TEST_DELAY_SERVER_SEND_OBJECT_UPDATED_MS").ok()?)?;
    let every_n = std::env::var("JAZZ_TEST_DELAY_SERVER_SEND_OBJECT_UPDATED_EVERY")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1);

    static SERVER_SEND_OBJECT_UPDATED_COUNT: AtomicU64 = AtomicU64::new(0);
    let seq = SERVER_SEND_OBJECT_UPDATED_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if !seq.is_multiple_of(every_n) {
        return None;
    }

    Some(delay)
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub port: u16,
    pub data_root: String,
    pub internal_api_secret: String,
    pub secret_hash_key: String,
    pub worker_threads: usize,
    pub catalogue_authority: CatalogueAuthorityMode,
}

struct WorkerPool {
    workers: Vec<WorkerHandle>,
}

struct WorkerHandle {
    ingress: Option<tokio::sync::mpsc::Sender<WorkerCommand>>,
    join: Option<std::thread::JoinHandle<()>>,
}

#[derive(Debug)]
enum WorkerDispatchError {
    QueueFull { worker: usize },
    WorkerClosed { worker: usize },
    WorkerUnavailable { worker: usize },
    RuntimeError { worker: usize, message: String },
}

enum WorkerCommand {
    CreateRuntime {
        app_id: AppId,
        data_dir: PathBuf,
        sync_broadcast: tokio::sync::broadcast::Sender<ClientSyncUpdate>,
        send_seq_by_client: ClientSendSeqMap,
        response: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    EnsureClientWithSession {
        app_id: AppId,
        client_id: ClientId,
        session: Session,
        response: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    EnsureClientAsBackend {
        app_id: AppId,
        client_id: ClientId,
        response: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    SyncAsSession {
        app_id: AppId,
        client_id: ClientId,
        session: Session,
        payload: SyncPayload,
        response: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    SyncAsBackend {
        app_id: AppId,
        client_id: ClientId,
        payload: SyncPayload,
        response: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    SyncAsAdmin {
        app_id: AppId,
        client_id: ClientId,
        payload: SyncPayload,
        response: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    GetCatalogueSchema {
        app_id: AppId,
        schema_hash: SchemaHash,
        response: tokio::sync::oneshot::Sender<Result<Option<Schema>, String>>,
    },
    PublishSchema {
        app_id: AppId,
        schema: Schema,
        response: tokio::sync::oneshot::Sender<Result<ObjectId, String>>,
    },
    PublishPermissions {
        app_id: AppId,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
        response: tokio::sync::oneshot::Sender<Result<Option<PermissionsHeadSummary>, String>>,
    },
    GetPermissionsHead {
        app_id: AppId,
        response: tokio::sync::oneshot::Sender<Result<Option<PermissionsHeadSummary>, String>>,
    },
    GetSchemaHashes {
        app_id: AppId,
        response: tokio::sync::oneshot::Sender<Result<Vec<String>, String>>,
    },
    GetCatalogueStateHash {
        app_id: AppId,
        response: tokio::sync::oneshot::Sender<Result<String, String>>,
    },
}

impl WorkerCommand {
    fn app_id(&self) -> AppId {
        match self {
            WorkerCommand::CreateRuntime { app_id, .. }
            | WorkerCommand::EnsureClientWithSession { app_id, .. }
            | WorkerCommand::EnsureClientAsBackend { app_id, .. }
            | WorkerCommand::SyncAsSession { app_id, .. }
            | WorkerCommand::SyncAsBackend { app_id, .. }
            | WorkerCommand::SyncAsAdmin { app_id, .. }
            | WorkerCommand::GetCatalogueSchema { app_id, .. }
            | WorkerCommand::PublishSchema { app_id, .. }
            | WorkerCommand::PublishPermissions { app_id, .. }
            | WorkerCommand::GetPermissionsHead { app_id, .. }
            | WorkerCommand::GetSchemaHashes { app_id, .. }
            | WorkerCommand::GetCatalogueStateHash { app_id, .. } => *app_id,
        }
    }
}

#[cfg(feature = "otel")]
fn worker_command_meta(cmd: &WorkerCommand) -> (&'static str, String) {
    match cmd {
        WorkerCommand::CreateRuntime { app_id, .. } => ("CreateRuntime", app_id.to_string()),
        WorkerCommand::EnsureClientWithSession { app_id, .. } => {
            ("EnsureClientWithSession", app_id.to_string())
        }
        WorkerCommand::EnsureClientAsBackend { app_id, .. } => {
            ("EnsureClientAsBackend", app_id.to_string())
        }
        WorkerCommand::SyncAsSession { app_id, .. } => ("SyncAsSession", app_id.to_string()),
        WorkerCommand::SyncAsBackend { app_id, .. } => ("SyncAsBackend", app_id.to_string()),
        WorkerCommand::SyncAsAdmin { app_id, .. } => ("SyncAsAdmin", app_id.to_string()),
        WorkerCommand::GetCatalogueSchema { app_id, .. } => {
            ("GetCatalogueSchema", app_id.to_string())
        }
        WorkerCommand::PublishSchema { app_id, .. } => ("PublishSchema", app_id.to_string()),
        WorkerCommand::PublishPermissions { app_id, .. } => {
            ("PublishPermissions", app_id.to_string())
        }
        WorkerCommand::GetPermissionsHead { app_id, .. } => {
            ("GetPermissionsHead", app_id.to_string())
        }
        WorkerCommand::GetSchemaHashes { app_id, .. } => ("GetSchemaHashes", app_id.to_string()),
        WorkerCommand::GetCatalogueStateHash { app_id, .. } => {
            ("GetCatalogueStateHash", app_id.to_string())
        }
    }
}

#[derive(Debug)]
struct FairAppQueue<T> {
    pending_by_app: HashMap<AppId, VecDeque<T>>,
    runnable_apps: VecDeque<AppId>,
    pending_total: usize,
}

impl<T> Default for FairAppQueue<T> {
    fn default() -> Self {
        Self {
            pending_by_app: HashMap::new(),
            runnable_apps: VecDeque::new(),
            pending_total: 0,
        }
    }
}

impl<T> FairAppQueue<T> {
    fn push(&mut self, app_id: AppId, item: T) {
        let queue = self.pending_by_app.entry(app_id).or_default();
        if queue.is_empty() {
            self.runnable_apps.push_back(app_id);
        }
        queue.push_back(item);
        self.pending_total += 1;
    }

    fn pop_batch(&mut self, quantum: usize) -> Option<Vec<T>> {
        let quantum = quantum.max(1);
        while let Some(app_id) = self.runnable_apps.pop_front() {
            let mut queue = match self.pending_by_app.remove(&app_id) {
                Some(queue) => queue,
                None => continue,
            };

            if queue.is_empty() {
                continue;
            }

            let take = queue.len().min(quantum);
            let mut batch = Vec::with_capacity(take);
            for _ in 0..take {
                if let Some(item) = queue.pop_front() {
                    batch.push(item);
                }
            }

            self.pending_total = self.pending_total.saturating_sub(batch.len());

            if !queue.is_empty() {
                self.pending_by_app.insert(app_id, queue);
                self.runnable_apps.push_back(app_id);
            }

            return Some(batch);
        }

        None
    }

    fn is_empty(&self) -> bool {
        self.pending_total == 0
    }
}

impl WorkerPool {
    fn new(
        workers: usize,
        #[cfg(feature = "otel")] metrics: std::sync::Arc<crate::metrics::SyncMetrics>,
    ) -> Self {
        let worker_count = workers.max(1);
        let mut handles = Vec::with_capacity(worker_count);

        for worker_idx in 0..worker_count {
            let (ingress_tx, ingress_rx) =
                tokio::sync::mpsc::channel::<WorkerCommand>(WORKER_SYNC_QUEUE_CAPACITY);
            #[cfg(feature = "otel")]
            let metrics = metrics.clone();
            let thread_name = format!("jazz-multi-worker-{worker_idx}");
            let join = std::thread::Builder::new()
                .name(thread_name)
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build worker tokio runtime");
                    runtime.block_on(run_worker_loop(
                        worker_idx,
                        ingress_rx,
                        #[cfg(feature = "otel")]
                        metrics,
                    ));
                })
                .expect("failed to spawn worker thread");

            handles.push(WorkerHandle {
                ingress: Some(ingress_tx),
                join: Some(join),
            });
        }

        Self { workers: handles }
    }

    fn send_command(&self, command: WorkerCommand) -> Result<usize, WorkerDispatchError> {
        let app_id = command.app_id();
        let worker = self.worker_for_app(&app_id);
        let Some(ingress) = self.workers[worker].ingress.as_ref() else {
            return Err(WorkerDispatchError::WorkerClosed { worker });
        };

        ingress.try_send(command).map_err(|err| match err {
            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                WorkerDispatchError::QueueFull { worker }
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                WorkerDispatchError::WorkerClosed { worker }
            }
        })?;

        Ok(worker)
    }

    async fn await_result(
        worker: usize,
        response: tokio::sync::oneshot::Receiver<Result<(), String>>,
    ) -> Result<(), WorkerDispatchError> {
        match response.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(WorkerDispatchError::RuntimeError { worker, message }),
            Err(_) => Err(WorkerDispatchError::WorkerUnavailable { worker }),
        }
    }

    async fn create_runtime(
        &self,
        app_id: AppId,
        data_dir: PathBuf,
        sync_broadcast: tokio::sync::broadcast::Sender<ClientSyncUpdate>,
        send_seq_by_client: ClientSendSeqMap,
    ) -> Result<(), WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::CreateRuntime {
            app_id,
            data_dir,
            sync_broadcast,
            send_seq_by_client,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        Self::await_result(worker, response_rx).await
    }

    async fn ensure_client_with_session(
        &self,
        app_id: AppId,
        client_id: ClientId,
        session: Session,
    ) -> Result<(), WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::EnsureClientWithSession {
            app_id,
            client_id,
            session,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        Self::await_result(worker, response_rx).await
    }

    async fn ensure_client_as_backend(
        &self,
        app_id: AppId,
        client_id: ClientId,
    ) -> Result<(), WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::EnsureClientAsBackend {
            app_id,
            client_id,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        Self::await_result(worker, response_rx).await
    }

    async fn sync_as_session(
        &self,
        app_id: AppId,
        client_id: ClientId,
        session: Session,
        payload: SyncPayload,
    ) -> Result<(), WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::SyncAsSession {
            app_id,
            client_id,
            session,
            payload,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        Self::await_result(worker, response_rx).await
    }

    async fn sync_as_backend(
        &self,
        app_id: AppId,
        client_id: ClientId,
        payload: SyncPayload,
    ) -> Result<(), WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::SyncAsBackend {
            app_id,
            client_id,
            payload,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        Self::await_result(worker, response_rx).await
    }

    async fn sync_as_admin(
        &self,
        app_id: AppId,
        client_id: ClientId,
        payload: SyncPayload,
    ) -> Result<(), WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::SyncAsAdmin {
            app_id,
            client_id,
            payload,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        Self::await_result(worker, response_rx).await
    }

    async fn get_catalogue_schema(
        &self,
        app_id: AppId,
        schema_hash: SchemaHash,
    ) -> Result<Option<Schema>, WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::GetCatalogueSchema {
            app_id,
            schema_hash,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        match response_rx.await {
            Ok(Ok(schema)) => Ok(schema),
            Ok(Err(message)) => Err(WorkerDispatchError::RuntimeError { worker, message }),
            Err(_) => Err(WorkerDispatchError::WorkerUnavailable { worker }),
        }
    }

    async fn publish_schema(
        &self,
        app_id: AppId,
        schema: Schema,
    ) -> Result<ObjectId, WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::PublishSchema {
            app_id,
            schema,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        match response_rx.await {
            Ok(Ok(object_id)) => Ok(object_id),
            Ok(Err(message)) => Err(WorkerDispatchError::RuntimeError { worker, message }),
            Err(_) => Err(WorkerDispatchError::WorkerUnavailable { worker }),
        }
    }

    async fn publish_permissions(
        &self,
        app_id: AppId,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<PermissionsHeadSummary>, WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::PublishPermissions {
            app_id,
            schema_hash,
            permissions,
            expected_parent_bundle_object_id,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        match response_rx.await {
            Ok(Ok(head)) => Ok(head),
            Ok(Err(message)) => Err(WorkerDispatchError::RuntimeError { worker, message }),
            Err(_) => Err(WorkerDispatchError::WorkerUnavailable { worker }),
        }
    }

    async fn get_permissions_head(
        &self,
        app_id: AppId,
    ) -> Result<Option<PermissionsHeadSummary>, WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::GetPermissionsHead {
            app_id,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        match response_rx.await {
            Ok(Ok(head)) => Ok(head),
            Ok(Err(message)) => Err(WorkerDispatchError::RuntimeError { worker, message }),
            Err(_) => Err(WorkerDispatchError::WorkerUnavailable { worker }),
        }
    }

    async fn get_schema_hashes(&self, app_id: AppId) -> Result<Vec<String>, WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::GetSchemaHashes {
            app_id,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        match response_rx.await {
            Ok(Ok(schema_hashes)) => Ok(schema_hashes),
            Ok(Err(message)) => Err(WorkerDispatchError::RuntimeError { worker, message }),
            Err(_) => Err(WorkerDispatchError::WorkerUnavailable { worker }),
        }
    }

    async fn get_catalogue_state_hash(&self, app_id: AppId) -> Result<String, WorkerDispatchError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WorkerCommand::GetCatalogueStateHash {
            app_id,
            response: response_tx,
        };
        let worker = self.send_command(command)?;
        match response_rx.await {
            Ok(Ok(catalogue_state_hash)) => Ok(catalogue_state_hash),
            Ok(Err(message)) => Err(WorkerDispatchError::RuntimeError { worker, message }),
            Err(_) => Err(WorkerDispatchError::WorkerUnavailable { worker }),
        }
    }

    fn worker_count(&self) -> usize {
        self.workers.len()
    }

    fn worker_for_app(&self, app_id: &AppId) -> usize {
        let mut hasher = DefaultHasher::new();
        app_id.hash(&mut hasher);
        (hasher.finish() as usize) % self.workers.len()
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            worker.ingress.take();
        }

        for worker in &mut self.workers {
            if let Some(join) = worker.join.take() {
                let _ = join.join();
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
enum AppStatus {
    #[default]
    Active,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalAuthMode {
    Anonymous,
    Demo,
}

impl LocalAuthMode {
    fn from_header(value: &str) -> Option<Self> {
        match value {
            "anonymous" => Some(Self::Anonymous),
            "demo" => Some(Self::Demo),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Anonymous => "anonymous",
            Self::Demo => "demo",
        }
    }
}

impl AppStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct AppConfig {
    app_name: String,
    jwks_endpoint: String,
    jwks_cache_ttl_secs: u64,
    jwks_max_stale_secs: u64,
    allow_anonymous: bool,
    allow_demo: bool,
    backend_secret_hash: String,
    admin_secret_hash: String,
    status: AppStatus,
}

impl AppConfig {
    fn jwks_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.jwks_cache_ttl_secs)
    }

    fn jwks_max_stale(&self) -> Duration {
        Duration::from_secs(self.jwks_max_stale_secs)
    }
}

#[derive(Debug, Clone)]
struct MetaAppRow {
    object_id: ObjectId,
    app_id: AppId,
    app_name: String,
    jwks_endpoint: String,
    jwks_cache_ttl_secs: u64,
    jwks_max_stale_secs: u64,
    allow_anonymous: bool,
    allow_demo: bool,
    backend_secret_hash: String,
    admin_secret_hash: String,
    status: AppStatus,
    created_at: u64,
    updated_at: u64,
    admin_secret: Option<String>,
}

#[derive(Debug, Clone)]
struct MetaExternalIdentityRow {
    principal_id: String,
}

struct MetaStore {
    runtime: TokioRuntime<RocksDBStorage>,
    secret_hash_key: String,
    apps_insert_descriptor: RowDescriptor,
    apps_descriptor: RowDescriptor,
    external_identities_insert_descriptor: RowDescriptor,
    external_identities_descriptor: RowDescriptor,
}

fn normalize_row_descriptor(descriptor: &mut RowDescriptor) {
    descriptor
        .columns
        .sort_unstable_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
}

fn descriptor_value<'a>(
    descriptor: &RowDescriptor,
    values: &'a [Value],
    column: &str,
) -> Option<&'a Value> {
    descriptor
        .column_index(column)
        .and_then(|index| values.get(index))
}

fn encode_u64_config_value(field: &str, value: u64) -> Result<Value, String> {
    let encoded = i64::try_from(value).map_err(|_| format!("{field} is out of range"))?;
    Ok(Value::BigInt(encoded))
}

fn decode_u64_config_value(
    descriptor: &RowDescriptor,
    values: &[Value],
    field: &str,
    default: u64,
) -> Result<u64, String> {
    match descriptor_value(descriptor, values, field) {
        Some(Value::BigInt(value)) => u64::try_from(*value)
            .map_err(|_| format!("meta row field {field} expected non-negative bigint")),
        Some(Value::Integer(value)) => u64::try_from(*value)
            .map_err(|_| format!("meta row field {field} expected non-negative integer")),
        Some(Value::Timestamp(value)) => Ok(*value),
        Some(other) => Err(format!(
            "meta row field {field} expected integer-compatible numeric value, got {other:?}"
        )),
        None => Ok(default),
    }
}

impl MetaStore {
    fn new(data_root: &Path, secret_hash_key: String) -> Result<Self, String> {
        let meta_dir = data_root.join("meta");
        std::fs::create_dir_all(&meta_dir)
            .map_err(|e| format!("failed to create meta dir '{}': {e}", meta_dir.display()))?;

        let meta_schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("apps")
                    .column("app_id", ColumnType::Uuid)
                    .column("app_name", ColumnType::Text)
                    .column("jwks_endpoint", ColumnType::Text)
                    .column("jwks_cache_ttl_secs", ColumnType::BigInt)
                    .column("jwks_max_stale_secs", ColumnType::BigInt)
                    .column("allow_anonymous", ColumnType::Boolean)
                    .column("allow_demo", ColumnType::Boolean)
                    .column("backend_secret_hash", ColumnType::Text)
                    .column("admin_secret_hash", ColumnType::Text)
                    .column("status", ColumnType::Text)
                    .column("created_at", ColumnType::Timestamp)
                    .column("updated_at", ColumnType::Timestamp)
                    .column("admin_secret", ColumnType::Text),
            )
            .table(
                TableSchema::builder("external_identities")
                    .column("app_id", ColumnType::Uuid)
                    .column("issuer", ColumnType::Text)
                    .column("subject", ColumnType::Text)
                    .column("principal_id", ColumnType::Text)
                    .column("created_at", ColumnType::Timestamp)
                    .column("updated_at", ColumnType::Timestamp),
            )
            .build();

        let apps_insert_descriptor = meta_schema
            .get(&TableName::new("apps"))
            .ok_or_else(|| "meta schema missing apps table".to_string())?
            .columns
            .clone();
        let mut apps_descriptor = apps_insert_descriptor.clone();
        normalize_row_descriptor(&mut apps_descriptor);

        let external_identities_insert_descriptor = meta_schema
            .get(&TableName::new("external_identities"))
            .ok_or_else(|| "meta schema missing external_identities table".to_string())?
            .columns
            .clone();
        let mut external_identities_descriptor = external_identities_insert_descriptor.clone();
        normalize_row_descriptor(&mut external_identities_descriptor);

        let sync_manager = SyncManager::new().with_durability_tiers(vec![
            DurabilityTier::EdgeServer,
            DurabilityTier::GlobalServer,
        ]);
        let schema_manager = SchemaManager::new(
            sync_manager,
            meta_schema,
            AppId::from_name("jazz-cloud-server-meta"),
            "meta",
            "main",
        )
        .map_err(|e| format!("failed to initialize meta schema manager: {e:?}"))?;

        let db_path = meta_dir.join("jazz.rocksdb");
        let storage = RocksDBStorage::open(&db_path, 64 * 1024 * 1024)
            .map_err(|e| format!("failed to open meta storage '{}': {e:?}", db_path.display()))?;

        // Meta app is local-only; no sync callback needed yet.
        let runtime = TokioRuntime::new(schema_manager, storage, |_entry| {});

        Ok(Self {
            runtime,
            secret_hash_key,
            apps_insert_descriptor,
            apps_descriptor,
            external_identities_insert_descriptor,
            external_identities_descriptor,
        })
    }

    fn hash_secret(&self, secret: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.secret_hash_key.as_bytes())
            .expect("HMAC key creation should not fail for arbitrary key length");
        mac.update(secret.as_bytes());
        let bytes = mac.finalize().into_bytes();
        hex::encode(bytes)
    }

    fn verify_secret(&self, provided_secret: &str, expected_hash: &str) -> bool {
        let hashed = self.hash_secret(provided_secret);
        constant_time_eq(&hashed, expected_hash)
    }

    async fn list_apps(&self) -> Result<Vec<MetaAppRow>, String> {
        let query = QueryBuilder::new("apps").build();
        let future = self
            .runtime
            .query(query, None, ReadDurabilityOptions::default())
            .map_err(|e| format!("meta query error: {e}"))?;

        let rows = future
            .await
            .map_err(|e| format!("meta query await error: {e}"))?;

        rows.into_iter()
            .map(|(object_id, values)| self.decode_row(object_id, &values))
            .collect()
    }

    async fn get_by_app_id(&self, app_id: AppId) -> Result<Option<MetaAppRow>, String> {
        let query = QueryBuilder::new("apps")
            .filter_eq("app_id", Value::Uuid(app_id.as_object_id()))
            .build();
        let future = self
            .runtime
            .query(query, None, ReadDurabilityOptions::default())
            .map_err(|e| format!("meta query error: {e}"))?;
        let mut rows = future
            .await
            .map_err(|e| format!("meta query await error: {e}"))?;

        if let Some((object_id, values)) = rows.pop() {
            Ok(Some(self.decode_row(object_id, &values)?))
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_app(
        &self,
        app_id: AppId,
        app_name: String,
        jwks_endpoint: String,
        jwks_cache_ttl_secs: u64,
        jwks_max_stale_secs: u64,
        allow_anonymous: bool,
        allow_demo: bool,
        backend_secret_hash: String,
        admin_secret_hash: String,
        status: AppStatus,
        admin_secret: Option<String>,
    ) -> Result<MetaAppRow, String> {
        let now = now_timestamp_us();
        let mut values = HashMap::with_capacity(self.apps_insert_descriptor.columns.len());
        for column in &self.apps_insert_descriptor.columns {
            let value = match column.name.as_str() {
                "admin_secret" => match &admin_secret {
                    Some(value) => Value::Text(value.clone()),
                    None => Value::Null,
                },
                "admin_secret_hash" => Value::Text(admin_secret_hash.clone()),
                "allow_anonymous" => Value::Boolean(allow_anonymous),
                "allow_demo" => Value::Boolean(allow_demo),
                "app_id" => Value::Uuid(app_id.as_object_id()),
                "app_name" => Value::Text(app_name.clone()),
                "backend_secret_hash" => Value::Text(backend_secret_hash.clone()),
                "created_at" => Value::Timestamp(now),
                "jwks_endpoint" => Value::Text(jwks_endpoint.clone()),
                "jwks_cache_ttl_secs" => {
                    encode_u64_config_value("jwks_cache_ttl_secs", jwks_cache_ttl_secs)?
                }
                "jwks_max_stale_secs" => {
                    encode_u64_config_value("jwks_max_stale_secs", jwks_max_stale_secs)?
                }
                "status" => Value::Text(status.as_str().to_string()),
                "updated_at" => Value::Timestamp(now),
                other => panic!("unexpected meta apps column {other}"),
            };
            values.insert(column.name.to_string(), value);
        }

        let (object_id, _row_values) = self
            .runtime
            .insert("apps", values, None)
            .map_err(|e| format!("failed to insert meta app record: {e}"))?;
        self.runtime
            .flush()
            .await
            .map_err(|e| format!("failed to flush meta app record: {e}"))?;

        Ok(MetaAppRow {
            object_id,
            app_id,
            app_name,
            jwks_endpoint,
            jwks_cache_ttl_secs,
            jwks_max_stale_secs,
            allow_anonymous,
            allow_demo,
            backend_secret_hash,
            admin_secret_hash,
            status,
            created_at: now,
            updated_at: now,
            admin_secret,
        })
    }

    async fn update_app(&self, row: &MetaAppRow) -> Result<(), String> {
        let mut updates = vec![
            ("app_name".to_string(), Value::Text(row.app_name.clone())),
            (
                "jwks_endpoint".to_string(),
                Value::Text(row.jwks_endpoint.clone()),
            ),
            (
                "allow_anonymous".to_string(),
                Value::Boolean(row.allow_anonymous),
            ),
            ("allow_demo".to_string(), Value::Boolean(row.allow_demo)),
            (
                "backend_secret_hash".to_string(),
                Value::Text(row.backend_secret_hash.clone()),
            ),
            (
                "admin_secret_hash".to_string(),
                Value::Text(row.admin_secret_hash.clone()),
            ),
            (
                "status".to_string(),
                Value::Text(row.status.as_str().to_string()),
            ),
            ("updated_at".to_string(), Value::Timestamp(row.updated_at)),
            (
                "admin_secret".to_string(),
                match &row.admin_secret {
                    Some(value) => Value::Text(value.clone()),
                    None => Value::Null,
                },
            ),
        ];
        updates.push((
            "jwks_cache_ttl_secs".to_string(),
            encode_u64_config_value("jwks_cache_ttl_secs", row.jwks_cache_ttl_secs)?,
        ));
        updates.push((
            "jwks_max_stale_secs".to_string(),
            encode_u64_config_value("jwks_max_stale_secs", row.jwks_max_stale_secs)?,
        ));

        self.runtime
            .update(row.object_id, updates, None)
            .map_err(|e| format!("failed to update meta app record: {e}"))?;
        self.runtime
            .flush()
            .await
            .map_err(|e| format!("failed to flush meta app update: {e}"))?;
        Ok(())
    }

    async fn delete_app(&self, object_id: ObjectId) -> Result<(), String> {
        self.runtime
            .delete(object_id, None)
            .map_err(|e| format!("failed to delete meta app record: {e}"))?;
        self.runtime
            .flush()
            .await
            .map_err(|e| format!("failed to flush meta app delete: {e}"))?;
        Ok(())
    }

    async fn get_external_identity(
        &self,
        app_id: AppId,
        issuer: &str,
        subject: &str,
    ) -> Result<Option<MetaExternalIdentityRow>, String> {
        let query = QueryBuilder::new("external_identities")
            .filter_eq("app_id", Value::Uuid(app_id.as_object_id()))
            .filter_eq("issuer", Value::Text(issuer.to_string()))
            .filter_eq("subject", Value::Text(subject.to_string()))
            .build();

        let future = self
            .runtime
            .query(query, None, ReadDurabilityOptions::default())
            .map_err(|e| format!("external identity query error: {e}"))?;
        let mut rows = future
            .await
            .map_err(|e| format!("external identity query await error: {e}"))?;

        if let Some((object_id, values)) = rows.pop() {
            Ok(Some(self.decode_external_identity_row(object_id, &values)?))
        } else {
            Ok(None)
        }
    }

    async fn create_external_identity(
        &self,
        app_id: AppId,
        issuer: &str,
        subject: &str,
        principal_id: &str,
    ) -> Result<MetaExternalIdentityRow, String> {
        let now = now_timestamp_us();
        let values: HashMap<String, Value> = self
            .external_identities_insert_descriptor
            .columns
            .iter()
            .map(|column| {
                let value = match column.name.as_str() {
                    "app_id" => Value::Uuid(app_id.as_object_id()),
                    "created_at" => Value::Timestamp(now),
                    "issuer" => Value::Text(issuer.to_string()),
                    "principal_id" => Value::Text(principal_id.to_string()),
                    "subject" => Value::Text(subject.to_string()),
                    "updated_at" => Value::Timestamp(now),
                    other => panic!("unexpected external identity column {other}"),
                };
                (column.name.to_string(), value)
            })
            .collect();

        let object_id = self
            .runtime
            .insert("external_identities", values, None)
            .map_err(|e| format!("failed to insert external identity: {e}"))?;
        self.runtime
            .flush()
            .await
            .map_err(|e| format!("failed to flush external identity: {e}"))?;

        let _ = object_id;
        Ok(MetaExternalIdentityRow {
            principal_id: principal_id.to_string(),
        })
    }

    fn decode_row(&self, object_id: ObjectId, values: &[Value]) -> Result<MetaAppRow, String> {
        let app_obj_id = match descriptor_value(&self.apps_descriptor, values, "app_id") {
            Some(Value::Uuid(id)) => *id,
            Some(other) => {
                return Err(format!(
                    "meta row field app_id expected uuid, got {other:?}"
                ));
            }
            None => return Err("meta row missing app_id".to_string()),
        };

        let app_name = match descriptor_value(&self.apps_descriptor, values, "app_name") {
            Some(Value::Text(s)) => s.clone(),
            Some(other) => {
                return Err(format!(
                    "meta row field app_name expected text, got {other:?}"
                ));
            }
            None => return Err("meta row missing app_name".to_string()),
        };

        let jwks_endpoint = match descriptor_value(&self.apps_descriptor, values, "jwks_endpoint") {
            Some(Value::Text(s)) => s.clone(),
            Some(other) => {
                return Err(format!(
                    "meta row field jwks_endpoint expected text, got {other:?}"
                ));
            }
            None => return Err("meta row missing jwks_endpoint".to_string()),
        };
        let jwks_cache_ttl_secs = decode_u64_config_value(
            &self.apps_descriptor,
            values,
            "jwks_cache_ttl_secs",
            DEFAULT_JWKS_CACHE_TTL_SECS,
        )?;
        let jwks_max_stale_secs = decode_u64_config_value(
            &self.apps_descriptor,
            values,
            "jwks_max_stale_secs",
            DEFAULT_JWKS_MAX_STALE_SECS,
        )?;

        let allow_anonymous =
            match descriptor_value(&self.apps_descriptor, values, "allow_anonymous") {
                Some(Value::Boolean(v)) => *v,
                Some(other) => {
                    return Err(format!(
                        "meta row field allow_anonymous expected boolean, got {other:?}"
                    ));
                }
                None => true,
            };

        let allow_demo = match descriptor_value(&self.apps_descriptor, values, "allow_demo") {
            Some(Value::Boolean(v)) => *v,
            Some(other) => {
                return Err(format!(
                    "meta row field allow_demo expected boolean, got {other:?}"
                ));
            }
            None => true,
        };

        let backend_secret_hash =
            match descriptor_value(&self.apps_descriptor, values, "backend_secret_hash") {
                Some(Value::Text(s)) => s.clone(),
                Some(other) => {
                    return Err(format!(
                        "meta row field backend_secret_hash expected text, got {other:?}"
                    ));
                }
                None => return Err("meta row missing backend_secret_hash".to_string()),
            };

        let admin_secret_hash =
            match descriptor_value(&self.apps_descriptor, values, "admin_secret_hash") {
                Some(Value::Text(s)) => s.clone(),
                Some(other) => {
                    return Err(format!(
                        "meta row field admin_secret_hash expected text, got {other:?}"
                    ));
                }
                None => return Err("meta row missing admin_secret_hash".to_string()),
            };

        let status = match descriptor_value(&self.apps_descriptor, values, "status") {
            Some(Value::Text(s)) => AppStatus::from_str(s)
                .ok_or_else(|| format!("meta row has invalid status value: {s}"))?,
            Some(other) => {
                return Err(format!(
                    "meta row field status expected text, got {other:?}"
                ));
            }
            None => return Err("meta row missing status".to_string()),
        };

        let created_at = match descriptor_value(&self.apps_descriptor, values, "created_at") {
            Some(Value::Timestamp(ts)) => *ts,
            Some(other) => {
                return Err(format!(
                    "meta row field created_at expected timestamp, got {other:?}"
                ));
            }
            None => return Err("meta row missing created_at".to_string()),
        };

        let updated_at = match descriptor_value(&self.apps_descriptor, values, "updated_at") {
            Some(Value::Timestamp(ts)) => *ts,
            Some(other) => {
                return Err(format!(
                    "meta row field updated_at expected timestamp, got {other:?}"
                ));
            }
            None => return Err("meta row missing updated_at".to_string()),
        };

        let admin_secret = match descriptor_value(&self.apps_descriptor, values, "admin_secret") {
            Some(Value::Text(value)) => Some(value.clone()),
            Some(Value::Null) | None => None,
            Some(other) => {
                return Err(format!(
                    "meta row field admin_secret expected text|null, got {other:?}"
                ));
            }
        };

        Ok(MetaAppRow {
            object_id,
            app_id: AppId::from_object_id(app_obj_id),
            app_name,
            jwks_endpoint,
            jwks_cache_ttl_secs,
            jwks_max_stale_secs,
            allow_anonymous,
            allow_demo,
            backend_secret_hash,
            admin_secret_hash,
            status,
            created_at,
            updated_at,
            admin_secret,
        })
    }

    fn decode_external_identity_row(
        &self,
        _object_id: ObjectId,
        values: &[Value],
    ) -> Result<MetaExternalIdentityRow, String> {
        let principal_id =
            match descriptor_value(&self.external_identities_descriptor, values, "principal_id") {
                Some(Value::Text(s)) => s.clone(),
                Some(other) => {
                    return Err(format!(
                        "external identity field principal_id expected text, got {other:?}"
                    ));
                }
                None => return Err("external identity row missing principal_id".to_string()),
            };

        Ok(MetaExternalIdentityRow { principal_id })
    }
}

struct ConnectionState {
    _client_id: ClientId,
}

#[derive(Debug, Clone)]
struct CachedJwks {
    endpoint: String,
    fetched_at_us: u64,
    /// Per-app cooldown: timestamp of last forced refresh for this app's JWKS.
    last_forced_refresh_us: u64,
    set: JwkSet,
}

struct AppRuntime {
    runtime: TokioRuntime<RocksDBStorage>,
}

impl AppRuntime {
    fn new(
        app_id: AppId,
        data_dir: &Path,
        sync_broadcast: tokio::sync::broadcast::Sender<ClientSyncUpdate>,
        send_seq_by_client: ClientSendSeqMap,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(data_dir).map_err(|e| {
            format!(
                "failed to create app data dir '{}': {e}",
                data_dir.display()
            )
        })?;

        let sync_manager = SyncManager::new().with_durability_tiers(vec![
            DurabilityTier::EdgeServer,
            DurabilityTier::GlobalServer,
        ]);
        let mut schema_manager = SchemaManager::new_server(sync_manager, app_id, "prod");

        let db_path = data_dir.join("jazz.rocksdb");
        let storage = RocksDBStorage::open(&db_path, 64 * 1024 * 1024)
            .map_err(|e| format!("failed to open storage '{}': {e:?}", db_path.display()))?;

        rehydrate_schema_manager_from_manifest(&mut schema_manager, &storage, app_id)?;

        let sync_tx_clone = sync_broadcast.clone();
        let send_seq_by_client_clone = send_seq_by_client.clone();
        let runtime = TokioRuntime::new(schema_manager, storage, move |entry| {
            if let Destination::Client(client_id) = entry.destination {
                let mut payload = entry.payload;

                let (last_seq, seq) = {
                    let mut counters = send_seq_by_client_clone
                        .lock()
                        .expect("send sequence mutex poisoned");
                    let last = counters.entry(client_id).or_insert(0);
                    let last_seq = *last;
                    *last += 1;
                    (last_seq, *last)
                };

                if let SyncPayload::QuerySettled { through_seq, .. } = &mut payload {
                    *through_seq = last_seq;
                }

                if let Some(delay) = test_delay_server_send_object_updated(&payload) {
                    let tx = sync_tx_clone.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = tx.send((client_id, seq, payload));
                    });
                } else {
                    let _ = sync_tx_clone.send((client_id, seq, payload));
                }
            }
        });

        Ok(Self { runtime })
    }
}

struct AppEntry {
    app_id: AppId,
    meta_object_id: ObjectId,
    config: tokio::sync::RwLock<AppConfig>,
    connections: tokio::sync::RwLock<HashMap<u64, ConnectionState>>,
    next_connection_id: AtomicU64,
    sync_broadcast: tokio::sync::broadcast::Sender<ClientSyncUpdate>,
    send_seq_by_client: ClientSendSeqMap,
}

impl AppEntry {
    fn new(
        app_id: AppId,
        meta_object_id: ObjectId,
        config: AppConfig,
        sync_broadcast: tokio::sync::broadcast::Sender<ClientSyncUpdate>,
        send_seq_by_client: ClientSendSeqMap,
    ) -> Arc<Self> {
        Arc::new(Self {
            app_id,
            meta_object_id,
            config: tokio::sync::RwLock::new(config),
            connections: tokio::sync::RwLock::new(HashMap::new()),
            next_connection_id: AtomicU64::new(1),
            sync_broadcast,
            send_seq_by_client,
        })
    }
}

async fn run_worker_loop(
    worker: usize,
    mut ingress_rx: tokio::sync::mpsc::Receiver<WorkerCommand>,
    #[cfg(feature = "otel")] metrics: std::sync::Arc<crate::metrics::SyncMetrics>,
) {
    let mut fair_queue = FairAppQueue::<WorkerCommand>::default();
    let mut app_runtimes = HashMap::<AppId, AppRuntime>::new();

    loop {
        if fair_queue.is_empty() {
            match ingress_rx.recv().await {
                Some(command) => fair_queue.push(command.app_id(), command),
                None => {
                    info!(worker, "sync worker stopped");
                    return;
                }
            }
        }

        while let Ok(command) = ingress_rx.try_recv() {
            fair_queue.push(command.app_id(), command);
        }

        let Some(batch) = fair_queue.pop_batch(WORKER_APP_QUANTUM) else {
            continue;
        };

        for command in batch {
            #[cfg(feature = "otel")]
            let cmd_start = std::time::Instant::now();

            #[cfg(feature = "otel")]
            let (cmd_type, cmd_app_id) = worker_command_meta(&command);

            match command {
                WorkerCommand::CreateRuntime {
                    app_id,
                    data_dir,
                    sync_broadcast,
                    send_seq_by_client,
                    response,
                } => {
                    let result = if let std::collections::hash_map::Entry::Vacant(entry) =
                        app_runtimes.entry(app_id)
                    {
                        AppRuntime::new(app_id, &data_dir, sync_broadcast, send_seq_by_client).map(
                            |runtime| {
                                entry.insert(runtime);
                            },
                        )
                    } else {
                        Err(format!("app runtime already exists for {app_id}"))
                    };

                    #[cfg(feature = "otel")]
                    if result.is_ok() {
                        metrics.app_runtime_created.add(
                            1,
                            &[
                                opentelemetry::KeyValue::new("app_id", app_id.to_string()),
                                opentelemetry::KeyValue::new("worker", worker as i64),
                            ],
                        );
                        metrics
                            .worker_apps_active
                            .add(1, &[opentelemetry::KeyValue::new("worker", worker as i64)]);
                    }

                    if response.send(result).is_err() {
                        warn!(worker, app_id = %app_id, "create runtime response receiver dropped");
                    }
                }
                WorkerCommand::EnsureClientWithSession {
                    app_id,
                    client_id,
                    session,
                    response,
                } => {
                    let result = app_runtimes
                        .get(&app_id)
                        .ok_or_else(|| format!("missing runtime for app {app_id}"))
                        .and_then(|runtime| {
                            runtime
                                .runtime
                                .ensure_client_with_session(client_id, session)
                                .map_err(|err| err.to_string())
                        });
                    if response.send(result).is_err() {
                        warn!(
                            worker,
                            app_id = %app_id,
                            client_id = %client_id,
                            "ensure-client response receiver dropped"
                        );
                    }
                }
                WorkerCommand::EnsureClientAsBackend {
                    app_id,
                    client_id,
                    response,
                } => {
                    let result = app_runtimes
                        .get(&app_id)
                        .ok_or_else(|| format!("missing runtime for app {app_id}"))
                        .and_then(|runtime| {
                            runtime
                                .runtime
                                .ensure_client_as_backend(client_id)
                                .map_err(|err| err.to_string())
                        });
                    if response.send(result).is_err() {
                        warn!(
                            worker,
                            app_id = %app_id,
                            client_id = %client_id,
                            "ensure-backend response receiver dropped"
                        );
                    }
                }
                WorkerCommand::SyncAsSession {
                    app_id,
                    client_id,
                    session,
                    payload,
                    response,
                } => {
                    let result = match app_runtimes.get(&app_id) {
                        Some(runtime) => {
                            if let Err(err) = runtime
                                .runtime
                                .ensure_client_with_session(client_id, session)
                            {
                                Err(err.to_string())
                            } else if let Err(err) = runtime.runtime.push_sync_inbox(InboxEntry {
                                source: Source::Client(client_id),
                                payload,
                            }) {
                                Err(err.to_string())
                            } else {
                                runtime.runtime.flush().await.map_err(|err| err.to_string())
                            }
                        }
                        None => Err(format!("missing runtime for app {app_id}")),
                    };
                    if response.send(result).is_err() {
                        warn!(
                            worker,
                            app_id = %app_id,
                            client_id = %client_id,
                            "session-sync response receiver dropped"
                        );
                    }
                }
                WorkerCommand::SyncAsBackend {
                    app_id,
                    client_id,
                    payload,
                    response,
                } => {
                    let result = match app_runtimes.get(&app_id) {
                        Some(runtime) => {
                            if let Err(err) = runtime.runtime.ensure_client_as_backend(client_id) {
                                Err(err.to_string())
                            } else if let Err(err) = runtime.runtime.push_sync_inbox(InboxEntry {
                                source: Source::Client(client_id),
                                payload,
                            }) {
                                Err(err.to_string())
                            } else {
                                runtime.runtime.flush().await.map_err(|err| err.to_string())
                            }
                        }
                        None => Err(format!("missing runtime for app {app_id}")),
                    };
                    if response.send(result).is_err() {
                        warn!(
                            worker,
                            app_id = %app_id,
                            client_id = %client_id,
                            "backend-sync response receiver dropped"
                        );
                    }
                }
                WorkerCommand::SyncAsAdmin {
                    app_id,
                    client_id,
                    payload,
                    response,
                } => {
                    let result = match app_runtimes.get(&app_id) {
                        Some(runtime) => {
                            if let Err(err) = runtime.runtime.ensure_client_as_admin(client_id) {
                                Err(err.to_string())
                            } else if let Err(err) = runtime.runtime.push_sync_inbox(InboxEntry {
                                source: Source::Client(client_id),
                                payload,
                            }) {
                                Err(err.to_string())
                            } else {
                                runtime.runtime.flush().await.map_err(|err| err.to_string())
                            }
                        }
                        None => Err(format!("missing runtime for app {app_id}")),
                    };
                    if response.send(result).is_err() {
                        warn!(
                            worker,
                            app_id = %app_id,
                            client_id = %client_id,
                            "admin-sync response receiver dropped"
                        );
                    }
                }
                WorkerCommand::GetCatalogueSchema {
                    app_id,
                    schema_hash,
                    response,
                } => {
                    let result = app_runtimes
                        .get(&app_id)
                        .ok_or_else(|| format!("missing runtime for app {app_id}"))
                        .and_then(|runtime| {
                            let maybe_schema = runtime
                                .runtime
                                .known_schema(&schema_hash)
                                .map_err(|err| err.to_string())?;
                            Ok(maybe_schema.as_ref().cloned())
                        });
                    if response.send(result).is_err() {
                        warn!(worker, app_id = %app_id, "schema response receiver dropped");
                    }
                }
                WorkerCommand::PublishSchema {
                    app_id,
                    schema,
                    response,
                } => {
                    let result = app_runtimes
                        .get(&app_id)
                        .ok_or_else(|| format!("missing runtime for app {app_id}"))
                        .and_then(|runtime| {
                            runtime
                                .runtime
                                .publish_schema(schema)
                                .map_err(|err| err.to_string())
                        });
                    if response.send(result).is_err() {
                        warn!(worker, app_id = %app_id, "publish-schema response receiver dropped");
                    }
                }
                WorkerCommand::PublishPermissions {
                    app_id,
                    schema_hash,
                    permissions,
                    expected_parent_bundle_object_id,
                    response,
                } => {
                    let result = app_runtimes
                        .get(&app_id)
                        .ok_or_else(|| format!("missing runtime for app {app_id}"))
                        .and_then(|runtime| {
                            runtime
                                .runtime
                                .publish_permissions_bundle(
                                    schema_hash,
                                    permissions,
                                    expected_parent_bundle_object_id,
                                )
                                .and_then(|_| runtime.runtime.current_permissions_head())
                                .map_err(|err| err.to_string())
                        });
                    if response.send(result).is_err() {
                        warn!(
                            worker,
                            app_id = %app_id,
                            "publish-permissions response receiver dropped"
                        );
                    }
                }
                WorkerCommand::GetPermissionsHead { app_id, response } => {
                    let result = app_runtimes
                        .get(&app_id)
                        .ok_or_else(|| format!("missing runtime for app {app_id}"))
                        .and_then(|runtime| {
                            runtime
                                .runtime
                                .current_permissions_head()
                                .map_err(|err| err.to_string())
                        });
                    if response.send(result).is_err() {
                        warn!(
                            worker,
                            app_id = %app_id,
                            "permissions head response receiver dropped"
                        );
                    }
                }
                WorkerCommand::GetSchemaHashes { app_id, response } => {
                    let result = app_runtimes
                        .get(&app_id)
                        .ok_or_else(|| format!("missing runtime for app {app_id}"))
                        .and_then(|runtime| {
                            runtime
                                .runtime
                                .known_schema_hashes()
                                .map(|schema_hashes| {
                                    schema_hashes.iter().map(ToString::to_string).collect()
                                })
                                .map_err(|err| err.to_string())
                        });
                    if response.send(result).is_err() {
                        warn!(worker, app_id = %app_id, "schema hashes response receiver dropped");
                    }
                }
                WorkerCommand::GetCatalogueStateHash { app_id, response } => {
                    let result = app_runtimes
                        .get(&app_id)
                        .ok_or_else(|| format!("missing runtime for app {app_id}"))
                        .and_then(|runtime| {
                            runtime
                                .runtime
                                .catalogue_state_hash()
                                .map_err(|err| err.to_string())
                        });
                    if response.send(result).is_err() {
                        warn!(
                            worker,
                            app_id = %app_id,
                            "catalogue state hash response receiver dropped"
                        );
                    }
                }
            }

            #[cfg(feature = "otel")]
            {
                let elapsed = cmd_start.elapsed().as_secs_f64() * 1000.0;
                metrics.worker_command_duration_ms.record(
                    elapsed,
                    &[
                        opentelemetry::KeyValue::new("command_type", cmd_type),
                        opentelemetry::KeyValue::new("app_id", cmd_app_id),
                        opentelemetry::KeyValue::new("worker", worker as i64),
                    ],
                );
                metrics.worker_commands_total.add(
                    1,
                    &[
                        opentelemetry::KeyValue::new("command_type", cmd_type),
                        opentelemetry::KeyValue::new("worker", worker as i64),
                    ],
                );
            }
        }

        #[cfg(feature = "otel")]
        {
            metrics.worker_queue_depth.record(
                fair_queue.pending_total as i64,
                &[opentelemetry::KeyValue::new("worker", worker as i64)],
            );
        }

        tokio::task::yield_now().await;
    }
}

struct ServerState {
    apps: tokio::sync::RwLock<HashMap<AppId, Arc<AppEntry>>>,
    data_root: PathBuf,
    internal_api_secret: String,
    workers: WorkerPool,
    meta_store: Arc<MetaStore>,
    jwks_cache: tokio::sync::RwLock<HashMap<AppId, CachedJwks>>,
    http_client: reqwest::Client,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    #[cfg(feature = "otel")]
    metrics: std::sync::Arc<crate::metrics::SyncMetrics>,
}

impl ServerState {
    async fn get_app(&self, app_id: AppId) -> Option<Arc<AppEntry>> {
        self.apps.read().await.get(&app_id).cloned()
    }

    fn app_data_dir(&self, app_id: AppId) -> PathBuf {
        self.data_root.join("apps").join(app_id.to_string())
    }

    async fn app_count(&self) -> usize {
        self.apps.read().await.len()
    }
}

#[derive(Debug, Deserialize)]
struct AppPath {
    app_id: String,
}

#[derive(Debug, Deserialize)]
struct SchemaPath {
    app_id: String,
    hash: String,
}

#[derive(Debug, Deserialize)]
struct EventsParams {
    client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateAppRequest {
    app_name: String,
    jwks_endpoint: Option<String>,
    jwks_cache_ttl_secs: Option<u64>,
    jwks_max_stale_secs: Option<u64>,
    allow_anonymous: Option<bool>,
    allow_demo: Option<bool>,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateAppRequest {
    app_name: Option<String>,
    jwks_endpoint: Option<String>,
    jwks_cache_ttl_secs: Option<u64>,
    jwks_max_stale_secs: Option<u64>,
    allow_anonymous: Option<bool>,
    allow_demo: Option<bool>,
    status: Option<AppStatus>,
    rotate_backend_secret: Option<bool>,
    rotate_admin_secret: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ManageSetStatusRequest {
    status: AppStatus,
}

#[derive(Debug, Deserialize)]
struct ManageUpdateAuthRequest {
    jwks_endpoint: Option<String>,
    jwks_cache_ttl_secs: Option<u64>,
    jwks_max_stale_secs: Option<u64>,
    allow_anonymous: Option<bool>,
    allow_demo: Option<bool>,
}

#[derive(Debug, Serialize)]
struct AppSummaryResponse {
    app_id: String,
    app_name: String,
    jwks_endpoint: String,
    jwks_cache_ttl_secs: u64,
    jwks_max_stale_secs: u64,
    allow_anonymous: bool,
    allow_demo: bool,
    status: AppStatus,
    worker: usize,
}

#[derive(Debug, Serialize)]
struct CreateAppResponse {
    app_id: String,
    app_name: String,
    jwks_endpoint: String,
    jwks_cache_ttl_secs: u64,
    jwks_max_stale_secs: u64,
    allow_anonymous: bool,
    allow_demo: bool,
    backend_secret: String,
    admin_secret: String,
    status: AppStatus,
    worker: usize,
}

#[derive(Debug, Serialize)]
struct UpdateAppResponse {
    app_id: String,
    app_name: String,
    jwks_endpoint: String,
    jwks_cache_ttl_secs: u64,
    jwks_max_stale_secs: u64,
    allow_anonymous: bool,
    allow_demo: bool,
    status: AppStatus,
    worker: usize,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
}

#[derive(Debug, Serialize)]
struct LinkExternalResponse {
    app_id: String,
    principal_id: String,
    issuer: String,
    subject: String,
    created: bool,
}

#[derive(Debug, Serialize)]
struct ManageAdminSecretResponse {
    app_id: String,
    admin_secret: Option<String>,
}

#[derive(Debug, Serialize)]
struct SchemaHashesResponse {
    hashes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PublishSchemaRequest {
    schema: Schema,
    permissions: Option<HashMap<TableName, TablePolicies>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishPermissionsRequest {
    schema_hash: String,
    permissions: HashMap<String, TablePolicies>,
    expected_parent_bundle_object_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublishSchemaResponse {
    object_id: String,
    hash: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionsHeadView {
    schema_hash: String,
    version: u64,
    parent_bundle_object_id: Option<String>,
    bundle_object_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionsHeadResponse {
    head: Option<PermissionsHeadView>,
}

pub async fn run(config: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let data_root = PathBuf::from(&config.data_root);
    std::fs::create_dir_all(data_root.join("apps"))?;

    let meta_store = Arc::new(
        MetaStore::new(&data_root, config.secret_hash_key)
            .map_err(|e| format!("failed to initialize meta store: {e}"))?,
    );

    #[cfg(feature = "otel")]
    let metrics = std::sync::Arc::new(crate::metrics::SyncMetrics::new());
    let workers = WorkerPool::new(
        config.worker_threads,
        #[cfg(feature = "otel")]
        metrics.clone(),
    );
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| format!("failed to initialize HTTP client: {e}"))?;

    let persisted_rows = meta_store
        .list_apps()
        .await
        .map_err(|e| format!("failed to load app registry from meta store: {e}"))?;

    let mut app_map = HashMap::new();
    for row in persisted_rows {
        let app_id = row.app_id;
        let app_config = app_config_from_row(&row);
        let app_dir = data_root.join("apps").join(app_id.to_string());
        let (sync_tx, _) = tokio::sync::broadcast::channel::<ClientSyncUpdate>(256);
        let send_seq_by_client: ClientSendSeqMap = Arc::new(Mutex::new(HashMap::new()));

        match workers
            .create_runtime(app_id, app_dir, sync_tx.clone(), send_seq_by_client.clone())
            .await
        {
            Ok(()) => {
                app_map.insert(
                    app_id,
                    AppEntry::new(
                        app_id,
                        row.object_id,
                        app_config,
                        sync_tx,
                        send_seq_by_client,
                    ),
                );
            }
            Err(err) => {
                warn!(
                    app_id = %app_id,
                    error = ?err,
                    "failed to rehydrate app runtime from meta store"
                );
            }
        }
    }

    let (shutdown_tx, _) = tokio::sync::watch::channel(false);

    let state = Arc::new(ServerState {
        apps: tokio::sync::RwLock::new(app_map),
        data_root,
        internal_api_secret: config.internal_api_secret,
        workers,
        meta_store,
        jwks_cache: tokio::sync::RwLock::new(HashMap::new()),
        http_client,
        shutdown_tx: shutdown_tx.clone(),
        #[cfg(feature = "otel")]
        metrics,
    });

    info!(
        catalogue_authority = match &config.catalogue_authority {
            CatalogueAuthorityMode::Local => "local",
            CatalogueAuthorityMode::Forward => "forward",
        },
        workers = state.workers.worker_count(),
        data_root = %state.data_root.display(),
        app_count = state.app_count().await,
        "starting multi-tenant Jazz server"
    );
    warn!(
        "TODO(security): JWT auth currently validates signatures only; add claim validation before production."
    );
    info!(
        username = MANAGEMENT_USERNAME,
        "management UI enabled at /manage (HTTP basic auth, password = JAZZ_INTERNAL_API_SECRET)"
    );

    let app = create_router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!("listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_tx))
        .await?;
    Ok(())
}

async fn shutdown_signal(shutdown_tx: tokio::sync::watch::Sender<bool>) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            let _ = signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    let _ = shutdown_tx.send(true);
    info!("shutdown signal received");
}

#[cfg(feature = "otel")]
async fn otel_http_metrics(
    axum::Extension(metrics): axum::Extension<std::sync::Arc<crate::metrics::SyncMetrics>>,
    matched_path: Option<axum::extract::MatchedPath>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let method = request.method().to_string();
    let route = matched_path
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let active_attrs = vec![
        opentelemetry::KeyValue::new("http.request.method", method.clone()),
        opentelemetry::KeyValue::new("http.route", route.clone()),
    ];
    metrics.http_active_requests.add(1, &active_attrs);

    let start = std::time::Instant::now();
    let response = next.run(request).await;
    let elapsed = start.elapsed().as_secs_f64();

    let status = response.status().as_u16().to_string();
    let attrs = vec![
        opentelemetry::KeyValue::new("http.request.method", method),
        opentelemetry::KeyValue::new("http.route", route),
        opentelemetry::KeyValue::new("http.response.status_code", status),
    ];
    metrics.http_request_duration.record(elapsed, &attrs);
    metrics.http_active_requests.add(-1, &active_attrs);

    response
}

fn create_router(state: Arc<ServerState>) -> Router {
    let router = Router::new()
        .route("/apps/:app_id/events", get(events_handler))
        .route("/apps/:app_id/sync", post(sync_handler))
        .route("/apps/:app_id/schema/:hash", get(schema_catalogue_handler))
        .route("/apps/:app_id/schemas", get(schema_hashes_handler))
        .route("/apps/:app_id/admin/schemas", post(publish_schema_handler))
        .route(
            "/apps/:app_id/admin/permissions/head",
            get(permissions_head_handler),
        )
        .route(
            "/apps/:app_id/admin/permissions",
            post(publish_permissions_handler),
        )
        .route(
            "/apps/:app_id/auth/link-external",
            post(link_external_handler),
        )
        .route(
            "/internal/apps",
            post(create_app_handler).get(list_apps_handler),
        )
        .route(
            "/internal/apps/:app_id",
            get(get_app_handler).patch(update_app_handler),
        )
        .route("/health", get(health_handler))
        .route("/manage", get(manage_page_handler))
        .route(
            "/manage/api/apps",
            get(manage_list_apps_handler).post(manage_create_app_handler),
        )
        .route(
            "/manage/api/apps/:app_id/status",
            post(manage_set_status_handler),
        )
        .route(
            "/manage/api/apps/:app_id/auth",
            patch(manage_update_auth_handler),
        )
        .route(
            "/manage/api/apps/:app_id/admin-secret",
            get(manage_get_admin_secret_handler),
        )
        .route(
            "/manage/api/apps/:app_id/admin-secret/rotate",
            post(manage_rotate_admin_secret_handler),
        );

    #[cfg(feature = "otel")]
    let router = router
        .layer(axum::Extension(state.metrics.clone()))
        .layer(axum::middleware::from_fn(otel_http_metrics));

    router
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

fn app_config_from_row(row: &MetaAppRow) -> AppConfig {
    AppConfig {
        app_name: row.app_name.clone(),
        jwks_endpoint: row.jwks_endpoint.clone(),
        jwks_cache_ttl_secs: row.jwks_cache_ttl_secs,
        jwks_max_stale_secs: row.jwks_max_stale_secs,
        allow_anonymous: row.allow_anonymous,
        allow_demo: row.allow_demo,
        backend_secret_hash: row.backend_secret_hash.clone(),
        admin_secret_hash: row.admin_secret_hash.clone(),
        status: row.status,
    }
}

fn now_timestamp_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros().min(u128::from(u64::MAX)) as u64,
        Err(_) => 0,
    }
}

fn parse_app_id(value: &str) -> Result<AppId, (StatusCode, String)> {
    AppId::from_string(value)
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("invalid app_id: {value}")))
}

fn parse_schema_hash(value: &str) -> Result<SchemaHash, (StatusCode, String)> {
    let decoded_hash_bytes = hex::decode(value).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "invalid schema hash: expected hex".to_string(),
        )
    })?;
    if decoded_hash_bytes.len() != 32 {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid schema hash: expected 64 hex chars".to_string(),
        ));
    }

    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&decoded_hash_bytes);
    Ok(SchemaHash::from_bytes(hash_bytes))
}

fn encode_frame(event: &ServerEvent) -> Bytes {
    let json = serde_json::to_vec(event).unwrap_or_default();
    let len = (json.len() as u32).to_be_bytes();
    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&len);
    buf.extend_from_slice(&json);
    Bytes::from(buf)
}

fn decode_session_header(b64: &str) -> Option<Session> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let json_str = std::str::from_utf8(&bytes).ok()?;
    serde_json::from_str(json_str).ok()
}

fn derive_local_principal_id(app_id: AppId, mode: LocalAuthMode, token: &str) -> String {
    let input = format!("{app_id}:{}:{token}", mode.as_str());
    let digest = Sha256::digest(input.as_bytes());
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
    format!("local:{encoded}")
}

fn derive_external_principal_id(app_id: AppId, issuer: &str, subject: &str) -> String {
    let input = format!("{app_id}:external:{issuer}:{subject}");
    let digest = Sha256::digest(input.as_bytes());
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
    format!("external:{encoded}")
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut diff: u8 = 0;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn resolve_external_session(
    state: &ServerState,
    app_id: AppId,
    verified: VerifiedJwt,
) -> Result<Session, (StatusCode, &'static str)> {
    let subject = verified.subject.trim();
    if subject.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "Invalid bearer token subject"));
    }

    let issuer = verified
        .issuer
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let principal_claim = verified
        .principal_id_claim
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

    let mapped_principal = if let Some(iss) = issuer {
        match state
            .meta_store
            .get_external_identity(app_id, iss, subject)
            .await
        {
            Ok(Some(row)) => Some(row.principal_id),
            Ok(None) => None,
            Err(err) => {
                warn!(
                    app_id = %app_id,
                    issuer = %iss,
                    subject = %subject,
                    error = %err,
                    "failed to resolve external identity mapping"
                );
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to resolve external identity",
                ));
            }
        }
    } else {
        None
    };

    match (principal_claim, mapped_principal.as_deref()) {
        (Some(claim), Some(mapped)) if claim != mapped => {
            warn!(
                app_id = %app_id,
                claim_principal = %claim,
                mapped_principal = %mapped,
                issuer = issuer.unwrap_or("<missing>"),
                subject = %subject,
                "external principal claim mismatches persisted identity mapping"
            );
            return Err((
                StatusCode::UNAUTHORIZED,
                "External identity mapping conflict",
            ));
        }
        _ => {}
    }

    let principal_id = if let Some(claim) = principal_claim {
        claim.to_string()
    } else if let Some(mapped) = mapped_principal {
        mapped
    } else if let Some(iss) = issuer {
        derive_external_principal_id(app_id, iss, subject)
    } else {
        subject.to_string()
    };

    let claims = match verified.claims {
        serde_json::Value::Object(mut map) => {
            map.insert("auth_mode".to_string(), serde_json::json!("external"));
            map.insert("subject".to_string(), serde_json::json!(subject));
            if let Some(iss) = issuer {
                map.insert("issuer".to_string(), serde_json::json!(iss));
            }
            serde_json::Value::Object(map)
        }
        other => serde_json::json!({
            "auth_mode": "external",
            "subject": subject,
            "issuer": issuer,
            "raw_claims": other,
        }),
    };

    Ok(Session {
        user_id: principal_id,
        claims,
    })
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    sub: String,
    #[serde(default)]
    iss: Option<String>,
    #[serde(default)]
    jazz_principal_id: Option<String>,
    #[serde(default = "default_session_claims")]
    claims: serde_json::Value,
}

fn default_session_claims() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

#[derive(Debug, Clone)]
struct VerifiedJwt {
    subject: String,
    issuer: Option<String>,
    principal_id_claim: Option<String>,
    claims: serde_json::Value,
}

#[derive(Debug)]
enum JwtVerificationError {
    Retryable(String),
    Fatal(String),
}

fn map_key_algorithm(alg: KeyAlgorithm) -> Option<Algorithm> {
    match alg {
        KeyAlgorithm::HS256 => Some(Algorithm::HS256),
        KeyAlgorithm::HS384 => Some(Algorithm::HS384),
        KeyAlgorithm::HS512 => Some(Algorithm::HS512),
        KeyAlgorithm::ES256 => Some(Algorithm::ES256),
        KeyAlgorithm::ES384 => Some(Algorithm::ES384),
        KeyAlgorithm::RS256 => Some(Algorithm::RS256),
        KeyAlgorithm::RS384 => Some(Algorithm::RS384),
        KeyAlgorithm::RS512 => Some(Algorithm::RS512),
        KeyAlgorithm::PS256 => Some(Algorithm::PS256),
        KeyAlgorithm::PS384 => Some(Algorithm::PS384),
        KeyAlgorithm::PS512 => Some(Algorithm::PS512),
        KeyAlgorithm::EdDSA => Some(Algorithm::EdDSA),
        KeyAlgorithm::RSA1_5 | KeyAlgorithm::RSA_OAEP | KeyAlgorithm::RSA_OAEP_256 => None,
    }
}

fn signature_only_validation(alg: Algorithm) -> Validation {
    let mut validation = Validation::new(alg);
    // TODO(security): MVP intentionally validates JWT signatures only.
    // Add exp/nbf/aud/iss/sub policy enforcement before production launch.
    validation.required_spec_claims.clear();
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.validate_aud = false;
    validation
}

fn select_jwk_candidates<'a>(jwks: &'a JwkSet, kid: Option<&str>, alg: Algorithm) -> Vec<&'a Jwk> {
    let mut candidates = Vec::new();

    for jwk in &jwks.keys {
        match kid {
            Some(expected_kid) if jwk.common.key_id.as_deref() != Some(expected_kid) => continue,
            _ => {}
        }

        if let Some(key_alg) = jwk.common.key_algorithm {
            match map_key_algorithm(key_alg) {
                Some(mapped_alg) if mapped_alg == alg => {}
                Some(_) | None => continue,
            }
        }

        candidates.push(jwk);
    }

    candidates
}

fn verify_jwt_signature_with_jwks(
    token: &str,
    jwks: &JwkSet,
) -> Result<VerifiedJwt, JwtVerificationError> {
    let header = decode_header(token)
        .map_err(|err| JwtVerificationError::Fatal(format!("invalid JWT header: {err}")))?;

    let candidates = select_jwk_candidates(jwks, header.kid.as_deref(), header.alg);
    if candidates.is_empty() {
        let reason = match header.kid.as_deref() {
            Some(kid) => format!("no JWKS key matched token kid '{kid}'"),
            None => "no compatible JWKS key found for token algorithm".to_string(),
        };
        return Err(JwtVerificationError::Retryable(reason));
    }

    let mut last_error = None;
    let validation = signature_only_validation(header.alg);

    for jwk in candidates {
        let decoding_key = match DecodingKey::from_jwk(jwk) {
            Ok(key) => key,
            Err(err) => {
                last_error = Some(format!("failed to build decoding key from JWK: {err}"));
                continue;
            }
        };

        match decode::<JwtClaims>(token, &decoding_key, &validation) {
            Ok(token_data) => {
                return Ok(VerifiedJwt {
                    subject: token_data.claims.sub,
                    issuer: token_data.claims.iss,
                    principal_id_claim: token_data.claims.jazz_principal_id,
                    claims: token_data.claims.claims,
                });
            }
            Err(err) => {
                last_error = Some(format!("JWT signature verification failed: {err}"));
            }
        }
    }

    Err(JwtVerificationError::Retryable(last_error.unwrap_or_else(
        || "JWT signature verification failed".to_string(),
    )))
}

async fn fetch_jwks(http_client: &reqwest::Client, jwks_endpoint: &str) -> Result<JwkSet, String> {
    let response = http_client
        .get(jwks_endpoint)
        .send()
        .await
        .map_err(|err| format!("JWKS request failed: {err}"))?;

    let status = response.status();
    if !status.is_success() {
        return Err(format!("JWKS endpoint returned status {status}"));
    }

    let jwks = response
        .json::<JwkSet>()
        .await
        .map_err(|err| format!("failed to parse JWKS response: {err}"))?;

    if jwks.keys.is_empty() {
        return Err("JWKS response contained no keys".to_string());
    }

    Ok(jwks)
}

async fn load_jwks_for_app(
    state: &ServerState,
    app_id: AppId,
    app_config: &AppConfig,
    force_refresh: bool,
) -> Result<JwkSet, String> {
    let jwks_endpoint = app_config.jwks_endpoint.as_str();
    let ttl_us = app_config
        .jwks_cache_ttl()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;
    let cooldown_us = JWKS_FORCED_REFRESH_COOLDOWN
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;

    // Single read-lock: check cooldown and attempt cache hit together.
    let (force_refresh, cached_jwks) = {
        let cache = state.jwks_cache.read().await;
        let cached = cache.get(&app_id).cloned();

        // Downgrade forced refresh if this app's cooldown is still active.
        let force_refresh = if force_refresh {
            match &cached {
                Some(entry) if entry.endpoint == jwks_endpoint => {
                    let age_us = now_timestamp_us().saturating_sub(entry.last_forced_refresh_us);
                    age_us > cooldown_us
                }
                None => true, // no cache entry → no cooldown → allow refresh
                Some(_) => true,
            }
        } else {
            false
        };

        (
            force_refresh,
            if force_refresh {
                None
            } else {
                cached.filter(|entry| entry.endpoint == jwks_endpoint)
            },
        )
    };

    if let Some(cached) = cached_jwks {
        let age_us = now_timestamp_us().saturating_sub(cached.fetched_at_us);
        if cached.endpoint == jwks_endpoint && age_us <= ttl_us {
            return Ok(cached.set);
        }
    }

    let max_stale_us = (app_config.jwks_cache_ttl() + app_config.jwks_max_stale())
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;

    let jwks = match fetch_jwks(&state.http_client, jwks_endpoint).await {
        Ok(jwks) => jwks,
        Err(e) => {
            // Stale-if-error: serve the cached keyset if it's not too old.
            if let Some(cached) = state.jwks_cache.read().await.get(&app_id) {
                let age_us = now_timestamp_us().saturating_sub(cached.fetched_at_us);
                if cached.endpoint == jwks_endpoint && age_us <= max_stale_us {
                    warn!(
                        app_id = %app_id,
                        error = %e,
                        "JWKS fetch failed, serving stale cached keyset"
                    );
                    return Ok(cached.set.clone());
                }
                warn!(
                    app_id = %app_id,
                    error = %e,
                    "JWKS fetch failed and stale keyset has expired"
                );
            }
            return Err(e);
        }
    };

    let now = now_timestamp_us();
    let mut cache = state.jwks_cache.write().await;
    let prev_forced_refresh = cache
        .get(&app_id)
        .filter(|cached| cached.endpoint == jwks_endpoint)
        .map(|c| c.last_forced_refresh_us)
        .unwrap_or(0);
    cache.insert(
        app_id,
        CachedJwks {
            endpoint: jwks_endpoint.to_string(),
            fetched_at_us: now,
            last_forced_refresh_us: if force_refresh {
                now
            } else {
                prev_forced_refresh
            },
            set: jwks.clone(),
        },
    );

    Ok(jwks)
}

async fn validate_jwt_with_jwks(
    state: &ServerState,
    app_id: AppId,
    app_config: &AppConfig,
    token: &str,
) -> Result<VerifiedJwt, (StatusCode, &'static str)> {
    if app_config.jwks_endpoint.trim().is_empty() {
        return Err((StatusCode::FORBIDDEN, "External auth disabled for app"));
    }

    let cached_jwks = load_jwks_for_app(state, app_id, app_config, false)
        .await
        .map_err(|err| {
            warn!(
                app_id = %app_id,
                endpoint = %app_config.jwks_endpoint,
                error = %err,
                "failed to load cached JWKS"
            );
            (StatusCode::UNAUTHORIZED, "Unable to load JWKS")
        })?;

    match verify_jwt_signature_with_jwks(token, &cached_jwks) {
        Ok(session) => return Ok(session),
        Err(JwtVerificationError::Fatal(err)) => {
            warn!(app_id = %app_id, error = %err, "JWT validation failed");
            return Err((StatusCode::UNAUTHORIZED, "Invalid bearer token"));
        }
        Err(JwtVerificationError::Retryable(err)) => {
            warn!(
                app_id = %app_id,
                error = %err,
                "JWT validation failed with cached JWKS; forcing one refresh"
            );
        }
    }

    let refreshed_jwks = load_jwks_for_app(state, app_id, app_config, true)
        .await
        .map_err(|err| {
            warn!(
                app_id = %app_id,
                endpoint = %app_config.jwks_endpoint,
                error = %err,
                "failed to refresh JWKS"
            );
            (StatusCode::UNAUTHORIZED, "Unable to refresh JWKS")
        })?;

    match verify_jwt_signature_with_jwks(token, &refreshed_jwks) {
        Ok(session) => Ok(session),
        Err(JwtVerificationError::Retryable(err)) | Err(JwtVerificationError::Fatal(err)) => {
            warn!(
                app_id = %app_id,
                error = %err,
                "JWT validation failed after JWKS refresh"
            );
            Err((StatusCode::UNAUTHORIZED, "Invalid bearer token"))
        }
    }
}

async fn extract_session(
    headers: &HeaderMap,
    app_id: AppId,
    app_config: &AppConfig,
    state: &ServerState,
) -> Result<Option<Session>, (StatusCode, &'static str)> {
    if let Some(session_b64) = headers.get("X-Jazz-Session").and_then(|v| v.to_str().ok()) {
        let backend_secret = headers
            .get("X-Jazz-Backend-Secret")
            .and_then(|v| v.to_str().ok());

        match backend_secret {
            Some(got)
                if state
                    .meta_store
                    .verify_secret(got, &app_config.backend_secret_hash) =>
            {
                let session = decode_session_header(session_b64)
                    .ok_or((StatusCode::BAD_REQUEST, "Invalid session format"))?;
                return Ok(Some(session));
            }
            Some(_) => {
                return Err((StatusCode::UNAUTHORIZED, "Invalid backend secret"));
            }
            None => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    "Backend secret required for session impersonation",
                ));
            }
        }
    }

    if let Some(token) = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|auth_value| auth_value.strip_prefix("Bearer "))
    {
        let token = token.trim();
        if token.is_empty() {
            return Err((StatusCode::UNAUTHORIZED, "Empty bearer token"));
        }

        let verified = validate_jwt_with_jwks(state, app_id, app_config, token).await?;
        let session = resolve_external_session(state, app_id, verified).await?;
        return Ok(Some(session));
    }

    let local_mode = headers.get(LOCAL_MODE_HEADER).and_then(|v| v.to_str().ok());
    let local_token = headers
        .get(LOCAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok());

    match (local_mode, local_token) {
        (Some(mode), Some(token)) => {
            let mode = LocalAuthMode::from_header(mode)
                .ok_or((StatusCode::BAD_REQUEST, "Invalid local auth mode"))?;
            if mode == LocalAuthMode::Anonymous && !app_config.allow_anonymous {
                return Err((StatusCode::FORBIDDEN, "Anonymous auth disabled for app"));
            }
            if mode == LocalAuthMode::Demo && !app_config.allow_demo {
                return Err((StatusCode::FORBIDDEN, "Demo auth disabled for app"));
            }
            let token = token.trim();
            if token.is_empty() {
                return Err((StatusCode::UNAUTHORIZED, "Empty local auth token"));
            }

            let principal_id = derive_local_principal_id(app_id, mode, token);
            return Ok(Some(Session {
                user_id: principal_id,
                claims: serde_json::json!({
                    "auth_mode": "local",
                    "local_mode": mode.as_str(),
                }),
            }));
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "Both X-Jazz-Local-Mode and X-Jazz-Local-Token are required",
            ));
        }
        (None, None) => {}
    }

    Ok(None)
}

fn validate_admin_secret(
    headers: &HeaderMap,
    app_config: &AppConfig,
    meta_store: &MetaStore,
) -> Result<bool, (StatusCode, &'static str)> {
    let provided = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());

    match provided {
        Some(got) if meta_store.verify_secret(got, &app_config.admin_secret_hash) => Ok(true),
        Some(_) => Err((StatusCode::UNAUTHORIZED, "Invalid admin secret")),
        None => Ok(false),
    }
}

fn validate_backend_secret(
    headers: &HeaderMap,
    app_config: &AppConfig,
    meta_store: &MetaStore,
) -> Result<bool, (StatusCode, &'static str)> {
    let provided = headers
        .get("X-Jazz-Backend-Secret")
        .and_then(|v| v.to_str().ok());

    match provided {
        Some(got) if meta_store.verify_secret(got, &app_config.backend_secret_hash) => Ok(true),
        Some(_) => Err((StatusCode::UNAUTHORIZED, "Invalid backend secret")),
        None => Ok(false),
    }
}

fn validate_internal_secret(
    headers: &HeaderMap,
    expected_secret: &str,
) -> Result<(), (StatusCode, &'static str)> {
    let provided = headers
        .get("X-Jazz-Internal-Secret")
        .and_then(|v| v.to_str().ok());

    match provided {
        Some(got) if got == expected_secret => Ok(()),
        _ => Err((StatusCode::UNAUTHORIZED, "Invalid internal API secret")),
    }
}

#[derive(Debug, Clone, Copy)]
enum ManageAuthError {
    Unauthorized,
}

impl ManageAuthError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::Unauthorized => management_basic_auth_challenge(),
        }
    }
}

fn build_internal_secret_headers(secret: &str) -> Result<HeaderMap, &'static str> {
    let secret_value = HeaderValue::from_str(secret)
        .map_err(|_| "configured internal API secret is invalid for HTTP header transport")?;

    let mut headers = HeaderMap::new();
    headers.insert("X-Jazz-Internal-Secret", secret_value);
    Ok(headers)
}

fn management_basic_auth_challenge() -> axum::response::Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            WWW_AUTHENTICATE,
            format!("Basic realm=\"{MANAGEMENT_BASIC_AUTH_REALM}\""),
        )],
        "Unauthorized",
    )
        .into_response()
}

fn authorize_management_request(
    headers: &HeaderMap,
    state: &ServerState,
) -> Result<(), ManageAuthError> {
    let encoded = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ManageAuthError::Unauthorized)?;

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| ManageAuthError::Unauthorized)?;
    let decoded = std::str::from_utf8(&decoded).map_err(|_| ManageAuthError::Unauthorized)?;
    let (username, password) = decoded
        .split_once(':')
        .ok_or(ManageAuthError::Unauthorized)?;

    let username_valid = constant_time_eq(username, MANAGEMENT_USERNAME);
    let password_valid = constant_time_eq(password, &state.internal_api_secret);
    if username_valid && password_valid {
        Ok(())
    } else {
        Err(ManageAuthError::Unauthorized)
    }
}

fn generate_secret() -> String {
    format!("{}{}", Uuid::now_v7().simple(), Uuid::new_v4().simple())
}

fn worker_dispatch_status_and_message(err: WorkerDispatchError) -> (StatusCode, String) {
    match err {
        WorkerDispatchError::QueueFull { worker } => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("worker {worker} queue is full"),
        ),
        WorkerDispatchError::WorkerClosed { worker } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("worker {worker} is closed"),
        ),
        WorkerDispatchError::WorkerUnavailable { worker } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("worker {worker} is unavailable"),
        ),
        WorkerDispatchError::RuntimeError { worker, message } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("worker {worker} runtime error: {message}"),
        ),
    }
}

async fn app_summary(app: Arc<AppEntry>, worker: usize) -> AppSummaryResponse {
    let cfg = app.config.read().await;
    AppSummaryResponse {
        app_id: app.app_id.to_string(),
        app_name: cfg.app_name.clone(),
        jwks_endpoint: cfg.jwks_endpoint.clone(),
        jwks_cache_ttl_secs: cfg.jwks_cache_ttl_secs,
        jwks_max_stale_secs: cfg.jwks_max_stale_secs,
        allow_anonymous: cfg.allow_anonymous,
        allow_demo: cfg.allow_demo,
        status: cfg.status,
        worker,
    }
}

async fn manage_page_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = authorize_management_request(&headers, &state) {
        return err.into_response();
    }

    Html(MANAGEMENT_PAGE_HTML).into_response()
}

async fn manage_list_apps_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = authorize_management_request(&headers, &state) {
        return err.into_response();
    }

    let internal_headers = match build_internal_secret_headers(&state.internal_api_secret) {
        Ok(headers) => headers,
        Err(msg) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(msg)),
            )
                .into_response();
        }
    };

    list_apps_handler(State(state), internal_headers)
        .await
        .into_response()
}

async fn manage_create_app_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<CreateAppRequest>,
) -> impl IntoResponse {
    if let Err(err) = authorize_management_request(&headers, &state) {
        return err.into_response();
    }

    let internal_headers = match build_internal_secret_headers(&state.internal_api_secret) {
        Ok(headers) => headers,
        Err(msg) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(msg)),
            )
                .into_response();
        }
    };

    create_app_handler(State(state), internal_headers, Json(request))
        .await
        .into_response()
}

async fn manage_set_status_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Json(request): Json<ManageSetStatusRequest>,
) -> impl IntoResponse {
    if let Err(err) = authorize_management_request(&headers, &state) {
        return err.into_response();
    }

    let internal_headers = match build_internal_secret_headers(&state.internal_api_secret) {
        Ok(headers) => headers,
        Err(msg) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(msg)),
            )
                .into_response();
        }
    };

    update_app_handler(
        State(state),
        AxumPath(path),
        internal_headers,
        Json(UpdateAppRequest {
            app_name: None,
            jwks_endpoint: None,
            jwks_cache_ttl_secs: None,
            jwks_max_stale_secs: None,
            allow_anonymous: None,
            allow_demo: None,
            status: Some(request.status),
            rotate_backend_secret: None,
            rotate_admin_secret: None,
        }),
    )
    .await
    .into_response()
}

async fn manage_update_auth_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Json(request): Json<ManageUpdateAuthRequest>,
) -> impl IntoResponse {
    if let Err(err) = authorize_management_request(&headers, &state) {
        return err.into_response();
    }

    let internal_headers = match build_internal_secret_headers(&state.internal_api_secret) {
        Ok(headers) => headers,
        Err(msg) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(msg)),
            )
                .into_response();
        }
    };

    update_app_handler(
        State(state),
        AxumPath(path),
        internal_headers,
        Json(UpdateAppRequest {
            app_name: None,
            jwks_endpoint: request.jwks_endpoint,
            jwks_cache_ttl_secs: request.jwks_cache_ttl_secs,
            jwks_max_stale_secs: request.jwks_max_stale_secs,
            allow_anonymous: request.allow_anonymous,
            allow_demo: request.allow_demo,
            status: None,
            rotate_backend_secret: None,
            rotate_admin_secret: None,
        }),
    )
    .await
    .into_response()
}

async fn manage_get_admin_secret_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = authorize_management_request(&headers, &state) {
        return err.into_response();
    }

    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let row = match state.meta_store.get_by_app_id(app_id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(err)),
            )
                .into_response();
        }
    };

    Json(ManageAdminSecretResponse {
        app_id: app_id.to_string(),
        admin_secret: row.admin_secret,
    })
    .into_response()
}

async fn manage_rotate_admin_secret_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = authorize_management_request(&headers, &state) {
        return err.into_response();
    }

    let internal_headers = match build_internal_secret_headers(&state.internal_api_secret) {
        Ok(headers) => headers,
        Err(msg) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(msg)),
            )
                .into_response();
        }
    };

    update_app_handler(
        State(state),
        AxumPath(path),
        internal_headers,
        Json(UpdateAppRequest {
            app_name: None,
            jwks_endpoint: None,
            jwks_cache_ttl_secs: None,
            jwks_max_stale_secs: None,
            allow_anonymous: None,
            allow_demo: None,
            status: None,
            rotate_backend_secret: None,
            rotate_admin_secret: Some(true),
        }),
    )
    .await
    .into_response()
}

async fn events_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Query(params): Query<EventsParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let app_id = parse_app_id(&path.app_id)?;
    let worker = state.workers.worker_for_app(&app_id);
    let app = state.get_app(app_id).await.ok_or((
        StatusCode::NOT_FOUND,
        format!("unknown app_id: {}", path.app_id),
    ))?;

    let cfg = app.config.read().await.clone();
    if cfg.status == AppStatus::Disabled {
        return Err((
            StatusCode::FORBIDDEN,
            "app is disabled for sync traffic".to_string(),
        ));
    }

    let is_backend = validate_backend_secret(&headers, &cfg, &state.meta_store)
        .map_err(|(status, msg)| (status, msg.to_string()))?;
    let has_session_header = headers.get("X-Jazz-Session").is_some();

    let client_id = match params.client_id {
        Some(s) => ClientId::parse(&s)
            .ok_or((StatusCode::BAD_REQUEST, format!("invalid client_id: {s}")))?,
        None => ClientId::new(),
    };

    if is_backend && !has_session_header {
        if let Err(err) = state
            .workers
            .ensure_client_as_backend(app_id, client_id)
            .await
        {
            return Err(worker_dispatch_status_and_message(err));
        }
    } else {
        let session = extract_session(&headers, app_id, &cfg, &state)
            .await
            .map_err(|(status, msg)| (status, msg.to_string()))?
            .ok_or((
                StatusCode::UNAUTHORIZED,
                "session required for event stream".to_string(),
            ))?;

        if let Err(err) = state
            .workers
            .ensure_client_with_session(app_id, client_id, session)
            .await
        {
            return Err(worker_dispatch_status_and_message(err));
        }
    }

    let catalogue_state_hash = match state.workers.get_catalogue_state_hash(app_id).await {
        Ok(hash) => Some(hash),
        Err(err) => {
            warn!(
                app_id = %app_id,
                worker,
                client_id = %client_id,
                ?err,
                "failed to read catalogue state hash for events handshake"
            );
            None
        }
    };

    let connection_id = app.next_connection_id.fetch_add(1, Ordering::SeqCst);
    {
        let mut connections = app.connections.write().await;
        connections.insert(
            connection_id,
            ConnectionState {
                _client_id: client_id,
            },
        );
    }

    // New stream connection defines a fresh sequencing epoch for this client.
    {
        let mut seqs = app
            .send_seq_by_client
            .lock()
            .expect("send sequence mutex poisoned");
        seqs.insert(client_id, 0);
    }

    info!(
        app_id = %app_id,
        worker,
        client_id = %client_id,
        connection_id,
        "events stream connected"
    );

    #[cfg(feature = "otel")]
    {
        let attrs = vec![
            opentelemetry::KeyValue::new("app_id", app_id.to_string()),
            opentelemetry::KeyValue::new("env", "prod".to_string()),
            opentelemetry::KeyValue::new("worker", worker as i64),
        ];
        state.metrics.connections_active.add(1, &attrs);
        state.metrics.connections_total.add(1, &attrs);
    }

    #[cfg(feature = "otel")]
    let connect_attrs = vec![
        opentelemetry::KeyValue::new("app_id", app_id.to_string()),
        opentelemetry::KeyValue::new("env", "prod".to_string()),
        opentelemetry::KeyValue::new("worker", worker as i64),
    ];

    #[cfg(feature = "otel")]
    let otel_metrics = state.metrics.clone();
    let mut sync_rx = app.sync_broadcast.subscribe();
    let mut shutdown_rx = state.shutdown_tx.subscribe();
    let app_cleanup = app.clone();
    let client_id_str = client_id.to_string();
    let next_sync_seq = 1u64;
    #[cfg(feature = "otel")]
    let otel_env = "prod".to_string();
    #[cfg(feature = "otel")]
    let otel_app_id = app_id.to_string();
    let stream = async_stream::stream! {
        // Guard lives inside the stream — dropped when the stream is dropped
        // (client disconnect), ensuring the active counter always decrements.
        #[cfg(feature = "otel")]
        let _conn_guard = crate::metrics::ConnectionMetricsGuard {
            metrics: otel_metrics.clone(),
            attrs: connect_attrs.clone(),
        };

        let connected = ServerEvent::Connected {
            connection_id: ConnectionId(connection_id),
            client_id: client_id_str,
            next_sync_seq: Some(next_sync_seq),
            catalogue_state_hash: catalogue_state_hash.clone(),
        };
        yield Ok::<Bytes, std::convert::Infallible>(encode_frame(&connected));

        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                result = sync_rx.recv() => {
                    match result {
                        Ok((target_client_id, seq, payload)) => {
                            if target_client_id == client_id {
                                #[cfg(feature = "otel")]
                                {
                                    match &payload {
                                        SyncPayload::PersistenceAck { tier, .. } => {
                                            otel_metrics.persistence_acks_total.add(1, &[
                                                opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                                                opentelemetry::KeyValue::new("env", otel_env.clone()),
                                                opentelemetry::KeyValue::new("tier", tier.as_str()),
                                            ]);
                                        }
                                        SyncPayload::QuerySettled { tier, .. } => {
                                            otel_metrics.query_settled_total.add(1, &[
                                                opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                                                opentelemetry::KeyValue::new("env", otel_env.clone()),
                                                opentelemetry::KeyValue::new("tier", tier.as_str()),
                                            ]);
                                        }
                                        SyncPayload::Error(_err) => {
                                            otel_metrics.errors_total.add(1, &[
                                                opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                                                opentelemetry::KeyValue::new("env", otel_env.clone()),
                                                opentelemetry::KeyValue::new("direction", "outbound"),
                                            ]);
                                        }
                                        SyncPayload::SchemaWarning(_) => {
                                            otel_metrics.schema_warnings_total.add(1, &[
                                                opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                                                opentelemetry::KeyValue::new("env", otel_env.clone()),
                                            ]);
                                        }
                                        _ => {}
                                    }
                                }
                                #[cfg(feature = "otel")]
                                let pt = payload_type_name(&payload);
                                let event = ServerEvent::SyncUpdate {
                                    seq: Some(seq),
                                    payload: Box::new(payload),
                                };
                                let frame = encode_frame(&event);
                                #[cfg(feature = "otel")]
                                {
                                    let attrs = [
                                        opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                                        opentelemetry::KeyValue::new("env", otel_env.clone()),
                                        opentelemetry::KeyValue::new("payload_type", pt),
                                        opentelemetry::KeyValue::new("direction", "outbound"),
                                    ];
                                    otel_metrics.messages_sent.add(1, &attrs);
                                    otel_metrics.message_size_bytes.record(frame.len() as f64, &attrs);
                                }
                                yield Ok(frame);
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            warn!(app_id = %app_id, connection_id, "events stream lagged");
                            #[cfg(feature = "otel")]
                            {
                                otel_metrics.broadcast_lag_events.add(1, &[
                                    opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                                    opentelemetry::KeyValue::new("worker", worker as i64),
                                ]);
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
                _ = heartbeat_interval.tick() => {
                    yield Ok(encode_frame(&ServerEvent::Heartbeat));
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }

        let mut connections = app_cleanup.connections.write().await;
        connections.remove(&connection_id);
        // Note: active connection decrement is handled by ConnectionMetricsGuard (Drop)
    };

    Ok(axum::response::Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header("Transfer-Encoding", "chunked")
        .header("Cache-Control", "no-cache")
        .body(axum::body::Body::from_stream(stream))
        .unwrap())
}

#[cfg(feature = "otel")]
fn payload_type_name(payload: &jazz_tools::sync_manager::SyncPayload) -> &'static str {
    use jazz_tools::sync_manager::SyncPayload;
    match payload {
        SyncPayload::ObjectUpdated { .. } => "ObjectUpdated",
        SyncPayload::ObjectTruncated { .. } => "ObjectTruncated",
        SyncPayload::QuerySubscription { .. } => "QuerySubscription",
        SyncPayload::QueryUnsubscription { .. } => "QueryUnsubscription",
        SyncPayload::PersistenceAck { .. } => "PersistenceAck",
        SyncPayload::QuerySettled { .. } => "QuerySettled",
        SyncPayload::SchemaWarning(_) => "SchemaWarning",
        SyncPayload::Error(_) => "Error",
    }
}

async fn sync_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Json(request): Json<SyncBatchRequest>,
) -> impl IntoResponse {
    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let cfg = app.config.read().await.clone();
    if cfg.status == AppStatus::Disabled {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::forbidden("app is disabled for sync traffic")),
        )
            .into_response();
    }

    let is_admin = match validate_admin_secret(&headers, &cfg, &state.meta_store) {
        Ok(value) => value,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    };
    let is_backend = match validate_backend_secret(&headers, &cfg, &state.meta_store) {
        Ok(value) => value,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    };
    let has_session_header = headers.get("X-Jazz-Session").is_some();

    // Resolve session once for the whole batch (only needed for non-admin/non-backend).
    let session = if !(is_admin || is_backend && !has_session_header) {
        match extract_session(&headers, app_id, &cfg, &state).await {
            Ok(Some(session)) => Some(session),
            Ok(None) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse::unauthorized(
                        "session required for sync. provide JWT or backend secret",
                    )),
                )
                    .into_response();
            }
            Err((status, msg)) => {
                return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
            }
        }
    } else {
        None
    };

    // Apply each payload in order, collecting per-payload results.
    let mut results = Vec::with_capacity(request.payloads.len());

    #[cfg(feature = "otel")]
    let handler_start = std::time::Instant::now();
    #[cfg(feature = "otel")]
    let otel_app_id = app_id.to_string();

    #[cfg(feature = "otel")]
    if let Some(content_length) = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<f64>().ok())
    {
        state.metrics.message_size_bytes.record(
            content_length,
            &[
                opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                opentelemetry::KeyValue::new("env", "prod"),
                opentelemetry::KeyValue::new("direction", "inbound"),
            ],
        );
    }

    for payload in request.payloads {
        #[cfg(feature = "otel")]
        {
            match &payload {
                SyncPayload::QuerySubscription { .. } => {
                    state.metrics.subscriptions_total.add(
                        1,
                        &[
                            opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                            opentelemetry::KeyValue::new("env", "prod"),
                        ],
                    );
                }
                SyncPayload::Error(_err) => {
                    state.metrics.errors_total.add(
                        1,
                        &[
                            opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                            opentelemetry::KeyValue::new("env", "prod"),
                            opentelemetry::KeyValue::new("direction", "inbound"),
                        ],
                    );
                }
                SyncPayload::SchemaWarning(_) => {
                    state.metrics.schema_warnings_total.add(
                        1,
                        &[
                            opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                            opentelemetry::KeyValue::new("env", "prod"),
                        ],
                    );
                }
                _ => {}
            }
        }

        #[cfg(feature = "otel")]
        {
            let pt = payload_type_name(&payload);
            let attrs = [
                opentelemetry::KeyValue::new("app_id", otel_app_id.clone()),
                opentelemetry::KeyValue::new("env", "prod"),
                opentelemetry::KeyValue::new("payload_type", pt),
                opentelemetry::KeyValue::new("direction", "inbound"),
            ];
            state.metrics.messages_received.add(1, &attrs);
        }

        let dispatch_result = if is_admin {
            state
                .workers
                .sync_as_admin(app_id, request.client_id, payload)
                .await
        } else if is_backend && !has_session_header {
            state
                .workers
                .sync_as_backend(app_id, request.client_id, payload)
                .await
        } else {
            state
                .workers
                .sync_as_session(
                    app_id,
                    request.client_id,
                    session.clone().expect("session resolved above"),
                    payload,
                )
                .await
        };

        match dispatch_result {
            Ok(()) => results.push(SyncPayloadResult {
                ok: true,
                error: None,
            }),
            Err(err) => {
                let (_status, message) = worker_dispatch_status_and_message(err);
                results.push(SyncPayloadResult {
                    ok: false,
                    error: Some(message),
                });
            }
        }
    }

    #[cfg(feature = "otel")]
    {
        let elapsed = handler_start.elapsed().as_secs_f64() * 1000.0;
        state.metrics.handler_duration_ms.record(
            elapsed,
            &[
                opentelemetry::KeyValue::new("app_id", otel_app_id),
                opentelemetry::KeyValue::new("env", "prod"),
            ],
        );
    }

    Json(SyncBatchResponse { results }).into_response()
}

fn permissions_head_view(head: PermissionsHeadSummary) -> PermissionsHeadView {
    PermissionsHeadView {
        schema_hash: head.schema_hash.to_string(),
        version: head.version,
        parent_bundle_object_id: head
            .parent_bundle_object_id
            .map(|object_id: ObjectId| object_id.to_string()),
        bundle_object_id: head.bundle_object_id.to_string(),
    }
}

async fn publish_schema_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Json(request): Json<PublishSchemaRequest>,
) -> impl IntoResponse {
    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let cfg = app.config.read().await.clone();
    match validate_admin_secret(&headers, &cfg, &state.meta_store) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::unauthorized("admin secret required")),
            )
                .into_response();
        }
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    };

    if request.permissions.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(
                "inline permissions are no longer supported; publish permissions separately",
            )),
        )
            .into_response();
    }

    let schema_hash = SchemaHash::compute(&request.schema);
    match state.workers.publish_schema(app_id, request.schema).await {
        Ok(object_id) => (
            StatusCode::CREATED,
            Json(PublishSchemaResponse {
                object_id: object_id.to_string(),
                hash: schema_hash.to_string(),
            }),
        )
            .into_response(),
        Err(err) => {
            let (status, message) = worker_dispatch_status_and_message(err);
            (status, Json(ErrorResponse::internal(message))).into_response()
        }
    }
}

async fn permissions_head_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let cfg = app.config.read().await.clone();
    match validate_admin_secret(&headers, &cfg, &state.meta_store) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::unauthorized("admin secret required")),
            )
                .into_response();
        }
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    };

    match state.workers.get_permissions_head(app_id).await {
        Ok(head) => Json(PermissionsHeadResponse {
            head: head.map(permissions_head_view),
        })
        .into_response(),
        Err(err) => {
            let (status, message) = worker_dispatch_status_and_message(err);
            (status, Json(ErrorResponse::internal(message))).into_response()
        }
    }
}

async fn publish_permissions_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Json(request): Json<PublishPermissionsRequest>,
) -> impl IntoResponse {
    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let cfg = app.config.read().await.clone();
    match validate_admin_secret(&headers, &cfg, &state.meta_store) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::unauthorized("admin secret required")),
            )
                .into_response();
        }
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    };

    let schema_hash = match parse_schema_hash(&request.schema_hash) {
        Ok(hash) => hash,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let mut permissions = HashMap::new();
    for (table_name, policies) in request.permissions {
        permissions.insert(TableName::new(&table_name), policies);
    }

    let expected_parent_bundle_object_id = match request.expected_parent_bundle_object_id {
        Some(object_id) => match Uuid::parse_str(&object_id) {
            Ok(uuid) => Some(ObjectId::from_uuid(uuid)),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse::bad_request(
                        "invalid expectedParentBundleObjectId",
                    )),
                )
                    .into_response();
            }
        },
        None => None,
    };

    match state
        .workers
        .publish_permissions(
            app_id,
            schema_hash,
            permissions,
            expected_parent_bundle_object_id,
        )
        .await
    {
        Ok(head) => (
            StatusCode::CREATED,
            Json(PermissionsHeadResponse {
                head: head.map(permissions_head_view),
            }),
        )
            .into_response(),
        Err(WorkerDispatchError::RuntimeError { message, .. })
            if message.contains("stale parent") =>
        {
            (
                StatusCode::CONFLICT,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response()
        }
        Err(err) => {
            let (status, message) = worker_dispatch_status_and_message(err);
            (status, Json(ErrorResponse::internal(message))).into_response()
        }
    }
}

async fn schema_catalogue_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<SchemaPath>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let cfg = app.config.read().await.clone();
    match validate_admin_secret(&headers, &cfg, &state.meta_store) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::unauthorized("admin secret required")),
            )
                .into_response();
        }
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    };

    let schema_hash = match parse_schema_hash(&path.hash) {
        Ok(h) => h,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    match state
        .workers
        .get_catalogue_schema(app_id, schema_hash)
        .await
    {
        Ok(Some(schema)) => Json(schema).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found("schema catalogue not found")),
        )
            .into_response(),
        Err(err) => {
            let (status, message) = worker_dispatch_status_and_message(err);
            (status, Json(ErrorResponse::internal(message))).into_response()
        }
    }
}

async fn schema_hashes_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let cfg = app.config.read().await.clone();
    match validate_admin_secret(&headers, &cfg, &state.meta_store) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::unauthorized("admin secret required")),
            )
                .into_response();
        }
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    };

    match state.workers.get_schema_hashes(app_id).await {
        Ok(schema_hashes) => Json(SchemaHashesResponse {
            hashes: schema_hashes,
        })
        .into_response(),
        Err(err) => {
            let (status, message) = worker_dispatch_status_and_message(err);
            (status, Json(ErrorResponse::internal(message))).into_response()
        }
    }
}

async fn create_app_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<CreateAppRequest>,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_internal_secret(&headers, &state.internal_api_secret) {
        return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
    }

    let app_name = request.app_name.trim().to_string();
    if app_name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request("app_name is required")),
        )
            .into_response();
    }
    let jwks_endpoint = request.jwks_endpoint.unwrap_or_default().trim().to_string();
    let jwks_cache_ttl_secs = request
        .jwks_cache_ttl_secs
        .unwrap_or(DEFAULT_JWKS_CACHE_TTL_SECS);
    let jwks_max_stale_secs = request
        .jwks_max_stale_secs
        .unwrap_or(DEFAULT_JWKS_MAX_STALE_SECS);

    let backend_secret = request.backend_secret.unwrap_or_else(generate_secret);
    let admin_secret = request.admin_secret.unwrap_or_else(generate_secret);
    let allow_anonymous = request.allow_anonymous.unwrap_or(true);
    let allow_demo = request.allow_demo.unwrap_or(true);

    let backend_secret_hash = state.meta_store.hash_secret(&backend_secret);
    let admin_secret_hash = state.meta_store.hash_secret(&admin_secret);

    let app_id = loop {
        let candidate = AppId::random();
        if state.apps.read().await.contains_key(&candidate) {
            continue;
        }
        break candidate;
    };

    let meta_row = match state
        .meta_store
        .create_app(
            app_id,
            app_name,
            jwks_endpoint,
            jwks_cache_ttl_secs,
            jwks_max_stale_secs,
            allow_anonymous,
            allow_demo,
            backend_secret_hash,
            admin_secret_hash,
            AppStatus::Active,
            Some(admin_secret.clone()),
        )
        .await
    {
        Ok(row) => row,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(err)),
            )
                .into_response();
        }
    };

    let app_config = app_config_from_row(&meta_row);
    let data_dir = state.app_data_dir(app_id);
    let (sync_tx, _) = tokio::sync::broadcast::channel::<ClientSyncUpdate>(256);
    let send_seq_by_client: ClientSendSeqMap = Arc::new(Mutex::new(HashMap::new()));

    if let Err(err) = state
        .workers
        .create_runtime(
            app_id,
            data_dir,
            sync_tx.clone(),
            send_seq_by_client.clone(),
        )
        .await
    {
        let _ = state.meta_store.delete_app(meta_row.object_id).await;
        let (status, message) = worker_dispatch_status_and_message(err);
        return (status, Json(ErrorResponse::internal(message))).into_response();
    }

    let app_entry = AppEntry::new(
        app_id,
        meta_row.object_id,
        app_config,
        sync_tx,
        send_seq_by_client,
    );

    if state.apps.write().await.insert(app_id, app_entry).is_some() {
        let _ = state.meta_store.delete_app(meta_row.object_id).await;
        return (
            StatusCode::CONFLICT,
            Json(ErrorResponse::bad_request(
                "app runtime already exists for generated app id",
            )),
        )
            .into_response();
    }

    let worker = state.workers.worker_for_app(&app_id);

    Json(CreateAppResponse {
        app_id: app_id.to_string(),
        app_name: meta_row.app_name,
        jwks_endpoint: meta_row.jwks_endpoint,
        jwks_cache_ttl_secs: meta_row.jwks_cache_ttl_secs,
        jwks_max_stale_secs: meta_row.jwks_max_stale_secs,
        allow_anonymous: meta_row.allow_anonymous,
        allow_demo: meta_row.allow_demo,
        backend_secret,
        admin_secret,
        status: meta_row.status,
        worker,
    })
    .into_response()
}

async fn list_apps_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_internal_secret(&headers, &state.internal_api_secret) {
        return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
    }

    let apps: Vec<Arc<AppEntry>> = state.apps.read().await.values().cloned().collect();
    let mut response = Vec::with_capacity(apps.len());

    for app in apps {
        let worker = state.workers.worker_for_app(&app.app_id);
        response.push(app_summary(app, worker).await);
    }

    Json(response).into_response()
}

async fn get_app_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_internal_secret(&headers, &state.internal_api_secret) {
        return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
    }

    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let worker = state.workers.worker_for_app(&app_id);
    Json(app_summary(app, worker).await).into_response()
}

async fn update_app_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Json(request): Json<UpdateAppRequest>,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_internal_secret(&headers, &state.internal_api_secret) {
        return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
    }

    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let mut row = match state.meta_store.get_by_app_id(app_id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(err)),
            )
                .into_response();
        }
    };

    if row.object_id != app.meta_object_id {
        warn!(
            app_id = %app_id,
            expected = %app.meta_object_id,
            actual = %row.object_id,
            "meta object id mismatch for app; using runtime's known object id"
        );
        row.object_id = app.meta_object_id;
    }

    let mut new_backend_secret = None;
    let mut new_admin_secret = None;

    if let Some(app_name) = request.app_name {
        row.app_name = app_name;
    }
    if let Some(jwks_endpoint) = request.jwks_endpoint {
        row.jwks_endpoint = jwks_endpoint;
    }
    if let Some(jwks_cache_ttl_secs) = request.jwks_cache_ttl_secs {
        row.jwks_cache_ttl_secs = jwks_cache_ttl_secs;
    }
    if let Some(jwks_max_stale_secs) = request.jwks_max_stale_secs {
        row.jwks_max_stale_secs = jwks_max_stale_secs;
    }
    if let Some(allow_anonymous) = request.allow_anonymous {
        row.allow_anonymous = allow_anonymous;
    }
    if let Some(allow_demo) = request.allow_demo {
        row.allow_demo = allow_demo;
    }
    if let Some(status) = request.status {
        row.status = status;
    }
    if request.rotate_backend_secret.unwrap_or(false) {
        let secret = generate_secret();
        row.backend_secret_hash = state.meta_store.hash_secret(&secret);
        new_backend_secret = Some(secret);
    }
    if request.rotate_admin_secret.unwrap_or(false) {
        let secret = generate_secret();
        row.admin_secret_hash = state.meta_store.hash_secret(&secret);
        row.admin_secret = Some(secret.clone());
        new_admin_secret = Some(secret);
    }
    row.updated_at = now_timestamp_us().max(row.created_at);

    if let Err(err) = state.meta_store.update_app(&row).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(err)),
        )
            .into_response();
    }

    {
        let mut cfg = app.config.write().await;
        *cfg = app_config_from_row(&row);
    }

    let worker = state.workers.worker_for_app(&app_id);

    Json(UpdateAppResponse {
        app_id: app_id.to_string(),
        app_name: row.app_name,
        jwks_endpoint: row.jwks_endpoint,
        jwks_cache_ttl_secs: row.jwks_cache_ttl_secs,
        jwks_max_stale_secs: row.jwks_max_stale_secs,
        allow_anonymous: row.allow_anonymous,
        allow_demo: row.allow_demo,
        status: row.status,
        worker,
        backend_secret: new_backend_secret,
        admin_secret: new_admin_secret,
    })
    .into_response()
}

async fn link_external_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let cfg = app.config.read().await.clone();

    let local_mode = headers.get(LOCAL_MODE_HEADER).and_then(|v| v.to_str().ok());
    let local_token = headers
        .get(LOCAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok());
    let (mode, token) = match (local_mode, local_token) {
        (Some(mode), Some(token)) => (mode, token.trim()),
        (Some(_), None) | (None, Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(
                    "Both X-Jazz-Local-Mode and X-Jazz-Local-Token are required",
                )),
            )
                .into_response();
        }
        (None, None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(
                    "Local auth headers are required for link-external",
                )),
            )
                .into_response();
        }
    };

    let mode = match LocalAuthMode::from_header(mode) {
        Some(mode) => mode,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request("Invalid local auth mode")),
            )
                .into_response();
        }
    };

    if token.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request("Empty local auth token")),
        )
            .into_response();
    }

    if mode == LocalAuthMode::Anonymous && !cfg.allow_anonymous {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::unauthorized(
                "Anonymous auth disabled for app",
            )),
        )
            .into_response();
    }
    if mode == LocalAuthMode::Demo && !cfg.allow_demo {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::unauthorized("Demo auth disabled for app")),
        )
            .into_response();
    }

    let auth_value = match headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        Some(value) => value,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(
                    "Authorization bearer token is required",
                )),
            )
                .into_response();
        }
    };

    let token_bearer = match auth_value.strip_prefix("Bearer ") {
        Some(token) if !token.trim().is_empty() => token.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(
                    "Invalid Authorization header format",
                )),
            )
                .into_response();
        }
    };

    let verified = match validate_jwt_with_jwks(&state, app_id, &cfg, token_bearer).await {
        Ok(verified) => verified,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    };

    let issuer = match verified.issuer.as_deref().map(str::trim) {
        Some(iss) if !iss.is_empty() => iss.to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(
                    "JWT issuer (iss) is required for link-external",
                )),
            )
                .into_response();
        }
    };
    let subject = verified.subject.trim().to_string();
    if subject.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request("JWT subject (sub) is required")),
        )
            .into_response();
    }

    let local_principal_id = derive_local_principal_id(app_id, mode, token);

    match verified
        .principal_id_claim
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        Some(claim_principal) if claim_principal != local_principal_id => {
            return (
                StatusCode::CONFLICT,
                Json(ErrorResponse::bad_request(
                    "JWT jazz_principal_id claim does not match local principal",
                )),
            )
                .into_response();
        }
        _ => {}
    }

    let mut created = false;

    match state
        .meta_store
        .get_external_identity(app_id, &issuer, &subject)
        .await
    {
        Ok(Some(row)) => {
            if row.principal_id != local_principal_id {
                return (
                    StatusCode::CONFLICT,
                    Json(ErrorResponse::bad_request(
                        "external identity is already linked to a different principal",
                    )),
                )
                    .into_response();
            }
        }
        Ok(None) => {
            if let Err(err) = state
                .meta_store
                .create_external_identity(app_id, &issuer, &subject, &local_principal_id)
                .await
            {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::internal(err)),
                )
                    .into_response();
            }
            created = true;
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(err)),
            )
                .into_response();
        }
    }

    Json(LinkExternalResponse {
        app_id: app_id.to_string(),
        principal_id: local_principal_id,
        issuer,
        subject,
        created,
    })
    .into_response()
}

async fn health_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "apps": state.app_count().await,
        "workers": state.workers.worker_count(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn fair_app_queue_round_robins_apps_with_quantum_one() {
        let app_a = AppId::from_name("app-a");
        let app_b = AppId::from_name("app-b");
        let mut queue = FairAppQueue::default();

        queue.push(app_a, "a1");
        queue.push(app_a, "a2");
        queue.push(app_b, "b1");
        queue.push(app_b, "b2");

        assert_eq!(queue.pop_batch(1), Some(vec!["a1"]));
        assert_eq!(queue.pop_batch(1), Some(vec!["b1"]));
        assert_eq!(queue.pop_batch(1), Some(vec!["a2"]));
        assert_eq!(queue.pop_batch(1), Some(vec!["b2"]));
        assert_eq!(queue.pop_batch(1), None);
    }

    #[test]
    fn fair_app_queue_honors_quantum_then_requeues_app() {
        let app_a = AppId::from_name("app-a");
        let app_b = AppId::from_name("app-b");
        let mut queue = FairAppQueue::default();

        queue.push(app_a, 1);
        queue.push(app_a, 2);
        queue.push(app_a, 3);
        queue.push(app_b, 9);
        queue.push(app_b, 10);

        assert_eq!(queue.pop_batch(2), Some(vec![1, 2]));
        assert_eq!(queue.pop_batch(2), Some(vec![9, 10]));
        assert_eq!(queue.pop_batch(2), Some(vec![3]));
        assert_eq!(queue.pop_batch(2), None);
    }

    #[tokio::test]
    async fn meta_store_create_app_uses_declared_schema_order() {
        let data_root = tempdir().unwrap();
        let store = MetaStore::new(data_root.path(), "meta-store-test-key".to_string()).unwrap();
        let app_id = AppId::from_name("meta-store-app");

        let created = store
            .create_app(
                app_id,
                "Meta Store App".to_string(),
                "https://issuer.example/jwks".to_string(),
                45,
                90,
                true,
                false,
                "backend-secret-hash".to_string(),
                "admin-secret-hash".to_string(),
                AppStatus::Active,
                Some("admin-secret".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(created.app_id, app_id);
        assert_eq!(created.app_name, "Meta Store App");
        assert_eq!(created.admin_secret.as_deref(), Some("admin-secret"));

        let loaded = store.get_by_app_id(app_id).await.unwrap().unwrap();
        assert_eq!(loaded.app_id, app_id);
        assert_eq!(loaded.app_name, "Meta Store App");
        assert_eq!(loaded.jwks_endpoint, "https://issuer.example/jwks");
        assert_eq!(loaded.jwks_cache_ttl_secs, 45);
        assert_eq!(loaded.jwks_max_stale_secs, 90);
        assert!(loaded.allow_anonymous);
        assert!(!loaded.allow_demo);
        assert_eq!(loaded.backend_secret_hash, "backend-secret-hash");
        assert_eq!(loaded.admin_secret_hash, "admin-secret-hash");
        assert_eq!(loaded.status, AppStatus::Active);
        assert_eq!(loaded.admin_secret.as_deref(), Some("admin-secret"));
    }

    #[tokio::test]
    async fn meta_store_create_external_identity_uses_declared_schema_order() {
        let data_root = tempdir().unwrap();
        let store = MetaStore::new(data_root.path(), "meta-store-test-key".to_string()).unwrap();
        let app_id = AppId::from_name("meta-store-app");

        store
            .create_external_identity(
                app_id,
                "https://issuer.example",
                "subject-123",
                "principal-456",
            )
            .await
            .unwrap();

        let loaded = store
            .get_external_identity(app_id, "https://issuer.example", "subject-123")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.principal_id, "principal-456");
    }
}
