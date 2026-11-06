//! A production core served from Node: the `jazz_server::run` process shell
//! (graceful SIGTERM shutdown, connection and memory limits), started by a
//! launcher such as the `vercel()` store's Sandbox instead of a separate
//! server executable.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServeCoreOptions {
    app_id: String,
    port: u16,
    data_dir: String,
    blobs: ServeCoreBlobs,
    /// Trust this OIDC workload instead of the shared secrets.
    trusted_workload: Option<ServeCoreWorkload>,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
    allow_local_first_auth: Option<bool>,
    shutdown_timeout_secs: Option<u64>,
    max_connections: Option<usize>,
    memory_limit_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServeCoreWorkload {
    issuer: String,
    audience: String,
    subject: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum ServeCoreBlobs {
    Sqlite,
    #[serde(rename_all = "camelCase")]
    Files {
        dir: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    S3 {
        bucket: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        prefix: Option<String>,
        #[serde(default)]
        role_from_workload: bool,
        environment: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    VercelBlob {
        store_id: Option<String>,
        prefix: Option<String>,
        #[serde(default)]
        token_from_workload: bool,
        environment: Option<String>,
    },
}

impl ServeCoreBlobs {
    fn into_options(self) -> jazz_server::BlobOptions {
        match self {
            Self::Sqlite => jazz_server::BlobOptions {
                kind: "sqlite".into(),
                ..Default::default()
            },
            Self::Files { dir } => jazz_server::BlobOptions {
                kind: "files".into(),
                dir,
                ..Default::default()
            },
            Self::S3 {
                bucket,
                region,
                endpoint,
                prefix,
                role_from_workload,
                environment,
            } => jazz_server::BlobOptions {
                kind: "s3".into(),
                s3_bucket: bucket,
                s3_region: region,
                s3_endpoint: endpoint,
                s3_prefix: prefix,
                s3_role_from_workload: role_from_workload,
                environment,
                ..Default::default()
            },
            Self::VercelBlob {
                store_id,
                prefix,
                token_from_workload,
                environment,
            } => jazz_server::BlobOptions {
                kind: "vercel-blob".into(),
                vercel_blob_store_id: store_id,
                vercel_blob_prefix: prefix,
                vercel_blob_token_from_workload: token_from_workload,
                environment,
                ..Default::default()
            },
        }
    }
}

/// A core [`serve_core`] started. The host owns the process's signals and
/// calls [`ServedCore::stop`] to drain it; dropping it without a stop drains
/// it too.
#[napi]
pub struct ServedCore {
    stop: tokio::sync::watch::Sender<bool>,
    closed:
        std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<std::result::Result<(), String>>>>,
}

#[napi]
impl ServedCore {
    /// Start the controlled shutdown: refuse new work, drain, then close.
    #[napi]
    pub fn stop(&self) {
        let _ = self.stop.send(true);
    }

    /// Resolves once the core has stopped; rejects if it failed. Only one
    /// caller may wait.
    #[napi]
    pub async fn closed(&self) -> Result<()> {
        let receiver = self
            .closed
            .lock()
            .map_err(|_| Error::from_reason("lock"))?
            .take()
            .ok_or_else(|| Error::from_reason("closed() is already being awaited"))?;
        match receiver.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(Error::from_reason(format!("core stopped: {error}"))),
            Err(_) => Err(Error::from_reason("core stopped without a result")),
        }
    }
}

/// Serve a production core (the `jazz_server::run` shell: connection and
/// memory limits, graceful drain) until [`ServedCore::stop`]. One per
/// process at a time: the cores would share the process's storage cache
/// budget, so a second call fails until the first core has stopped.
#[napi(
    js_name = "serveCore",
    ts_args_type = "options: { appId: string; port: number; dataDir: string; blobs: { kind: \"sqlite\" } | { kind: \"files\"; dir?: string } | { kind: \"s3\"; bucket?: string; region?: string; endpoint?: string; prefix?: string; roleFromWorkload?: boolean; environment?: string } | { kind: \"vercel-blob\"; storeId?: string; prefix?: string; tokenFromWorkload?: boolean; environment?: string }; trustedWorkload?: { issuer: string; audience: string; subject: string }; backendSecret?: string; adminSecret?: string; allowLocalFirstAuth?: boolean; shutdownTimeoutSecs?: number; maxConnections?: number; memoryLimitBytes?: number }"
)]
pub fn serve_core(options: JsonValue) -> Result<ServedCore> {
    let options: ServeCoreOptions = serde_json::from_value(options)
        .map_err(|error| Error::from_reason(format!("Invalid serveCore options: {error}")))?;
    let trusted_workload = options.trusted_workload.map(|workload| {
        jazz_server::middleware::workload::TrustedWorkload {
            issuer: workload.issuer,
            audience: workload.audience,
            subject: workload.subject,
        }
    });
    if trusted_workload.is_some()
        && (options.backend_secret.is_some() || options.admin_secret.is_some())
    {
        return Err(Error::from_reason(
            "a trustedWorkload replaces adminSecret and backendSecret; pass neither",
        ));
    }
    let blobs = options
        .blobs
        .into_options()
        .resolve()
        .map_err(Error::from_reason)?;
    let serving = ServingCore::claim()?;
    let auth_config = jazz_server::AuthConfig {
        allow_local_first_auth: options.allow_local_first_auth.unwrap_or(false),
        backend_secret: options.backend_secret,
        admin_secret: options.admin_secret,
        trusted_workload,
        ..Default::default()
    };
    crate::init_server_logs();
    let (stop, stop_requested) = tokio::sync::watch::channel(false);
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    napi::bindgen_prelude::spawn(async move {
        let result = jazz_server::run_until(
            &options.app_id,
            options.port,
            &options.data_dir,
            false,
            blobs,
            auth_config,
            None,
            std::time::Duration::from_secs(options.shutdown_timeout_secs.unwrap_or(30)),
            jazz_server::profiling::DiagnosticsConfig {
                heap_profiler: None,
                listen: None,
                token: None,
            },
            jazz_server::resources::ServerResourceLimits {
                max_websocket_connections: options
                    .max_connections
                    .unwrap_or(jazz_server::server::DEFAULT_MAX_WEBSOCKET_CONNECTIONS),
                memory_limit_bytes: options
                    .memory_limit_bytes
                    .or_else(jazz_server::resources::detect_memory_limit_bytes),
                stored_arrangements: true,
                query_cache_budget_bytes: None,
            },
            jazz_server::ShutdownSignal::External(stop_requested),
        )
        .await
        .map_err(|error| error.to_string());
        drop(serving);
        let _ = closed_tx.send(result);
    });
    Ok(ServedCore {
        stop,
        closed: std::sync::Mutex::new(Some(closed_rx)),
    })
}

/// Whether this process serves a core now.
static SERVING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// This process's one serving core, released when the core has stopped.
struct ServingCore;

impl ServingCore {
    fn claim() -> Result<Self> {
        use std::sync::atomic::Ordering;
        SERVING
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ServingCore)
            .map_err(|_| {
                Error::from_reason(
                    "serveCore: serving more than one core per process is not supported yet",
                )
            })
    }
}

impl Drop for ServingCore {
    fn drop(&mut self) {
        SERVING.store(false, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::{ServeCoreBlobs, ServingCore, serve_core};

    #[test]
    fn a_trusted_workload_takes_no_secrets() {
        let refused = serve_core(serde_json::json!({
            "appId": "00000000-0000-0000-0000-000000000001",
            "port": 0,
            "dataDir": "/nonexistent",
            "blobs": { "kind": "sqlite" },
            "trustedWorkload": { "issuer": "https://oidc.example", "audience": "a", "subject": "s" },
            "backendSecret": "secret",
        }))
        .err()
        .expect("secrets beside a trusted workload are refused");
        assert!(
            refused
                .reason
                .contains("a trustedWorkload replaces adminSecret and backendSecret")
        );
    }

    #[test]
    fn serves_one_core_per_process_at_a_time() {
        let first = ServingCore::claim().unwrap();
        let refused = ServingCore::claim()
            .err()
            .expect("a second core is refused");
        assert!(
            refused
                .reason
                .contains("more than one core per process is not supported yet")
        );
        drop(first);
        let _again = ServingCore::claim().unwrap();
    }

    #[test]
    fn claims_a_blob_store_for_its_environment() {
        let blobs: ServeCoreBlobs = serde_json::from_str(
            r#"{"kind":"vercel-blob","storeId":"abc","tokenFromWorkload":true,"environment":"production"}"#,
        )
        .unwrap();
        let options = blobs.into_options();
        assert_eq!(options.kind, "vercel-blob");
        assert_eq!(options.environment.as_deref(), Some("production"));

        let blobs: ServeCoreBlobs = serde_json::from_str(
            r#"{"kind":"s3","roleFromWorkload":true,"environment":"preview"}"#,
        )
        .unwrap();
        assert_eq!(blobs.into_options().environment.as_deref(), Some("preview"));
    }
}
