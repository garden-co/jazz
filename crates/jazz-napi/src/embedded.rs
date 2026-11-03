//! `JazzCore`: a Jazz core served by the host's own HTTP server.
//!
//! The JavaScript handler (`createJazz` in jazz-tools) passes requests to
//! [`JazzCore::handle`] and each socket it upgraded to
//! [`JazzCore::open_socket`]. Nothing listens on a port.

use std::sync::{Arc, Mutex};

use jazz::tools::AppId;
use jazz_server::{
    AuthConfig, BlobStore, EmbeddedOutgoing, EmbeddedServer, ServerBuilder, StorageBackend,
};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JazzCoreOpenOptions {
    app_id: String,
    backend_secret: String,
    admin_secret: String,
    data_dir: Option<String>,
    in_memory: Option<bool>,
    /// `"files"`, `"s3"` or `"sqlite"` (the default): where settled
    /// large-value bytes of a persistent core live.
    blobs: Option<String>,
    /// The directory for `blobs: "files"`; defaults to `<dataDir>/blobs`.
    blobs_dir: Option<String>,
    /// For `blobs: "s3"`, each defaulting to the environment
    /// (`S3_BUCKET_NAME`, `AWS_REGION`; credentials always come from it).
    s3_bucket: Option<String>,
    s3_region: Option<String>,
    s3_endpoint: Option<String>,
    s3_prefix: Option<String>,
    jwt_public_key: Option<String>,
    jwt_issuer: Option<String>,
    jwt_audience: Option<String>,
    allow_local_first_auth: Option<bool>,
    telemetry_collector_url: Option<String>,
}

#[napi(object)]
pub struct JazzCoreResponse {
    pub status: u32,
    pub headers: Vec<Vec<String>>,
    pub body: Buffer,
}

#[napi(object)]
pub struct JazzCoreSocketEvent {
    /// `"message"` or `"close"`.
    pub kind: String,
    pub data: Option<Uint8Array>,
    pub code: Option<u32>,
    pub reason: Option<String>,
}

#[napi]
pub struct JazzCore {
    server: Arc<EmbeddedServer>,
    /// The OS lock on `<dataDir>/jazz.lock`, held while the core is open.
    /// The kernel drops it when the process exits, however it exits.
    dir_lock: Mutex<Option<std::fs::File>>,
}

#[napi]
impl JazzCore {
    /// Open the core's storage and serve it in this process.
    #[napi(
        factory,
        ts_args_type = "options: { appId: string; backendSecret: string; adminSecret: string; dataDir?: string; inMemory?: boolean; blobs?: \"files\" | \"s3\" | \"sqlite\"; blobsDir?: string; s3Bucket?: string; s3Region?: string; s3Endpoint?: string; s3Prefix?: string; jwtPublicKey?: string; jwtIssuer?: string; jwtAudience?: string; allowLocalFirstAuth?: boolean; telemetryCollectorUrl?: string }",
        ts_return_type = "Promise<JazzCore>"
    )]
    pub async fn open(options: serde_json::Value) -> napi::Result<Self> {
        let opts: JazzCoreOpenOptions = serde_json::from_value(options).map_err(|error| {
            napi::Error::from_reason(format!("Invalid JazzCore options: {error}"))
        })?;
        crate::init_jazz_server_telemetry(opts.telemetry_collector_url.as_deref());
        let app_id =
            AppId::from_string(&opts.app_id).unwrap_or_else(|_| AppId::from_name(&opts.app_id));
        let auth_config = AuthConfig {
            jwt_public_key: opts.jwt_public_key,
            jwt_issuer: opts.jwt_issuer,
            jwt_audience: opts.jwt_audience,
            allow_local_first_auth: opts.allow_local_first_auth.unwrap_or(true),
            backend_secret: Some(opts.backend_secret),
            admin_secret: Some(opts.admin_secret),
            ..Default::default()
        };
        let mut dir_lock = None;
        let storage = if opts.in_memory.unwrap_or(false) {
            StorageBackend::InMemory
        } else {
            let data_dir = opts.data_dir.ok_or_else(|| {
                napi::Error::from_reason("JazzCore needs dataDir unless inMemory is set")
            })?;
            dir_lock = Some(lock_data_dir(std::path::Path::new(&data_dir))?);
            StorageBackend::Persistent {
                path: data_dir.into(),
            }
        };
        let blobs = match (opts.blobs.as_deref(), opts.blobs_dir) {
            (None | Some("sqlite"), None) => BlobStore::Sqlite,
            (Some("files"), dir) => BlobStore::Files {
                dir: dir.map(Into::into),
            },
            (Some("s3"), None) => BlobStore::S3(
                jazz_server::s3::S3Options::from_env(
                    opts.s3_bucket,
                    opts.s3_region,
                    opts.s3_endpoint,
                    opts.s3_prefix,
                )
                .map_err(|error| napi::Error::from_reason(format!("JazzCore: {error}")))?,
            ),
            (Some("sqlite" | "s3") | None, Some(_)) => {
                return Err(napi::Error::from_reason(
                    "JazzCore: blobsDir needs blobs: \"files\"",
                ));
            }
            (Some(other), _) => {
                return Err(napi::Error::from_reason(format!(
                    "JazzCore: blobs must be \"files\", \"s3\" or \"sqlite\", not {other:?}"
                )));
            }
        };
        let built = ServerBuilder::new(app_id)
            .with_auth_config(auth_config)
            .with_storage(storage)
            .with_blob_store(blobs)
            .build()
            .await
            .map_err(napi::Error::from_reason)?;
        Ok(Self {
            server: Arc::new(EmbeddedServer::new(built)),
            dir_lock: Mutex::new(dir_lock),
        })
    }

    /// Route one HTTP request. `url` is the path and query, as the core's
    /// router sees them (`/apps/<appId>/...` or `/health`).
    #[napi]
    pub async fn handle(
        &self,
        method: String,
        url: String,
        headers: Vec<Vec<String>>,
        body: Buffer,
    ) -> napi::Result<JazzCoreResponse> {
        let headers = pairs(&headers);
        let (status, headers, body) = self
            .server
            .handle_parts(&method, &url, &headers, body.to_vec())
            .await
            .map_err(napi::Error::from_reason)?;
        let headers = headers
            .into_iter()
            .map(|(name, value)| vec![name, value])
            .collect();
        Ok(JazzCoreResponse {
            status: u32::from(status),
            headers,
            body: body.into(),
        })
    }

    /// Start a sync session over a socket the host upgraded. `onEvent` gets
    /// each message for the client and, last, the close.
    #[napi(
        js_name = "openSocket",
        ts_args_type = "headers: Array<Array<string>>, onEvent: (err: Error | null, event: JazzCoreSocketEvent) => void"
    )]
    pub fn open_socket(
        &self,
        headers: Vec<Vec<String>>,
        on_event: Function<'_, JazzCoreSocketEvent, ()>,
    ) -> napi::Result<JazzCoreSocket> {
        let callback: ThreadsafeFunction<
            JazzCoreSocketEvent,
            (),
            JazzCoreSocketEvent,
            Status,
            true,
        > = on_event
            .build_threadsafe_function::<JazzCoreSocketEvent>()
            .callee_handled::<true>()
            .build()?;
        let mut peer = self
            .server
            .open_socket_with(&pairs(&headers))
            .map_err(|refused| napi::Error::from_reason(refused.to_string()))?;
        let sender = peer.sender();
        self.server.runtime().spawn(async move {
            loop {
                let event = match peer.recv().await {
                    Some(EmbeddedOutgoing::Binary(bytes)) => JazzCoreSocketEvent {
                        kind: "message".to_owned(),
                        data: Some(bytes.into()),
                        code: None,
                        reason: None,
                    },
                    Some(EmbeddedOutgoing::Close { code, reason }) => JazzCoreSocketEvent {
                        kind: "close".to_owned(),
                        data: None,
                        code: Some(u32::from(code)),
                        reason: Some(reason),
                    },
                    None => JazzCoreSocketEvent {
                        kind: "close".to_owned(),
                        data: None,
                        code: Some(1000),
                        reason: Some(String::new()),
                    },
                };
                let last = event.kind == "close";
                callback.call(Ok(event), ThreadsafeFunctionCallMode::NonBlocking);
                if last {
                    break;
                }
            }
        });
        Ok(JazzCoreSocket {
            peer: Mutex::new(Some(sender)),
        })
    }

    /// Close every session and the storage.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let phase = self.server.shutdown().await;
        if phase == jazz_server::ShutdownPhase::StorageClosed {
            self.dir_lock.lock().expect("dir lock").take();
            Ok(())
        } else {
            Err(napi::Error::from_reason(format!(
                "JazzCore shutdown ended in {phase:?}"
            )))
        }
    }
}

/// The host's handle on one session.
#[napi]
pub struct JazzCoreSocket {
    peer: Mutex<Option<jazz_server::EmbeddedSocketSender>>,
}

#[napi]
impl JazzCoreSocket {
    /// Pass one binary message from the client. False once the session ended.
    #[napi]
    pub fn send(&self, data: Uint8Array) -> bool {
        let peer = self.peer.lock().expect("socket lock");
        peer.as_ref().is_some_and(|peer| peer.send(data.to_vec()))
    }

    /// The client's socket closed.
    #[napi]
    pub fn close(&self) {
        if let Some(peer) = self.peer.lock().expect("socket lock").take() {
            peer.close();
        }
    }
}

/// Hold `dir` for this core with an OS file lock: two cores on one sqlite
/// directory (a hot reload without a cached instance, cluster mode, a
/// rolling deploy on one volume) would each believe they own its data. The
/// lock is per open file, so a second core in the same process is refused
/// too, and it goes away with the process, so a crash or `docker stop`
/// never leaves the directory locked.
fn lock_data_dir(dir: &std::path::Path) -> napi::Result<std::fs::File> {
    jazz_server::lock_data_dir(dir).map_err(|error| match error {
        jazz_server::DataDirLockError::Held(holder) => napi::Error::from_reason(format!(
            "sqlite(): {} is already open{}. One core per directory: create Jazz once and reuse it \
             (cache it on globalThis across hot reloads), and give each process its own directory or use remote().",
            dir.display(),
            if holder.is_empty() {
                String::new()
            } else {
                format!(" ({holder})")
            },
        )),
        jazz_server::DataDirLockError::Io(error) => {
            napi::Error::from_reason(format!("sqlite(): {error}"))
        }
    })
}

fn pairs(headers: &[Vec<String>]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|pair| match pair.as_slice() {
            [name, value] => Some((name.clone(), value.clone())),
            _ => None,
        })
        .collect()
}
