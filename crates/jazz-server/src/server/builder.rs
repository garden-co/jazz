use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::{AppStore, NodeRole, StorageConfig};
use axum::Router;
use jazz::schema::JazzSchema;
use tracing::info;

use crate::middleware::AuthConfig;
use crate::profiling::{self, HeapProfileDump};
use crate::server::routes;
use crate::server::{ServerState, StoredCatalogue};
use jazz::tools::AppId;
#[allow(deprecated)]
use jazz::tools::public_schema::Schema;
#[cfg(test)]
use jazz::tx::DurabilityTier;

/// The one file an app's state lives in: rows, catalogue, account registry.
const STORE_FILE: &str = "jazz.sqlite";
/// Stores from before SQLite. A data directory that holds one and no SQLite
/// store must be migrated first; the server never starts empty beside it.
const PRE_SQLITE_STORES: [&str; 3] = [
    "server-shell.rocksdb",
    "catalogue.rocksdb",
    "accounts.rocksdb",
];
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
pub struct BuiltServer {
    #[cfg_attr(not(test), allow(dead_code))]
    pub state: Arc<ServerState>,
    pub app: Router,
}

impl BuiltServer {
    /// Stop this server and wait until its owned runtime and durable storage
    /// have been closed.
    ///
    /// A builder owns a shell even when it is used without the test-server
    /// listener wrapper. Callers that reopen the same persistent path must use
    /// this lifecycle boundary rather than relying on field drop order. The
    /// close work runs on the server's dedicated lifecycle thread, so callers
    /// may await this method from any async executor. The operation is
    /// idempotent: subsequent calls return the terminal shutdown phase
    /// recorded by the state.
    pub async fn shutdown(&self) -> crate::server::ShutdownPhase {
        self.state.shutdown.request_shutdown();
        self.state.run_shutdown_finalization().await
    }
}

#[cfg_attr(not(test), allow(dead_code))]
enum ServerSchemaMode {
    Dynamic,
    Fixed(Schema),
}

/// Storage backend selection for [`ServerBuilder::with_storage`].
///
/// `Persistent` keeps the runtime's rows, the catalogue and the account
/// registry in one SQLite file, `jazz.sqlite`, in the data directory.
#[derive(Debug, Clone)]
pub enum StorageBackend {
    InMemory,
    Persistent { path: PathBuf },
}

/// Where a persistent store keeps the bytes of large values once they
/// settle (see [`jazz::chunk_store`]). The rows and the chunk metadata
/// always stay in `jazz.sqlite`.
#[derive(Debug, Clone)]
pub enum BlobStore {
    /// In `jazz.sqlite` itself. Simplest, but the file grows with every
    /// large value.
    Sqlite,
    /// As files under `dir`; `None` is `blobs/` in the data directory.
    Files { dir: Option<PathBuf> },
    /// As objects in an S3-compatible bucket ([`crate::s3`]). The bucket
    /// and prefix belong to this data directory alone: a store deletes the
    /// values it reclaims.
    S3(crate::s3::S3Options),
    /// As private blobs in a Vercel Blob store ([`crate::vercel_blob`]),
    /// under a prefix that belongs to this data directory alone.
    VercelBlob(crate::vercel_blob::VercelBlobOptions),
}

/// Records which [`BlobStore`] a data directory was started with.
const BLOB_STORE_MARKER: &str = "blob-store";
/// A data directory's own id, which its blob directory is claimed for.
const STORE_ID_FILE: &str = "store-id";
/// In a blob directory: the id of the data directory that owns it.
const BLOB_OWNER_FILE: &str = "owner";

pub struct ServerBuilder {
    app_id: AppId,
    blobs: BlobStore,
    auth_config: AuthConfig,
    schema_mode: ServerSchemaMode,
    storage_backend: StorageBackend,
    core_server_shell_schema: Option<JazzSchema>,
    shutdown_timeout: Duration,
    heap_profiler: Option<HeapProfileDump>,
    max_websocket_connections: usize,
}

impl ServerBuilder {
    pub fn new(app_id: AppId) -> Self {
        Self {
            app_id,
            auth_config: AuthConfig {
                allow_local_first_auth: true,
                ..Default::default()
            },
            schema_mode: ServerSchemaMode::Dynamic,
            blobs: BlobStore::Sqlite,
            storage_backend: StorageBackend::Persistent {
                path: PathBuf::from("./data"),
            },
            core_server_shell_schema: None,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            heap_profiler: None,
            max_websocket_connections: crate::server::DEFAULT_MAX_WEBSOCKET_CONNECTIONS,
        }
    }

    pub fn with_auth_config(mut self, auth_config: AuthConfig) -> Self {
        self.auth_config = auth_config;
        self
    }

    pub fn with_local_first_auth(mut self, enabled: bool) -> Self {
        self.auth_config.allow_local_first_auth = enabled;
        self
    }

    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Cap live WebSocket connections; upgrades beyond it get `503`.
    pub fn with_max_websocket_connections(mut self, max: usize) -> Self {
        self.max_websocket_connections = max;
        self
    }

    pub fn with_storage(mut self, backend: StorageBackend) -> Self {
        self.storage_backend = backend;
        self
    }

    /// Where a persistent store keeps settled large-value bytes. Defaults to
    /// [`BlobStore::Sqlite`]. A data directory keeps the blob store it was
    /// started with, except that one whose blobs are in SQLite may move
    /// them out (they move on later ticks).
    pub fn with_blob_store(mut self, blobs: BlobStore) -> Self {
        self.blobs = blobs;
        self
    }

    /// Serve the executable's heap profile at `/debug/pprof/heap` to admins.
    ///
    /// Without one, the route answers `501 Not Implemented`.
    pub fn with_heap_profiler(mut self, dump: HeapProfileDump) -> Self {
        self.heap_profiler = Some(dump);
        self
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema_mode = ServerSchemaMode::Fixed(schema);
        self
    }

    pub fn with_core_server_shell_schema(mut self, schema: JazzSchema) -> Self {
        self.core_server_shell_schema = Some(schema);
        self
    }

    pub async fn build(self) -> Result<BuiltServer, String> {
        let mut auth_config = self.auth_config.clone();
        validate_server_config(&auth_config)?;
        // A trusted workload replaces shared secrets: the server holds random
        // ones only internally, and a verified token stands in for them.
        // Otherwise the admin secret doubles as a backend credential, so a
        // backend needs only that one secret; backend access through it gets
        // the backend secret, or an internal one when none is configured.
        use crate::middleware::workload::{BackendCredential, InternalSecrets, WorkloadVerifier};
        let (backend_credential, workload_secrets) = match (
            auth_config.trusted_workload.clone(),
            auth_config.admin_secret.clone(),
        ) {
            (Some(workload), _) => {
                let secrets = InternalSecrets::generate();
                auth_config.admin_secret = Some(secrets.admin.clone());
                auth_config.backend_secret = Some(secrets.backend.clone());
                (
                    Some(Arc::new(BackendCredential::Workload(Box::new(
                        WorkloadVerifier::new(workload),
                    )))),
                    Some(secrets),
                )
            }
            (None, Some(admin)) => {
                let backend = auth_config
                    .backend_secret
                    .get_or_insert_with(|| InternalSecrets::generate().backend)
                    .clone();
                (
                    Some(Arc::new(BackendCredential::AdminSecret(admin.clone()))),
                    Some(InternalSecrets { admin, backend }),
                )
            }
            (None, None) => (None, None),
        };
        if self.max_websocket_connections == 0 {
            return Err("max websocket connections must be at least 1".to_owned());
        }
        if let StorageBackend::Persistent { path } = &self.storage_backend {
            refuse_pre_sqlite_data_dir(path)?;
        }
        log_auth_config(&auth_config);

        // One store per app: the catalogue and the account registry are host
        // tables in the file the runtime's rows live in, all on its thread.
        let (storage_config, blob_store_record) = self.build_core_server_shell_storage_config()?;
        let store = AppStore::open(&storage_config)?;
        // Only a store that opened is recorded as having moved its blobs.
        if let Some(record) = blob_store_record {
            record.write()?;
        }
        store.set_app(self.app_id.to_string());
        let (catalogue_store, latest_catalogue_schema) = self.build_catalogue_store(&store)?;
        let core_server_shell = self.build_core_server_shell(latest_catalogue_schema, &store)?;

        let accounts = Some(crate::server::accounts::AccountRegistryOwner::open(
            store.sql()?,
        )?);

        let state = Arc::new(ServerState {
            accounts,
            catalogue_store,
            catalogue: crate::server::ServerCatalogue,
            app_id: self.app_id,
            auth_config,
            backend_credential,
            workload_secrets,
            core_server_shell: std::sync::RwLock::new(core_server_shell),
            websocket_admissions: Arc::new(
                crate::server::routes::WebSocketAdmissionState::default(),
            ),
            max_websocket_connections: self.max_websocket_connections,
            store: Some(store),
            runtime_catalogue_publication: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            panic_during_shutdown_for_test: Default::default(),
            shutdown: crate::server::ShutdownController::new(self.shutdown_timeout),
        });

        let app = routes::create_router(state.clone()).merge(profiling::admin_router(
            self.heap_profiler,
            state.auth_config.clone(),
        ));
        Ok(BuiltServer { state, app })
    }

    /// Build the direct admin catalogue store used by HTTP catalogue routes.
    ///
    fn build_catalogue_store(
        &self,
        store: &AppStore,
    ) -> Result<(StoredCatalogue, Option<Schema>), String> {
        let storage = store.catalogue()?;
        let initial_schema = match &self.schema_mode {
            ServerSchemaMode::Fixed(schema) => Some(schema.clone()),
            ServerSchemaMode::Dynamic => None,
        };

        #[cfg(test)]
        let store = {
            let schema_branches = test_schema_branches(initial_schema.as_ref());
            let local_durability_tiers =
                std::collections::HashSet::from([self.local_durability_tier()]);
            StoredCatalogue::with_test_observability(
                initial_schema,
                storage,
                schema_branches,
                local_durability_tiers,
            )
            .map_err(|error| format!("failed to read durable catalogue: {error}"))?
        };
        #[cfg(not(test))]
        let store = StoredCatalogue::new(initial_schema, storage)
            .map_err(|error| format!("failed to read durable catalogue: {error}"))?;

        let latest_catalogue_schema = store
            .latest_published_schema()
            .map_err(|error| format!("failed to read latest catalogue schema: {error:?}"))?;
        Ok((store, latest_catalogue_schema))
    }

    fn build_core_server_shell(
        &self,
        latest_catalogue_schema: Option<Schema>,
        store: &AppStore,
    ) -> Result<Option<crate::server::ServerRuntimeHandle>, String> {
        let role = NodeRole::Core;
        if let Some(schema) = &self.core_server_shell_schema {
            return Ok(Some(store.start_runtime(schema.clone(), role, false)?));
        }

        let (schema, dynamic) = match &self.schema_mode {
            ServerSchemaMode::Fixed(schema) => (Some(schema.clone()), false),
            ServerSchemaMode::Dynamic => (latest_catalogue_schema, true),
        };
        let Some(schema) = schema else {
            return Ok(None);
        };
        let schema = jazz::schema::JazzSchema::new(&schema)
            .map_err(|error| format!("failed to build server shell schema: {error}"))?;
        // The newest published schema may never have reached the shell store
        // (published without a lens, or its lens bridge failed). A dynamic
        // server then reopens with the store's own current schema.
        Ok(Some(store.start_runtime(schema, role, dynamic)?))
    }

    /// The storage to open, and the blob-store record to write once it has
    /// opened (if it changes).
    fn build_core_server_shell_storage_config(
        &self,
    ) -> Result<(StorageConfig, Option<BlobStoreRecord>), String> {
        match &self.storage_backend {
            StorageBackend::InMemory => Ok((StorageConfig::InMemory, None)),
            StorageBackend::Persistent { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;
                let blobs = match &self.blobs {
                    // A bucket's default prefix is this store's own.
                    BlobStore::S3(options) if options.prefix.is_empty() => {
                        let mut options = options.clone();
                        options.prefix = format!("jazz/{}/", store_id(path)?);
                        BlobStore::S3(options)
                    }
                    BlobStore::VercelBlob(options) if options.prefix.is_empty() => {
                        let mut options = options.clone();
                        options.prefix = format!("jazz/{}/", store_id(path)?);
                        BlobStore::VercelBlob(options)
                    }
                    blobs => blobs.clone(),
                };
                let plan = plan_blob_store(path, &blobs)?;
                let config = match (&plan.blob_dir, &blobs) {
                    (Some(dir), _) => {
                        claim_blob_dir(path, dir)?;
                        StorageConfig::data_dir(path).with_blob_dir(dir.clone())
                    }
                    (None, BlobStore::S3(options)) => {
                        warn_on_distant_bucket(options);
                        let store = crate::s3::S3ChunkStore::new(options.clone(), store_id(path)?)?;
                        StorageConfig::data_dir(path).with_blobs(crate::runtime::Blobs::Network(
                            crate::runtime::NetworkChunkStore {
                                store: Arc::new(store),
                                name: options.describe(),
                            },
                        ))
                    }
                    (None, BlobStore::VercelBlob(options)) => {
                        let store = crate::vercel_blob::VercelBlobChunkStore::new(
                            options.clone(),
                            store_id(path)?,
                        )?;
                        StorageConfig::data_dir(path).with_blobs(crate::runtime::Blobs::Network(
                            crate::runtime::NetworkChunkStore {
                                store: Arc::new(store),
                                name: options.describe(),
                            },
                        ))
                    }
                    (None, _) => StorageConfig::data_dir(path),
                };
                Ok((config, plan.record))
            }
        }
    }

    #[cfg(test)]
    fn local_durability_tier(&self) -> DurabilityTier {
        DurabilityTier::Global
    }
}

/// Where a data directory's large values go, and the record of that choice
/// still to be written.
struct BlobStorePlan {
    /// `None` keeps them in SQLite.
    blob_dir: Option<PathBuf>,
    record: Option<BlobStoreRecord>,
}

/// A data directory's blob-store record, written once its store has opened.
struct BlobStoreRecord {
    marker: PathBuf,
    contents: String,
}

impl BlobStoreRecord {
    fn write(&self) -> Result<(), String> {
        write_durably(&self.marker, self.contents.as_bytes())
    }
}

/// Refuse to start `data_dir` with a blob store other than the one it was
/// started with. A directory with no record keeps its blobs in SQLite (every
/// store did before blob stores), and may move them out.
///
/// The record names the blob directory relative to the data directory when
/// it is inside it (so the two move together) and absolute otherwise, so a
/// relative `dir` means the same directory whatever the working directory.
fn plan_blob_store(data_dir: &Path, blobs: &BlobStore) -> Result<BlobStorePlan, String> {
    let marker = data_dir.join(BLOB_STORE_MARKER);
    let recorded = match std::fs::read_to_string(&marker) {
        Ok(recorded) => recorded.trim_end_matches('\n').to_owned(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => "sqlite".to_owned(),
        Err(error) => return Err(format!("read {}: {error}", marker.display())),
    };
    let data_dir = absolute(data_dir)?;
    let (wanted, blob_dir) = match blobs {
        BlobStore::Sqlite => ("sqlite".to_owned(), None),
        BlobStore::Files { dir } => {
            let dir = match dir {
                Some(dir) => absolute(dir)?,
                None => data_dir.join("blobs"),
            };
            let named = if dir == data_dir.join("blobs") {
                "files".to_owned()
            } else if let Ok(inside) = dir.strip_prefix(&data_dir) {
                format!("files {}", inside.display())
            } else {
                format!("files {}", dir.display())
            };
            (named, Some(dir))
        }
        BlobStore::S3(options) => (options.describe(), None),
        BlobStore::VercelBlob(options) => (options.describe(), None),
    };
    if recorded == wanted {
        return Ok(BlobStorePlan {
            blob_dir,
            record: None,
        });
    }
    if recorded != "sqlite" {
        return Err(format!(
            "data dir '{}' keeps its large values in {recorded:?}; moving them to {wanted:?} is not supported yet",
            data_dir.display(),
        ));
    }
    Ok(BlobStorePlan {
        blob_dir,
        record: Some(BlobStoreRecord {
            marker,
            contents: format!("{wanted}\n"),
        }),
    })
}

/// Vercel functions and Sandboxes run in AWS regions: a bucket in another
/// region pays transfer both ways and adds latency to every cold read.
fn warn_on_distant_bucket(options: &crate::s3::S3Options) {
    if options.endpoint.is_some() {
        return;
    }
    let Some(vercel) = std::env::var("VERCEL_REGION").ok() else {
        return;
    };
    if let Some(aws) = aws_region_of_vercel(&vercel)
        && aws != options.region
    {
        tracing::warn!(
            bucket = %options.bucket,
            bucket_region = %options.region,
            vercel_region = %vercel,
            "the large-value bucket is in {}, but this deployment runs in {vercel} ({aws}): \
             put the bucket in {aws} to avoid cross-region transfer",
            options.region,
        );
    }
}

/// The AWS region a Vercel region runs in.
pub fn aws_region_of_vercel(vercel: &str) -> Option<&'static str> {
    Some(match vercel {
        "iad1" => "us-east-1",
        "cle1" => "us-east-2",
        "sfo1" => "us-west-1",
        "pdx1" => "us-west-2",
        "yul1" => "ca-central-1",
        "gru1" => "sa-east-1",
        "dub1" => "eu-west-1",
        "lhr1" => "eu-west-2",
        "cdg1" => "eu-west-3",
        "fra1" => "eu-central-1",
        "arn1" => "eu-north-1",
        "cpt1" => "af-south-1",
        "dxb1" => "me-central-1",
        "bom1" => "ap-south-1",
        "sin1" => "ap-southeast-1",
        "syd1" => "ap-southeast-2",
        "hkg1" => "ap-east-1",
        "hnd1" => "ap-northeast-1",
        "icn1" => "ap-northeast-2",
        "kix1" => "ap-northeast-3",
        _ => return None,
    })
}

/// Claim `blob_dir` for `data_dir`, or refuse it if another data directory
/// owns it: a store deletes the values it reclaims, so two stores sharing a
/// directory would delete each other's values. The claim is exclusive: the
/// owner file is created only if none exists, so of two data dirs claiming
/// one blob dir at once, one wins and the other reads its id.
fn claim_blob_dir(data_dir: &Path, blob_dir: &Path) -> Result<(), String> {
    let id = store_id(data_dir)?;
    std::fs::create_dir_all(blob_dir)
        .map_err(|e| format!("failed to create blob dir '{}': {e}", blob_dir.display()))?;
    let owner_file = blob_dir.join(BLOB_OWNER_FILE);
    if !owner_file.exists() {
        create_exclusively(&owner_file, format!("{id}\n").as_bytes())?;
    }
    let owner = read_owner(&owner_file)?;
    if owner != id {
        return Err(format!(
            "blob dir '{}' belongs to another data dir; give each data dir its own blob dir",
            blob_dir.display()
        ));
    }
    Ok(())
}

/// Create `path` holding `contents`, unless it already exists (which is
/// not an error: the caller reads who won). A synced temporary file is
/// hard-linked into place, so the file appears whole. Where the file system
/// has no hard links (FAT, SMB, some FUSE and NFS mounts), it is created
/// with `O_EXCL` and then written, and readers wait out the moment it is
/// empty.
fn create_exclusively(path: &Path, contents: &[u8]) -> Result<(), String> {
    use std::io::{ErrorKind, Write};
    let parent = path.parent().unwrap_or(Path::new("."));
    let claim = temporary_beside(path);
    write_synced(&claim, contents)?;
    let linked = std::fs::hard_link(&claim, path);
    let _ = std::fs::remove_file(&claim);
    match linked {
        Ok(()) => return sync_dir(parent),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => return Ok(()),
        // No hard links here; any real failure surfaces from the create below.
        Err(_) => {}
    }
    let mut file = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => return Ok(()),
        Err(error) => return Err(format!("claim {}: {error}", path.display())),
    };
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    sync_dir(parent)
}

/// The id in an owner file, waiting briefly while one created without hard
/// links is still empty.
fn read_owner(owner_file: &Path) -> Result<String, String> {
    for _ in 0..50 {
        let owner = std::fs::read_to_string(owner_file)
            .map_err(|e| format!("read {}: {e}", owner_file.display()))?;
        if owner.ends_with('\n') {
            return Ok(owner.trim().to_owned());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Err(format!(
        "{} is incomplete; if no other data dir uses this blob dir, delete it and start again",
        owner_file.display()
    ))
}

/// The data directory's id, made on first use.
fn store_id(data_dir: &Path) -> Result<String, String> {
    let file = data_dir.join(STORE_ID_FILE);
    match std::fs::read_to_string(&file) {
        Ok(id) => return Ok(id.trim().to_owned()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("read {}: {error}", file.display())),
    }
    let id = uuid::Uuid::new_v4().to_string();
    write_durably(&file, format!("{id}\n").as_bytes())?;
    Ok(id)
}

/// `path` as one name per directory, read the way the file system reads it:
/// each part that exists is canonicalized as it is reached (so a symlink,
/// and a `..` after it, resolve as they would on disk), and parts that do
/// not exist yet are appended, with `.` and `..` resolved as written.
fn absolute(path: &Path) -> Result<PathBuf, String> {
    let path =
        std::path::absolute(path).map_err(|e| format!("resolve '{}': {e}", path.display()))?;
    let mut resolved = PathBuf::new();
    for part in path.components() {
        match part {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                resolved.pop();
            }
            std::path::Component::Normal(name) => {
                resolved.push(name);
                match std::fs::canonicalize(&resolved) {
                    Ok(on_disk) => resolved = without_verbatim_prefix(on_disk),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(format!("resolve '{}': {error}", resolved.display()));
                    }
                }
            }
            // The drive or share, and the root: kept as written.
            other => resolved.push(other),
        }
    }
    Ok(resolved)
}

/// Windows' canonical paths are verbatim (`\\?\C:\x`, `\\?\UNC\host\share\x`);
/// the plain spelling (`C:\x`, `\\host\share\x`) names the same file and is
/// the one people write, so it is the one recorded.
fn without_verbatim_prefix(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        let text = path.to_string_lossy();
        if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
            return PathBuf::from(format!(r"\\{rest}"));
        }
        if let Some(rest) = text.strip_prefix(r"\\?\") {
            if rest.as_bytes().get(1) == Some(&b':') {
                return PathBuf::from(rest);
            }
        }
    }
    path
}

/// Write `path` so that a crash leaves either the old file or the whole
/// new one: a synced temporary file renamed into place, then the directory
/// synced.
fn write_durably(path: &Path, contents: &[u8]) -> Result<(), String> {
    let temporary = temporary_beside(path);
    let written = write_synced(&temporary, contents).and_then(|()| {
        std::fs::rename(&temporary, path)
            .map_err(|e| format!("rename {}: {e}", temporary.display()))
    });
    if written.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    written?;
    sync_dir(path.parent().unwrap_or(Path::new(".")))
}

/// A temporary name next to `path`, unique to this write.
fn temporary_beside(path: &Path) -> PathBuf {
    path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4().simple()))
}

fn write_synced(path: &Path, contents: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let mut file =
        std::fs::File::create(path).map_err(|e| format!("create {}: {e}", path.display()))?;
    let written = file
        .write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|e| format!("write {}: {e}", path.display()));
    if written.is_err() {
        let _ = std::fs::remove_file(path);
    }
    written
}

fn sync_dir(dir: &Path) -> Result<(), String> {
    std::fs::File::open(dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| format!("sync {}: {e}", dir.display()))
}

fn refuse_pre_sqlite_data_dir(path: &std::path::Path) -> Result<(), String> {
    let started = path.join(STORE_FILE).exists();
    match PRE_SQLITE_STORES.iter().find(|dir| path.join(dir).exists()) {
        Some(old) if !started => Err(format!(
            "data dir '{}' holds a store from before SQLite ({old}); migrate it before starting this server",
            path.display()
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
fn test_schema_branches(schema: Option<&Schema>) -> Vec<String> {
    schema.map(|_| "main".to_string()).into_iter().collect()
}

fn validate_server_config(auth_config: &AuthConfig) -> Result<(), String> {
    if let Some(workload) = &auth_config.trusted_workload {
        if auth_config.admin_secret.is_some() || auth_config.backend_secret.is_some() {
            return Err(
                "a trusted workload replaces the admin and backend secrets; configure one or the other"
                    .to_owned(),
            );
        }
        for (name, value) in [
            ("issuer", &workload.issuer),
            ("audience", &workload.audience),
            ("subject", &workload.subject),
        ] {
            if value.trim().is_empty() {
                return Err(format!("trusted workload {name} cannot be empty"));
            }
        }
    }
    if auth_config
        .admin_secret
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("admin secret cannot be empty".to_owned());
    }
    if auth_config
        .backend_secret
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("backend secret cannot be empty".to_owned());
    }

    Ok(())
}

fn log_auth_config(auth_config: &AuthConfig) {
    info!(
        "Auth configured: local_first={}, backend={}, admin={}",
        auth_config.allow_local_first_auth,
        auth_config.backend_secret.is_some(),
        auth_config.admin_secret.is_some()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::catalogue::CatalogueStore;
    use jazz::tools::AppId;

    fn dynamic_bootstrap_schema() -> jazz::tools::public_schema::Schema {
        jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("notes")
                    .column("body", jazz::tools::public_schema::ColumnType::Text),
            )
            .build()
    }

    #[tokio::test]
    async fn builder_omitted_secrets_preserve_unconfigured_auth() {
        let built = ServerBuilder::new(AppId::from_name("builder-omitted-secrets"))
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("builder accepts omitted credentials");

        assert!(built.state.auth_config.admin_secret.is_none());
        assert!(built.state.auth_config.backend_secret.is_none());
    }

    #[tokio::test]
    async fn builder_rejects_empty_and_whitespace_only_admin_and_backend_secrets() {
        for (field, expected_error, value) in [
            ("admin_secret", "admin secret cannot be empty", ""),
            ("admin_secret", "admin secret cannot be empty", " \t\n"),
            ("backend_secret", "backend secret cannot be empty", ""),
            ("backend_secret", "backend secret cannot be empty", " \t\n"),
        ] {
            let mut auth_config = AuthConfig::default();
            match field {
                "admin_secret" => auth_config.admin_secret = Some(value.to_owned()),
                "backend_secret" => auth_config.backend_secret = Some(value.to_owned()),
                _ => unreachable!("test field is known"),
            }

            let result = ServerBuilder::new(AppId::from_name("builder-blank-secrets"))
                .with_auth_config(auth_config)
                .with_storage(StorageBackend::InMemory)
                .build()
                .await;
            let error = result
                .err()
                .expect("blank credentials must be rejected during build");

            assert_eq!(error, expected_error);
            if !value.is_empty() {
                assert!(
                    !error.contains(value),
                    "validation error must not expose the configured credential"
                );
            }
        }
    }

    #[tokio::test]
    async fn builder_refuses_shared_secrets_next_to_a_trusted_workload() {
        let workload = crate::middleware::workload::TrustedWorkload {
            issuer: "https://oidc.example".to_owned(),
            audience: "aud".to_owned(),
            subject: "sub".to_owned(),
        };
        let config = AuthConfig {
            trusted_workload: Some(workload.clone()),
            backend_secret: Some("shared".to_owned()),
            ..AuthConfig::default()
        };
        assert!(
            validate_server_config(&config)
                .unwrap_err()
                .contains("trusted workload")
        );
        let blank = AuthConfig {
            trusted_workload: Some(crate::middleware::workload::TrustedWorkload {
                subject: " ".to_owned(),
                ..workload
            }),
            ..AuthConfig::default()
        };
        assert!(
            validate_server_config(&blank)
                .unwrap_err()
                .contains("subject")
        );
    }

    #[tokio::test]
    async fn builder_accepts_nonblank_admin_and_backend_secrets() {
        let admin_secret = " configured-admin ";
        let backend_secret = " configured-backend ";
        let built = ServerBuilder::new(AppId::from_name("builder-nonblank-secrets"))
            .with_auth_config(AuthConfig {
                admin_secret: Some(admin_secret.to_owned()),
                backend_secret: Some(backend_secret.to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("builder accepts nonblank credentials");

        assert_eq!(
            built.state.auth_config.admin_secret.as_deref(),
            Some(admin_secret)
        );
        assert_eq!(
            built.state.auth_config.backend_secret.as_deref(),
            Some(backend_secret)
        );
    }

    #[tokio::test]
    async fn builder_uses_global_tier_without_upstream() {
        let built = ServerBuilder::new(AppId::from_name("global-builder-tier"))
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build global server");

        let tiers = built
            .state
            .catalogue_store
            .local_durability_tiers_for_test()
            .expect("read catalogue durability tiers");

        assert_eq!(
            tiers,
            std::collections::HashSet::from([DurabilityTier::Global])
        );
    }

    #[tokio::test]
    async fn a_data_dir_keeps_its_blob_store_except_moving_out_of_sqlite() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let start = |blobs: BlobStore| {
            ServerBuilder::new(AppId::from_name("blob-store-pin"))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .with_blob_store(blobs)
                .build()
        };
        let files = || BlobStore::Files { dir: None };

        // A store from before blob stores keeps its blobs in SQLite, and may
        // move them out.
        start(BlobStore::Sqlite)
            .await
            .expect("sqlite")
            .shutdown()
            .await;
        assert!(!data_dir.path().join(BLOB_STORE_MARKER).exists());
        start(files())
            .await
            .expect("sqlite to files")
            .shutdown()
            .await;
        assert_eq!(
            std::fs::read_to_string(data_dir.path().join(BLOB_STORE_MARKER)).unwrap(),
            "files\n"
        );
        start(files()).await.expect("files again").shutdown().await;

        for other in [
            BlobStore::Sqlite,
            BlobStore::Files {
                dir: Some(data_dir.path().join("elsewhere")),
            },
        ] {
            let error = start(other.clone())
                .await
                .err()
                .unwrap_or_else(|| panic!("{other:?} after files starts"));
            assert!(error.contains("not supported yet"), "{error}");
        }
    }

    /// Without a prefix, each data dir gets its own under the app's, so two
    /// stores sharing a bucket (preview and production) never share keys.
    #[tokio::test]
    async fn a_bucket_without_a_prefix_gets_one_per_data_dir() {
        let app = AppId::from_name("blob-store-prefix");
        let blobs = BlobStore::S3(crate::s3::S3Options {
            bucket: "jazz".to_owned(),
            region: "auto".to_owned(),
            endpoint: Some("http://127.0.0.1:9".to_owned()),
            prefix: String::new(),
            path_style: true,
            credentials: crate::s3::S3Credentials::Static(crate::s3::Keys {
                access_key_id: "k".to_owned(),
                secret_access_key: "s".to_owned(),
                session_token: None,
            }),
            sts_endpoint: None,
            environment: None,
        });
        let mut recorded = Vec::new();
        for _ in 0..2 {
            let data_dir = tempfile::TempDir::new().expect("temp data dir");
            ServerBuilder::new(app)
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .with_blob_store(blobs.clone())
                .build()
                .await
                .expect("start")
                .shutdown()
                .await;
            let id = std::fs::read_to_string(data_dir.path().join(STORE_ID_FILE)).unwrap();
            let marker = std::fs::read_to_string(data_dir.path().join(BLOB_STORE_MARKER)).unwrap();
            assert_eq!(
                marker,
                format!("s3 http://127.0.0.1:9 jazz/jazz/{}/\n", id.trim())
            );
            recorded.push(marker);
        }
        assert_ne!(recorded[0], recorded[1]);
    }

    /// A Blob store's default prefix is the data dir's own too.
    #[tokio::test]
    async fn a_vercel_blob_store_without_a_prefix_gets_one_per_data_dir() {
        let app = AppId::from_name("vercel-blob-prefix");
        let blobs = BlobStore::VercelBlob(crate::vercel_blob::VercelBlobOptions {
            store_id: "abc".to_owned(),
            prefix: String::new(),
            credentials: crate::vercel_blob::VercelBlobCredentials::ReadWriteToken("t".to_owned()),
            api_url: "http://127.0.0.1:9".to_owned(),
            blob_url: Some("http://127.0.0.1:9".to_owned()),
            environment: None,
        });
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        ServerBuilder::new(app)
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .with_blob_store(blobs)
            .build()
            .await
            .expect("start")
            .shutdown()
            .await;
        let id = std::fs::read_to_string(data_dir.path().join(STORE_ID_FILE)).unwrap();
        let marker = std::fs::read_to_string(data_dir.path().join(BLOB_STORE_MARKER)).unwrap();
        assert_eq!(marker, format!("vercel-blob abc/jazz/{}/\n", id.trim()));
    }

    #[tokio::test]
    async fn a_data_dir_may_move_its_blobs_from_sqlite_to_a_bucket_and_keeps_it() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let bucket = |prefix: &str| {
            BlobStore::S3(crate::s3::S3Options {
                bucket: "jazz".to_owned(),
                region: "auto".to_owned(),
                endpoint: Some("http://127.0.0.1:9".to_owned()),
                prefix: prefix.to_owned(),
                path_style: true,
                credentials: crate::s3::S3Credentials::Static(crate::s3::Keys {
                    access_key_id: "k".to_owned(),
                    secret_access_key: "s".to_owned(),
                    session_token: None,
                }),
                sts_endpoint: None,
                environment: None,
            })
        };
        let start = |blobs: BlobStore| {
            ServerBuilder::new(AppId::from_name("blob-store-s3"))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .with_blob_store(blobs)
                .build()
        };
        start(BlobStore::Sqlite)
            .await
            .expect("sqlite")
            .shutdown()
            .await;
        start(bucket("a/"))
            .await
            .expect("sqlite to s3")
            .shutdown()
            .await;
        assert_eq!(
            std::fs::read_to_string(data_dir.path().join(BLOB_STORE_MARKER)).unwrap(),
            "s3 http://127.0.0.1:9 jazz/a/\n"
        );
        start(bucket("a/"))
            .await
            .expect("the same bucket")
            .shutdown()
            .await;
        for other in [
            bucket("b/"),
            BlobStore::Files { dir: None },
            BlobStore::Sqlite,
        ] {
            let error = start(other.clone())
                .await
                .err()
                .unwrap_or_else(|| panic!("{other:?} after s3 starts"));
            assert!(error.contains("not supported yet"), "{error}");
        }
    }

    #[tokio::test]
    async fn a_blob_dir_is_named_the_same_however_it_is_spelled() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let start = |dir: PathBuf| {
            ServerBuilder::new(AppId::from_name("blob-dir-spelling"))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .with_blob_store(BlobStore::Files { dir: Some(dir) })
                .build()
        };
        // The default, spelled out with a trailing separator, is the default.
        let spelled_out = PathBuf::from(format!("{}/blobs/", data_dir.path().display()));
        start(spelled_out)
            .await
            .expect("default spelled out")
            .shutdown()
            .await;
        assert_eq!(
            std::fs::read_to_string(data_dir.path().join(BLOB_STORE_MARKER)).unwrap(),
            "files\n"
        );
        start(data_dir.path().join("x/../blobs"))
            .await
            .expect("default through ..")
            .shutdown()
            .await;
    }

    #[cfg(unix)]
    #[test]
    fn a_blob_dir_through_a_symlink_is_the_directory_it_points_to() {
        let root = tempfile::TempDir::new().expect("temp root");
        let data_dir = root.path().join("data");
        std::fs::create_dir_all(data_dir.join("deep")).unwrap();
        std::os::unix::fs::symlink(data_dir.join("deep"), root.path().join("link")).unwrap();
        // `link/..` is `data`, not `root`, on disk.
        let through_link = root.path().join("link/../blobs");
        let plan = plan_blob_store(
            &data_dir,
            &BlobStore::Files {
                dir: Some(through_link),
            },
        )
        .expect("plan");
        assert_eq!(plan.record.expect("a record").contents, "files\n");
    }

    #[test]
    fn two_data_dirs_claiming_one_blob_dir_at_once_leave_one_owner() {
        let blobs = tempfile::TempDir::new().expect("temp blob dir");
        let dirs: Vec<_> = (0..8).map(|_| tempfile::TempDir::new().unwrap()).collect();
        let start = std::sync::Arc::new(std::sync::Barrier::new(dirs.len()));
        let claims: Vec<_> = dirs
            .iter()
            .map(|dir| {
                let (data, blob) = (dir.path().to_path_buf(), blobs.path().to_path_buf());
                let start = start.clone();
                std::thread::spawn(move || {
                    let id = store_id(&data).unwrap();
                    start.wait();
                    (id, claim_blob_dir(&data, &blob).is_ok())
                })
            })
            .collect();
        let winners: Vec<String> = claims
            .into_iter()
            .map(|claim| claim.join().unwrap())
            .filter_map(|(id, won)| won.then_some(id))
            .collect();
        assert_eq!(winners.len(), 1);
        assert_eq!(
            std::fs::read_to_string(blobs.path().join(BLOB_OWNER_FILE)).unwrap(),
            format!("{}\n", winners[0])
        );
    }

    #[test]
    fn a_blob_dir_inside_the_data_dir_is_recorded_relative_to_it() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let outside = tempfile::TempDir::new().expect("temp blob dir");
        let plan = |dir: PathBuf| {
            plan_blob_store(data_dir.path(), &BlobStore::Files { dir: Some(dir) })
                .expect("plan")
                .record
                .expect("a record to write")
                .contents
        };
        assert_eq!(plan(data_dir.path().join("large")), "files large\n");
        assert_eq!(
            plan(outside.path().to_path_buf()),
            format!("files {}\n", absolute(outside.path()).unwrap().display())
        );
    }

    #[tokio::test]
    async fn two_data_dirs_cannot_share_a_blob_dir() {
        let first = tempfile::TempDir::new().expect("temp data dir");
        let second = tempfile::TempDir::new().expect("temp data dir");
        let blobs = tempfile::TempDir::new().expect("temp blob dir");
        let start = |data_dir: &tempfile::TempDir| {
            ServerBuilder::new(AppId::from_name("shared-blob-dir"))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .with_blob_store(BlobStore::Files {
                    dir: Some(blobs.path().to_path_buf()),
                })
                .build()
        };
        start(&first).await.expect("first").shutdown().await;
        let error = start(&second).await.err().expect("second refused");
        assert!(error.contains("belongs to another data dir"), "{error}");
        assert!(
            !second.path().join(BLOB_STORE_MARKER).exists(),
            "a refused start records nothing"
        );
        start(&first).await.expect("first again").shutdown().await;
    }

    #[tokio::test]
    async fn persistent_builder_fails_when_a_catalogue_definition_is_corrupt() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let builder = || {
            ServerBuilder::new(AppId::from_name("corrupt-durable-catalogue")).with_storage(
                StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                },
            )
        };
        let server = builder().build().await.expect("first start");
        server
            .state
            .catalogue_store
            .publish_schema(dynamic_bootstrap_schema())
            .expect("publish schema");
        server.shutdown().await;
        let file = data_dir.path().join(STORE_FILE);
        let def = |update: &str| {
            rusqlite::Connection::open(&file)
                .unwrap()
                .execute(update, [])
                .unwrap();
        };
        def("UPDATE _jazz_schemas SET def = replace(def, 'notes', 'notez')");

        let error = builder()
            .build()
            .await
            .err()
            .expect("startup fails rather than serving a corrupt catalogue");
        assert!(
            error.contains("failed to read durable catalogue")
                && error.contains("does not hash to its key"),
            "{error}"
        );

        def("UPDATE _jazz_schemas SET def = replace(def, 'notez', 'notes')");
        builder()
            .build()
            .await
            .expect("builder starts after the definition is repaired")
            .shutdown()
            .await;
    }

    #[tokio::test]
    async fn dynamic_builder_starts_core_server_shell_from_rehydrated_catalogue_schema() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("dynamic-server-shell-rehydrate");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("todos")
                    .column("title", jazz::tools::public_schema::ColumnType::Text)
                    .column("workspace_id", jazz::tools::public_schema::ColumnType::Uuid)
                    .branch_by("workspace_id"),
            )
            .build();
        let schema_hash = jazz::tools::public_schema::SchemaHash::compute(&schema);

        {
            let built = ServerBuilder::new(app_id)
                .with_schema(schema)
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build fixed schema server");
            assert!(built.state.runtime().is_some());
            built
                .state
                .catalogue_store
                .persist_schema()
                .expect("publish fixed schema catalogue");
        }

        let rebuilt = ServerBuilder::new(app_id)
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("build dynamic server from rehydrated catalogue");

        assert!(rebuilt.state.runtime().is_some());
        let restored = rebuilt
            .state
            .catalogue
            .known_schema(&rebuilt.state.catalogue_store, &schema_hash)
            .expect("read rehydrated schema")
            .expect("rehydrated schema is present");
        let restored_todos = restored
            .get(&jazz::tools::public_schema::TableName::new("todos"))
            .expect("restored todos table");
        assert_eq!(
            restored_todos.branch_by,
            vec![jazz::tools::public_schema::ColumnName::new("workspace_id")]
        );
    }

    #[tokio::test]
    async fn persistent_adapter_starts_core_server_shell_with_catalogue_storage_after_restart() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("persistent-server-shell-restart");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("todos")
                    .column("title", jazz::tools::public_schema::ColumnType::Text),
            )
            .build();

        let retained_state = {
            let built = ServerBuilder::new(app_id)
                .with_schema(schema.clone())
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build persistent server with server shell");

            assert!(built.state.runtime().is_some());
            assert!(data_dir.path().join(STORE_FILE).exists());
            let files: Vec<String> = std::fs::read_dir(data_dir.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .filter(|name| !name.ends_with("-wal") && !name.ends_with("-shm"))
                .collect();
            assert_eq!(files, vec![STORE_FILE.to_owned()], "one file per app");
            assert_eq!(
                built.shutdown().await,
                crate::server::ShutdownPhase::StorageClosed,
                "the public builder lifecycle must join the shell before its store is reopened"
            );
            Arc::clone(&built.state)
        };
        assert!(
            retained_state.runtime().is_none(),
            "shutdown must retire the shell even if request/router state outlives BuiltServer"
        );

        let rebuilt = ServerBuilder::new(app_id)
            .with_schema(schema.clone())
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("rebuild persistent server with server shell");

        assert!(rebuilt.state.runtime().is_some());
        assert!(data_dir.path().join("jazz.sqlite").exists());
        rebuilt.shutdown().await;

        // Some direct builder consumers own only `BuiltServer` and use Rust
        // scope exit as their lifecycle. Its last-shell fallback must join as
        // well: this reopen has no timeout or sleep to mask an owner-thread
        // race.
        {
            let dropped = ServerBuilder::new(app_id)
                .with_schema(schema.clone())
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build persistent server for direct-drop lifecycle");
            assert!(dropped.state.runtime().is_some());
        }

        let reopened_after_drop = ServerBuilder::new(app_id)
            .with_schema(schema)
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("reopen persistent server after direct builder drop");
        assert!(reopened_after_drop.state.runtime().is_some());
        assert_eq!(
            reopened_after_drop.shutdown().await,
            crate::server::ShutdownPhase::StorageClosed
        );
        drop(retained_state);
    }

    #[tokio::test]
    async fn a_data_dir_from_another_layout_is_refused_and_left_unchanged() {
        let app_id = AppId::from_name("refused-data-dir");
        for old in PRE_SQLITE_STORES.iter() {
            let data_dir = tempfile::TempDir::new().expect("temp data dir");
            let old_path = data_dir.path().join(old);
            if old.ends_with(".rocksdb") {
                std::fs::create_dir(&old_path).unwrap();
            } else {
                std::fs::write(&old_path, b"state this build does not read").unwrap();
            }
            let Err(error) = ServerBuilder::new(app_id)
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
            else {
                panic!("a data dir holding {old} must be refused");
            };
            assert!(error.contains(old), "{error}");
            assert!(
                !data_dir.path().join(STORE_FILE).exists(),
                "a refused data dir gets no store beside {old}"
            );
        }
    }

    #[tokio::test]
    async fn a_dynamic_server_keeps_catalogue_accounts_and_rows_in_one_file() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let built = ServerBuilder::new(AppId::from_name("one-file-per-app"))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("build dynamic persistent server");
        assert!(built.state.runtime().is_none(), "no schema published yet");
        let alice = jazz::account_registry::Principal {
            issuer: "https://issuer.example".into(),
            subject: "alice".into(),
        };
        let accounts = built.state.accounts.as_ref().unwrap();
        // The account registry is written before the runtime opens on the
        // store...
        let first = accounts.login_or_register(alice.clone()).await.unwrap();
        built
            .state
            .start_core_server_shell(
                jazz::schema::JazzSchema::new(&dynamic_bootstrap_schema()).unwrap(),
            )
            .expect("start the runtime on the store");
        // ...and read through it afterwards.
        assert_eq!(accounts.login(alice.clone()).await.unwrap(), first);
        assert_eq!(
            built.shutdown().await,
            crate::server::ShutdownPhase::StorageClosed
        );
        let files: Vec<String> = std::fs::read_dir(data_dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| !name.ends_with("-wal") && !name.ends_with("-shm"))
            .collect();
        assert_eq!(files, vec![STORE_FILE.to_owned()]);

        let reopened = ServerBuilder::new(AppId::from_name("one-file-per-app"))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("reopen");
        assert_eq!(
            reopened
                .state
                .accounts
                .as_ref()
                .unwrap()
                .login(alice)
                .await
                .unwrap(),
            first
        );
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn persistent_adapter_reopens_after_first_shutdown_waiter_is_aborted() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("persistent-server-shell-aborted-shutdown");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(jazz::tools::public_schema::TableSchema::builder("todos"))
            .build();
        let built = ServerBuilder::new(app_id)
            .with_schema(schema.clone())
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("build persistent server");
        let state = Arc::clone(&built.state);
        let request = state
            .shutdown
            .try_enter_app_request()
            .expect("running server accepts request");
        state.shutdown.request_shutdown();

        let first_state = Arc::clone(&state);
        let first = tokio::spawn(async move { first_state.run_shutdown_finalization().await });
        let mut phases = state.shutdown.subscribe();
        while *phases.borrow_and_update() != crate::server::ShutdownPhase::DrainingConnections {
            phases
                .changed()
                .await
                .expect("detached finalizer remains alive");
        }
        first.abort();
        let _ = first.await;
        drop(request);
        assert_eq!(
            state.run_shutdown_finalization().await,
            crate::server::ShutdownPhase::StorageClosed
        );

        let reopened = ServerBuilder::new(app_id)
            .with_schema(schema)
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("reopen persistent server after aborted shutdown waiter");
        assert_eq!(
            reopened.shutdown().await,
            crate::server::ShutdownPhase::StorageClosed
        );
    }

    #[test]
    fn persistent_adapter_shutdown_survives_initiating_runtime_drop() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("persistent-server-shell-foreign-shutdown");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(jazz::tools::public_schema::TableSchema::builder("todos"))
            .build();
        let (built, state, request) = {
            let first_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("first shutdown runtime");
            let built = first_runtime
                .block_on(
                    ServerBuilder::new(app_id)
                        .with_schema(schema.clone())
                        .with_storage(StorageBackend::Persistent {
                            path: data_dir.path().to_path_buf(),
                        })
                        .build(),
                )
                .expect("build persistent server");
            let state = Arc::clone(&built.state);
            let request = state
                .shutdown
                .try_enter_app_request()
                .expect("running server accepts request");
            state.shutdown.request_shutdown();
            first_runtime.block_on(async {
                let first_state = Arc::clone(&state);
                tokio::spawn(async move { first_state.run_shutdown_finalization().await });
                let mut phases = state.shutdown.subscribe();
                while *phases.borrow_and_update()
                    != crate::server::ShutdownPhase::DrainingConnections
                {
                    phases
                        .changed()
                        .await
                        .expect("dedicated finalizer remains alive");
                }
            });
            // Dropping `first_runtime` cancels the initiating caller after it
            // has begun but before the request guard lets teardown progress.
            (built, state, request)
        };
        drop(request);

        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("second shutdown runtime");
        let reopened = second_runtime.block_on(async {
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(1), built.shutdown())
                    .await
                    .expect("later runtime reaches durable-close barrier"),
                crate::server::ShutdownPhase::StorageClosed
            );
            assert!(state.runtime().is_none());
            ServerBuilder::new(app_id)
                .with_schema(schema)
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("reopen persistent server after live shutdown")
        });
        assert_eq!(
            second_runtime.block_on(reopened.shutdown()),
            crate::server::ShutdownPhase::StorageClosed
        );
    }
}
