use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use jazz::groove::storage::StorageFactory;
use jazz::schema::JazzSchema;
use jazz::serving::{NodeRole, StorageConfig};
use tracing::info;

use crate::middleware::AuthConfig;
use crate::middleware::auth::{
    JWKS_CACHE_TTL, JWKS_MAX_STALE, JwksCache, JwtVerifier, StaticJwtVerifier,
};
use crate::server::routes;
use crate::server::{
    CatalogueKvStorage, CatalogueMemoryStorage, DynCatalogueStorage, ServerState, StoredCatalogue,
};
use jazz::tools::AppId;
#[allow(deprecated)]
use jazz::tools::public_schema::Schema;
#[cfg(test)]
use jazz::tools::sync::DurabilityTier;

const CATALOGUE_ROCKSDB_DIR: &str = "catalogue.rocksdb";
const SERVER_SHELL_ROCKSDB_DIR: &str = "server-shell.rocksdb";
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
/// `Persistent` requires a target-owned [`StorageFactory`] supplied at the
/// native composition boundary. SQLite remains a client/native storage
/// backend, but is not a supported server shell backend.
#[derive(Debug, Clone)]
pub enum StorageBackend {
    InMemory,
    Persistent {
        path: PathBuf,
    },
    #[cfg(feature = "sqlite")]
    Sqlite {
        path: PathBuf,
    },
}

pub struct ServerBuilder {
    app_id: AppId,
    auth_config: AuthConfig,
    schema_mode: ServerSchemaMode,
    storage_backend: StorageBackend,
    core_server_shell_schema: Option<JazzSchema>,
    shutdown_timeout: Duration,
    storage_factory: Option<Arc<dyn StorageFactory>>,
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
            storage_backend: StorageBackend::Persistent {
                path: PathBuf::from("./data"),
            },
            core_server_shell_schema: None,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            storage_factory: None,
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

    pub fn with_storage(mut self, backend: StorageBackend) -> Self {
        self.storage_backend = backend;
        self
    }

    /// Supply the target-owned durable storage adapter.
    pub fn with_storage_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(factory);
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
        let auth_config = self.auth_config.clone();
        validate_server_config(&auth_config)?;
        let jwt_verifier = build_jwt_verifier(&auth_config).await?;
        log_auth_config(&auth_config);

        let (catalogue_store, latest_catalogue_schema) = self.build_catalogue_store()?;
        let core_server_shell_storage_config = self.build_core_server_shell_storage_config();
        let core_server_shell = self.build_core_server_shell(
            latest_catalogue_schema,
            core_server_shell_storage_config.clone(),
        )?;
        let core_server_shell_storage_config = core_server_shell_storage_config.ok();

        let accounts = {
            let durable = match &self.storage_backend {
                StorageBackend::InMemory => None,
                StorageBackend::Persistent { path } => Some((
                    self.storage_factory
                        .clone()
                        .ok_or("account registry requires storage factory")?,
                    path.join("accounts.rocksdb"),
                )),
                #[cfg(feature = "sqlite")]
                StorageBackend::Sqlite { .. } => {
                    return Err("server account registry does not support sqlite".into());
                }
            };
            Some(crate::server::accounts::AccountRegistryOwner::open(
                durable,
            )?)
        };

        let state = Arc::new(ServerState {
            accounts,
            catalogue_store,
            catalogue: crate::server::ServerCatalogue,
            app_id: self.app_id,
            auth_config,
            jwt_verifier,
            core_server_shell: std::sync::RwLock::new(core_server_shell),
            websocket_admissions: Arc::new(
                crate::server::routes::WebSocketAdmissionState::default(),
            ),
            core_server_shell_storage_config,
            storage_factory: self.storage_factory.clone(),
            runtime_catalogue_publication: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            runtime_catalogue_before_publication_hook: std::sync::Mutex::new(None),
            #[cfg(test)]
            runtime_catalogue_after_permissions_read_hook: std::sync::Mutex::new(None),
            shutdown: crate::server::ShutdownController::new(self.shutdown_timeout),
        });

        // Restore the durable administrative selection before serving snapshots.
        super::runtime_catalogue::publish_runtime_catalogue(&state, &[], &[])
            .await
            .map_err(|error| format!("restore active schema before serving: {error}"))?;

        let app = routes::create_router(state.clone());
        Ok(BuiltServer { state, app })
    }

    /// Build the direct admin catalogue store used by HTTP catalogue routes.
    ///
    fn build_catalogue_store(&self) -> Result<(StoredCatalogue, Option<Schema>), String> {
        let storage = self.build_catalogue_storage()?;
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
                self.app_id,
                initial_schema,
                storage,
                schema_branches,
                local_durability_tiers,
            )
            .map_err(|error| format!("failed to read durable catalogue: {error}"))?
        };
        #[cfg(not(test))]
        let store = StoredCatalogue::new(self.app_id, initial_schema, storage)
            .map_err(|error| format!("failed to read durable catalogue: {error}"))?;

        let latest_catalogue_schema = store
            .latest_published_schema()
            .map_err(|error| format!("failed to read latest catalogue schema: {error:?}"))?;
        Ok((store, latest_catalogue_schema))
    }

    fn build_core_server_shell(
        &self,
        latest_catalogue_schema: Option<Schema>,
        storage_config: Result<StorageConfig, String>,
    ) -> Result<Option<crate::server::ServerRuntimeHandle>, String> {
        let role = NodeRole::Core;
        if let Some(schema) = &self.core_server_shell_schema {
            let storage_config = storage_config?;
            return Ok(Some(
                crate::server::ServerRuntimeHandle::start_with_storage_config(
                    schema.clone(),
                    storage_config,
                    self.storage_factory.clone(),
                    role,
                    None,
                )?,
            ));
        }

        let schema = match &self.schema_mode {
            ServerSchemaMode::Fixed(schema) => Some(schema.clone()),
            ServerSchemaMode::Dynamic => latest_catalogue_schema,
        };
        let Some(schema) = schema else {
            return Ok(None);
        };
        let storage_config = storage_config?;
        let schema = jazz::schema::JazzSchema::new(&schema)
            .map_err(|error| format!("failed to build server shell schema: {error}"))?;
        Ok(Some(
            crate::server::ServerRuntimeHandle::start_with_storage_config(
                schema,
                storage_config,
                self.storage_factory.clone(),
                role,
                None,
            )?,
        ))
    }

    fn build_core_server_shell_storage_config(&self) -> Result<StorageConfig, String> {
        match &self.storage_backend {
            StorageBackend::InMemory => Ok(StorageConfig::InMemory),
            StorageBackend::Persistent { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;

                Ok(StorageConfig::RocksDb {
                    path: path.join(SERVER_SHELL_ROCKSDB_DIR),
                })
            }
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite { .. } => {
                Err("server shell storage does not support sqlite yet".to_owned())
            }
        }
    }

    fn build_catalogue_storage(&self) -> Result<DynCatalogueStorage, String> {
        match &self.storage_backend {
            StorageBackend::Persistent { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;

                let factory = self.storage_factory.as_ref().ok_or_else(|| {
                    "persistent catalogue storage requires a target-shell storage factory"
                        .to_owned()
                })?;
                let db_path = path.join(CATALOGUE_ROCKSDB_DIR);
                let storage = CatalogueKvStorage::open(Arc::clone(factory), db_path.clone())
                    .map_err(|error| {
                        format!(
                            "failed to open catalogue storage '{}': {error}",
                            db_path.display()
                        )
                    })?;
                Ok(Box::new(storage))
            }
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite { .. } => {
                Err("server catalogue storage does not support sqlite".to_owned())
            }
            StorageBackend::InMemory => Ok(Box::new(CatalogueMemoryStorage::new())),
        }
    }

    #[cfg(test)]
    fn local_durability_tier(&self) -> DurabilityTier {
        DurabilityTier::GlobalServer
    }
}

#[cfg(test)]
fn test_schema_branches(schema: Option<&Schema>) -> Vec<String> {
    schema.map(|_| "main".to_string()).into_iter().collect()
}

async fn build_jwt_verifier(auth_config: &AuthConfig) -> Result<Option<Arc<JwtVerifier>>, String> {
    match (
        auth_config.jwks_url.as_ref(),
        auth_config.jwt_public_key.as_ref(),
    ) {
        (Some(_), Some(_)) => Err(
            "configure either --jwks-url / JAZZ_JWKS_URL or --jwt-public-key / JAZZ_JWT_PUBLIC_KEY, not both"
                .to_string(),
        ),
        (None, None) => Ok(None),
        (None, Some(public_key)) => {
            let verifier = StaticJwtVerifier::from_public_key(public_key)?;
            Ok(Some(Arc::new(JwtVerifier::Static(verifier))))
        }
        (Some(jwks_url), None) => {
            let jwks_ttl = std::env::var("JAZZ_JWKS_CACHE_TTL_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(JWKS_CACHE_TTL);
            let jwks_max_stale = std::env::var("JAZZ_JWKS_MAX_STALE_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(JWKS_MAX_STALE);

            let http_client = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| format!("failed to build JWKS HTTP client: {e}"))?;

            let verifier = Arc::new(JwtVerifier::Jwks(JwksCache::new(
                jwks_url.clone(),
                http_client,
                jwks_ttl,
                jwks_max_stale,
            )));

            // Warm the cache in the background. The JWKS endpoint may not be
            // available yet (e.g. Jazz server starts during Next.js config resolution,
            // before the app is listening). First auth request will block on fetch
            // if the background warm hasn't completed.
            {
                let verifier = Arc::clone(&verifier);
                tokio::spawn(async move {
                    if let JwtVerifier::Jwks(cache) = verifier.as_ref()
                        && let Err(e) = cache.load(false).await
                    {
                        tracing::warn!(
                            "Background JWKS warm failed (will retry on first auth request): {e}"
                        );
                    }
                });
            }

            Ok(Some(verifier))
        }
    }
}

fn validate_server_config(auth_config: &AuthConfig) -> Result<(), String> {
    let has_jwt_key = auth_config.jwks_url.is_some() || auth_config.jwt_public_key.is_some();
    if auth_config
        .jwt_issuer
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("external JWT issuer cannot be empty".to_owned());
    }
    if auth_config
        .jwt_audience
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("external JWT audience cannot be empty".to_owned());
    }

    match (
        auth_config.jwt_issuer.as_ref(),
        auth_config.jwt_audience.as_ref(),
    ) {
        (Some(_), Some(_)) if !has_jwt_key => {
            return Err(
                "external JWT issuer/audience require --jwks-url / JAZZ_JWKS_URL or --jwt-public-key / JAZZ_JWT_PUBLIC_KEY"
                    .to_owned(),
            );
        }
        (Some(_), None) => {
            return Err(
                "external JWT verification requires --jwt-audience / JAZZ_JWT_AUDIENCE".to_owned(),
            );
        }
        (None, Some(_)) => {
            return Err(
                "external JWT verification requires --jwt-issuer / JAZZ_JWT_ISSUER".to_owned(),
            );
        }
        (None, None) if has_jwt_key && !auth_config.allow_local_first_auth => {
            return Err(
                "external JWT verification requires --jwt-issuer / JAZZ_JWT_ISSUER and --jwt-audience / JAZZ_JWT_AUDIENCE"
                    .to_owned(),
            );
        }
        _ => {}
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
        "Auth configured: local_first={}, jwks={}, static_jwt_key={}, jwt_issuer={}, jwt_audience={}, cookie={}, trust_forwarded_host={}, backend={}, admin={}",
        auth_config.allow_local_first_auth,
        auth_config.jwks_url.is_some(),
        auth_config.jwt_public_key.is_some(),
        auth_config.jwt_issuer.is_some(),
        auth_config.jwt_audience.is_some(),
        auth_config.auth_cookie_name.is_some(),
        auth_config.trust_forwarded_host,
        auth_config.backend_secret.is_some(),
        auth_config.admin_secret.is_some()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::catalogue::CatalogueStore;
    use crate::server::catalogue_entry::CatalogueEntry;
    use crate::server::catalogue_storage::catalogue_storage_codec_profile;
    use jazz::groove::storage::OrderedKvStorage;
    use jazz::tools::AppId;
    use jazz::tools::metadata::{MetadataKey, ObjectType};
    use jazz::tools::public_schema::SchemaHash;
    use jazz::tools::schema_lens::LensTransform;

    fn dynamic_bootstrap_schema() -> jazz::tools::public_schema::Schema {
        jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("notes")
                    .column("body", jazz::tools::public_schema::ColumnType::Text),
            )
            .build()
    }

    fn write_raw_catalogue_entry(catalogue_path: &std::path::Path, entry: &CatalogueEntry) {
        let storage = open_raw_catalogue_storage(catalogue_path);
        jazz::db::block_on(storage.set(
            "default".to_owned(),
            crate::server::catalogue_storage::CatalogueKvStorage::entry_key(entry.object_id),
            entry.encode_storage_row().expect("encode catalogue entry"),
        ))
        .expect("write raw catalogue entry");
    }

    fn open_raw_catalogue_storage(
        catalogue_path: &std::path::Path,
    ) -> jazz_storage_rocksdb::RocksDbStorage {
        jazz_storage_rocksdb::RocksDbStorage::open_with_durability_and_codec_profile(
            catalogue_path,
            &["default"],
            jazz_storage_rocksdb::Durability::WalNoSync,
            &catalogue_storage_codec_profile().expect("settled catalogue profile"),
        )
        .expect("open raw catalogue storage")
    }

    #[test]
    fn local_first_server_may_start_with_unbound_jwks_but_external_only_may_not() {
        let local_first = AuthConfig {
            jwks_url: Some("http://127.0.0.1:9/jwks".to_owned()),
            allow_local_first_auth: true,
            ..Default::default()
        };
        validate_server_config(&local_first)
            .expect("local-first admission does not require external JWT bindings");

        let external_only = AuthConfig {
            allow_local_first_auth: false,
            ..local_first
        };
        let error = validate_server_config(&external_only)
            .expect_err("an external-only verifier must be explicitly bound");
        assert!(error.contains("--jwt-issuer"), "{error}");
        assert!(error.contains("--jwt-audience"), "{error}");
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
            std::collections::HashSet::from([DurabilityTier::GlobalServer])
        );
    }

    #[tokio::test]
    async fn persistent_builder_fails_when_catalogue_scan_is_corrupt() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
        {
            let storage = open_raw_catalogue_storage(&catalogue_path);
            jazz::db::block_on(storage.set(
                "default".to_owned(),
                b"cat:not-a-uuid".to_vec(),
                vec![0],
            ))
            .expect("write malformed catalogue entry");
        }

        let result = ServerBuilder::new(AppId::from_name("corrupt-durable-catalogue"))
            .with_schema(dynamic_bootstrap_schema())
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await;

        let error = result.err().expect(
            "server startup must fail rather than treating a corrupt durable catalogue as empty",
        );
        assert!(
            error.contains(
                "failed to read durable catalogue: Storage error: IO error: catalogue key uses an unsupported namespace"
            ),
            "startup error retains the catalogue-read context and storage corruption: {error}"
        );

        let storage = open_raw_catalogue_storage(&catalogue_path);
        jazz::db::block_on(storage.delete("default".to_owned(), b"cat:not-a-uuid".to_vec()))
            .expect("remove corrupt catalogue entry");
        drop(storage);
        ServerBuilder::new(AppId::from_name("corrupt-durable-catalogue"))
            .with_schema(dynamic_bootstrap_schema())
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("builder retries after repaired catalogue");
    }

    #[tokio::test]
    async fn persistent_builder_fails_when_known_catalogue_payload_is_corrupt() {
        for (object_type, decode_context) in [
            (ObjectType::CatalogueSchema, "decode schema payload"),
            (ObjectType::CatalogueLens, "decode lens payload"),
            (
                ObjectType::CataloguePermissionsBundle,
                "decode permissions bundle payload",
            ),
            (
                ObjectType::CataloguePermissionsHead,
                "decode permissions head payload",
            ),
        ] {
            let data_dir = tempfile::TempDir::new().expect("temp data dir");
            let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
            let app_id = AppId::from_name(&format!("corrupt-{}", object_type.as_str()));
            let object_id = jazz::tools::ObjectId::new();
            let mut metadata = std::collections::HashMap::from([
                (MetadataKey::Type.to_string(), object_type.to_string()),
                (MetadataKey::AppId.to_string(), app_id.uuid().to_string()),
            ]);
            if object_type == ObjectType::CatalogueLens {
                let schema_hash = SchemaHash::compute(&dynamic_bootstrap_schema());
                metadata.insert(MetadataKey::SourceHash.to_string(), schema_hash.to_string());
                metadata.insert(MetadataKey::TargetHash.to_string(), schema_hash.to_string());
            }
            let entry = CatalogueEntry {
                object_id,
                metadata,
                content: vec![0],
            };
            write_raw_catalogue_entry(&catalogue_path, &entry);

            let result = ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await;
            let error = result
                .err()
                .expect("corrupt known catalogue payload must fail startup");
            assert!(
                error.contains("failed to read durable catalogue: Decode error"),
                "startup error retains the durable-catalogue context: {error}"
            );
            assert!(
                error.contains(object_type.as_str()) && error.contains(decode_context),
                "startup error identifies the corrupt known entry type and decoder: {error}"
            );
            assert!(
                error.contains(&object_id.to_string()),
                "startup error identifies the corrupt durable object: {error}"
            );

            let storage = open_raw_catalogue_storage(&catalogue_path);
            jazz::db::block_on(storage.delete(
                "default".to_owned(),
                crate::server::catalogue_storage::CatalogueKvStorage::entry_key(object_id),
            ))
            .expect("remove corrupt catalogue entry");
            drop(storage);
            ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("builder retries after repaired catalogue");
        }
    }

    #[tokio::test]
    async fn persistent_builder_rejects_nested_catalogue_codec_corruption_before_recovery() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
        let app_id = AppId::from_name("corrupt-nested-catalogue-codec");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("documents").column(
                    "payload",
                    jazz::tools::public_schema::ColumnType::Json {
                        schema: Some(serde_json::json!({"type": "object"})),
                    },
                ),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let object_id = schema_hash.to_object_id();
        let mut content = crate::server::catalogue_payload_codec::encode_schema(&schema);
        let nested_version = content
            .windows(3)
            .position(|window| window == [12, 1, 1])
            .expect("nested JSON codec marker");
        content[nested_version + 2] = 2;
        write_raw_catalogue_entry(
            &catalogue_path,
            &CatalogueEntry {
                object_id,
                metadata: std::collections::HashMap::from([
                    (
                        MetadataKey::Type.to_string(),
                        ObjectType::CatalogueSchema.to_string(),
                    ),
                    (MetadataKey::AppId.to_string(), app_id.uuid().to_string()),
                    (MetadataKey::SchemaHash.to_string(), schema_hash.to_string()),
                    (MetadataKey::PublishedAt.to_string(), "1".to_owned()),
                ]),
                content,
            },
        );

        let error = ServerBuilder::new(app_id)
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .err()
            .expect("nested corruption must reject before catalogue recovery becomes resident");
        assert!(
            error.contains("decode schema payload")
                && error.contains(&object_id.to_string())
                && error.contains("unsupported version"),
            "startup error retains the nested-codec failure context: {error}"
        );

        let storage = open_raw_catalogue_storage(&catalogue_path);
        jazz::db::block_on(storage.delete(
            "default".to_owned(),
            crate::server::catalogue_storage::CatalogueKvStorage::entry_key(object_id),
        ))
        .expect("remove corrupt nested catalogue entry");
        drop(storage);
        ServerBuilder::new(app_id)
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("repair permits a fresh atomic catalogue recovery");
    }

    #[tokio::test]
    async fn persistent_builder_fails_when_known_catalogue_payload_has_trailing_garbage() {
        let schema = dynamic_bootstrap_schema();
        let schema_hash = SchemaHash::compute(&schema);
        let permissions = std::collections::HashMap::new();
        for (object_type, decode_context, mut content) in [
            (
                ObjectType::CatalogueSchema,
                "decode schema payload",
                crate::server::catalogue_payload_codec::encode_schema(&schema),
            ),
            (
                ObjectType::CatalogueLens,
                "decode lens payload",
                crate::server::catalogue_payload_codec::encode_lens_transform(&LensTransform::new()),
            ),
            (
                ObjectType::CataloguePermissionsBundle,
                "decode permissions bundle payload",
                crate::server::catalogue_payload_codec::encode_permissions_bundle(
                    schema_hash,
                    1,
                    None,
                    &permissions,
                ),
            ),
            (
                ObjectType::CataloguePermissionsHead,
                "decode permissions head payload",
                crate::server::catalogue_payload_codec::encode_permissions_head(
                    schema_hash,
                    1,
                    None,
                    jazz::tools::ObjectId::new(),
                ),
            ),
        ] {
            let data_dir = tempfile::TempDir::new().expect("temp data dir");
            let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
            let app_id = AppId::from_name(&format!("trailing-{}", object_type.as_str()));
            let object_id = jazz::tools::ObjectId::new();
            let mut metadata = std::collections::HashMap::from([
                (MetadataKey::Type.to_string(), object_type.to_string()),
                (MetadataKey::AppId.to_string(), app_id.uuid().to_string()),
            ]);
            if object_type == ObjectType::CatalogueLens {
                metadata.insert(MetadataKey::SourceHash.to_string(), schema_hash.to_string());
                metadata.insert(MetadataKey::TargetHash.to_string(), schema_hash.to_string());
            }
            content.push(0xff);
            write_raw_catalogue_entry(
                &catalogue_path,
                &CatalogueEntry {
                    object_id,
                    metadata,
                    content,
                },
            );

            let result = ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await;
            let error = result
                .err()
                .expect("known catalogue payload with trailing garbage must fail startup");
            assert!(
                error.contains("failed to read durable catalogue: Decode error"),
                "startup error retains durable-catalogue context: {error}"
            );
            assert!(
                error.contains(object_type.as_str())
                    && error.contains(decode_context)
                    && error.contains("trailing data after decoded payload"),
                "startup error identifies the known decoder and trailing payload data: {error}"
            );
            assert!(
                error.contains(&object_id.to_string()),
                "startup error identifies the corrupt durable object: {error}"
            );

            let storage = open_raw_catalogue_storage(&catalogue_path);
            jazz::db::block_on(storage.delete(
                "default".to_owned(),
                crate::server::catalogue_storage::CatalogueKvStorage::entry_key(object_id),
            ))
            .expect("remove corrupt catalogue entry");
            drop(storage);
            ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("builder retries after repaired catalogue");
        }
    }

    #[tokio::test]
    async fn persistent_builder_fails_when_catalogue_schema_publish_time_is_missing_or_invalid() {
        let schema = dynamic_bootstrap_schema();
        let schema_hash = SchemaHash::compute(&schema);
        for published_at in [None, Some("not-a-timestamp")] {
            let data_dir = tempfile::TempDir::new().expect("temp data dir");
            let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
            let app_id = AppId::from_name("corrupt-schema-publish-time");
            let object_id = schema_hash.to_object_id();
            let mut metadata = std::collections::HashMap::from([
                (
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                ),
                (MetadataKey::AppId.to_string(), app_id.uuid().to_string()),
                (MetadataKey::SchemaHash.to_string(), schema_hash.to_string()),
            ]);
            if let Some(published_at) = published_at {
                metadata.insert(
                    MetadataKey::PublishedAt.to_string(),
                    published_at.to_owned(),
                );
            }
            write_raw_catalogue_entry(
                &catalogue_path,
                &CatalogueEntry {
                    object_id,
                    metadata,
                    content: crate::server::catalogue_payload_codec::encode_schema(&schema),
                },
            );

            let error = ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .err()
                .expect("missing or malformed schema publication time must fail startup");
            assert!(
                error.contains("failed to read durable catalogue: Decode error")
                    && error.contains(ObjectType::CatalogueSchema.as_str())
                    && error.contains("published_at metadata")
                    && error.contains(&object_id.to_string()),
                "startup error identifies the corrupt schema metadata: {error}"
            );
        }
    }

    #[tokio::test]
    async fn persistent_builder_ignores_unknown_forward_compatible_catalogue_entries() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
        let app_id = AppId::from_name("unknown-durable-catalogue-entry");
        let entry = CatalogueEntry {
            object_id: jazz::tools::ObjectId::new(),
            metadata: std::collections::HashMap::from([
                (
                    MetadataKey::Type.to_string(),
                    "future_catalogue_kind".to_owned(),
                ),
                (MetadataKey::AppId.to_string(), app_id.uuid().to_string()),
            ]),
            content: vec![0],
        };
        write_raw_catalogue_entry(&catalogue_path, &entry);

        ServerBuilder::new(app_id)
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("unknown forward-compatible catalogue entry does not block startup");
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
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
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
            built
                .state
                .catalogue_store
                .flush()
                .expect("flush fixed schema catalogue");
        }

        let rebuilt = ServerBuilder::new(app_id)
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
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

    /// Internal fixture access is needed to recreate a pre-active-schema store.
    /// Startup must restore the administrative revision before clients receive schema B.
    #[tokio::test]
    async fn legacy_authority_restores_active_descendant_before_serving_snapshots() {
        use jazz::tools::schema_lens::{Lens, LensOp};
        use jazz::tools::{
            ColumnType, PolicyExpr, SchemaBuilder, TableName, TablePolicies, TableSchema,
        };

        let dir = tempfile::tempdir().unwrap();
        let app_id = AppId::from_name("legacy-authority-active-descendant");
        let base = dynamic_bootstrap_schema();
        let target = SchemaBuilder::new()
            .table(TableSchema::builder("notes").column("content", ColumnType::Text))
            .build();
        let target_runtime = jazz::schema::JazzSchema::new(&target).unwrap();
        let lens = Lens::new(
            SchemaHash::compute(&base),
            SchemaHash::compute(&target),
            LensTransform::with_ops(vec![LensOp::RenameColumn {
                table: "notes".into(),
                old_name: "body".into(),
                new_name: "content".into(),
            }]),
        );
        let builder = || {
            ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: dir.path().to_path_buf(),
                })
        };
        let core = builder().with_schema(base).build().await.unwrap();
        core.state
            .catalogue
            .publish_schema(&core.state.catalogue_store, target.clone())
            .unwrap();
        core.state
            .catalogue
            .publish_lens(&core.state.catalogue_store, &lens)
            .unwrap();
        super::super::runtime_catalogue::publish_runtime_catalogue(
            &core.state,
            &[target.clone()],
            &[lens],
        )
        .await
        .unwrap();
        let permissions = std::collections::HashMap::from([(
            TableName::new("notes"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);
        let selection = super::super::runtime_catalogue::publish_permissions_and_runtime(
            &core.state,
            SchemaHash::compute(&target),
            permissions.clone(),
            None,
        )
        .await
        .ok()
        .expect("activate descendant");
        core.shutdown().await;
        drop(core);

        // Kind 8 / nil UUID is the existing active-schema record key. Removing
        // it leaves the old selected pointer and structural catalogue intact;
        // no storage encoding or codec profile changes are involved.
        {
            let families = target_runtime.column_families();
            let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
            let storage =
                jazz_storage_rocksdb::RocksDbStorage::open_with_durability_and_codec_profile(
                    dir.path().join(SERVER_SHELL_ROCKSDB_DIR),
                    &refs,
                    jazz_storage_rocksdb::Durability::WalNoSync,
                    &jazz::storage_codec_profile::epoch_1_storage_codec_profile().unwrap(),
                )
                .unwrap();
            let mut database = groove::db::Database::new_with_storage_layout(
                target_runtime.lower_catalogue_meta_to_groove(),
                storage,
                groove::storage::StorageLayout::jazz_class_v1(),
            )
            .await
            .unwrap();
            let mut batch = database.open_batch();
            batch.delete(
                "jazz_catalogue",
                groove::db::PrimaryKeyValue::Composite(vec![
                    groove::db::PrimaryKeyValue::U64(8),
                    groove::db::PrimaryKeyValue::Uuid(uuid::Uuid::nil()),
                ]),
            );
            let applied = database.apply_batch(batch).await.unwrap();
            let persisted = applied.persist().await;
            database.finish_persistence(persisted).unwrap();
        }

        // No HTTP publication or test-side activation after reopen.
        let reopened = builder().build().await.unwrap();
        let snapshot = reopened
            .state
            .runtime()
            .unwrap()
            .trusted_catalogue_snapshot_for_test()
            .await
            .unwrap();
        assert_eq!(
            snapshot.current_write_schema.schema,
            target_runtime.version_id()
        );
        assert_eq!(snapshot.current_write_schema.revision, selection.version);
        assert!(selection.version > 0);
        let selected = snapshot
            .schemas
            .iter()
            .find(|schema| schema.id == target_runtime.version_id())
            .unwrap();
        assert_eq!(
            selected.schema.public_schema()[&TableName::new("notes")].policies,
            permissions[&TableName::new("notes")]
        );
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn persistent_adapter_starts_core_server_shell_with_catalogue_storage_after_restart() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-server-shell-restart");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("todos")
                    .column("title", jazz::tools::public_schema::ColumnType::Text),
            )
            .build();

        let retained_state = {
            let built = ServerBuilder::new(app_id)
                .with_schema(schema.clone())
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build RocksDB server with server shell");

            assert!(built.state.runtime().is_some());
            assert!(data_dir.path().join(CATALOGUE_ROCKSDB_DIR).exists());
            assert!(data_dir.path().join(SERVER_SHELL_ROCKSDB_DIR).exists());
            assert_eq!(
                built.shutdown().await,
                crate::server::ShutdownPhase::StorageClosed,
                "the public builder lifecycle must join the shell before its RocksDB path is reopened"
            );
            Arc::clone(&built.state)
        };
        assert!(
            retained_state.runtime().is_none(),
            "shutdown must retire the shell even if request/router state outlives BuiltServer"
        );

        let rebuilt = ServerBuilder::new(app_id)
            .with_schema(schema.clone())
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("rebuild RocksDB server with server shell");

        assert!(rebuilt.state.runtime().is_some());
        assert!(data_dir.path().join(SERVER_SHELL_ROCKSDB_DIR).exists());
        rebuilt.shutdown().await;

        // Some direct builder consumers own only `BuiltServer` and use Rust
        // scope exit as their lifecycle. Its last-shell fallback must join as
        // well: this reopen has no timeout or sleep to mask an owner-thread
        // race.
        {
            let dropped = ServerBuilder::new(app_id)
                .with_schema(schema.clone())
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build RocksDB server for direct-drop lifecycle");
            assert!(dropped.state.runtime().is_some());
        }

        let reopened_after_drop = ServerBuilder::new(app_id)
            .with_schema(schema)
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("reopen RocksDB server after direct builder drop");
        assert!(reopened_after_drop.state.runtime().is_some());
        assert_eq!(
            reopened_after_drop.shutdown().await,
            crate::server::ShutdownPhase::StorageClosed
        );
        drop(retained_state);
    }

    #[tokio::test]
    async fn persistent_adapter_reopens_after_first_shutdown_waiter_is_aborted() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-server-shell-aborted-shutdown");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(jazz::tools::public_schema::TableSchema::builder("todos"))
            .build();
        let built = ServerBuilder::new(app_id)
            .with_schema(schema.clone())
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("build RocksDB server");
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
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("reopen RocksDB after aborted shutdown waiter");
        assert_eq!(
            reopened.shutdown().await,
            crate::server::ShutdownPhase::StorageClosed
        );
    }

    #[test]
    fn persistent_adapter_shutdown_survives_initiating_runtime_drop() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-server-shell-foreign-shutdown");
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
                        .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                        .with_storage(StorageBackend::Persistent {
                            path: data_dir.path().to_path_buf(),
                        })
                        .build(),
                )
                .expect("build RocksDB server");
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
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("reopen RocksDB after live shutdown")
        });
        assert_eq!(
            second_runtime.block_on(reopened.shutdown()),
            crate::server::ShutdownPhase::StorageClosed
        );
    }
}
