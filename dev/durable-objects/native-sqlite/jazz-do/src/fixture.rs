//! Fixture setup and assertions for the production WASM server host.
use jazz::binding_host::{self, OpenAs};
use jazz::catalogue::{bundle_id, BundleId};
use jazz::db::Db;
use jazz::host::{HostTables, StoreFile};
use jazz_server::runtime::catalogue_schema;
use jazz_server::runtime::deployment::{DeployRequest, DeploySchema};
use jazz_wasm::server::{ServerTransport, SocketAdmission, WasmServerCore};
use jazz_wasm::{durable_object_storage, ObjectStorage};
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;

fn error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

#[wasm_bindgen]
pub struct FixtureServer {
    host: WasmServerCore,
    bundle: BundleId,
    #[cfg(feature = "scope-tests")]
    checks: Db,
}

#[wasm_bindgen]
impl FixtureServer {
    #[wasm_bindgen(js_name = openDurableObject)]
    pub async fn open(
        storage: ObjectStorage,
        schema: Vec<u8>,
        config: Vec<u8>,
        schedule: js_sys::Function,
    ) -> Result<FixtureServer, JsValue> {
        let (schema, mut config) = binding_host::decode_open(&schema, &config).map_err(error)?;
        let (identity, _) =
            binding_host::open_identity(&mut config, OpenAs::Backend).map_err(error)?;
        let db_config = jazz::tools::deployment::published::with_client_migrations(
            binding_host::db_config(schema, &config, identity),
            config.migrations.as_deref(),
        )
        .map_err(error)?;
        let db = Db::open_storage(db_config, durable_object_storage(storage))
            .await
            .map_err(error)?;
        let mut source = db.current_schema().public_schema().clone();
        let permissions: jazz::catalogue::Permissions = source
            .iter()
            .map(|(name, table)| (*name, table.policies.clone()))
            .collect();
        for table in source.values_mut() {
            table.policies = Default::default();
        }
        let hash = jazz::tools::public_schema::SchemaHash::compute(&source);
        let bundle = bundle_id(&hash, &permissions).map_err(error)?;
        Self::host(
            db,
            bundle,
            schedule,
            Some(DeployRequest {
                target_schema_hash: hash.to_string(),
                schemas: vec![DeploySchema {
                    hash: hash.to_string(),
                    schema: source,
                }],
                migrations: Vec::new(),
                permissions: permissions.into_iter().collect(),
                allow_dropping_history: false,
            }),
        )
        .await
    }

    /// Reconstruct the fixture authority without accepting or publishing schema
    /// or permissions from the client. Production hosts select a bundle during
    /// authenticated admission; this harness requires exactly one stored bundle.
    #[wasm_bindgen(js_name = reopenDurableObject)]
    pub async fn reopen(
        storage: ObjectStorage,
        config: Vec<u8>,
        schedule: js_sys::Function,
    ) -> Result<FixtureServer, JsValue> {
        let store = StoreFile::from_storage(durable_object_storage(storage)).map_err(error)?;
        let rows = store.catalogue().map_err(error)?;
        let schema = catalogue_schema(&rows)
            .map_err(error)?
            .ok_or_else(|| error("cannot reopen: no admitted catalogue schema"))?;
        let [bundle] = rows.bundles.as_slice() else {
            return Err(error(
                "fixture reopen requires exactly one stored permissions bundle",
            ));
        };
        let bundle = bundle.id;
        let schema = serde_json::to_vec(schema.public_schema()).map_err(error)?;
        let (schema, mut config) = binding_host::decode_open(&schema, &config).map_err(error)?;
        if config.migrations.is_some() {
            return Err(error(
                "fixture reopen loads migrations from the catalogue, not the client",
            ));
        }
        let (identity, _) =
            binding_host::open_identity(&mut config, OpenAs::Backend).map_err(error)?;
        let db = Db::open_file(binding_host::db_config(schema, &config, identity), store)
            .await
            .map_err(error)?;
        Self::host(db, bundle, schedule, None).await
    }

    pub async fn tick(&self) -> Result<(), JsValue> {
        self.host.tick().await
    }

    #[wasm_bindgen(js_name = acceptSocket)]
    pub fn accept_socket(
        &self,
        prelude: &[u8],
        hello: &[u8],
        backend_secret: Option<String>,
    ) -> Result<SocketAdmission, JsValue> {
        self.host.accept_socket(prelude, hello, backend_secret)
    }

    #[wasm_bindgen(getter)]
    pub fn bundle(&self) -> String {
        self.bundle.to_hex()
    }

    #[wasm_bindgen(js_name = acceptSubscriber)]
    pub fn accept_subscriber(&self, author: Vec<u8>) -> Result<ServerTransport, JsValue> {
        let author = jazz::binding_args::author_subject(&author).map_err(error)?;
        self.host
            .open_session(author, BTreeMap::new(), Some(self.bundle), false)
            .map_err(error)
    }

    #[cfg(feature = "scope-tests")]
    #[wasm_bindgen(js_name = checkRejectedRow)]
    pub fn check_rejected_row(&self, row: Vec<u8>, table: Option<String>) -> Result<(), JsValue> {
        if !self.host.is_idle() {
            return Err(error("checks require an idle core"));
        }
        let row =
            jazz::ids::RowUuid::from_bytes(row.try_into().map_err(|_| error("expected row UUID"))?);
        self.checks
            .check_rejected_row_for_test(table.as_deref().unwrap_or("tasks"), row)
            .map_err(error)
    }

    #[cfg(feature = "scope-tests")]
    #[wasm_bindgen(js_name = checkSqlScopes)]
    pub fn check_sql_scopes(&self) -> Result<(), JsValue> {
        if !self.host.is_idle() {
            return Err(error("checks require an idle core"));
        }
        jazz_wasm::check_durable_object_scopes(&self.checks)
    }
}
impl FixtureServer {
    async fn host(
        db: Db,
        bundle: BundleId,
        schedule: js_sys::Function,
        publish: Option<DeployRequest>,
    ) -> Result<Self, JsValue> {
        #[cfg(feature = "scope-tests")]
        let checks = db
            .governed_view(db.current_schema().version_id())
            .ok_or_else(|| error("missing fixture schema view"))?;
        let host = WasmServerCore::from_db(db, None, schedule)?;
        if let Some(request) = publish {
            let request = serde_json::to_string(&request).map_err(error)?;
            let response = host.publish_catalogue(&request).await?;
            let response: serde_json::Value = serde_json::from_str(&response).map_err(error)?;
            if response["status"] != 200 || response["body"]["bundle"] != bundle.to_hex() {
                return Err(error(format!("fixture publication failed: {response}")));
            }
        }
        Ok(Self {
            host,
            bundle,
            #[cfg(feature = "scope-tests")]
            checks,
        })
    }
}
