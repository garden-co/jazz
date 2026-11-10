//! Startup on a DO's owning connection. The host wraps first creation and
//! publication in one storage transaction and discards this host on rollback.
use super::{error, WasmServerCore};
use crate::{durable_object_storage, ObjectStorage};
use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::host::{HostTables, StoreFile};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz_server::runtime::{catalogue_schema, deployment};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
impl WasmServerCore {
    /// Reopen admitted storage, or use a validated deployment's genesis for a
    /// new store. With no stored schema and no deployment, return undefined.
    /// The JS host owns the initial transaction, including publishCatalogue.
    #[wasm_bindgen(js_name = openDurableObject)]
    pub async fn open_durable_object(
        storage: ObjectStorage,
        app_id: String,
        schedule: js_sys::Function,
        initial_deployment: Option<String>,
    ) -> Result<Option<WasmServerCore>, JsValue> {
        let app = uuid::Uuid::parse_str(&app_id).map_err(error)?.to_string();
        let file = StoreFile::from_storage(durable_object_storage(storage)).map_err(error)?;
        let rows = file.catalogue().map_err(error)?;
        let schema = match catalogue_schema(&rows).map_err(error)? {
            Some(schema) => schema,
            None => {
                let Some(request) = initial_deployment else {
                    return Ok(None);
                };
                let request =
                    serde_json::from_str::<deployment::DeployRequest>(&request).map_err(|err| {
                        response_error(
                            400,
                            serde_json::json!({"code":"bad_request", "error":err.to_string()}),
                        )
                    })?;
                let prepared = deployment::prepare(&Default::default(), request)
                    .map_err(|err| response_error(err.status.as_u16(), err.body))?;
                JazzSchema::new(&prepared.schemas()[0]).map_err(error)?
            }
        };
        // Same authority identity convention as the native server. Its clocks,
        // admissions and data are recovered from this object's persisted store.
        let config = DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x5e; 16]),
                author: AuthorSubject::SYSTEM,
            },
        );
        let db = Db::open_file(config, file).await.map_err(error)?;
        WasmServerCore::from_db(db, Some(app), schedule).map(Some)
    }
}

fn response_error(status: u16, body: serde_json::Value) -> JsValue {
    js_sys::JSON::parse(&serde_json::json!({"status":status, "body":body}).to_string())
        .unwrap_or_else(|err| err)
}

/// The remote provider presents the app's admin secret as its backend
/// credential, and must also explicitly request admin access.
#[wasm_bindgen(js_name = authorizeDurableObjectAdmin)]
pub fn authorize_admin(credential: &str, access: &str, expected: &str) -> bool {
    access == "admin" && authorize_access(credential, access, expected)
}

/// Verify the remote provider's upgrade credential and access vocabulary.
#[wasm_bindgen(js_name = authorizeDurableObjectAccess)]
pub fn authorize_access(credential: &str, access: &str, expected: &str) -> bool {
    !expected.is_empty()
        && matches!(access, "" | "backend" | "admin")
        && jazz_server::runtime::session_admission::constant_time_eq(credential, expected)
}
