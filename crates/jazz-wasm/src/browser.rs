//! Durable browser stores: the worker's SQLite file on IndexedDB.
//!
//! The IndexedDB VFS ([`crate::idb_vfs`]) keeps the whole file in memory
//! and queues every commit as one IndexedDB transaction, applied in order.
//! IndexedDB always holds one whole committed state, but the newest commits
//! are only in memory until [`Store::flush`] resolves, and a failed write
//! (storage full) keeps them there until a flush succeeds. So the database
//! holds back the acks that tell a tab its commit is `Local` until a flush
//! covers it ([`jazz::db::Db::defer_durability_acks`]). It works in any
//! worker, so one shared worker can own the file for every tab.
//!
//! The VFS has no real locks or shared memory, so the file runs a non-WAL
//! journal (see [`jazz::db::Journal`]) and must have one owner.

use std::path::Path;
use std::rc::Rc;

use crate::idb_vfs::{self, IdbVfsUtil};
use jazz::db::{Db, DbConfig, FileOptions, Journal};
use sha2::{Digest, Sha256};
use wasm_bindgen::prelude::*;

const IDB_VFS: &str = "jazz-idb";

pub(crate) struct StoreSpec {
    path: String,
}

impl StoreSpec {
    /// `storage` is `{ backend?: "indexeddb", path?: string }`, or
    /// `undefined` for the IndexedDB default. Without a path the file is
    /// named after `database`, the physical database name the worker
    /// admitted (one per app, environment, auth scope and dbName).
    pub(crate) fn from_js(storage: &JsValue, database: &str) -> Result<Self, JsValue> {
        let field = |name: &str| -> Result<Option<String>, JsValue> {
            if storage.is_undefined() || storage.is_null() {
                return Ok(None);
            }
            let value = js_sys::Reflect::get(storage, &JsValue::from_str(name))?;
            if value.is_undefined() || value.is_null() {
                return Ok(None);
            }
            value
                .as_string()
                .map(Some)
                .ok_or_else(|| JsValue::from_str(&format!("storage.{name} must be a string")))
        };
        match field("backend")?.as_deref() {
            None | Some("indexeddb") => {}
            Some(other) => {
                return Err(crate::not_supported(&format!("storage backend {other:?}")));
            }
        }
        let path = match field("path")? {
            Some(path) => path,
            // Hashed: the physical name is long and holds characters a VFS
            // may not take.
            None => {
                let digest = Sha256::digest(database.as_bytes());
                let hex: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
                format!("jazz-{hex}.sqlite")
            }
        };
        // The VFS keys files by absolute name.
        let path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };
        Ok(Self { path })
    }
}

/// The open file's backing storage, kept by the database's host.
#[derive(Clone)]
pub(crate) struct Store {
    util: Rc<IdbVfsUtil>,
    /// The file's path.
    path: Rc<str>,
}

impl Store {
    /// Resolves once every commit so far is in the backing storage, or
    /// fails with why it could not be written (it stays in memory, and the
    /// next flush tries again).
    pub(crate) async fn flush(&self) -> Result<(), String> {
        self.util
            .flush(&self.path)
            .map_err(|error| error.to_string())?
            .await
            .map_err(|error| error.to_string())
    }

    /// Call `hook` when a commit's own write to the backing storage fails
    /// (`None` removes it).
    pub(crate) fn on_write_failed(&self, hook: Option<Rc<dyn Fn()>>) {
        self.util.on_write_failed(&self.path, hook);
    }

    /// Whether the last write to the backing storage failed, so commits
    /// since wait in memory for a flush to retry them.
    pub(crate) fn write_failed(&self) -> bool {
        self.util.write_failed(&self.path)
    }
}

fn vfs_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!("browser storage: {error}"))
}

async fn install_idb() -> Result<IdbVfsUtil, JsValue> {
    idb_vfs::install::<sqlite_wasm_rs::WasmOsCallback>(IDB_VFS)
        .await
        .map_err(vfs_error)
}

pub(crate) async fn open(spec: &StoreSpec, config: DbConfig) -> Result<(Db, Store), JsValue> {
    let util = install_idb().await?;
    // IndexedDB reads are async; SQLite's are not, so the file is loaded
    // whole before opening.
    util.preload_db(vec![spec.path.clone()])
        .await
        .map_err(vfs_error)?;
    let options = FileOptions {
        vfs: Some(IDB_VFS),
        journal: Journal::Memory,
        // Each page is one IndexedDB record, and opening reads them all:
        // few large records load far faster than many small ones.
        page_size: Some(65536),
        ..FileOptions::default()
    };
    let db = Db::open_at(config, Path::new(&spec.path), options)
        .await
        // Keeps the stable code, so a refused store's
        // `unsupported_storage_codecs` reaches JavaScript.
        .map_err(|error| crate::core_error_to_js(&error))?;
    Ok((
        db,
        Store {
            util: Rc::new(util),
            path: Rc::from(spec.path.as_str()),
        },
    ))
}

pub(crate) async fn destroy(spec: &StoreSpec) -> Result<(), JsValue> {
    let util = install_idb().await?;
    util.delete_db(&spec.path)
        .map_err(vfs_error)?
        .await
        .map_err(vfs_error)?;
    // The journal of a closed database is already gone.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    fn storage(backend: &str) -> JsValue {
        let storage = js_sys::Object::new();
        js_sys::Reflect::set(&storage, &"backend".into(), &backend.into()).unwrap();
        storage.into()
    }

    #[wasm_bindgen_test]
    fn indexeddb_is_the_only_backend() {
        for spec in [JsValue::UNDEFINED, storage("indexeddb")] {
            let spec = StoreSpec::from_js(&spec, "db").unwrap();
            assert!(spec.path.starts_with("/jazz-") && spec.path.ends_with(".sqlite"));
        }
    }

    #[wasm_bindgen_test]
    fn other_backends_fail_with_a_coded_not_supported_error() {
        let Err(error) = StoreSpec::from_js(&storage("opfs"), "db") else {
            panic!("opfs must be refused");
        };
        let error: js_sys::Error = error.dyn_into().expect("a coded Error, not a bare string");
        assert_eq!(
            String::from(error.message()),
            "Query: storage backend \"opfs\" is not supported yet"
        );
        let code = js_sys::Reflect::get(&error, &"code".into()).unwrap();
        assert_eq!(
            code.as_string().as_deref(),
            Some(jazz::db::ErrorCode::Query.as_str())
        );
    }
}
