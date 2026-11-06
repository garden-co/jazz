//! Durable browser stores: the worker's SQLite file on IndexedDB.
//!
//! The `relaxed-idb` VFS keeps the whole file in memory and queues every
//! commit as one IndexedDB transaction, applied in order. A commit is never
//! torn, but a crash can lose the newest ones until [`Store::flush`]
//! resolves. It works in any worker, so one shared worker can own the file
//! for every tab.
//!
//! The VFS has no real locks or shared memory, so the file runs a non-WAL
//! journal (see [`jazz::db::Journal`]) and must have one owner.

use std::path::Path;
use std::rc::Rc;

use jazz::db::{Db, DbConfig, FileOptions, Journal};
use sha2::{Digest, Sha256};
use sqlite_wasm_vfs::relaxed_idb::{self, Preload, RelaxedIdbCfgBuilder, RelaxedIdbUtil};
use wasm_bindgen::prelude::*;

const IDB_VFS: &str = "jazz-idb";
/// Deleting this never-opened file is queued behind every pending commit,
/// so its completion is a flush barrier.
const IDB_BARRIER: &str = "/.jazz-flush-barrier";

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
                return Err(JsValue::from_str(&format!(
                    "storage backend {other:?} is not supported yet (use \"indexeddb\")"
                )));
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
pub(crate) struct Store(Rc<RelaxedIdbUtil>);

impl Store {
    /// Resolves once every commit so far is in the backing storage.
    pub(crate) async fn flush(&self) -> Result<(), JsValue> {
        self.0
            .delete_db(IDB_BARRIER)
            .map_err(vfs_error)?
            .await
            .map_err(vfs_error)
    }
}

fn vfs_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!("browser storage: {error}"))
}

async fn install_idb() -> Result<RelaxedIdbUtil, JsValue> {
    let cfg = RelaxedIdbCfgBuilder::new()
        .vfs_name(IDB_VFS)
        .preload(Preload::None)
        .build();
    relaxed_idb::install::<sqlite_wasm_rs::WasmOsCallback>(&cfg, false)
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
    Ok((db, Store(Rc::new(util))))
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
