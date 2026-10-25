//! Durable browser stores: the worker's SQLite file on one of two VFSes.
//!
//! - **IndexedDB** (`relaxed-idb`, the default): the whole file lives in
//!   memory and every commit is queued as one IndexedDB transaction, applied
//!   in order. A commit is never torn, but a crash can lose the newest ones
//!   until [`Store::flush`] resolves. Works in any worker, so one shared
//!   worker can own the file for every tab.
//! - **OPFS** (`opfs-sahpool`): sync access handles, written through on
//!   every commit. Dedicated workers only, and no WAL.
//!
//! Neither VFS has real locks or shared memory, so the file runs a
//! non-WAL journal (see [`jazz::db::Journal`]) and must have one owner.

use std::path::Path;
use std::rc::Rc;

use jazz::db::{Db, DbConfig, FileOptions, Journal};
use sha2::{Digest, Sha256};
use sqlite_wasm_vfs::relaxed_idb::{self, Preload, RelaxedIdbCfgBuilder, RelaxedIdbUtil};
use sqlite_wasm_vfs::sahpool::{self, OpfsSAHPoolCfgBuilder};
use wasm_bindgen::prelude::*;

const IDB_VFS: &str = "jazz-idb";
const OPFS_VFS: &str = "jazz-opfs";
const OPFS_DIRECTORY: &str = "jazz";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Backend {
    IndexedDb,
    Opfs,
}

pub(crate) struct StoreSpec {
    backend: Backend,
    path: String,
    require_existing: bool,
}

impl StoreSpec {
    /// `storage` accepts `backend`, `path` and `requireExisting`, or is
    /// `undefined` for the IndexedDB default. Without an explicit path, the
    /// versioned filename hashes the complete storage owner. A ready SDK
    /// sidecar passes its replica-specific path and requires an existing file.
    pub(crate) fn from_js(storage: &JsValue, storage_owner: &str) -> Result<Self, JsValue> {
        // A removed page-store callback object must not silently become the
        // default SQLite VFS. Reject it before opening or deleting any file,
        // without invoking its callbacks or property getters.
        if !storage.is_undefined() && !storage.is_null() {
            for callback in ["metadata", "readPage", "commitPages"] {
                if js_sys::Reflect::has(storage, &JsValue::from_str(callback))? {
                    return Err(crate::to_js_error(jazz::db::Error::new(
                        jazz::db::ErrorCode::Query,
                        "custom browser page store does not provide durable admission for SQLite; use { backend, path }",
                    )));
                }
            }
        }
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
        let backend = match field("backend")?.as_deref() {
            None | Some("indexeddb") => Backend::IndexedDb,
            Some("opfs") => Backend::Opfs,
            Some(other) => {
                return Err(JsValue::from_str(&format!(
                    "storage backend {other:?} is not supported yet (use \"indexeddb\" or \"opfs\")"
                )));
            }
        };
        let path = match field("path")? {
            Some(path) => path,
            None => {
                // Punctuation replacement aliases distinct admitted owners.
                // This named default-path format hashes the complete UTF-8 owner.
                let mut hash = Sha256::new();
                hash.update(b"jazz.browser.default-path.v1\0");
                hash.update(storage_owner.as_bytes());
                format!("jazz-owner-v1-{:x}.sqlite", hash.finalize())
            }
        };
        // Both VFSes key files by absolute name.
        let path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };
        let require_existing = if storage.is_null() || storage.is_undefined() {
            false
        } else {
            let value = js_sys::Reflect::get(storage, &JsValue::from_str("requireExisting"))?;
            if value.is_undefined() {
                false
            } else {
                value
                    .as_bool()
                    .ok_or_else(|| JsValue::from_str("storage.requireExisting must be a boolean"))?
            }
        };
        Ok(Self {
            backend,
            path,
            require_existing,
        })
    }
}

/// The open file's backing storage, kept by the database's host.
#[derive(Clone)]
pub(crate) enum Store {
    IndexedDb {
        util: Rc<RelaxedIdbUtil>,
        path: String,
    },
    Opfs,
}

impl Store {
    pub(crate) fn needs_flush(&self) -> bool {
        matches!(self, Self::IndexedDb { .. })
    }

    /// Resolves once every commit so far is in the backing storage.
    pub(crate) async fn flush(&self) -> Result<(), JsValue> {
        match self {
            Store::IndexedDb { util, path } => util
                .flush_db(path)
                .map_err(vfs_error)?
                .await
                .map_err(vfs_error),
            // Sync access handles write through on every commit.
            Store::Opfs => Ok(()),
        }
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

/// OPFS sync access handles exist only in dedicated workers; elsewhere
/// the pool would wait for them forever.
fn require_sync_access_handles() -> Result<(), JsValue> {
    let global = js_sys::global();
    if js_sys::Reflect::has(&global, &JsValue::from_str("FileSystemSyncAccessHandle"))? {
        Ok(())
    } else {
        Err(JsValue::from_str(
            "OPFS storage is not supported here yet: it needs a dedicated worker (use \"indexeddb\" in shared workers)",
        ))
    }
}

async fn install_opfs() -> Result<sahpool::OpfsSAHPoolUtil, JsValue> {
    let cfg = OpfsSAHPoolCfgBuilder::new()
        .vfs_name(OPFS_VFS)
        .directory(OPFS_DIRECTORY)
        .build();
    sahpool::install::<sqlite_wasm_rs::WasmOsCallback>(&cfg, false)
        .await
        .map_err(vfs_error)
}

pub(crate) async fn open(spec: &StoreSpec, config: DbConfig) -> Result<(Db, Store), JsValue> {
    let (store, options) = match spec.backend {
        Backend::IndexedDb => {
            let util = install_idb().await?;
            // IndexedDB reads are async; SQLite's are not, so the file is
            // loaded whole before opening.
            util.preload_db(vec![spec.path.clone()])
                .await
                .map_err(vfs_error)?;
            if spec.require_existing && !util.exists(&spec.path) {
                return Err(JsValue::from_str("Admitted SQLite browser file is missing; reset this storage root before reopening"));
            }
            (
                Store::IndexedDb {
                    util: Rc::new(util),
                    path: spec.path.clone(),
                },
                FileOptions {
                    vfs: Some(IDB_VFS),
                    journal: Journal::Memory,
                    ..FileOptions::default()
                },
            )
        }
        Backend::Opfs => {
            require_sync_access_handles()?;
            let util = install_opfs().await?;
            if spec.require_existing && !util.exists(&spec.path).map_err(vfs_error)? {
                return Err(JsValue::from_str("Admitted SQLite browser file is missing; reset this storage root before reopening"));
            }
            (
                Store::Opfs,
                FileOptions {
                    vfs: Some(OPFS_VFS),
                    journal: Journal::Rollback,
                    ..FileOptions::default()
                },
            )
        }
    };
    let db = Db::open_at(config, Path::new(&spec.path), options)
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    Ok((db, store))
}

pub(crate) async fn destroy(spec: &StoreSpec) -> Result<(), JsValue> {
    match spec.backend {
        Backend::IndexedDb => {
            let util = install_idb().await?;
            util.delete_db(&spec.path)
                .map_err(vfs_error)?
                .await
                .map_err(vfs_error)?;
            // The journal of a closed database is already gone.
        }
        Backend::Opfs => {
            require_sync_access_handles()?;
            let util = install_opfs().await?;
            util.delete_db(&spec.path).map_err(vfs_error)?;
        }
    }
    Ok(())
}
