//! Durable browser stores: the worker's SQLite file on one of two VFSes.
//!
//! - **IndexedDB** ([`crate::idb_vfs`], the default): the whole file lives
//!   in memory and every commit is queued as one IndexedDB transaction,
//!   applied in order. IndexedDB always holds one whole committed state, but
//!   the newest commits are only in memory until [`Store::flush`] resolves,
//!   and a failed write (storage full) keeps them there until a flush
//!   succeeds. So the database holds back the acks that tell a tab its
//!   commit is `Local` until a flush covers it
//!   ([`jazz::db::Db::defer_durability_acks`]). Works in any worker, so one
//!   shared worker can own the file for every tab.
//! - **OPFS** (`opfs-sahpool`): sync access handles, written through on
//!   every commit. Dedicated workers only, and no WAL.
//!
//! Neither VFS has real locks or shared memory, so the file runs a
//! non-WAL journal (see [`jazz::db::Journal`]) and must have one owner.

use std::path::Path;
use std::rc::Rc;

use jazz::db::{Db, DbConfig, FileOptions, Journal};
use sha2::{Digest, Sha256};
use sqlite_wasm_vfs::sahpool::{self, OpfsSAHPoolCfgBuilder};

use crate::idb_vfs::{self, IdbVfsUtil};
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
}

impl StoreSpec {
    /// `storage` is `{ backend?: "indexeddb" | "opfs", path?: string }`, or
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
            // Hashed: the physical name is long and holds characters a VFS
            // may not take.
            None => {
                let digest = Sha256::digest(database.as_bytes());
                let hex: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
                format!("jazz-{hex}.sqlite")
            }
        };
        // Both VFSes key files by absolute name.
        let path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };
        Ok(Self { backend, path })
    }
}

/// The open file's backing storage, kept by the database's host.
#[derive(Clone)]
pub(crate) enum Store {
    /// The VFS and the file's path.
    IndexedDb(Rc<IdbVfsUtil>, Rc<str>),
    Opfs,
}

impl Store {
    /// Resolves once every commit so far is in the backing storage, or
    /// fails with why it could not be written (it stays in memory, and the
    /// next flush tries again).
    pub(crate) async fn flush(&self) -> Result<(), String> {
        match self {
            Store::IndexedDb(util, path) => util
                .flush(path)
                .map_err(|error| error.to_string())?
                .await
                .map_err(|error| error.to_string()),
            // Sync access handles write through on every commit.
            Store::Opfs => Ok(()),
        }
    }

    /// Call `hook` when a commit's own write to the backing storage fails
    /// (`None` removes it).
    pub(crate) fn on_write_failed(&self, hook: Option<Rc<dyn Fn()>>) {
        if let Store::IndexedDb(util, path) = self {
            util.on_write_failed(path, hook);
        }
    }

    /// Whether the last write to the backing storage failed, so commits
    /// since wait in memory for a flush to retry them.
    pub(crate) fn write_failed(&self) -> bool {
        match self {
            Store::IndexedDb(util, path) => util.write_failed(path),
            Store::Opfs => false,
        }
    }

    /// Whether commits reach the backing storage only after they return.
    pub(crate) fn defers_writes(&self) -> bool {
        matches!(self, Store::IndexedDb(..))
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
            (
                Store::IndexedDb(Rc::new(util), Rc::from(spec.path.as_str())),
                FileOptions {
                    vfs: Some(IDB_VFS),
                    journal: Journal::Memory,
                    // Each page is one IndexedDB record, and opening reads
                    // them all: few large records load far faster than
                    // many small ones.
                    page_size: Some(65536),
                    ..FileOptions::default()
                },
            )
        }
        Backend::Opfs => {
            require_sync_access_handles()?;
            install_opfs().await?;
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
        // Keeps the stable code, so a refused store's
        // `unsupported_storage_codecs` reaches JavaScript.
        .map_err(|error| crate::core_error_to_js(&error))?;
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
