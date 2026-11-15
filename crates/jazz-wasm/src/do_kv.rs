//! A Jazz store on a Cloudflare Durable Object's synchronous KV storage
//! (`durable-object-kv` feature): the engine's SQLite file on
//! [`jazz::kv_vfs`], bound to the object's `ctx.storage`.
//!
//! Reads are synchronous, so nothing is preloaded and nothing waits;
//! every commit is one `transactionSync` of the pages it changed, and the
//! object's output gate holds back replies until it is on disk, so no
//! flush is needed (see the VFS's docs).
//!
//! A commit that cannot be written fails the open store for good (see the
//! VFS's docs) and resets the object with `ctx.abort()`, as the runtime does
//! when a storage write fails: the next request reopens the store from
//! storage, at the last whole commit. The abort is queued, not thrown from
//! inside SQLite: a JavaScript exception unwinding through the wasm stack
//! would leave the module's state (shared by every object in the isolate)
//! mid-borrow.

use std::path::Path;
use std::rc::Rc;

use jazz::db::{Db, DbConfig, FileOptions, Journal};
use js_sys::{Array, Uint8Array};
use wasm_bindgen::prelude::*;

use jazz::kv_vfs::{self, KvStats, KvVfsUtil, PageKv, PAGE_SIZE};

const VFS: &str = "jazz-do-kv";

/// The file's name in the object's storage (its keys start with it).
const STORE_NAME: &str = "store";

#[wasm_bindgen(inline_js = r#"
export function kvGet(storage, key) {
  return storage.kv.get(key);
}
export function abortSoon(ctx, message) {
  queueMicrotask(() => ctx.abort(message));
}
export function kvCommit(storage, keys, values, deletes) {
  // All or nothing: a throw rolls the transaction back.
  storage.transactionSync(() => {
    for (let i = 0; i < keys.length; i++) storage.kv.put(keys[i], values[i]);
    for (const key of deletes) storage.kv.delete(key);
  });
}
"#)]
extern "C" {
    #[wasm_bindgen(catch, js_name = kvGet)]
    fn kv_get(storage: &JsValue, key: &str) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(js_name = abortSoon)]
    fn abort_soon(ctx: &JsValue, message: &str);
    #[wasm_bindgen(catch, js_name = kvCommit)]
    fn kv_commit(
        storage: &JsValue,
        keys: &Array,
        values: &Array,
        deletes: &Array,
    ) -> Result<(), JsValue>;
}

/// The storage of a SQLite-backed Durable Object (`ctx`, `ctx.storage`).
struct ObjectStorage {
    ctx: JsValue,
    storage: JsValue,
}

fn js_error(error: JsValue) -> String {
    error
        .dyn_ref::<js_sys::Error>()
        .map(|error| String::from(error.message()))
        .or_else(|| error.as_string())
        .unwrap_or_else(|| format!("{error:?}"))
}

impl PageKv for ObjectStorage {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let value = kv_get(&self.storage, key).map_err(js_error)?;
        if value.is_undefined() {
            return Ok(None);
        }
        if !(value.is_instance_of::<Uint8Array>() || value.is_instance_of::<js_sys::ArrayBuffer>())
        {
            return Err(format!("{key} does not hold bytes"));
        }
        Ok(Some(Uint8Array::new(&value).to_vec()))
    }

    fn commit(&self, puts: &[(String, &[u8])], deletes: &[String]) -> Result<(), String> {
        let keys = Array::new_with_length(puts.len() as u32);
        let values = Array::new_with_length(puts.len() as u32);
        for (i, (key, value)) in puts.iter().enumerate() {
            keys.set(i as u32, JsValue::from_str(key));
            values.set(i as u32, Uint8Array::from(*value).into());
        }
        let delete_keys = deletes.iter().map(|key| JsValue::from_str(key)).collect();
        kv_commit(&self.storage, &keys, &values, &delete_keys).map_err(|error| {
            let error = js_error(error);
            abort_soon(
                &self.ctx,
                &format!("Jazz store: a commit could not be written ({error})"),
            );
            error
        })
    }
}

fn vfs_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!("Durable Object storage: {error}"))
}

fn install() -> Result<KvVfsUtil, JsValue> {
    kv_vfs::install::<sqlite_wasm_rs::WasmOsCallback>(VFS).map_err(vfs_error)
}

/// The parts of `ctx` the store uses: its storage and its object id.
fn object(ctx: &JsValue) -> Result<(ObjectStorage, String), JsValue> {
    let get = |target: &JsValue, name: &str| js_sys::Reflect::get(target, &JsValue::from_str(name));
    if ctx.is_undefined() || ctx.is_null() {
        return Err(vfs_error("ctx is missing"));
    }
    let storage = get(ctx, "storage")?;
    if storage.is_undefined() || get(&storage, "kv")?.is_undefined() {
        return Err(crate::not_supported(
            "a Durable Object without synchronous KV storage (it must be SQLite-backed)",
        ));
    }
    let id = get(ctx, "id")?;
    let id = js_sys::Reflect::apply(
        &get(&id, "toString")?.dyn_into::<js_sys::Function>()?,
        &id,
        &Array::new(),
    )?
    .as_string()
    .filter(|id| !id.is_empty() && !id.contains('/'))
    .ok_or_else(|| vfs_error("ctx.id is not an object id"))?;
    Ok((
        ObjectStorage {
            ctx: ctx.clone(),
            storage,
        },
        id,
    ))
}

/// A VFS path for one open of the object's store. Paths are per isolate,
/// which may host several objects and outlive an aborted instance whose
/// connections were never closed, so each open gets its own.
fn path(object_id: &str) -> String {
    thread_local! {
        static OPENS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    let open = OPENS.with(|opens| {
        let open = opens.get();
        opens.set(open + 1);
        open
    });
    format!("/{object_id}-{open}.sqlite")
}

/// An open store's file, kept by its host.
pub struct Store {
    util: Rc<KvVfsUtil>,
    path: String,
}

impl Store {
    /// The storage operations the file made since it was opened.
    pub fn stats(&self) -> KvStats {
        self.util.stats(&self.path).unwrap_or_default()
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.util.detach(&self.path);
    }
}

/// Open the store of the object whose state is `ctx` (the Durable Object's
/// constructor argument). The returned [`Store`] must outlive the [`Db`]:
/// dropping it unbinds the file.
pub async fn open(ctx: JsValue, config: DbConfig) -> Result<(Db, Store), JsValue> {
    open_with_page_size(ctx, config, PAGE_SIZE).await
}

/// [`open`], creating a new file with `page_size` (for comparisons).
pub(crate) async fn open_with_page_size(
    ctx: JsValue,
    config: DbConfig,
    page_size: u32,
) -> Result<(Db, Store), JsValue> {
    let (storage, id) = object(&ctx)?;
    let util = Rc::new(install()?);
    let path = path(&id);
    util.attach(&path, Rc::new(storage), STORE_NAME)
        .map_err(vfs_error)?;
    let store = Store { util, path };
    let options = FileOptions {
        vfs: Some(VFS),
        // See the VFS docs: commits are atomic in storage, so the journal
        // never has to survive a crash.
        journal: Journal::Memory,
        page_size: Some(page_size),
        ..FileOptions::default()
    };
    let db = Db::open_at(config, Path::new(&store.path), options)
        .await
        .map_err(|error| crate::core_error_to_js(&error))?;
    Ok((db, store))
}

/// Delete the object's store (closed) from its storage.
pub fn destroy(ctx: JsValue) -> Result<(), JsValue> {
    let (storage, id) = object(&ctx)?;
    let util = install()?;
    let path = path(&id);
    util.attach(&path, Rc::new(storage), STORE_NAME)
        .map_err(vfs_error)?;
    util.destroy(&path).map_err(vfs_error)
}
