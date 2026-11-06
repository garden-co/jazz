//! The browser worker's IndexedDB SQLite VFS.
//!
//! Adapted from `src/relaxed_idb.rs` of `sqlite-wasm-vfs` 0.2.0
//! (<https://github.com/Spxg/sqlite-wasm-rs>), itself after wa-sqlite's
//! `IDBMirrorVFS`. The IndexedDB layout is unchanged (database named after
//! the VFS, object store `blocks`, one record per `[path, offset]` page),
//! so files the upstream VFS wrote open as before.
//!
//! Removed from upstream: `Preload::All`, `clear_on_init`, import, export,
//! `clear_all`, `exists`, `list` and `count` (Jazz preloads by path and
//! uses none of the rest), and the VFS's own test. Changed: a failed write
//! (below), an explicit flush in place of a barrier, a hook for failed
//! writes, and names.
//!
//! Upstream's licence:
//!
//! > MIT License
//! >
//! > Copyright (c) 2024 Spxg
//! >
//! > Permission is hereby granted, free of charge, to any person obtaining a
//! > copy of this software and associated documentation files (the
//! > "Software"), to deal in the Software without restriction, including
//! > without limitation the rights to use, copy, modify, merge, publish,
//! > distribute, sublicense, and/or sell copies of the Software, and to
//! > permit persons to whom the Software is furnished to do so, subject to
//! > the following conditions:
//! >
//! > The above copyright notice and this permission notice shall be included
//! > in all copies or substantial portions of the Software.
//! >
//! > THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
//! > OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
//! > MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
//! > IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
//! > CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
//! > TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
//! > SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//!
//! The whole file is loaded into memory before SQLite opens it, and SQLite
//! reads and writes that copy synchronously. Each commit queues one
//! IndexedDB transaction holding every page changed since the last one
//! that succeeded, applied in order.
//!
//! What changes from upstream is what a failed IndexedDB transaction
//! (storage full, most often) does:
//!
//! - **Its pages stay dirty.** Upstream forgot them, so the next commit
//!   that succeeded wrote only its own pages over the file as it was before
//!   the failed one: IndexedDB then held a mix of two commits, and SQLite
//!   later read it as "database disk image is malformed". Here the next
//!   write carries every page changed since the last success, so IndexedDB
//!   always holds one whole committed state of the file, never a torn one.
//! - **It is reported.** Upstream dropped the error, and its flush barrier
//!   (an unrelated delete queued behind the commits) succeeded anyway, so
//!   every write looked saved. Here [`IdbVfsUtil::flush`] writes whatever is
//!   still dirty and fails with the storage error if that write fails.
//! - **It stops the per-commit writes.** While the file's last write
//!   failed, commits only mark pages dirty; the host retries with
//!   [`IdbVfsUtil::flush`] (each retry rewrites everything still dirty, so
//!   doing that on every commit of a full store would only burn time). A
//!   commit's own write that fails calls the file's
//!   [`IdbVfsUtil::on_write_failed`] hook, so the host starts retrying even
//!   when nothing waits on a flush.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, CStr};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use indexed_db_futures::database::Database;
use indexed_db_futures::prelude::*;
use indexed_db_futures::transaction::TransactionMode;
use js_sys::{Number, Object, Reflect, Uint8Array};
use rsqlite_vfs::{
    bail, check_option, check_result,
    ffi::{
        sqlite3_file, sqlite3_vfs, SQLITE_ERROR, SQLITE_FCNTL_COMMIT_PHASETWO, SQLITE_FCNTL_PRAGMA,
        SQLITE_FCNTL_SYNC, SQLITE_IOERR, SQLITE_IOERR_DELETE, SQLITE_NOTFOUND, SQLITE_OK,
        SQLITE_OPEN_MAIN_DB,
    },
    register_vfs, registered_vfs, MemChunksFile, OsCallback, RegisterVfsError, SQLiteIoMethods,
    SQLiteVfs, SQLiteVfsFile, VfsAppData, VfsError, VfsFile, VfsResult, VfsStore,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use wasm_bindgen::JsValue;

type Result<T> = std::result::Result<T, IdbVfsError>;

#[derive(thiserror::Error, Debug)]
pub enum IdbVfsError {
    #[error(transparent)]
    Vfs(#[from] RegisterVfsError),
    #[error(transparent)]
    OpenDb(#[from] indexed_db_futures::error::OpenDbError),
    #[error(transparent)]
    IndexedDb(#[from] indexed_db_futures::error::Error),
    /// A write of the file's pages to IndexedDB failed; they stay dirty.
    #[error(
        "could not write the database to IndexedDB ({0}); the browser may be out of storage \
         space for this site. Writes since the last successful save are kept in memory and \
         saved once a retry succeeds"
    )]
    Persist(String),
    #[error("{0}")]
    Generic(String),
}

fn page_read<T, G: Fn(usize) -> Option<T>, R: Fn(T, &mut [u8], (usize, usize))>(
    buf: &mut [u8],
    page_size: usize,
    file_size: usize,
    offset: usize,
    get_page: G,
    read_fn: R,
) -> bool {
    if page_size == 0 || file_size == 0 {
        buf.fill(0);
        return false;
    }

    let mut bytes_read = 0;
    let mut p_data_offset = 0;
    let p_data_length = buf.len();

    while p_data_offset < p_data_length {
        let file_offset = offset + p_data_offset;
        let page_idx = file_offset / page_size;
        let page_offset = file_offset % page_size;
        let page_addr = page_idx * page_size;

        let Some(page) = get_page(page_addr) else {
            break;
        };

        let page_length = (page_size - page_offset).min(p_data_length - p_data_offset);
        read_fn(
            page,
            &mut buf[p_data_offset..p_data_offset + page_length],
            (page_offset, page_offset + page_length),
        );

        p_data_offset += page_length;
        bytes_read += page_length;
    }

    if bytes_read < p_data_length {
        buf[bytes_read..].fill(0);
        return false;
    }

    true
}

struct IdbCommit {
    op: IdbCommitOp,
    notify: Option<tokio::sync::oneshot::Sender<Result<()>>>,
}

enum IdbCommitOp {
    Sync(String),
    Delete(String),
}

enum IdbFile {
    Main(IdbPageFile),
    Temp(MemChunksFile),
}

impl IdbFile {
    fn new(flags: i32) -> Self {
        if flags & SQLITE_OPEN_MAIN_DB == 0 {
            Self::Temp(MemChunksFile::default())
        } else {
            Self::Main(IdbPageFile::default())
        }
    }
}

#[derive(Default)]
struct IdbPageFile {
    file_size: usize,
    block_size: usize,
    blocks: HashMap<usize, Uint8Array>,
    /// Pages changed since the last write to IndexedDB that succeeded.
    tx_blocks: HashSet<usize>,
    /// A write of this file is queued.
    sync_notified: bool,
    /// Why the file's last write to IndexedDB failed, until one succeeds.
    persist_error: Option<String>,
}

impl VfsFile for IdbPageFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        Ok(page_read(
            buf,
            self.block_size,
            self.file_size,
            offset,
            |addr| self.blocks.get(&addr),
            |page, buf, (start, end)| {
                page.subarray(start as u32, end as u32).copy_to(buf);
            },
        ))
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        let page_size = buf.len();

        for fill in (self.file_size..offset).step_by(page_size) {
            self.blocks
                .insert(fill, Uint8Array::new_with_length(page_size as u32));
            self.tx_blocks.insert(fill);
        }

        if let Some(buffer) = self.blocks.get_mut(&offset) {
            buffer.copy_from(buf);
        } else {
            self.blocks.insert(offset, Uint8Array::new_from_slice(buf));
        }

        self.tx_blocks.insert(offset);
        self.block_size = page_size;
        self.file_size = self.file_size.max(offset + page_size);
        Ok(())
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        self.file_size = size;
        Ok(())
    }

    fn flush(&mut self) -> VfsResult<()> {
        Ok(())
    }

    fn size(&self) -> VfsResult<usize> {
        Ok(self.file_size)
    }
}

impl VfsFile for IdbFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        match self {
            IdbFile::Main(file) => file.read(buf, offset),
            IdbFile::Temp(file) => file.read(buf, offset),
        }
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        match self {
            IdbFile::Main(file) => file.write(buf, offset),
            IdbFile::Temp(file) => file.write(buf, offset),
        }
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        match self {
            IdbFile::Main(file) => file.truncate(size),
            IdbFile::Temp(file) => file.truncate(size),
        }
    }

    fn flush(&mut self) -> VfsResult<()> {
        match self {
            IdbFile::Main(file) => file.flush(),
            IdbFile::Temp(file) => file.flush(),
        }
    }

    fn size(&self) -> VfsResult<usize> {
        match self {
            IdbFile::Main(file) => file.size(),
            IdbFile::Temp(file) => file.size(),
        }
    }
}

fn key_range(file: &str, start: usize) -> std::ops::RangeInclusive<[JsValue; 2]> {
    [JsValue::from(file), JsValue::from(start)]
        ..=[
            JsValue::from(file),
            JsValue::from(Number::POSITIVE_INFINITY),
        ]
}

async fn preload_paths(
    indexed_db: &Database,
    files: &[String],
) -> Result<HashMap<String, IdbFile>> {
    let mut name2file = HashMap::new();
    if files.is_empty() {
        return Ok(name2file);
    }
    let transaction = indexed_db
        .transaction("blocks")
        .with_mode(TransactionMode::Readonly)
        .build()?;
    let blocks = transaction.object_store("blocks")?;
    for file in files {
        for block in blocks
            .get_all::<JsValue>()
            .with_query(key_range(file, 0))
            .await?
        {
            let (path, offset, data) = get_block(block?);
            match name2file.entry(path) {
                std::collections::hash_map::Entry::Occupied(mut occupied) => {
                    let IdbFile::Main(db) = occupied.get_mut() else {
                        unreachable!("preloaded files are main databases");
                    };
                    db.file_size += db.block_size;
                    db.blocks.insert(offset, data);
                }
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    vacant.insert(IdbFile::Main(IdbPageFile {
                        file_size: data.length() as _,
                        block_size: data.length() as _,
                        blocks: HashMap::from([(offset, data)]),
                        ..IdbPageFile::default()
                    }));
                }
            }
        }
    }
    Ok(name2file)
}

struct IdbVfs {
    idb: Database,
    name2file: RefCell<HashMap<String, IdbFile>>,
    tx: UnboundedSender<IdbCommit>,
    /// Per file, called when a commit's own write of it fails.
    write_failed: RefCell<HashMap<String, std::rc::Rc<dyn Fn()>>>,
}

impl IdbVfs {
    async fn new(vfs_name: &str, tx: UnboundedSender<IdbCommit>) -> Result<Self> {
        let idb = Database::open(vfs_name)
            .with_version(1u8)
            .with_on_upgrade_needed(|_, db| {
                db.create_object_store("blocks")
                    .with_key_path(["path", "offset"].into())
                    .build()?;
                Ok(())
            })
            .await?;
        Ok(Self {
            idb,
            name2file: RefCell::new(HashMap::new()),
            tx,
            write_failed: RefCell::new(HashMap::new()),
        })
    }

    fn send(&self, op: IdbCommitOp, notify: bool) -> Result<Option<WaitCommit>> {
        let (sender, receiver) = if notify {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        self.tx
            .send(IdbCommit { op, notify: sender })
            .map_err(|_| IdbVfsError::Generic("the IndexedDB commit loop has stopped".into()))?;
        Ok(receiver.map(WaitCommit))
    }

    async fn preload_db(&self, files: Vec<String>) -> Result<()> {
        let missing = {
            let name2file = self.name2file.borrow();
            files
                .into_iter()
                .filter(|file| !name2file.contains_key(file))
                .collect::<Vec<_>>()
        };
        let loaded = preload_paths(&self.idb, &missing).await?;
        self.name2file.borrow_mut().extend(loaded);
        Ok(())
    }

    fn delete_db(&self, name: &str) -> Result<WaitCommit> {
        self.name2file.borrow_mut().remove(name);
        Ok(self
            .send(IdbCommitOp::Delete(name.into()), true)?
            .expect("notified"))
    }

    fn flush(&self, name: &str) -> Result<WaitCommit> {
        Ok(self
            .send(IdbCommitOp::Sync(name.into()), true)?
            .expect("notified"))
    }

    async fn delete_db_impl(&self, file: &str) -> Result<()> {
        let transaction = self
            .idb
            .transaction("blocks")
            .with_mode(TransactionMode::Readwrite)
            .build()?;
        let store = transaction.object_store("blocks")?;
        store.delete(key_range(file, 0)).build()?;
        transaction.commit().await?;
        Ok(())
    }

    /// Write every page of `file` changed since its last successful write
    /// in one IndexedDB transaction. If it fails, those pages stay dirty
    /// for the next write and the file records why.
    async fn sync_db_impl(&self, file: &str) -> Result<()> {
        let (transaction, written) = {
            let mut name2file = self.name2file.borrow_mut();
            let Some(IdbFile::Main(idb_blocks)) = name2file.get_mut(file) else {
                return Ok(());
            };
            idb_blocks.sync_notified = false;

            let file_size = idb_blocks.file_size;
            let mut truncated_offset = file_size;
            while idb_blocks.blocks.remove(&truncated_offset).is_some() {
                truncated_offset += idb_blocks.block_size;
            }
            if idb_blocks.tx_blocks.is_empty() && file_size == truncated_offset {
                // IndexedDB already holds the file as it is in memory.
                idb_blocks.persist_error = None;
                return Ok(());
            }

            let written = std::mem::take(&mut idb_blocks.tx_blocks);
            let path = JsValue::from(file);
            // `put` copies each page (structured clone) when called, so the
            // transaction holds the pages as they are now even if SQLite
            // writes them again before it completes.
            let queued = (|| -> Result<_> {
                let transaction = self
                    .idb
                    .transaction("blocks")
                    .with_mode(TransactionMode::Readwrite)
                    .build()?;
                let store = transaction.object_store("blocks")?;
                for offset in &written {
                    if let Some(buffer) = idb_blocks.blocks.get(offset) {
                        store.put(&set_block(&path, *offset, buffer)).build()?;
                    }
                }
                store.delete(key_range(file, file_size)).build()?;
                Ok(transaction)
            })();
            match queued {
                Ok(transaction) => (transaction, written),
                Err(error) => {
                    idb_blocks.tx_blocks.extend(written);
                    idb_blocks.persist_error = Some(error.to_string());
                    return Err(IdbVfsError::Persist(error.to_string()));
                }
            }
        };
        // The borrow ends above: SQLite runs between here and the commit.

        let committed = transaction.commit().await;

        let mut name2file = self.name2file.borrow_mut();
        // A file deleted meanwhile keeps nothing to retry.
        let Some(IdbFile::Main(idb_blocks)) = name2file.get_mut(file) else {
            return committed.map_err(|error| IdbVfsError::Persist(error.to_string()));
        };
        match committed {
            Ok(()) => {
                idb_blocks.persist_error = None;
                Ok(())
            }
            Err(error) => {
                idb_blocks.tx_blocks.extend(written);
                idb_blocks.persist_error = Some(error.to_string());
                Err(IdbVfsError::Persist(error.to_string()))
            }
        }
    }

    async fn commit_loop(&self, mut rx: UnboundedReceiver<IdbCommit>) {
        while let Some(IdbCommit { op, notify }) = rx.recv().await {
            let (result, synced) = match op {
                IdbCommitOp::Sync(file) => (self.sync_db_impl(&file).await, Some(file)),
                IdbCommitOp::Delete(file) => (self.delete_db_impl(&file).await, None),
            };
            if let Some(notify) = notify {
                // The waiter may have gone away.
                let _ = notify.send(result);
            } else if let Err(error) = result {
                // A per-commit write: the file now holds the error; its
                // host retries with a flush, which reports it.
                tracing::warn!(%error, "IndexedDB write failed");
                let hook = synced.and_then(|file| self.write_failed.borrow().get(&file).cloned());
                if let Some(hook) = hook {
                    hook();
                }
            }
        }
    }
}

fn get_block(value: JsValue) -> (String, usize, Uint8Array) {
    let path = Reflect::get(&value, &JsValue::from("path"))
        .unwrap()
        .as_string()
        .unwrap();
    let offset = Reflect::get(&value, &JsValue::from("offset"))
        .unwrap()
        .as_f64()
        .unwrap() as usize;
    let data = Reflect::get(&value, &JsValue::from("data")).unwrap();
    (path, offset, Uint8Array::from(data))
}

fn set_block(path: &JsValue, offset: usize, data: &Uint8Array) -> JsValue {
    let block = Object::new();
    Reflect::set(&block, &JsValue::from("path"), path).unwrap();
    Reflect::set(&block, &JsValue::from("offset"), &JsValue::from(offset)).unwrap();
    Reflect::set(&block, &JsValue::from("data"), &JsValue::from(data)).unwrap();
    block.into()
}

struct IdbStore;

impl VfsStore<IdbFile, IdbVfs> for IdbStore {
    fn add_file(vfs: *mut sqlite3_vfs, file: &str, flags: i32) -> VfsResult<()> {
        let pool = unsafe { Self::app_data(vfs) };
        pool.name2file
            .borrow_mut()
            .insert(file.into(), IdbFile::new(flags));
        Ok(())
    }

    fn contains_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<bool> {
        let pool = unsafe { Self::app_data(vfs) };
        Ok(pool.name2file.borrow().contains_key(file))
    }

    fn delete_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<()> {
        let pool = unsafe { Self::app_data(vfs) };
        let Some(idb_file) = pool.name2file.borrow_mut().remove(file) else {
            return Err(VfsError::new(
                SQLITE_IOERR_DELETE,
                format!("{file} not found"),
            ));
        };
        // A temporary file never reaches IndexedDB.
        if let IdbFile::Main(_) = &idb_file {
            if pool.send(IdbCommitOp::Delete(file.into()), false).is_err() {
                return Err(VfsError::new(
                    SQLITE_IOERR_DELETE,
                    format!("failed to queue the delete of {file}"),
                ));
            }
        }
        Ok(())
    }

    fn with_file<F: Fn(&IdbFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let name = unsafe { vfs_file.name() };
        let pool = unsafe { Self::app_data(vfs_file.vfs) };
        match pool.name2file.borrow().get(name) {
            Some(file) => f(file),
            None => Err(VfsError::new(SQLITE_IOERR, format!("{name} not found"))),
        }
    }

    fn with_file_mut<F: Fn(&mut IdbFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let name = unsafe { vfs_file.name() };
        let pool = unsafe { Self::app_data(vfs_file.vfs) };
        match pool.name2file.borrow_mut().get_mut(name) {
            Some(file) => f(file),
            None => Err(VfsError::new(SQLITE_IOERR, format!("{name} not found"))),
        }
    }
}

struct IdbIoMethods;

impl SQLiteIoMethods for IdbIoMethods {
    type File = IdbFile;
    type AppData = IdbVfs;
    type Store = IdbStore;

    const VERSION: ::std::os::raw::c_int = 1;

    unsafe extern "C" fn xFileControl(
        p_file: *mut sqlite3_file,
        op: ::std::os::raw::c_int,
        p_arg: *mut ::std::os::raw::c_void,
    ) -> ::std::os::raw::c_int {
        let vfs_file = SQLiteVfsFile::from_file(p_file);
        let pool = Self::Store::app_data(vfs_file.vfs);
        let name = vfs_file.name();

        let mut name2file = pool.name2file.borrow_mut();
        let file = check_option!(name2file.get_mut(name));

        let IdbFile::Main(file) = file else {
            return SQLITE_NOTFOUND;
        };

        match op {
            SQLITE_FCNTL_PRAGMA => {
                let p_arg = p_arg as *mut *mut c_char;
                let name = *p_arg.add(1);
                let value = *p_arg.add(2);

                bail!(name.is_null());
                bail!(value.is_null(), SQLITE_NOTFOUND);

                let key = check_result!(CStr::from_ptr(name).to_str());
                let value = check_result!(CStr::from_ptr(value).to_str());

                if key.eq_ignore_ascii_case("page_size") {
                    let page_size = check_result!(value.parse::<usize>());
                    if page_size == file.block_size {
                        return SQLITE_OK;
                    } else if file.block_size == 0 {
                        file.block_size = page_size;
                    } else {
                        return pool.store_err(VfsError::new(
                            SQLITE_ERROR,
                            "page_size cannot be changed".into(),
                        ));
                    }
                } else if key.eq_ignore_ascii_case("synchronous")
                    && !value.eq_ignore_ascii_case("off")
                {
                    return pool.store_err(VfsError::new(
                        SQLITE_ERROR,
                        "the IndexedDB vfs only supports synchronous=off".into(),
                    ));
                };
            }
            SQLITE_FCNTL_SYNC | SQLITE_FCNTL_COMMIT_PHASETWO => {
                // While the last write failed, only the host's flush
                // retries (see the module docs).
                if !file.sync_notified && file.persist_error.is_none() {
                    if pool.send(IdbCommitOp::Sync(name.into()), false).is_err() {
                        return pool.store_err(VfsError::new(
                            SQLITE_ERROR,
                            format!("failed to queue the write of {name}"),
                        ));
                    }
                    file.sync_notified = true;
                }
            }
            _ => (),
        }

        SQLITE_NOTFOUND
    }
}

struct IdbSqliteVfs<C>(PhantomData<C>);

impl<C: OsCallback> SQLiteVfs<IdbIoMethods> for IdbSqliteVfs<C> {
    const VERSION: ::std::os::raw::c_int = 1;

    fn sleep(dur: Duration) {
        C::sleep(dur);
    }

    fn random(buf: &mut [u8]) {
        C::random(buf);
    }

    fn epoch_timestamp_in_ms() -> i64 {
        C::epoch_timestamp_in_ms()
    }
}

/// Resolves when a queued IndexedDB operation completes.
pub struct WaitCommit(tokio::sync::oneshot::Receiver<Result<()>>);

impl Future for WaitCommit {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|ret| {
            ret.unwrap_or_else(|_| {
                Err(IdbVfsError::Generic(
                    "the IndexedDB commit loop dropped a waiter".into(),
                ))
            })
        })
    }
}

/// Handle on the registered VFS.
pub struct IdbVfsUtil {
    pool: &'static VfsAppData<IdbVfs>,
}

impl IdbVfsUtil {
    /// Load `files` into memory: SQLite reads synchronously, so a file
    /// must be loaded before it is opened.
    pub async fn preload_db(&self, files: Vec<String>) -> Result<()> {
        self.pool.preload_db(files).await
    }

    /// Delete `file` (closed) from memory and IndexedDB.
    pub fn delete_db(&self, file: &str) -> Result<WaitCommit> {
        self.pool.delete_db(file)
    }

    /// Resolves once IndexedDB holds `file` as it is in memory now, writing
    /// whatever is still dirty (a failed write's pages included), or fails
    /// with why that write failed.
    pub fn flush(&self, file: &str) -> Result<WaitCommit> {
        self.pool.flush(file)
    }

    /// Call `hook` when a commit's own write of `file` fails (`None`
    /// removes it): the host then retries with [`Self::flush`].
    pub fn on_write_failed(&self, file: &str, hook: Option<std::rc::Rc<dyn Fn()>>) {
        let mut hooks = self.pool.write_failed.borrow_mut();
        match hook {
            Some(hook) => hooks.insert(file.to_owned(), hook),
            None => hooks.remove(file),
        };
    }

    /// Whether `file`'s last write to IndexedDB failed (its pages wait for
    /// a flush to retry them).
    pub fn write_failed(&self, file: &str) -> bool {
        matches!(
            self.pool.name2file.borrow().get(file),
            Some(IdbFile::Main(IdbPageFile {
                persist_error: Some(_),
                ..
            }))
        )
    }
}

/// Register the VFS as `vfs_name` (once; later calls return a handle on
/// the registered one).
pub async fn install<C: OsCallback>(vfs_name: &str) -> Result<IdbVfsUtil> {
    static REGISTER_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = REGISTER_GUARD.lock().await;

    let pool = if let Some(vfs) = registered_vfs(vfs_name)? {
        unsafe { IdbStore::app_data(vfs) }
    } else {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let pool = IdbVfs::new(vfs_name, tx).await?;
        let vfs = register_vfs::<IdbIoMethods, IdbSqliteVfs<C>>(vfs_name, pool, false)?;
        let app_data = unsafe { IdbStore::app_data(vfs) };
        wasm_bindgen_futures::spawn_local(app_data.commit_loop(rx));
        app_data
    };

    Ok(IdbVfsUtil { pool })
}
