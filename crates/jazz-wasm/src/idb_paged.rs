//! A spike (`browser-bench` feature, never shipped): the IndexedDB VFS with
//! a bounded page cache instead of a whole-file preload.
//!
//! A read of a page that is not resident records the page and fails with
//! `SQLITE_IOERR_READ`. The caller rolls back, awaits [`PagedUtil::load`]
//! (every recorded page in one IndexedDB transaction) and runs the same
//! operation again: about one round per B-tree level or overflow page it
//! has not seen. Pages read or loaded while an operation is in flight stay
//! resident until [`PagedUtil::end_op`], so an operation whose working set
//! exceeds the budget still completes; then the cache is trimmed back to the
//! budget, least recently used first. Dirty pages are never evicted before
//! their flush, and page 1 (the header SQLite rereads at every transaction)
//! never is.
//!
//! The file layout is relaxed-idb's (database `jazz-idb`, store `blocks`,
//! key `[path, offset]`), so both VFSes open the same stored file.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::{c_char, CStr};
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::time::Duration;

use indexed_db_futures::database::Database;
use indexed_db_futures::prelude::*;
use indexed_db_futures::transaction::TransactionMode;
use js_sys::{Number, Object, Reflect, Uint8Array};
use rsqlite_vfs::{
    bail, check_option, check_result,
    ffi::{
        sqlite3_file, sqlite3_vfs, SQLITE_ERROR, SQLITE_FCNTL_COMMIT_PHASETWO, SQLITE_FCNTL_PRAGMA,
        SQLITE_FCNTL_SYNC, SQLITE_IOERR, SQLITE_IOERR_DELETE, SQLITE_IOERR_READ, SQLITE_NOTFOUND,
        SQLITE_OK, SQLITE_OPEN_MAIN_DB,
    },
    register_vfs, registered_vfs, MemChunksFile, OsCallback, SQLiteIoMethods, SQLiteVfs,
    SQLiteVfsFile, VfsAppData, VfsError, VfsFile, VfsResult, VfsStore,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use wasm_bindgen::JsValue;

pub(crate) const PAGED_VFS: &str = "jazz-idb-paged";
/// The IndexedDB database relaxed-idb keeps the `jazz-idb` VFS's files in.
const IDB_DATABASE: &str = "jazz-idb";

fn idb_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!("paged IndexedDB: {error}"))
}

/// Counters since install (or the last [`PagedUtil::reset_stats`]).
#[derive(Clone, Copy, Default, Debug)]
pub(crate) struct Stats {
    /// Reads that found their page missing.
    pub faults: u64,
    /// [`PagedUtil::load`] calls that loaded at least one page.
    pub rounds: u64,
    /// IndexedDB transactions: loads, flushes and the open.
    pub idb_txs: u64,
    pub pages_loaded: u64,
    pub pages_evicted: u64,
    /// Pages the hint preloaded at attach.
    pub hot_pages: u64,
    /// Highest resident page bytes seen.
    pub peak_resident: usize,
}

struct Page {
    data: Uint8Array,
    used: Cell<u64>,
}

#[derive(Default)]
struct PageFile {
    file_size: usize,
    page_size: usize,
    pages: HashMap<usize, Page>,
    /// Written since the last flush was queued: never evicted.
    dirty: HashSet<usize>,
    sync_notified: bool,
    /// Pages a read needed and did not find.
    misses: RefCell<BTreeSet<usize>>,
    /// Last use of every page this file has used, resident or not, seeded
    /// from the stored hint: the next session's hot set.
    recency: RefCell<HashMap<usize, u64>>,
}

impl PageFile {
    fn touch(&self, addr: usize, at: u64) {
        self.recency.borrow_mut().insert(addr, at);
    }
}

impl PageFile {
    fn resident(&self) -> usize {
        self.pages.len() * self.page_size
    }
}

enum PagedFile {
    Main(PageFile),
    Temp(MemChunksFile),
}

struct Paged {
    idb: Database,
    files: RefCell<HashMap<String, PagedFile>>,
    tx: UnboundedSender<Commit>,
    budget: Cell<usize>,
    readahead: Cell<usize>,
    clock: Cell<u64>,
    /// An operation is in flight: nothing is evicted until it ends.
    in_op: Cell<bool>,
    stats: Cell<Stats>,
}

impl Paged {
    fn tick(&self) -> u64 {
        let now = self.clock.get() + 1;
        self.clock.set(now);
        now
    }

    fn stat(&self, f: impl FnOnce(&mut Stats)) {
        let mut stats = self.stats.get();
        f(&mut stats);
        self.stats.set(stats);
    }

    fn note_resident(&self) {
        let resident = self
            .files
            .borrow()
            .values()
            .map(|file| match file {
                PagedFile::Main(file) => file.resident(),
                PagedFile::Temp(_) => 0,
            })
            .sum::<usize>();
        self.stat(|s| s.peak_resident = s.peak_resident.max(resident));
    }

    /// Trim every file to the budget, least recently used clean pages first.
    fn evict(&self) {
        if self.in_op.get() {
            return;
        }
        let budget = self.budget.get();
        let mut evicted = 0;
        for file in self.files.borrow_mut().values_mut() {
            let PagedFile::Main(file) = file else {
                continue;
            };
            if file.resident() <= budget {
                continue;
            }
            let mut clean: Vec<(u64, usize)> = file
                .pages
                .iter()
                .filter(|(addr, _)| **addr != 0 && !file.dirty.contains(addr))
                .map(|(addr, page)| (page.used.get(), *addr))
                .collect();
            clean.sort_unstable();
            for (_, addr) in clean {
                if file.resident() <= budget {
                    break;
                }
                file.pages.remove(&addr);
                evicted += 1;
            }
        }
        self.stat(|s| s.pages_evicted += evicted);
    }
}

impl VfsFile for PageFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        if self.page_size == 0 || self.file_size == 0 {
            buf.fill(0);
            return Ok(false);
        }
        let mut done = 0;
        let mut missing = false;
        while done < buf.len() {
            let at = offset + done;
            if at >= self.file_size {
                buf[done..].fill(0);
                return if missing { Err(fault()) } else { Ok(false) };
            }
            let addr = at / self.page_size * self.page_size;
            let within = at - addr;
            let len = (self.page_size - within).min(buf.len() - done);
            match self.pages.get(&addr) {
                Some(page) => {
                    let at = now();
                    page.used.set(at);
                    self.touch(addr, at);
                    page.data
                        .subarray(within as u32, (within + len) as u32)
                        .copy_to(&mut buf[done..done + len]);
                }
                None => {
                    self.misses.borrow_mut().insert(addr);
                    missing = true;
                }
            }
            done += len;
        }
        if missing {
            return Err(fault());
        }
        Ok(true)
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        let page_size = buf.len();
        for fill in (self.file_size..offset).step_by(page_size) {
            self.pages.insert(
                fill,
                Page {
                    data: Uint8Array::new_with_length(page_size as u32),
                    used: Cell::new(now()),
                },
            );
            self.dirty.insert(fill);
        }
        match self.pages.get_mut(&offset) {
            Some(page) => {
                page.data.copy_from(buf);
                page.used.set(now());
            }
            None => {
                self.pages.insert(
                    offset,
                    Page {
                        data: Uint8Array::new_from_slice(buf),
                        used: Cell::new(now()),
                    },
                );
            }
        }
        self.misses.borrow_mut().remove(&offset);
        self.touch(offset, now());
        self.dirty.insert(offset);
        self.page_size = page_size;
        self.file_size = self.file_size.max(offset + page_size);
        Ok(())
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        self.file_size = size;
        self.pages.retain(|addr, _| *addr < size);
        self.recency.borrow_mut().retain(|addr, _| *addr < size);
        Ok(())
    }

    fn flush(&mut self) -> VfsResult<()> {
        Ok(())
    }

    fn size(&self) -> VfsResult<usize> {
        Ok(self.file_size)
    }
}

thread_local! {
    static PAGED: Cell<Option<&'static VfsAppData<Paged>>> = const { Cell::new(None) };
}

fn now() -> u64 {
    PAGED.with(|p| p.get().map(|p| p.tick()).unwrap_or(0))
}

fn fault() -> VfsError {
    PAGED.with(|p| {
        if let Some(p) = p.get() {
            p.stat(|s| s.faults += 1);
        }
    });
    VfsError::new(SQLITE_IOERR_READ, "page not resident (fault)".into())
}

impl VfsFile for PagedFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        match self {
            PagedFile::Main(file) => file.read(buf, offset),
            PagedFile::Temp(file) => file.read(buf, offset),
        }
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        match self {
            PagedFile::Main(file) => file.write(buf, offset),
            PagedFile::Temp(file) => file.write(buf, offset),
        }
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        match self {
            PagedFile::Main(file) => file.truncate(size),
            PagedFile::Temp(file) => file.truncate(size),
        }
    }

    fn flush(&mut self) -> VfsResult<()> {
        match self {
            PagedFile::Main(file) => file.flush(),
            PagedFile::Temp(file) => file.flush(),
        }
    }

    fn size(&self) -> VfsResult<usize> {
        match self {
            PagedFile::Main(file) => file.size(),
            PagedFile::Temp(file) => file.size(),
        }
    }
}

fn key_range(file: &str, from: usize, to: f64) -> std::ops::RangeInclusive<[JsValue; 2]> {
    [JsValue::from(file), JsValue::from(from)]..=[JsValue::from(file), JsValue::from(to)]
}

/// The hint record sits below every page's key, outside relaxed-idb's
/// `[path, 0..]` ranges.
const HINT_OFFSET: f64 = -1.0;

fn hint_key(file: &str) -> [JsValue; 2] {
    [JsValue::from(file), JsValue::from(HINT_OFFSET)]
}

fn block_data(value: &JsValue) -> (usize, Uint8Array) {
    let offset = Reflect::get(value, &JsValue::from("offset"))
        .ok()
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0) as usize;
    let data = Reflect::get(value, &JsValue::from("data")).unwrap_or(JsValue::NULL);
    (offset, Uint8Array::from(data))
}

fn set_block(path: &JsValue, offset: usize, data: &Uint8Array) -> JsValue {
    let block = Object::new();
    let _ = Reflect::set(&block, &JsValue::from("path"), path);
    let _ = Reflect::set(&block, &JsValue::from("offset"), &JsValue::from(offset));
    let _ = Reflect::set(&block, &JsValue::from("data"), &JsValue::from(data));
    block.into()
}

enum Commit {
    Sync(String),
    Barrier(tokio::sync::oneshot::Sender<()>),
}

impl Paged {
    #[allow(clippy::await_holding_refcell_ref)]
    async fn sync(&self, path: &str) -> Result<(), JsValue> {
        let mut files = self.files.borrow_mut();
        let Some(PagedFile::Main(file)) = files.get_mut(path) else {
            return Ok(());
        };
        file.sync_notified = false;
        let dirty = std::mem::take(&mut file.dirty);
        if dirty.is_empty() {
            return Ok(());
        }
        let key = JsValue::from(path);
        let transaction = self
            .idb
            .transaction("blocks")
            .with_mode(TransactionMode::Readwrite)
            .build()
            .map_err(idb_error)?;
        let store = transaction.object_store("blocks").map_err(idb_error)?;
        for offset in dirty {
            if let Some(page) = file.pages.get(&offset) {
                store
                    .put(&set_block(&key, offset, &page.data))
                    .build()
                    .map_err(idb_error)?;
            }
        }
        store
            .delete(key_range(path, file.file_size, Number::POSITIVE_INFINITY))
            .build()
            .map_err(idb_error)?;
        // Requests are queued; the borrow ends before the commit's await.
        drop(files);
        transaction.commit().await.map_err(idb_error)?;
        self.stat(|s| s.idb_txs += 1);
        self.evict();
        Ok(())
    }

    async fn commit_loop(&self, mut rx: UnboundedReceiver<Commit>) {
        while let Some(commit) = rx.recv().await {
            match commit {
                Commit::Sync(path) => {
                    if let Err(error) = self.sync(&path).await {
                        web_sys_log(&format!("paged IndexedDB sync failed: {error:?}"));
                    }
                }
                Commit::Barrier(done) => {
                    let _ = done.send(());
                }
            }
        }
    }
}

fn web_sys_log(message: &str) {
    let _ = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("console"))
        .ok()
        .and_then(|console| {
            js_sys::Reflect::get(&console, &JsValue::from_str("warn"))
                .ok()
                .and_then(|warn| {
                    js_sys::Function::from(warn)
                        .call1(&console, &JsValue::from_str(message))
                        .ok()
                })
        });
}

struct PagedStore;

impl VfsStore<PagedFile, Paged> for PagedStore {
    fn add_file(vfs: *mut sqlite3_vfs, file: &str, flags: i32) -> VfsResult<()> {
        let pool = unsafe { Self::app_data(vfs) };
        let new = if flags & SQLITE_OPEN_MAIN_DB == 0 {
            PagedFile::Temp(MemChunksFile::default())
        } else {
            PagedFile::Main(PageFile::default())
        };
        pool.files.borrow_mut().insert(file.into(), new);
        Ok(())
    }

    fn contains_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<bool> {
        let pool = unsafe { Self::app_data(vfs) };
        Ok(pool.files.borrow().contains_key(file))
    }

    fn delete_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<()> {
        let pool = unsafe { Self::app_data(vfs) };
        match pool.files.borrow_mut().remove(file) {
            // Only the in-memory journal and temp files are deleted here.
            Some(_) => Ok(()),
            None => Err(VfsError::new(
                SQLITE_IOERR_DELETE,
                format!("{file} not found"),
            )),
        }
    }

    fn with_file<F: Fn(&PagedFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let name = unsafe { vfs_file.name() };
        let pool = unsafe { Self::app_data(vfs_file.vfs) };
        match pool.files.borrow().get(name) {
            Some(file) => f(file),
            None => Err(VfsError::new(SQLITE_IOERR, format!("{name} not found"))),
        }
    }

    fn with_file_mut<F: Fn(&mut PagedFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let name = unsafe { vfs_file.name() };
        let pool = unsafe { Self::app_data(vfs_file.vfs) };
        let result = match pool.files.borrow_mut().get_mut(name) {
            Some(file) => f(file),
            None => Err(VfsError::new(SQLITE_IOERR, format!("{name} not found"))),
        };
        pool.note_resident();
        result
    }
}

struct PagedIoMethods;

impl SQLiteIoMethods for PagedIoMethods {
    type File = PagedFile;
    type AppData = Paged;
    type Store = PagedStore;

    const VERSION: ::std::os::raw::c_int = 1;

    unsafe extern "C" fn xFileControl(
        p_file: *mut sqlite3_file,
        op: ::std::os::raw::c_int,
        p_arg: *mut ::std::os::raw::c_void,
    ) -> ::std::os::raw::c_int {
        let vfs_file = SQLiteVfsFile::from_file(p_file);
        let pool = Self::Store::app_data(vfs_file.vfs);
        let name = vfs_file.name();
        let mut files = pool.files.borrow_mut();
        let file = check_option!(files.get_mut(name));
        let PagedFile::Main(file) = file else {
            return SQLITE_NOTFOUND;
        };
        match op {
            SQLITE_FCNTL_PRAGMA => {
                let p_arg = p_arg as *mut *mut c_char;
                let key = *p_arg.add(1);
                let value = *p_arg.add(2);
                bail!(key.is_null());
                bail!(value.is_null(), SQLITE_NOTFOUND);
                let key = check_result!(CStr::from_ptr(key).to_str());
                let value = check_result!(CStr::from_ptr(value).to_str());
                if key.eq_ignore_ascii_case("page_size") {
                    let page_size = check_result!(value.parse::<usize>());
                    if page_size == file.page_size {
                        return SQLITE_OK;
                    } else if file.page_size == 0 {
                        file.page_size = page_size;
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
                        "the paged IndexedDB vfs only supports synchronous=off".into(),
                    ));
                }
            }
            SQLITE_FCNTL_SYNC | SQLITE_FCNTL_COMMIT_PHASETWO => {
                if !file.sync_notified {
                    if pool.tx.send(Commit::Sync(name.into())).is_err() {
                        return pool.store_err(VfsError::new(
                            SQLITE_ERROR,
                            format!("failed to send sync task, file: {name}"),
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

struct PagedVfs<C>(PhantomData<C>);

impl<C: OsCallback> SQLiteVfs<PagedIoMethods> for PagedVfs<C> {
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

/// The paged VFS's handle.
#[derive(Clone, Copy)]
pub(crate) struct PagedUtil {
    pool: &'static VfsAppData<Paged>,
}

impl PagedUtil {
    /// Make the stored file at `path` openable: its size, page 1, and the
    /// most recently used `hot_bytes` of pages its last sessions used.
    pub(crate) async fn attach(&self, path: &str, hot_bytes: usize) -> Result<(), JsValue> {
        if self.pool.files.borrow().contains_key(path) {
            return Ok(());
        }
        let transaction = self
            .pool
            .idb
            .transaction("blocks")
            .with_mode(TransactionMode::Readonly)
            .build()
            .map_err(idb_error)?;
        let store = transaction.object_store("blocks").map_err(idb_error)?;
        let count = store
            .count()
            .with_query(key_range(path, 0, Number::POSITIVE_INFINITY))
            .await
            .map_err(idb_error)? as usize;
        let first = store
            .get_all::<JsValue>()
            .with_query(key_range(path, 0, 0.0))
            .await
            .map_err(idb_error)?;
        let hint: Vec<usize> = store
            .get::<JsValue, _, _>(hint_key(path))
            .await
            .map_err(idb_error)?
            .and_then(|record| Reflect::get(&record, &JsValue::from("hint")).ok())
            .filter(|hint| hint.is_object())
            .map(|hint| {
                js_sys::Float64Array::from(hint)
                    .to_vec()
                    .into_iter()
                    .map(|addr| addr as usize)
                    .collect()
            })
            .unwrap_or_default();
        self.pool.stat(|s| s.idb_txs += 1);
        let mut file = PageFile::default();
        for block in first {
            let (offset, data) = block_data(&block.map_err(idb_error)?);
            file.page_size = data.length() as usize;
            file.pages.insert(
                offset,
                Page {
                    data,
                    used: Cell::new(0),
                },
            );
        }
        if count == 0 || file.page_size == 0 {
            return Ok(());
        }
        file.file_size = count * file.page_size;
        // The hint is most recent first; its order carries over as the
        // oldest uses of this session.
        let hint: Vec<usize> = hint
            .into_iter()
            .filter(|addr| *addr < file.file_size && addr % file.page_size == 0)
            .collect();
        let base = self.pool.clock.get();
        for (rank, addr) in hint.iter().enumerate() {
            file.touch(*addr, base + (hint.len() - rank) as u64);
        }
        self.pool.clock.set(base + hint.len() as u64 + 1);
        let hot_pages = hot_bytes / file.page_size;
        let mut hot: Vec<usize> = hint
            .iter()
            .copied()
            .filter(|addr| !file.pages.contains_key(addr))
            .take(hot_pages)
            .collect();
        hot.sort_unstable();
        self.pool
            .files
            .borrow_mut()
            .insert(path.to_owned(), PagedFile::Main(file));
        let loaded = self.fetch(path, hot).await?;
        self.pool.stat(|s| s.hot_pages += loaded as u64);
        // Preloaded pages keep their rank from the hint, not this load.
        if let Some(PagedFile::Main(file)) = self.pool.files.borrow().get(path) {
            let recency = file.recency.borrow();
            for (addr, page) in &file.pages {
                page.used.set(recency.get(addr).copied().unwrap_or(0));
            }
        }
        self.pool.note_resident();
        Ok(())
    }

    /// Store the most recently used `max_pages` page addresses of `path`,
    /// for the next session's [`Self::attach`]. It is only a hint: a stale
    /// or wrong address loads a page that is not needed, nothing more.
    pub(crate) async fn save_hint(&self, path: &str, max_pages: usize) -> Result<(), JsValue> {
        let hint: Vec<f64> = {
            let files = self.pool.files.borrow();
            let Some(PagedFile::Main(file)) = files.get(path) else {
                return Ok(());
            };
            let mut used: Vec<(u64, usize)> = file
                .recency
                .borrow()
                .iter()
                .map(|(addr, at)| (*at, *addr))
                .collect();
            used.sort_unstable_by(|a, b| b.cmp(a));
            used.into_iter()
                .take(max_pages)
                .map(|(_, addr)| addr as f64)
                .collect()
        };
        let transaction = self
            .pool
            .idb
            .transaction("blocks")
            .with_mode(TransactionMode::Readwrite)
            .build()
            .map_err(idb_error)?;
        let store = transaction.object_store("blocks").map_err(idb_error)?;
        let record = Object::new();
        let _ = Reflect::set(&record, &JsValue::from("path"), &JsValue::from(path));
        let _ = Reflect::set(
            &record,
            &JsValue::from("offset"),
            &JsValue::from(HINT_OFFSET),
        );
        let _ = Reflect::set(
            &record,
            &JsValue::from("hint"),
            &js_sys::Float64Array::from(&hint[..]),
        );
        store
            .put(&JsValue::from(record))
            .build()
            .map_err(idb_error)?;
        transaction.commit().await.map_err(idb_error)?;
        self.pool.stat(|s| s.idb_txs += 1);
        Ok(())
    }

    /// Whether a read faulted since the last load.
    pub(crate) fn faulted(&self) -> bool {
        self.pool.files.borrow().values().any(|file| match file {
            PagedFile::Main(file) => !file.misses.borrow().is_empty(),
            PagedFile::Temp(_) => false,
        })
    }

    /// Load every recorded miss (and its readahead) in one IndexedDB
    /// transaction per file. Returns the pages loaded.
    pub(crate) async fn load(&self) -> Result<usize, JsValue> {
        let wanted: Vec<(String, Vec<usize>)> = {
            let files = self.pool.files.borrow();
            let readahead = self.pool.readahead.get();
            files
                .iter()
                .filter_map(|(path, file)| {
                    let PagedFile::Main(file) = file else {
                        return None;
                    };
                    let misses = std::mem::take(&mut *file.misses.borrow_mut());
                    if misses.is_empty() {
                        return None;
                    }
                    let mut want = BTreeSet::new();
                    for addr in misses {
                        for k in 0..=readahead {
                            let at = addr + k * file.page_size;
                            if at < file.file_size && !file.pages.contains_key(&at) {
                                want.insert(at);
                            }
                        }
                    }
                    Some((path.clone(), want.into_iter().collect()))
                })
                .collect()
        };
        let mut loaded = 0;
        for (path, addrs) in wanted {
            loaded += self.fetch(&path, addrs).await?;
        }
        if loaded > 0 {
            self.pool.stat(|s| {
                s.rounds += 1;
                s.pages_loaded += loaded as u64;
            });
        }
        self.pool.note_resident();
        self.pool.evict();
        Ok(loaded)
    }

    /// Load `addrs` (sorted, not resident) of `path` in one readonly
    /// IndexedDB transaction, contiguous runs as one range request each.
    async fn fetch(&self, path: &str, addrs: Vec<usize>) -> Result<usize, JsValue> {
        if addrs.is_empty() {
            return Ok(0);
        }
        let page_size = match self.pool.files.borrow().get(path) {
            Some(PagedFile::Main(file)) => file.page_size,
            _ => return Ok(0),
        };
        let transaction = self
            .pool
            .idb
            .transaction("blocks")
            .with_mode(TransactionMode::Readonly)
            .build()
            .map_err(idb_error)?;
        let store = transaction.object_store("blocks").map_err(idb_error)?;
        let mut runs: Vec<(usize, usize)> = Vec::new();
        for addr in addrs {
            match runs.last_mut() {
                Some((_, end)) if *end + page_size == addr => *end = addr,
                _ => runs.push((addr, addr)),
            }
        }
        let mut requests = Vec::with_capacity(runs.len());
        for (start, end) in &runs {
            requests.push(
                store
                    .get_all::<JsValue>()
                    .with_query(key_range(path, *start, *end as f64))
                    .into_future(),
            );
        }
        let results = futures_util::future::join_all(requests).await;
        self.pool.stat(|s| s.idb_txs += 1);
        let mut files = self.pool.files.borrow_mut();
        let Some(PagedFile::Main(file)) = files.get_mut(path) else {
            return Ok(0);
        };
        let mut loaded = 0;
        for result in results {
            for block in result.map_err(idb_error)? {
                let (offset, data) = block_data(&block.map_err(idb_error)?);
                file.pages.entry(offset).or_insert_with(|| {
                    loaded += 1;
                    Page {
                        data,
                        used: Cell::new(now()),
                    }
                });
            }
        }
        Ok(loaded)
    }

    /// Pin everything touched until [`Self::end_op`].
    pub(crate) fn begin_op(&self) {
        self.pool.in_op.set(true);
    }

    pub(crate) fn end_op(&self) {
        self.pool.in_op.set(false);
        self.pool.evict();
    }

    /// Resolves once every commit so far is in IndexedDB.
    pub(crate) async fn flush(&self) -> Result<(), JsValue> {
        let (done, wait) = tokio::sync::oneshot::channel();
        self.pool
            .tx
            .send(Commit::Barrier(done))
            .map_err(|_| idb_error("commit loop gone"))?;
        wait.await.map_err(|_| idb_error("commit loop gone"))
    }

    pub(crate) fn stats(&self) -> Stats {
        self.pool.stats.get()
    }

    pub(crate) fn reset_stats(&self) {
        self.pool.stats.set(Stats::default());
    }

    pub(crate) fn resident(&self) -> usize {
        self.pool
            .files
            .borrow()
            .values()
            .map(|file| match file {
                PagedFile::Main(file) => file.resident(),
                PagedFile::Temp(_) => 0,
            })
            .sum()
    }

    pub(crate) fn configure(&self, budget: usize, readahead: usize) {
        self.pool.budget.set(budget);
        self.pool.readahead.set(readahead);
    }
}

/// Register the paged VFS (once per worker).
pub(crate) async fn install() -> Result<PagedUtil, JsValue> {
    if let Some(vfs) = registered_vfs(PAGED_VFS).map_err(idb_error)? {
        return Ok(PagedUtil {
            pool: unsafe { PagedStore::app_data(vfs) },
        });
    }
    let idb = Database::open(IDB_DATABASE)
        .with_version(1u8)
        .with_on_upgrade_needed(|_, db| {
            db.create_object_store("blocks")
                .with_key_path(["path", "offset"].into())
                .build()?;
            Ok(())
        })
        .await
        .map_err(idb_error)?;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let pool = Paged {
        idb,
        files: RefCell::new(HashMap::new()),
        tx,
        budget: Cell::new(usize::MAX),
        readahead: Cell::new(0),
        clock: Cell::new(0),
        in_op: Cell::new(false),
        stats: Cell::new(Stats::default()),
    };
    let vfs = register_vfs::<PagedIoMethods, PagedVfs<sqlite_wasm_rs::WasmOsCallback>>(
        PAGED_VFS, pool, false,
    )
    .map_err(idb_error)?;
    let pool = unsafe { PagedStore::app_data(vfs) };
    PAGED.with(|p| p.set(Some(pool)));
    wasm_bindgen_futures::spawn_local(pool.commit_loop(rx));
    Ok(PagedUtil { pool })
}
