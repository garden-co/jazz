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
//! budget by SIEVE (FIFO with a visited bit). Dirty pages are never evicted before
//! their flush, and page 1 (the header SQLite rereads at every transaction)
//! never is.
//!
//! A hint stored next to the file (key `[path, -1]`) says which pages the
//! next session preloads: by default the SIEVE cache itself (its queue,
//! visited bits and hand), so a session resumes the last one's cache. It
//! is stored at most every few seconds, on the next flush or after an
//! operation, since a session may never see its end; a missing or
//! malformed hint only costs faults.
//!
//! The file layout is relaxed-idb's (database `jazz-idb`, store `blocks`,
//! key `[path, offset]`), so both VFSes open the same stored file.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
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
use wasm_bindgen::{JsCast, JsValue};

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
    /// Attaches that had to count the file's records (no valid hint).
    pub record_counts: u64,
    /// Highest resident page bytes seen.
    pub peak_resident: usize,
    /// Hints stored, and the last one's size.
    pub hint_persists: u64,
    pub hint_persist_bytes: usize,
    /// Of those, the ones that needed their own IndexedDB transaction (no
    /// flush to ride on), and the time those took.
    pub hint_solo_persists: u64,
    pub hint_solo_ms: f64,
    /// Time spent building hint records.
    pub hint_build_ms: f64,
}

struct Page {
    data: Uint8Array,
    /// SIEVE's bit: used since the hand last passed. The operation that
    /// loaded a page (and its retries) does not set it: only a later one.
    visited: Cell<bool>,
    /// The operation that loaded it (0: before the first, e.g. the hint).
    born: u64,
}

impl Page {
    fn new(data: Uint8Array, born: u64) -> Self {
        Self {
            data,
            visited: Cell::new(false),
            born,
        }
    }

    /// Whether it newly became visited.
    fn visit(&self, op: u64) -> bool {
        op != self.born && !self.visited.replace(true)
    }
}

/// Which pages the stored hint ranks first for the next session.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum HintPolicy {
    /// Most recently used first (a page address list, 8 bytes each).
    Mru,
    /// Aged LFU, 2 bits per page: each epoch `c = c / 2 + 2 * touched`
    /// (`c` counts in halves, so it reads `c/2 + touched`, 0..=3).
    LfuHalving,
    /// LFU, 2 bits per page: a touched page counts up (to 3), an untouched
    /// one counts down every fourth epoch.
    LfuSlow,
    /// The cache itself: SIEVE's queue with its visited bits and hand,
    /// trimmed by SIEVE to the hint's size (4 bytes and 1 bit per page).
    Sieve,
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
    /// SIEVE's queue (oldest first) and hand.
    fifo: VecDeque<usize>,
    hand: usize,
    /// MRU hint only: last use of every page used, seeded from the hint.
    recency: RefCell<HashMap<usize, u64>>,
    /// Pages read or written this session, one bit each.
    touched: RefCell<Vec<u64>>,
    /// LFU hint: pages read or written this epoch.
    epoch_touched: RefCell<Vec<u64>>,
    /// LFU hint: each page's counter as of the last epoch (0..=3).
    counters: Vec<u8>,
    /// The current operation (see [`Page::born`]).
    op: u64,
    /// Bumped by every change the stored hint would see.
    changes: Cell<u64>,
    /// `changes` as of the last stored hint.
    persisted: u64,
    /// Pages the hint preloaded (to measure its hit rate).
    hinted: Vec<u64>,
    mru: bool,
}

fn bit(bits: &[u64], n: usize) -> bool {
    bits.get(n / 64)
        .is_some_and(|word| word & (1 << (n % 64)) != 0)
}

fn set_bit(bits: &mut Vec<u64>, n: usize) {
    if bits.len() <= n / 64 {
        bits.resize(n / 64 + 1, 0);
    }
    bits[n / 64] |= 1 << (n % 64);
}

/// SIEVE's eviction on a stored queue (oldest first, with visited bits):
/// down to `keep` entries.
fn sieve_trim(fifo: &mut Vec<(usize, bool)>, hand: &mut usize, keep: usize) {
    while fifo.len() > keep {
        if *hand >= fifo.len() {
            *hand = 0;
        }
        if fifo[*hand].1 {
            fifo[*hand].1 = false;
            *hand += 1;
        } else {
            fifo.remove(*hand);
        }
    }
}

impl PageFile {
    fn touch(&self, addr: usize) {
        if self.mru {
            self.recency.borrow_mut().insert(addr, now());
            self.changed();
        }
        let page = addr / self.page_size.max(1);
        if !bit(&self.epoch_touched.borrow(), page) {
            set_bit(&mut self.epoch_touched.borrow_mut(), page);
            self.changed();
        }
        set_bit(&mut self.touched.borrow_mut(), page);
    }

    fn changed(&self) {
        self.changes.set(self.changes.get() + 1);
    }

    fn insert_page(&mut self, addr: usize, data: Uint8Array) {
        if self.pages.insert(addr, Page::new(data, self.op)).is_none() {
            self.fifo.push_back(addr);
            self.changed();
        }
    }

    /// SIEVE down to `budget`: the hand walks from the oldest page; a
    /// visited page loses its bit and stays, an unvisited one goes. Dirty
    /// pages and page 1 are never evicted.
    fn sieve(&mut self, budget: usize) -> u64 {
        let mut evicted = 0;
        let mut steps = 0;
        while self.resident() > budget && !self.fifo.is_empty() && steps <= 2 * self.fifo.len() {
            steps += 1;
            if self.hand >= self.fifo.len() {
                self.hand = 0;
            }
            let addr = self.fifo[self.hand];
            let keep = addr == 0
                || self.dirty.contains(&addr)
                || self
                    .pages
                    .get(&addr)
                    .is_some_and(|page| page.visited.replace(false));
            if keep {
                self.hand += 1;
            } else {
                self.fifo.remove(self.hand);
                self.pages.remove(&addr);
                evicted += 1;
            }
        }
        if evicted > 0 {
            self.changed();
        }
        evicted
    }
}

impl PageFile {
    fn resident(&self) -> usize {
        self.pages.len() * self.page_size
    }
}

enum PagedFile {
    Main(Box<PageFile>),
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
    policy: Cell<HintPolicy>,
    /// Epochs the hint has seen, this one included. A session's start is
    /// an epoch's start too.
    epochs: Cell<u64>,
    /// The hint is stored at most this often (ms), on the next flush or
    /// after an operation; it may never see a session end.
    hint_every: Cell<f64>,
    /// LFU ages its counters once per epoch (ms).
    epoch_ms: Cell<f64>,
    /// Pages a stored hint holds at most.
    hint_pages: Cell<usize>,
    last_persist: Cell<f64>,
    epoch_start: Cell<f64>,
    /// The current operation (see [`Page::born`]).
    op: Cell<u64>,
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

    /// Trim every file to the budget (SIEVE).
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
            evicted += file.sieve(budget);
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
                    if page.visit(self.op) {
                        self.changed();
                    }
                    self.touch(addr);
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
        self.page_size = page_size;
        for fill in (self.file_size..offset).step_by(page_size) {
            self.insert_page(fill, Uint8Array::new_with_length(page_size as u32));
            self.dirty.insert(fill);
        }
        match self.pages.get_mut(&offset) {
            Some(page) => {
                page.data.copy_from(buf);
                page.visit(self.op);
                self.changed();
            }
            None => self.insert_page(offset, Uint8Array::new_from_slice(buf)),
        }
        self.misses.borrow_mut().remove(&offset);
        self.touch(offset);
        self.dirty.insert(offset);
        self.file_size = self.file_size.max(offset + page_size);
        Ok(())
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        self.file_size = size;
        self.pages.retain(|addr, _| *addr < size);
        self.fifo.retain(|addr| *addr < size);
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

fn page_key(file: &str, offset: usize) -> [JsValue; 2] {
    [JsValue::from(file), JsValue::from(offset)]
}

/// The stored hint: the file's page count, the epochs so far, and the
/// policy's state (2-bit counters packed four to a byte, an MRU list, or
/// SIEVE's queue with its visited bits and hand). Whatever does not parse
/// is left out: a missing or malformed hint only costs faults.
#[derive(Default, Clone)]
struct Hint {
    pages: usize,
    epochs: u64,
    counters: Vec<u8>,
    mru: Vec<usize>,
    /// Page numbers, oldest first, with their visited bits.
    fifo: Vec<(usize, bool)>,
    hand: usize,
}

impl Hint {
    fn read(record: Option<JsValue>) -> Option<Self> {
        let record = record?;
        let field = |name: &str| Reflect::get(&record, &JsValue::from(name)).ok();
        let number = |name: &str| {
            field(name)
                .and_then(|v| v.as_f64())
                .filter(|n| n.is_finite() && *n >= 0.0)
                .unwrap_or(0.0)
        };
        let bytes = |name: &str| {
            field(name)
                .and_then(|v| v.dyn_into::<Uint8Array>().ok())
                .map(|v| v.to_vec())
        };
        let pages = number("pages") as usize;
        let counters = bytes("counters")
            .map(|packed| {
                (0..pages.min(packed.len() * 4))
                    .map(|page| (packed[page / 4] >> (2 * (page % 4))) & 3)
                    .collect()
            })
            .unwrap_or_default();
        let mru = field("mru")
            .and_then(|v| v.dyn_into::<js_sys::Float64Array>().ok())
            .map(|v| {
                v.to_vec()
                    .into_iter()
                    .filter(|addr| addr.is_finite() && *addr >= 0.0)
                    .map(|addr| addr as usize)
                    .collect()
            })
            .unwrap_or_default();
        let visited = bytes("visited").unwrap_or_default();
        let fifo = field("fifo")
            .and_then(|v| v.dyn_into::<js_sys::Uint32Array>().ok())
            .map(|v| {
                v.to_vec()
                    .into_iter()
                    .enumerate()
                    .map(|(at, page)| {
                        let seen = visited
                            .get(at / 8)
                            .is_some_and(|b| b & (1 << (at % 8)) != 0);
                        (page as usize, seen)
                    })
                    .collect()
            })
            .unwrap_or_default();
        Some(Self {
            pages,
            epochs: number("epochs") as u64,
            counters,
            mru,
            fifo,
            hand: number("hand") as usize,
        })
    }

    fn record(&self, path: &str) -> (JsValue, usize) {
        let record = Object::new();
        let set = |name: &str, value: &JsValue| {
            let _ = Reflect::set(&record, &JsValue::from(name), value);
        };
        set("path", &JsValue::from(path));
        set("offset", &JsValue::from(HINT_OFFSET));
        set("pages", &JsValue::from(self.pages as f64));
        set("epochs", &JsValue::from(self.epochs as f64));
        let bytes = if !self.mru.is_empty() {
            let mru: Vec<f64> = self.mru.iter().map(|addr| *addr as f64).collect();
            set("mru", &js_sys::Float64Array::from(&mru[..]).into());
            mru.len() * 8
        } else if !self.fifo.is_empty() {
            let pages: Vec<u32> = self.fifo.iter().map(|(page, _)| *page as u32).collect();
            let mut visited = vec![0u8; self.fifo.len().div_ceil(8)];
            for (at, (_, seen)) in self.fifo.iter().enumerate() {
                visited[at / 8] |= u8::from(*seen) << (at % 8);
            }
            set("fifo", &js_sys::Uint32Array::from(&pages[..]).into());
            set("visited", &Uint8Array::from(&visited[..]).into());
            set("hand", &JsValue::from(self.hand as f64));
            pages.len() * 4 + visited.len()
        } else {
            let mut packed = vec![0u8; self.counters.len().div_ceil(4)];
            for (page, c) in self.counters.iter().enumerate() {
                packed[page / 4] |= (c & 3) << (2 * (page % 4));
            }
            set("counters", &Uint8Array::from(&packed[..]).into());
            packed.len()
        };
        (record.into(), bytes)
    }
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
    /// Store the hint if it is still due (no flush took it along).
    Hint(String),
    Barrier(tokio::sync::oneshot::Sender<()>),
}

impl Paged {
    /// Whether `file`'s hint changed and the last one is old enough.
    fn hint_due(&self, file: &PageFile) -> bool {
        file.page_size > 0
            && file.changes.get() != file.persisted
            && js_sys::Date::now() - self.last_persist.get() >= self.hint_every.get()
    }

    /// The hint record to store now; ends the LFU epoch if it is over.
    fn take_hint(&self, path: &str, file: &mut PageFile) -> JsValue {
        let start = js_sys::Date::now();
        let hint = self.hint_for(file);
        let (record, bytes) = hint.record(path);
        let now = js_sys::Date::now();
        file.persisted = file.changes.get();
        self.last_persist.set(now);
        if now - self.epoch_start.get() >= self.epoch_ms.get() {
            file.counters = hint.counters;
            file.epoch_touched.borrow_mut().clear();
            self.epochs.set(self.epochs.get() + 1);
            self.epoch_start.set(now);
        }
        self.stat(|s| {
            s.hint_persists += 1;
            s.hint_persist_bytes = bytes;
            s.hint_build_ms += now - start;
        });
        record
    }

    /// `file`'s hint as of now. It is only a hint: a stale or wrong page
    /// loads a page that is not needed, nothing more.
    fn hint_for(&self, file: &PageFile) -> Hint {
        let epochs = self.epochs.get();
        let max_pages = self.hint_pages.get();
        let page_size = file.page_size.max(1);
        let pages = file.file_size / page_size;
        let touched = file.epoch_touched.borrow();
        let mut hint = Hint {
            pages,
            epochs,
            ..Hint::default()
        };
        match self.policy.get() {
            HintPolicy::Mru => {
                let mut used: Vec<(u64, usize)> = file
                    .recency
                    .borrow()
                    .iter()
                    .map(|(addr, at)| (*at, *addr))
                    .collect();
                used.sort_unstable_by(|a, b| b.cmp(a));
                hint.mru = used
                    .into_iter()
                    .take(max_pages)
                    .map(|(_, addr)| addr)
                    .collect();
            }
            HintPolicy::LfuHalving | HintPolicy::LfuSlow => {
                let mut counters = file.counters.clone();
                counters.resize(pages, 0);
                let halving = self.policy.get() == HintPolicy::LfuHalving;
                for (page, c) in counters.iter_mut().enumerate() {
                    let used = bit(&touched, page);
                    *c = if halving {
                        (*c >> 1) + 2 * u8::from(used)
                    } else if used {
                        (*c + 1).min(3)
                    } else if epochs.is_multiple_of(4) {
                        c.saturating_sub(1)
                    } else {
                        *c
                    };
                }
                hint.counters = counters;
            }
            HintPolicy::Sieve => {
                // Page 1 is always loaded; the hand keeps its place.
                let mut hand = 0;
                for (at, addr) in file.fifo.iter().enumerate() {
                    if *addr == 0 {
                        continue;
                    }
                    if at < file.hand {
                        hand += 1;
                    }
                    let visited = file.pages.get(addr).is_some_and(|page| page.visited.get());
                    hint.fifo.push((addr / page_size, visited));
                }
                sieve_trim(&mut hint.fifo, &mut hand, max_pages);
                hint.hand = hand;
            }
        }
        hint
    }

    /// Store `path`'s hint in its own transaction: when `force`, or when
    /// it is due and no flush took it along.
    async fn persist_hint(&self, path: &str, force: bool) -> Result<usize, JsValue> {
        let start = js_sys::Date::now();
        let record = {
            let mut files = self.files.borrow_mut();
            let Some(PagedFile::Main(file)) = files.get_mut(path) else {
                return Ok(0);
            };
            if file.page_size == 0 || !(force || self.hint_due(file)) {
                return Ok(0);
            }
            self.take_hint(path, file)
        };
        let transaction = self
            .idb
            .transaction("blocks")
            .with_mode(TransactionMode::Readwrite)
            .build()
            .map_err(idb_error)?;
        let store = transaction.object_store("blocks").map_err(idb_error)?;
        store.put(&record).build().map_err(idb_error)?;
        transaction.commit().await.map_err(idb_error)?;
        self.stat(|s| {
            s.idb_txs += 1;
            s.hint_solo_persists += 1;
            s.hint_solo_ms += js_sys::Date::now() - start;
        });
        Ok(self.stats.get().hint_persist_bytes)
    }

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
        if self.hint_due(file) {
            let record = self.take_hint(path, file);
            store.put(&record).build().map_err(idb_error)?;
        }
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
                Commit::Hint(path) => {
                    if let Err(error) = self.persist_hint(&path, false).await {
                        web_sys_log(&format!("paged IndexedDB hint failed: {error:?}"));
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
            PagedFile::Main(Box::default())
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
    /// `hot_bytes` of pages the stored hint ranks first.
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
        let first = store
            .get_all::<JsValue>()
            .with_query(key_range(path, 0, 0.0))
            .await
            .map_err(idb_error)?;
        let mut file = PageFile {
            mru: self.pool.policy.get() == HintPolicy::Mru,
            ..PageFile::default()
        };
        for block in first {
            let (offset, data) = block_data(&block.map_err(idb_error)?);
            file.page_size = data.length() as usize;
            file.insert_page(offset, data);
        }
        if file.page_size == 0 {
            self.pool.stat(|s| s.idb_txs += 1);
            return Ok(());
        }
        let hint = Hint::read(
            store
                .get::<JsValue, _, _>(hint_key(path))
                .await
                .map_err(idb_error)?,
        );
        // The hint's page count holds if its last page is there and the
        // one after is not; otherwise count every record.
        let mut count = None;
        if let Some(pages) = hint.as_ref().map(|hint| hint.pages).filter(|n| *n > 0) {
            let last = store
                .get::<JsValue, _, _>(page_key(path, (pages - 1) * file.page_size))
                .await
                .map_err(idb_error)?;
            let next = store
                .get::<JsValue, _, _>(page_key(path, pages * file.page_size))
                .await
                .map_err(idb_error)?;
            if last.is_some() && next.is_none() {
                count = Some(pages);
            }
        }
        let count = match count {
            Some(count) => count,
            None => {
                self.pool.stat(|s| s.record_counts += 1);
                store
                    .count()
                    .with_query(key_range(path, 0, Number::POSITIVE_INFINITY))
                    .await
                    .map_err(idb_error)? as usize
            }
        };
        self.pool.stat(|s| s.idb_txs += 1);
        file.file_size = count * file.page_size;
        let hint = hint.unwrap_or_default();
        file.counters = hint.counters.clone();
        file.counters.resize(count, 0);
        // The MRU list is most recent first; its order carries over as the
        // oldest uses of this session.
        let base = self.pool.clock.get();
        for (rank, addr) in hint.mru.iter().enumerate() {
            file.recency
                .borrow_mut()
                .insert(*addr, base + (hint.mru.len() - rank) as u64);
        }
        self.pool.clock.set(base + hint.mru.len() as u64 + 1);
        // SIEVE: the stored queue, trimmed by SIEVE to `hot_bytes`.
        let mut queue = hint.fifo.clone();
        let mut hand = hint.hand;
        sieve_trim(&mut queue, &mut hand, hot_bytes / file.page_size);
        let ranked: Vec<usize> = match self.pool.policy.get() {
            HintPolicy::Mru => hint.mru.clone(),
            HintPolicy::Sieve => queue
                .iter()
                .map(|(page, _)| page * file.page_size)
                .collect(),
            HintPolicy::LfuHalving | HintPolicy::LfuSlow => {
                let mut ranked: Vec<(u8, usize)> = file
                    .counters
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| **c > 0)
                    .map(|(page, c)| (*c, page * file.page_size))
                    .collect();
                ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
                ranked.into_iter().map(|(_, addr)| addr).collect()
            }
        };
        let mut hot: Vec<usize> = ranked
            .into_iter()
            .filter(|addr| *addr < file.file_size && !file.pages.contains_key(addr))
            .take(hot_bytes / file.page_size)
            .collect();
        hot.sort_unstable();
        for addr in &hot {
            set_bit(&mut file.hinted, addr / file.page_size);
        }
        self.pool
            .files
            .borrow_mut()
            .insert(path.to_owned(), PagedFile::Main(Box::new(file)));
        self.pool.epochs.set(hint.epochs + 1);
        let now = js_sys::Date::now();
        self.pool.epoch_start.set(now);
        self.pool.last_persist.set(now);
        let loaded = self.fetch(path, hot).await?;
        if self.pool.policy.get() == HintPolicy::Sieve {
            // The cache resumes where the stored one stopped: its queue
            // order, visited bits and hand.
            let mut files = self.pool.files.borrow_mut();
            if let Some(PagedFile::Main(file)) = files.get_mut(path) {
                let mut fifo = VecDeque::from([0]);
                let mut resumed_hand = 1;
                for (at, (page, visited)) in queue.iter().enumerate() {
                    let addr = page * file.page_size;
                    if let Some(loaded) = file.pages.get(&addr).filter(|_| addr != 0) {
                        loaded.visited.set(*visited);
                        if at < hand {
                            resumed_hand += 1;
                        }
                        fifo.push_back(addr);
                    }
                }
                let rest: Vec<usize> = file
                    .fifo
                    .iter()
                    .copied()
                    .filter(|addr| !fifo.contains(addr))
                    .collect();
                fifo.extend(rest);
                file.fifo = fifo;
                file.hand = resumed_hand;
            }
        }
        self.pool.stat(|s| s.hot_pages += loaded as u64);
        self.pool.note_resident();
        Ok(())
    }

    /// Of the pages the hint preloaded, how many this session used; and
    /// how many pages it used in all.
    pub(crate) fn hint_use(&self) -> (usize, usize, usize) {
        let files = self.pool.files.borrow();
        let (mut hinted, mut used, mut touched) = (0, 0, 0);
        for file in files.values() {
            if let PagedFile::Main(file) = file {
                let touched_bits = file.touched.borrow();
                for (word, hint) in file.hinted.iter().enumerate() {
                    hinted += hint.count_ones() as usize;
                    used +=
                        (hint & touched_bits.get(word).copied().unwrap_or(0)).count_ones() as usize;
                }
                touched += touched_bits
                    .iter()
                    .map(|w| w.count_ones() as usize)
                    .sum::<usize>();
            }
        }
        (hinted, used, touched)
    }

    /// Store the hint now (a session end, or `pagehide`): best effort,
    /// since a session may also just stop. Returns the record's hint bytes.
    pub(crate) async fn save_hint(&self, path: &str) -> Result<usize, JsValue> {
        self.pool.persist_hint(path, true).await
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
                if !file.pages.contains_key(&offset) {
                    loaded += 1;
                    file.insert_page(offset, data);
                }
            }
        }
        Ok(loaded)
    }

    /// Pin everything touched until [`Self::end_op`].
    pub(crate) fn begin_op(&self) {
        self.pool.in_op.set(true);
        let op = self.pool.op.get() + 1;
        self.pool.op.set(op);
        for file in self.pool.files.borrow_mut().values_mut() {
            if let PagedFile::Main(file) = file {
                file.op = op;
            }
        }
    }

    /// Unpin, trim, and queue the hint if it is due.
    pub(crate) fn end_op(&self) {
        self.pool.in_op.set(false);
        self.pool.evict();
        let due: Vec<String> = self
            .pool
            .files
            .borrow()
            .iter()
            .filter(|(_, file)| matches!(file, PagedFile::Main(file) if self.pool.hint_due(file)))
            .map(|(path, _)| path.clone())
            .collect();
        for path in due {
            let _ = self.pool.tx.send(Commit::Hint(path));
        }
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

    pub(crate) fn configure(&self, budget: usize, readahead: usize, policy: HintPolicy) {
        self.pool.budget.set(budget);
        self.pool.readahead.set(readahead);
        self.pool.policy.set(policy);
    }

    /// How often the hint is stored, how long an LFU epoch lasts (both
    /// ms), and how many pages a stored hint holds.
    pub(crate) fn configure_hint(&self, every_ms: f64, epoch_ms: f64, max_pages: usize) {
        self.pool.hint_every.set(every_ms);
        self.pool.epoch_ms.set(epoch_ms);
        self.pool.hint_pages.set(max_pages);
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
        policy: Cell::new(HintPolicy::Sieve),
        epochs: Cell::new(0),
        hint_every: Cell::new(5000.0),
        epoch_ms: Cell::new(3_600_000.0),
        hint_pages: Cell::new(4096),
        last_persist: Cell::new(0.0),
        epoch_start: Cell::new(0.0),
        op: Cell::new(0),
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
