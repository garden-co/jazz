//! A VFS that pages like the browser's IndexedDB one, for running the
//! engine's own suites with page faults (`testing` feature).
//!
//! Files live on the platform's default VFS. What this one adds is which
//! of their bytes count as loaded: a read inside an open operation that
//! touches a block not loaded records it and fails with
//! [`crate::pages::PAGE_FAULT`], and [`FaultSource::load`] marks every
//! recorded block loaded. Each operation's start evicts a random share of
//! the loaded blocks (all, most, or some), so operations meet faults in
//! B-tree interiors, leaves, overflow chains and the large-value reader
//! alike. Reads outside an operation never fault: those entry points do not
//! wait, as in the browser, where they are not used on a paged store.
//!
//! With `JAZZ_PAGE_FAULTS=<seed>` set, [`crate::db::Db::open`] opens a
//! temporary file on this VFS instead of an in-memory store, so any suite
//! runs with faults.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, c_char, c_int, c_void};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Once;

use rusqlite::ffi;

use crate::options::Error;
use crate::pages::{PAGE_FAULT, PageSource};

/// The VFS's registered name.
pub const FAULT_VFS: &str = "jazz-page-faults";
const BLOCK: i64 = 4096;

#[derive(Default)]
struct State {
    /// Operations open (they do not nest; a nested begin is counted).
    depth: u32,
    /// By file: the blocks loaded (ordered, so a seed evicts the same
    /// blocks every run).
    loaded: BTreeMap<String, BTreeSet<i64>>,
    /// Blocks a read faulted on since the last load.
    pending: BTreeSet<(String, i64)>,
    rng: u64,
    faults: u64,
    loads: u64,
    never_evict: bool,
}

thread_local! {
    // Stores are thread-affine, and so are their reads.
    static STATE: RefCell<State> = RefCell::new(State { rng: 0x9e37_79b9_7f4a_7c15, ..State::default() });
    static SEEDED: Cell<bool> = const { Cell::new(false) };
}

impl State {
    fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn evict(&mut self) {
        // Seed 0: the same files, never evicted (nothing faults).
        if self.never_evict {
            return;
        }
        // Keep none, a third or most of what is loaded.
        let keep = [0u64, 33, 80][(self.next() % 3) as usize];
        let mut loaded = std::mem::take(&mut self.loaded);
        for blocks in loaded.values_mut() {
            blocks.retain(|&block| block == 0 || self.next() % 100 < keep);
        }
        self.loaded = loaded;
    }
}

/// The engine's side of this VFS: operation bounds and loads.
pub struct FaultSource;

impl FaultSource {
    /// Faults so far on this thread.
    pub fn faults() -> u64 {
        STATE.with(|s| s.borrow().faults)
    }

    /// Loads so far on this thread.
    pub fn loads() -> u64 {
        STATE.with(|s| s.borrow().loads)
    }

    /// Seed this thread's evictions.
    pub fn seed(seed: u64) {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            s.rng = seed.max(1);
            s.never_evict = seed == 0;
        });
        SEEDED.with(|seeded| seeded.set(true));
    }
}

impl PageSource for FaultSource {
    fn begin_op(&self) {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            if s.depth == 0 {
                s.evict();
            }
            s.depth += 1;
        });
    }

    fn end_op(&self) {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            s.depth = s.depth.saturating_sub(1);
        });
    }

    fn load(&self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + '_>> {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let pending = std::mem::take(&mut s.pending);
            s.loads += 1;
            for (file, block) in pending {
                s.loaded.entry(file).or_default().insert(block);
            }
        });
        Box::pin(std::future::ready(Ok(())))
    }
}

/// `JAZZ_PAGE_FAULTS`, when set: the seed for this thread's evictions.
pub(crate) fn from_env() -> Option<u64> {
    let seed = std::env::var("JAZZ_PAGE_FAULTS").ok()?;
    let seed = seed.parse().unwrap_or(1);
    if !SEEDED.with(Cell::get) {
        FaultSource::seed(seed);
    }
    Some(seed)
}

/// Mark a block range loaded (a write, or a read past the end).
fn mark_loaded(file: &str, offset: i64, amount: i64) {
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        let blocks = s.loaded.entry(file.to_owned()).or_default();
        for block in offset / BLOCK..=(offset + amount - 1) / BLOCK {
            blocks.insert(block);
        }
    });
}

/// Whether a read of `amount` bytes at `offset` faults; records what it
/// misses if so.
fn faults(file: &str, offset: i64, amount: i64) -> bool {
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        if s.depth == 0 {
            return false;
        }
        let missing: Vec<i64> = (offset / BLOCK..=(offset + amount - 1) / BLOCK)
            .filter(|block| {
                !s.loaded
                    .get(file)
                    .is_some_and(|blocks| blocks.contains(block))
            })
            .collect();
        if missing.is_empty() {
            return false;
        }
        s.faults += 1;
        for block in missing {
            s.pending.insert((file.to_owned(), block));
        }
        true
    })
}

/// Forget a file's blocks (it was deleted or truncated to nothing).
fn forget(file: &str) {
    STATE.with(|s| {
        s.borrow_mut().loaded.remove(file);
    });
}

// ---- the VFS ---------------------------------------------------------------

#[repr(C)]
struct File {
    base: ffi::sqlite3_file,
    /// Main database files page; journals and temp files do not.
    paged: bool,
    name: *mut String,
    /// The default VFS's file, allocated right after this struct.
    real: *mut ffi::sqlite3_file,
}

static mut VFS: Option<ffi::sqlite3_vfs> = None;
static mut METHODS_V1: Option<ffi::sqlite3_io_methods> = None;
static mut METHODS_V3: Option<ffi::sqlite3_io_methods> = None;
static REGISTER: Once = Once::new();

fn default_vfs() -> *mut ffi::sqlite3_vfs {
    // SAFETY: a lookup; the default VFS lives as long as the process.
    unsafe { ffi::sqlite3_vfs_find(std::ptr::null()) }
}

unsafe fn file<'a>(f: *mut ffi::sqlite3_file) -> &'a mut File {
    // SAFETY: SQLite hands back the struct `x_open` initialised.
    unsafe { &mut *(f as *mut File) }
}

unsafe fn real_methods(f: &File) -> &ffi::sqlite3_io_methods {
    // SAFETY: the real file was opened, so its methods are set.
    unsafe { &*(*f.real).pMethods }
}

unsafe extern "C" fn x_close(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let file = file(f);
        let rc = (real_methods(file).xClose.unwrap())(file.real);
        drop(Box::from_raw(file.name));
        rc
    }
}

unsafe extern "C" fn x_read(
    f: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        let file = file(f);
        if file.paged && amount > 0 {
            let mut size: ffi::sqlite3_int64 = 0;
            (real_methods(file).xFileSize.unwrap())(file.real, &mut size);
            // A read past the end is SQLite looking for a page not yet
            // written: a paging VFS knows the file's length without
            // loading anything.
            if offset < size && faults(&*file.name, offset, amount as i64) {
                return PAGE_FAULT;
            }
        }
        (real_methods(file).xRead.unwrap())(file.real, buf, amount, offset)
    }
}

unsafe extern "C" fn x_write(
    f: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        let file = file(f);
        if file.paged && amount > 0 {
            mark_loaded(&*file.name, offset, amount as i64);
        }
        (real_methods(file).xWrite.unwrap())(file.real, buf, amount, offset)
    }
}

macro_rules! forward {
    ($name:ident, $method:ident, ($($arg:ident: $ty:ty),*) -> $ret:ty, $default:expr) => {
        unsafe extern "C" fn $name(f: *mut ffi::sqlite3_file $(, $arg: $ty)*) -> $ret {
            unsafe {
                let file = file(f);
                match real_methods(file).$method {
                    Some(method) => method(file.real $(, $arg)*),
                    None => $default,
                }
            }
        }
    };
}

forward!(x_truncate, xTruncate, (size: ffi::sqlite3_int64) -> c_int, ffi::SQLITE_OK);
forward!(x_sync, xSync, (flags: c_int) -> c_int, ffi::SQLITE_OK);
forward!(x_file_size, xFileSize, (size: *mut ffi::sqlite3_int64) -> c_int, ffi::SQLITE_OK);
forward!(x_lock, xLock, (level: c_int) -> c_int, ffi::SQLITE_OK);
forward!(x_unlock, xUnlock, (level: c_int) -> c_int, ffi::SQLITE_OK);
forward!(x_check_reserved_lock, xCheckReservedLock, (out: *mut c_int) -> c_int, ffi::SQLITE_OK);
forward!(x_file_control, xFileControl, (op: c_int, arg: *mut c_void) -> c_int, ffi::SQLITE_NOTFOUND);
forward!(x_sector_size, xSectorSize, () -> c_int, 4096);
forward!(x_device_characteristics, xDeviceCharacteristics, () -> c_int, 0);
forward!(x_shm_map, xShmMap, (page: c_int, size: c_int, extend: c_int, out: *mut *mut c_void) -> c_int, ffi::SQLITE_IOERR);
forward!(x_shm_lock, xShmLock, (offset: c_int, n: c_int, flags: c_int) -> c_int, ffi::SQLITE_IOERR);
forward!(x_shm_unmap, xShmUnmap, (delete: c_int) -> c_int, ffi::SQLITE_OK);
forward!(x_fetch, xFetch, (offset: ffi::sqlite3_int64, amount: c_int, out: *mut *mut c_void) -> c_int, {
    *out = std::ptr::null_mut();
    ffi::SQLITE_OK
});
forward!(x_unfetch, xUnfetch, (offset: ffi::sqlite3_int64, p: *mut c_void) -> c_int, ffi::SQLITE_OK);

unsafe extern "C" fn x_shm_barrier(f: *mut ffi::sqlite3_file) {
    unsafe {
        let file = file(f);
        if let Some(method) = real_methods(file).xShmBarrier {
            method(file.real);
        }
    }
}

fn methods(version: c_int) -> ffi::sqlite3_io_methods {
    ffi::sqlite3_io_methods {
        iVersion: version,
        xClose: Some(x_close),
        xRead: Some(x_read),
        xWrite: Some(x_write),
        xTruncate: Some(x_truncate),
        xSync: Some(x_sync),
        xFileSize: Some(x_file_size),
        xLock: Some(x_lock),
        xUnlock: Some(x_unlock),
        xCheckReservedLock: Some(x_check_reserved_lock),
        xFileControl: Some(x_file_control),
        xSectorSize: Some(x_sector_size),
        xDeviceCharacteristics: Some(x_device_characteristics),
        xShmMap: Some(x_shm_map),
        xShmLock: Some(x_shm_lock),
        xShmBarrier: Some(x_shm_barrier),
        xShmUnmap: Some(x_shm_unmap),
        // Memory-mapped reads would bypass `x_read`.
        xFetch: if version >= 3 { Some(x_fetch) } else { None },
        xUnfetch: if version >= 3 { Some(x_unfetch) } else { None },
    }
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    f: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    unsafe {
        let real_vfs = &*((*vfs).pAppData as *mut ffi::sqlite3_vfs);
        let file = f as *mut File;
        let real = (f as *mut u8).add(std::mem::size_of::<File>()) as *mut ffi::sqlite3_file;
        (*file).base.pMethods = std::ptr::null();
        let rc =
            (real_vfs.xOpen.unwrap())(real_vfs as *const _ as *mut _, name, real, flags, out_flags);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        let text = if name.is_null() {
            String::new()
        } else {
            CStr::from_ptr(name).to_string_lossy().into_owned()
        };
        (*file).paged = flags & ffi::SQLITE_OPEN_MAIN_DB != 0 && !name.is_null();
        (*file).name = Box::into_raw(Box::new(text));
        (*file).real = real;
        let version = (*(*real).pMethods).iVersion;
        // A file the open made new starts with nothing to load.
        let methods = if version >= 3 {
            (*std::ptr::addr_of!(METHODS_V3)).as_ref().unwrap()
        } else {
            (*std::ptr::addr_of!(METHODS_V1)).as_ref().unwrap()
        };
        (*file).base.pMethods = methods;
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    unsafe {
        let real_vfs = (*vfs).pAppData as *mut ffi::sqlite3_vfs;
        if !name.is_null() {
            forget(&CStr::from_ptr(name).to_string_lossy());
        }
        ((*real_vfs).xDelete.unwrap())(real_vfs, name, sync_dir)
    }
}

macro_rules! forward_vfs {
    ($name:ident, $method:ident, ($($arg:ident: $ty:ty),*) -> $ret:ty) => {
        unsafe extern "C" fn $name(vfs: *mut ffi::sqlite3_vfs $(, $arg: $ty)*) -> $ret {
            unsafe {
                let real_vfs = (*vfs).pAppData as *mut ffi::sqlite3_vfs;
                ((*real_vfs).$method.unwrap())(real_vfs $(, $arg)*)
            }
        }
    };
}

forward_vfs!(x_access, xAccess, (name: *const c_char, flags: c_int, out: *mut c_int) -> c_int);
forward_vfs!(x_full_pathname, xFullPathname, (name: *const c_char, n: c_int, out: *mut c_char) -> c_int);
forward_vfs!(x_randomness, xRandomness, (n: c_int, out: *mut c_char) -> c_int);
forward_vfs!(x_sleep, xSleep, (micros: c_int) -> c_int);
forward_vfs!(x_current_time, xCurrentTime, (out: *mut f64) -> c_int);
forward_vfs!(x_get_last_error, xGetLastError, (n: c_int, out: *mut c_char) -> c_int);
forward_vfs!(x_current_time_int64, xCurrentTimeInt64, (out: *mut ffi::sqlite3_int64) -> c_int);

/// Register the VFS (once per process).
pub fn register() {
    REGISTER.call_once(|| unsafe {
        let real = default_vfs();
        assert!(!real.is_null(), "SQLite has a default VFS");
        *std::ptr::addr_of_mut!(METHODS_V1) = Some(methods(1));
        *std::ptr::addr_of_mut!(METHODS_V3) = Some(methods(3));
        let name = Box::leak(format!("{FAULT_VFS}\0").into_boxed_str());
        *std::ptr::addr_of_mut!(VFS) = Some(ffi::sqlite3_vfs {
            iVersion: 2,
            szOsFile: (std::mem::size_of::<File>() as c_int) + (*real).szOsFile,
            mxPathname: (*real).mxPathname,
            pNext: std::ptr::null_mut(),
            zName: name.as_ptr() as *const c_char,
            pAppData: real as *mut c_void,
            xOpen: Some(x_open),
            xDelete: Some(x_delete),
            xAccess: Some(x_access),
            xFullPathname: Some(x_full_pathname),
            xDlOpen: None,
            xDlError: None,
            xDlSym: None,
            xDlClose: None,
            xRandomness: Some(x_randomness),
            xSleep: Some(x_sleep),
            xCurrentTime: Some(x_current_time),
            xGetLastError: Some(x_get_last_error),
            xCurrentTimeInt64: Some(x_current_time_int64),
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        });
        let vfs = (*std::ptr::addr_of_mut!(VFS)).as_mut().unwrap() as *mut ffi::sqlite3_vfs;
        let rc = ffi::sqlite3_vfs_register(vfs, 0);
        assert_eq!(rc, ffi::SQLITE_OK, "register {FAULT_VFS}");
    });
}

/// The page source for stores on this VFS.
pub fn source() -> Rc<dyn PageSource> {
    Rc::new(FaultSource)
}
