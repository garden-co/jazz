//! A SQLite VFS that keeps the store file's pages in a Cloudflare Durable
//! Object's synchronous KV storage (`ctx.storage.kv`, SQLite-backed objects
//! only).
//!
//! # Never faults
//!
//! Every read the VFS serves is answered synchronously from the object's
//! storage, so SQLite never waits on I/O it cannot see, and nothing is
//! loaded up front: opening reads only the file's metadata key, and every
//! other page is read when SQLite asks for it. There is no page-fault,
//! retry or preload machinery (unlike the IndexedDB VFS, whose reads are
//! asynchronous), and none must be added: a read either returns the page or
//! fails with an I/O error.
//!
//! # Layout
//!
//! One KV key per SQLite page, plus one metadata key per file, under the
//! file's `name` in the object's storage (see [`KvVfsUtil::attach`]):
//!
//! - `jazz-vfs:{name}:meta`: 16 bytes, little-endian: format version (u32,
//!   currently 1), page size (u32), file size in bytes (u64). Written only
//!   by commits that change the size.
//! - `jazz-vfs:{name}:p:{index}`: page `index` (zero-based, 8 lowercase hex
//!   digits so KV's sorted order is page order), exactly one page of bytes.
//!
//! Page size is the caller's choice (the file keeps the size it was created
//! with); Jazz uses 4 KiB ([`PAGE_SIZE`]). Durable Objects bill KV
//! operations as SQLite rows read and written, one row per key, not by bytes
//! (values may be up to 8 MB), so larger pages might seem to save rows. They
//! barely do: a Jazz commit writes about 4 pages at any page size (the
//! touched leaves and page 1), measured in local workerd for 5000
//! single-row commits as 4.25 rows per commit at 4 KiB against 4.0 at
//! 64 KiB. Each of those pages is copied into storage whole, though, so
//! 64 KiB pages commit 3x slower (2,300 against 7,000 rows/s) and store 5x
//! the bytes (every table and index takes at least one page).
//!
//! # Atomic commits
//!
//! Jazz opens the file with an in-memory journal (`Journal::Memory`) and no
//! cache spill, so SQLite writes the main file only while committing. The
//! VFS holds those writes in memory, and on `SQLITE_FCNTL_COMMIT_PHASETWO`
//! (sent once per commit, before SQLite unlocks) it writes every changed page,
//! the deletes of pages a truncation dropped, and the new metadata in one
//! `ctx.storage.transactionSync` call. That call commits all of them or none
//! (if it throws it rolls back), so storage always holds one whole committed
//! state of the file. So no journal has to survive a crash, and journal
//! files (of any rollback `journal_mode`, which SQLite uses before Jazz
//! switches to `MEMORY`) stay in memory: a durable one would only double
//! the rows written for atomicity the storage already gives. `MEMORY` beats
//! `Journal::Rollback` only by skipping that in-memory copy.
//!
//! A commit whose write fails returns `SQLITE_IOERR` from the commit, and
//! the VFS drops the commit's pages: storage still holds the commit before.
//! The engine above may already have moved on (its sequence numbers, caches
//! and subscribers saw the commit before SQLite's last step), so the file
//! fails too: every later read, write and commit on it returns `SQLITE_IOERR`
//! naming the failed commit, until it is attached again, and reopening it
//! from storage gives exactly the last whole commit. A Durable Object host
//! resets the object when that happens (`jazz-wasm`'s `do_kv` calls
//! `ctx.abort()`), as the runtime itself does when a storage write fails.
//!
//! Durability needs no flush: the Durable Object runtime holds back the
//! object's outgoing messages until its storage writes are on disk (its
//! output gate), so nothing acknowledged before a commit was written can
//! leave the object.
//!
//! # Memory
//!
//! A commit's pages are held three times while it is written: in SQLite's
//! page cache, in the VFS's dirty pages, and as the byte arrays handed to
//! `transactionSync`. A commit that changes N bytes of pages needs about 3N
//! bytes for that moment, against an isolate's 128 MB, so one commit
//! (a large value's chunks included: inserting one writes them all in its
//! commit) must stay well below ~40 MB. Not enforced yet. The page cache is
//! bounded by `FileOptions::cache_kib` per connection.
//!
//! # Not supported yet
//!
//! - WAL: the VFS has no shared memory, so SQLite keeps a rollback journal
//!   (Jazz's open fails with its "journal_mode WAL is not supported" error),
//!   and a WAL file is refused.
//! - One file opened by two objects: a file is bound to the storage of the
//!   one object that [`attach`]ed it. Paths are per object.
//!
//! Journals and temporary files (temp databases, statement journals) stay in
//! memory.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use rsqlite_vfs::{
    MemChunksFile, OsCallback, RegisterVfsError, SQLiteIoMethods, SQLiteVfs, SQLiteVfsFile,
    VfsAppData, VfsError, VfsFile, VfsResult, VfsStore,
    ffi::{
        SQLITE_CANTOPEN, SQLITE_FCNTL_COMMIT_PHASETWO, SQLITE_IOERR, SQLITE_IOERR_DELETE,
        SQLITE_IOERR_READ, SQLITE_IOERR_WRITE, SQLITE_NOTFOUND, SQLITE_OK, SQLITE_OPEN_MAIN_DB,
        SQLITE_OPEN_WAL, sqlite3_file, sqlite3_vfs,
    },
    register_vfs, registered_vfs,
};

/// The page size Jazz creates store files with (see the module docs).
pub const PAGE_SIZE: u32 = 4096;

const META_VERSION: u32 = 1;

/// A Durable Object's synchronous storage, as the VFS uses it. The
/// wasm build implements it on `ctx.storage`; tests use a double.
pub trait PageKv {
    /// The value at `key`, or `None`.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    /// Write all of `puts` and delete all of `deletes` atomically: all of
    /// them or, on an error, none.
    fn commit(&self, puts: &[(String, &[u8])], deletes: &[String]) -> Result<(), String>;
}

/// How many storage operations the file has made since it was attached.
/// Durable Objects bill each as one row read (`gets`) or written (`puts`
/// and `deletes`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KvStats {
    pub gets: u64,
    pub puts: u64,
    pub deletes: u64,
    pub commits: u64,
}

fn meta_key(path: &str) -> String {
    format!("jazz-vfs:{path}:meta")
}

fn page_key(path: &str, index: usize) -> String {
    format!("jazz-vfs:{path}:p:{index:08x}")
}

fn io_error(code: i32, message: String) -> VfsError {
    VfsError::new(code, format!("Durable Object storage: {message}"))
}

/// A main database file, its pages in storage.
struct KvPageFile {
    kv: Rc<dyn PageKv>,
    /// The file's path (VFS name), for errors.
    path: String,
    /// The file's name in storage, which its keys start with.
    name: String,
    /// 0 until the file's first page is written.
    page_size: usize,
    /// Size as SQLite sees it, this commit's writes included.
    size: usize,
    /// Size storage holds.
    stored_size: usize,
    /// Pages written since the last commit, by index.
    dirty: BTreeMap<usize, Vec<u8>>,
    stats: Cell<KvStats>,
    /// Connections open on the file.
    opens: u32,
    /// Unbound when its last connection closes (see
    /// [`KvVfsUtil::attach_ephemeral`]).
    ephemeral: bool,
    /// Why a commit could not be written: every later operation on the
    /// file fails until it is attached again (see the module docs).
    failed: Option<String>,
}

impl KvPageFile {
    fn load(kv: Rc<dyn PageKv>, path: &str, name: &str) -> Result<Self, String> {
        let mut stats = KvStats::default();
        stats.gets += 1;
        let (page_size, size) = match kv.get(&meta_key(name))? {
            None => (0, 0),
            Some(meta) => {
                let meta: [u8; 16] = meta
                    .try_into()
                    .map_err(|meta: Vec<u8>| format!("{path}: metadata of {} bytes", meta.len()))?;
                let version = u32::from_le_bytes(meta[0..4].try_into().unwrap());
                if version != META_VERSION {
                    return Err(format!(
                        "{path}: metadata version {version} is not supported yet"
                    ));
                }
                let page_size = u32::from_le_bytes(meta[4..8].try_into().unwrap()) as usize;
                let size = u64::from_le_bytes(meta[8..16].try_into().unwrap()) as usize;
                if page_size == 0 || !size.is_multiple_of(page_size) {
                    return Err(format!(
                        "{path}: metadata holds size {size} for page size {page_size}"
                    ));
                }
                (page_size, size)
            }
        };
        Ok(Self {
            kv,
            path: path.to_owned(),
            name: name.to_owned(),
            page_size,
            size,
            stored_size: size,
            dirty: BTreeMap::new(),
            stats: Cell::new(stats),
            opens: 0,
            ephemeral: false,
            failed: None,
        })
    }

    fn count(&self, f: impl FnOnce(&mut KvStats)) {
        let mut stats = self.stats.get();
        f(&mut stats);
        self.stats.set(stats);
    }

    fn pages(&self, size: usize) -> usize {
        if self.page_size == 0 {
            0
        } else {
            size.div_ceil(self.page_size)
        }
    }

    /// Fail if a commit of the file could not be written.
    fn check(&self) -> VfsResult<()> {
        match &self.failed {
            None => Ok(()),
            Some(error) => Err(io_error(
                SQLITE_IOERR,
                format!(
                    "{}: a commit could not be written ({error}); the store must be \
                     reopened from storage",
                    self.path
                ),
            )),
        }
    }

    /// Write the commit to storage in one atomic call. On failure, drop it
    /// and fail the file (see the module docs).
    fn commit(&mut self) -> VfsResult<()> {
        self.check()?;
        if self.dirty.is_empty() && self.size == self.stored_size {
            return Ok(());
        }
        let pages = self.pages(self.size);
        let keys: Vec<(String, usize)> = self
            .dirty
            .keys()
            .filter(|&&index| index < pages)
            .map(|&index| (page_key(&self.name, index), index))
            .collect();
        let mut meta = [0u8; 16];
        meta[0..4].copy_from_slice(&META_VERSION.to_le_bytes());
        meta[4..8].copy_from_slice(&(self.page_size as u32).to_le_bytes());
        meta[8..16].copy_from_slice(&(self.size as u64).to_le_bytes());
        let mut puts: Vec<(String, &[u8])> = keys
            .iter()
            .map(|(key, index)| (key.clone(), self.dirty[index].as_slice()))
            .collect();
        // The metadata changes only with the size (a row written saved on
        // most commits).
        if self.size != self.stored_size {
            puts.push((meta_key(&self.name), &meta));
        }
        let deletes: Vec<String> = (pages..self.pages(self.stored_size))
            .map(|index| page_key(&self.name, index))
            .collect();
        let result = self.kv.commit(&puts, &deletes);
        let (put_count, delete_count) = (puts.len() as u64, deletes.len() as u64);
        drop(puts);
        match result {
            Ok(()) => {
                self.count(|stats| {
                    stats.puts += put_count;
                    stats.deletes += delete_count;
                    stats.commits += 1;
                });
                self.dirty.clear();
                self.stored_size = self.size;
                Ok(())
            }
            Err(error) => {
                self.dirty.clear();
                self.size = self.stored_size;
                if self.stored_size == 0 {
                    // A new file's page size is set by its first commit.
                    self.page_size = 0;
                }
                self.failed = Some(error.clone());
                Err(io_error(
                    SQLITE_IOERR_WRITE,
                    format!("could not write a commit of {}: {error}", self.path),
                ))
            }
        }
    }

    fn delete_all(&mut self) -> Result<(), String> {
        let mut deletes: Vec<String> = (0..self.pages(self.stored_size))
            .map(|index| page_key(&self.name, index))
            .collect();
        deletes.push(meta_key(&self.name));
        self.kv
            .commit(&[], &deletes)
            .map_err(|error| format!("could not delete {}: {error}", self.path))?;
        self.count(|stats| stats.deletes += deletes.len() as u64);
        self.dirty.clear();
        self.size = 0;
        self.stored_size = 0;
        self.page_size = 0;
        Ok(())
    }
}

impl VfsFile for KvPageFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        self.check()?;
        if self.page_size == 0 {
            buf.fill(0);
            return Ok(false);
        }
        let mut done = 0;
        while done < buf.len() {
            let at = offset + done;
            if at >= self.size {
                buf[done..].fill(0);
                return Ok(false);
            }
            let index = at / self.page_size;
            let start = at % self.page_size;
            let len = (self.page_size - start).min(buf.len() - done);
            let out = &mut buf[done..done + len];
            if let Some(page) = self.dirty.get(&index) {
                out.copy_from_slice(&page[start..start + len]);
            } else {
                self.count(|stats| stats.gets += 1);
                let key = page_key(&self.name, index);
                let page = self
                    .kv
                    .get(&key)
                    .map_err(|error| io_error(SQLITE_IOERR_READ, format!("{key}: {error}")))?
                    .ok_or_else(|| io_error(SQLITE_IOERR_READ, format!("{key} is missing")))?;
                if page.len() != self.page_size {
                    return Err(io_error(
                        SQLITE_IOERR_READ,
                        format!("{key} holds {} bytes, not {}", page.len(), self.page_size),
                    ));
                }
                out.copy_from_slice(&page[start..start + len]);
            }
            done += len;
        }
        Ok(true)
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        self.check()?;
        if self.page_size == 0 {
            self.page_size = buf.len();
        }
        // SQLite writes a main file in whole pages only.
        if buf.len() != self.page_size || !offset.is_multiple_of(self.page_size) {
            return Err(io_error(
                SQLITE_IOERR_WRITE,
                format!(
                    "{}: a write of {} bytes at {offset} is not one page of {} bytes \
                     (a file's page size cannot change)",
                    self.path,
                    buf.len(),
                    self.page_size
                ),
            ));
        }
        let index = offset / self.page_size;
        // Pages a rollback writes back (SQLite restores the originals when a
        // transaction that already wrote the file aborts) stay dirty too; they
        // equal what storage holds, and the next commit rewrites them.
        // A gap reads as zeros, never as pages a truncation dropped.
        for gap in self.pages(self.size)..index {
            self.dirty.insert(gap, vec![0; self.page_size]);
        }
        self.dirty.insert(index, buf.to_vec());
        self.size = self.size.max(offset + self.page_size);
        Ok(())
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        self.check()?;
        if self.page_size != 0 && !size.is_multiple_of(self.page_size) {
            return Err(io_error(
                SQLITE_IOERR_WRITE,
                format!("{}: truncate to {size}, not a page boundary", self.path),
            ));
        }
        if size < self.size {
            let pages = self.pages(size);
            self.dirty.retain(|&index, _| index < pages);
            self.size = size;
        }
        Ok(())
    }

    fn flush(&mut self) -> VfsResult<()> {
        // Commits are written at `SQLITE_FCNTL_COMMIT_PHASETWO`.
        Ok(())
    }

    fn size(&self) -> VfsResult<usize> {
        Ok(self.size)
    }
}

enum KvFile {
    Main(KvPageFile),
    Temp(MemChunksFile),
}

impl VfsFile for KvFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        match self {
            KvFile::Main(file) => file.read(buf, offset),
            KvFile::Temp(file) => file.read(buf, offset),
        }
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        match self {
            KvFile::Main(file) => file.write(buf, offset),
            KvFile::Temp(file) => file.write(buf, offset),
        }
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        match self {
            KvFile::Main(file) => file.truncate(size),
            KvFile::Temp(file) => file.truncate(size),
        }
    }

    fn flush(&mut self) -> VfsResult<()> {
        match self {
            KvFile::Main(file) => file.flush(),
            KvFile::Temp(file) => file.flush(),
        }
    }

    fn size(&self) -> VfsResult<usize> {
        match self {
            KvFile::Main(file) => file.size(),
            KvFile::Temp(file) => file.size(),
        }
    }
}

#[derive(Default)]
struct KvVfs {
    files: RefCell<HashMap<String, KvFile>>,
}

struct KvStore;

impl VfsStore<KvFile, KvVfs> for KvStore {
    fn add_file(vfs: *mut sqlite3_vfs, file: &str, flags: i32) -> VfsResult<()> {
        if flags & SQLITE_OPEN_MAIN_DB != 0 {
            return Err(VfsError::new(
                SQLITE_CANTOPEN,
                format!("{file} is not attached to Durable Object storage"),
            ));
        }
        if flags & SQLITE_OPEN_WAL != 0 {
            return Err(VfsError::new(
                SQLITE_CANTOPEN,
                format!("{file}: WAL is not supported yet on Durable Object storage"),
            ));
        }
        let vfs = unsafe { Self::app_data(vfs) };
        vfs.files
            .borrow_mut()
            .insert(file.into(), KvFile::Temp(MemChunksFile::default()));
        Ok(())
    }

    fn contains_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<bool> {
        let vfs = unsafe { Self::app_data(vfs) };
        Ok(vfs.files.borrow().contains_key(file))
    }

    fn delete_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<()> {
        let vfs = unsafe { Self::app_data(vfs) };
        let mut files = vfs.files.borrow_mut();
        match files.get_mut(file) {
            // SQLite deletes only temporary files; a main file is deleted
            // through `destroy`.
            Some(KvFile::Main(_)) => Err(VfsError::new(
                SQLITE_IOERR_DELETE,
                format!("{file} is deleted only through destroy"),
            )),
            Some(KvFile::Temp(_)) => {
                files.remove(file);
                Ok(())
            }
            None => Err(VfsError::new(
                SQLITE_IOERR_DELETE,
                format!("{file} not found"),
            )),
        }
    }

    fn with_file<F: Fn(&KvFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let name = unsafe { vfs_file.name() };
        let vfs = unsafe { Self::app_data(vfs_file.vfs) };
        match vfs.files.borrow().get(name) {
            Some(file) => f(file),
            None => Err(VfsError::new(SQLITE_IOERR, format!("{name} not found"))),
        }
    }

    fn with_file_mut<F: Fn(&mut KvFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let name = unsafe { vfs_file.name() };
        let vfs = unsafe { Self::app_data(vfs_file.vfs) };
        match vfs.files.borrow_mut().get_mut(name) {
            Some(file) => f(file),
            None => Err(VfsError::new(SQLITE_IOERR, format!("{name} not found"))),
        }
    }
}

struct KvIoMethods;

impl SQLiteIoMethods for KvIoMethods {
    type File = KvFile;
    type AppData = KvVfs;
    type Store = KvStore;

    const VERSION: ::std::os::raw::c_int = 1;

    unsafe extern "C" fn xFileControl(
        p_file: *mut sqlite3_file,
        op: ::std::os::raw::c_int,
        _p_arg: *mut ::std::os::raw::c_void,
    ) -> ::std::os::raw::c_int {
        unsafe {
            if op != SQLITE_FCNTL_COMMIT_PHASETWO {
                return SQLITE_NOTFOUND;
            }
            let vfs_file = SQLiteVfsFile::from_file(p_file);
            let vfs = Self::Store::app_data(vfs_file.vfs);
            let name = vfs_file.name();

            let mut files = vfs.files.borrow_mut();
            let Some(KvFile::Main(file)) = files.get_mut(name) else {
                return SQLITE_NOTFOUND;
            };

            match file.commit() {
                Ok(()) => SQLITE_OK,
                Err(error) => vfs.store_err(error),
            }
        }
    }

    unsafe extern "C" fn xClose(p_file: *mut sqlite3_file) -> ::std::os::raw::c_int {
        unsafe {
            let vfs_file = SQLiteVfsFile::from_file(p_file);
            let vfs = Self::Store::app_data(vfs_file.vfs);
            // `xCloseImpl` frees the name.
            let name = vfs_file.name().to_owned();
            let closed = Self::xCloseImpl(p_file);
            let mut files = vfs.files.borrow_mut();
            if let Some(KvFile::Main(file)) = files.get_mut(&name) {
                file.opens = file.opens.saturating_sub(1);
                if file.ephemeral && file.opens == 0 {
                    files.remove(&name);
                }
            }
            closed
        }
    }
}

struct KvSqliteVfs<C>(PhantomData<C>);

impl<C: OsCallback> SQLiteVfs<KvIoMethods> for KvSqliteVfs<C> {
    const VERSION: ::std::os::raw::c_int = 1;

    unsafe extern "C" fn xOpen(
        p_vfs: *mut sqlite3_vfs,
        z_name: rsqlite_vfs::ffi::sqlite3_filename,
        p_file: *mut sqlite3_file,
        flags: ::std::os::raw::c_int,
        p_out_flags: *mut ::std::os::raw::c_int,
    ) -> ::std::os::raw::c_int {
        unsafe {
            let opened = Self::xOpenImpl(p_vfs, z_name, p_file, flags, p_out_flags);
            if opened == SQLITE_OK {
                let vfs_file = SQLiteVfsFile::from_file(p_file);
                let vfs = KvStore::app_data(p_vfs);
                if let Some(KvFile::Main(file)) = vfs.files.borrow_mut().get_mut(vfs_file.name()) {
                    file.opens += 1;
                }
            }
            opened
        }
    }

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

/// Handle on the registered VFS.
pub struct KvVfsUtil {
    vfs: &'static VfsAppData<KvVfs>,
}

impl KvVfsUtil {
    /// Bind the VFS file `path` to the file `name` in `kv` (one object's
    /// storage), reading its metadata. Paths are per isolate, which may
    /// host several objects, so each object's path must be its own; `name`
    /// is per object. Replaces an earlier binding of `path`, which must
    /// not be open.
    pub fn attach(&self, path: &str, kv: Rc<dyn PageKv>, name: &str) -> Result<(), String> {
        self.bind(path, KvPageFile::load(kv, path, name)?)
    }

    fn bind(&self, path: &str, file: KvPageFile) -> Result<(), String> {
        let mut files = self.vfs.files.borrow_mut();
        if let Some(KvFile::Main(open)) = files.get(path)
            && open.opens > 0
        {
            return Err(format!(
                "{path} is open ({} connections): close it before attaching it again",
                open.opens
            ));
        }
        files.insert(path.to_owned(), KvFile::Main(file));
        Ok(())
    }

    /// [`Self::attach`], unbinding `path` when the last connection on it
    /// closes (for stores that live as long as their database, as
    /// `JAZZ_KV_VFS` test stores do).
    pub fn attach_ephemeral(
        &self,
        path: &str,
        kv: Rc<dyn PageKv>,
        name: &str,
    ) -> Result<(), String> {
        let mut file = KvPageFile::load(kv, path, name)?;
        file.ephemeral = true;
        self.bind(path, file)
    }

    /// Unbind `path` (closed), keeping its storage.
    pub fn detach(&self, path: &str) {
        self.vfs.files.borrow_mut().remove(path);
    }

    /// Delete `path` (closed, attached) from storage, and unbind it.
    pub fn destroy(&self, path: &str) -> Result<(), String> {
        let mut files = self.vfs.files.borrow_mut();
        let Some(KvFile::Main(file)) = files.get_mut(path) else {
            return Err(format!("{path} is not attached to Durable Object storage"));
        };
        file.delete_all()?;
        files.remove(path);
        Ok(())
    }

    /// The storage operations `path` made since it was attached.
    pub fn stats(&self, path: &str) -> Option<KvStats> {
        match self.vfs.files.borrow().get(path) {
            Some(KvFile::Main(file)) => Some(file.stats.get()),
            _ => None,
        }
    }
}

/// Register the VFS as `vfs_name` (once; later calls return a handle on the
/// registered one). Not thread-safe: Durable Objects run one thread.
pub fn install<C: OsCallback>(vfs_name: &str) -> Result<KvVfsUtil, RegisterVfsError> {
    let vfs = match registered_vfs(vfs_name)? {
        Some(vfs) => vfs,
        None => register_vfs::<KvIoMethods, KvSqliteVfs<C>>(vfs_name, KvVfs::default(), false)?,
    };
    Ok(KvVfsUtil {
        vfs: unsafe { KvStore::app_data(vfs) },
    })
}

/// A KV store in memory, and SQLite's OS hooks natively: what the engine's
/// suites run the VFS on (`JAZZ_KV_VFS=1`, see `Db::open`).
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::sync::Mutex;
    use std::time::Duration;

    use rsqlite_vfs::OsCallback;

    use super::{KvVfsUtil, PageKv};

    /// One object's storage: a sorted map, whose commits can be made to
    /// fail.
    #[derive(Default)]
    pub struct MemKv {
        pub data: RefCell<BTreeMap<String, Vec<u8>>>,
        pub fail_commits: Cell<bool>,
        pub commits: Cell<u32>,
    }

    impl PageKv for MemKv {
        fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
            Ok(self.data.borrow().get(key).cloned())
        }

        fn commit(&self, puts: &[(String, &[u8])], deletes: &[String]) -> Result<(), String> {
            if self.fail_commits.get() {
                return Err("storage is full (test)".into());
            }
            let mut data = self.data.borrow_mut();
            for (key, value) in puts {
                data.insert(key.clone(), value.to_vec());
            }
            for key in deletes {
                data.remove(key);
            }
            self.commits.set(self.commits.get() + 1);
            Ok(())
        }
    }

    /// SQLite's OS hooks on the host.
    pub struct NativeOs;

    impl OsCallback for NativeOs {
        fn sleep(dur: Duration) {
            std::thread::sleep(dur);
        }

        fn random(buf: &mut [u8]) {
            // Names temporary files only.
            static STATE: Mutex<u64> = Mutex::new(0x9e37_79b9_7f4a_7c15);
            let mut state = STATE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for byte in buf {
                *state ^= *state << 13;
                *state ^= *state >> 7;
                *state ^= *state << 17;
                *byte = *state as u8;
            }
        }

        fn epoch_timestamp_in_ms() -> i64 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |since| since.as_millis() as i64)
        }
    }

    /// This thread's own registration of the VFS. Its state is
    /// single-threaded (as a Durable Object is) and stores are
    /// thread-affine, so each thread gets a VFS of its own.
    pub fn thread_vfs() -> (&'static str, Rc<KvVfsUtil>) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static THREADS: AtomicU64 = AtomicU64::new(0);
        thread_local! {
            static VFS: (&'static str, Rc<KvVfsUtil>) = {
                let name: &'static str = Box::leak(
                    format!("jazz-kv-test-{}", THREADS.fetch_add(1, Ordering::Relaxed))
                        .into_boxed_str(),
                );
                let util = super::install::<NativeOs>(name).expect("register the KV test VFS");
                (name, Rc::new(util))
            };
        }
        VFS.with(|(name, util)| (*name, util.clone()))
    }
}

#[cfg(test)]
mod tests;
