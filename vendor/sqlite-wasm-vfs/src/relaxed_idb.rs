//! relaxed-idb vfs implementation
//!
//! ```rust
//! use sqlite_wasm_rs as ffi;
//! use sqlite_wasm_vfs::relaxed_idb::{
//!     install as install_idb_vfs,
//!     RelaxedIdbCfg
//! };
//!
//! async fn open_db() {
//!     // install relaxed-idb persistent vfs and set as default vfs
//!     install_idb_vfs::<ffi::WasmOsCallback>(&RelaxedIdbCfg::default(), true)
//!         .await
//!         .unwrap();
//!
//!     // open with relaxed-idb vfs
//!     let mut db = std::ptr::null_mut();
//!     let ret = unsafe {
//!         ffi::sqlite3_open_v2(
//!             c"relaxed-idb.db".as_ptr().cast(),
//!             &mut db as *mut _,
//!             ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
//!             std::ptr::null()
//!         )
//!     };
//!     assert_eq!(ffi::SQLITE_OK, ret);
//! }
//! ```
//!
//! Inspired by wa-sqlite's [`IDBMirrorVFS`](https://github.com/rhashimoto/wa-sqlite/blob/master/src/examples/IDBMirrorVFS.js),
//! this is an VFS used in a synchronization context.
//!
//! The principle is to preload the db into memory before xOpen, and then all operations are synchronous.
//! When sqlite calls sync, it asynchronously writes the changed blocks to the indexed db through the indexed transaction.
//! The difference from IDBMirrorVFS is that `RelaxedIdbVFS` does only support pragma `synchronous=off`.
//!
//! As for performance, since both reading and writing are done in memory, the performance is very good.
//! However, we need to pay attention to the performance of preload the database, because the database is divided
//! into multiple blocks and stored in the indexed db, and it takes some time to read all of them into memory.
//! After my test, when page_size is 64k, the loading speed is the fastest.
//!
//! As with MemoryVFS, you also need to pay attention to the memory size limit of the browser page.
//!
//! It is particularly important to note that using it on multiple pages may cause DB corruption.
//! It is recommended to use it in SharedWorker.

use rsqlite_vfs::{
    bail, check_db_and_page_size, check_import_db, check_option, check_result,
    ffi::{
        sqlite3_file, sqlite3_vfs, SQLITE_ERROR, SQLITE_FCNTL_COMMIT_PHASETWO, SQLITE_FCNTL_PRAGMA,
        SQLITE_FCNTL_SYNC, SQLITE_IOERR, SQLITE_IOERR_DELETE, SQLITE_NOTFOUND, SQLITE_OK,
        SQLITE_OPEN_MAIN_DB,
    },
    register_vfs, registered_vfs, ImportDbError, MemChunksFile, OsCallback, RegisterVfsError,
    SQLiteIoMethods, SQLiteVfs, SQLiteVfsFile, VfsAppData, VfsError, VfsFile, VfsResult, VfsStore,
};
use std::time::Duration;
use std::{cell::RefCell, marker::PhantomData};

use indexed_db_futures::database::Database;
use indexed_db_futures::prelude::*;
use indexed_db_futures::transaction::TransactionMode;
use js_sys::{Number, Object, Reflect, Uint8Array};
use std::collections::{hash_map, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{
    collections::HashMap,
    ffi::{c_char, CStr},
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use wasm_bindgen::JsValue;

type Result<T> = std::result::Result<T, RelaxedIdbError>;

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
    let i_offset = offset;

    while p_data_offset < p_data_length {
        let file_offset = i_offset + p_data_offset;
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
    Flush(String),
    Delete(String),
    Clear,
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
    tx_blocks: HashSet<usize>,
    sync_notified: bool,
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
            IdbFile::Main(idb_page_file) => idb_page_file.read(buf, offset),
            IdbFile::Temp(mem_chunks_file) => mem_chunks_file.read(buf, offset),
        }
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        match self {
            IdbFile::Main(idb_page_file) => idb_page_file.write(buf, offset),
            IdbFile::Temp(mem_chunks_file) => mem_chunks_file.write(buf, offset),
        }
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        match self {
            IdbFile::Main(idb_page_file) => idb_page_file.truncate(size),
            IdbFile::Temp(mem_chunks_file) => mem_chunks_file.truncate(size),
        }
    }

    fn flush(&mut self) -> VfsResult<()> {
        match self {
            IdbFile::Main(idb_page_file) => idb_page_file.flush(),
            IdbFile::Temp(mem_chunks_file) => mem_chunks_file.flush(),
        }
    }

    fn size(&self) -> VfsResult<usize> {
        match self {
            IdbFile::Main(idb_page_file) => idb_page_file.size(),
            IdbFile::Temp(mem_chunks_file) => mem_chunks_file.size(),
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

async fn clear_impl(indexed_db: &Database) -> Result<()> {
    let transaction = indexed_db
        .transaction("blocks")
        .with_mode(TransactionMode::Readwrite)
        .build()?;
    let blocks = transaction.object_store("blocks")?;
    blocks.clear()?;
    transaction.commit().await?;
    Ok(())
}

async fn preload_db_impl(
    indexed_db: &Database,
    preload: &Preload,
) -> Result<HashMap<String, IdbFile>> {
    if matches!(preload, &Preload::None) {
        return Ok(HashMap::new());
    }

    let transaction = indexed_db
        .transaction("blocks")
        .with_mode(TransactionMode::Readonly)
        .build()?;
    let blocks = transaction.object_store("blocks")?;

    let mut name2file = HashMap::new();
    let mut insert_fn = |block: JsValue| {
        let (path, offset, data) = get_block(block);
        match name2file.entry(path) {
            hash_map::Entry::Occupied(mut occupied_entry) => {
                let IdbFile::Main(db) = occupied_entry.get_mut() else {
                    unreachable!();
                };
                db.file_size += db.block_size;
                db.blocks.insert(offset, data);
            }
            hash_map::Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(IdbFile::Main(IdbPageFile {
                    file_size: data.length() as _,
                    block_size: data.length() as _,
                    blocks: HashMap::from([(offset, data)]),
                    tx_blocks: HashSet::new(),
                    sync_notified: false,
                }));
            }
        }
    };

    match preload {
        Preload::All => {
            for block in blocks.get_all::<JsValue>().await? {
                insert_fn(block?);
            }
        }
        Preload::Paths(items) => {
            for file in items {
                for block in blocks
                    .get_all::<JsValue>()
                    .with_query(key_range(file, 0))
                    .await?
                {
                    insert_fn(block?);
                }
            }
        }
        Preload::None => unreachable!(),
    }

    Ok(name2file)
}

struct RelaxedIdb {
    idb: Database,
    name2file: RefCell<HashMap<String, IdbFile>>,
    // A failed commit may have consumed dirty blocks. A later successful queue
    // barrier cannot establish durability for that file. Retain the first
    // failure until an explicit, successful deletion resets the file.
    failed_files: RefCell<HashMap<String, String>>,
    tx: UnboundedSender<IdbCommit>,
}

impl RelaxedIdb {
    async fn new(options: &RelaxedIdbCfg, tx: UnboundedSender<IdbCommit>) -> Result<Self> {
        let indexed_db = Database::open(&options.vfs_name)
            .with_version(1u8)
            .with_on_upgrade_needed(|_, db| {
                db.create_object_store("blocks")
                    .with_key_path(["path", "offset"].into())
                    .build()?;
                Ok(())
            })
            .await?;

        if options.clear_on_init {
            clear_impl(&indexed_db).await?;
        }

        let name2file = preload_db_impl(&indexed_db, &options.preload).await?;
        Ok(RelaxedIdb {
            idb: indexed_db,
            name2file: RefCell::new(name2file),
            failed_files: RefCell::new(HashMap::new()),
            tx,
        })
    }

    fn send_task(&self, op: IdbCommitOp) -> Result<()> {
        if self.tx.send(IdbCommit { op, notify: None }).is_err() {
            return Err(RelaxedIdbError::Generic(
                "failed to send commit task".into(),
            ));
        }
        Ok(())
    }

    fn send_task_with_notify(&self, op: IdbCommitOp) -> Result<WaitCommit> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let commit = IdbCommit {
            op,
            notify: Some(tx),
        };
        if self.tx.send(commit).is_err() {
            return Err(RelaxedIdbError::Generic(
                "failed to send commit task".into(),
            ));
        }
        Ok(WaitCommit(rx))
    }

    async fn preload_db(&self, files: Vec<String>) -> Result<()> {
        for file in &files {
            self.check_file(file)?;
        }
        let preload = {
            let name2file = self.name2file.borrow();
            files
                .into_iter()
                .filter(|x| !name2file.contains_key(x))
                .collect::<Vec<_>>()
        };
        let preload = preload_db_impl(&self.idb, &Preload::Paths(preload)).await?;
        self.name2file.borrow_mut().extend(preload);
        Ok(())
    }

    async fn reload_db(&self, filename: &str) -> Result<()> {
        self.check_file(filename)?;
        let reloaded = preload_db_impl(&self.idb, &Preload::Paths(vec![filename.into()])).await?;
        // Only replace this file after the read succeeds. Absence must also
        // evict its stale image, so require-existing admission sees deletion.
        let mut files = self.name2file.borrow_mut();
        files.remove(filename);
        files.extend(reloaded);
        Ok(())
    }

    fn import_db(&self, filename: &str, bytes: &[u8]) -> Result<WaitCommit> {
        let page_size = check_import_db(bytes)?;
        self.import_db_unchecked(filename, bytes, page_size, true)
    }

    fn import_db_unchecked(
        &self,
        filename: &str,
        bytes: &[u8],
        page_size: usize,
        clear_wal: bool,
    ) -> Result<WaitCommit> {
        check_db_and_page_size(bytes.len(), page_size)?;

        if self.name2file.borrow().contains_key(filename) {
            return Err(RelaxedIdbError::Generic(format!(
                "{filename} file already exists"
            )));
        }

        let mut blocks: HashMap<usize, Uint8Array> = bytes
            .chunks(page_size)
            .enumerate()
            .map(|(idx, buffer)| (idx * page_size, Uint8Array::new_from_slice(buffer)))
            .collect();

        // forced to write back to legacy mode
        if clear_wal {
            let header = blocks.get_mut(&0).unwrap();
            header.subarray(18, 20).copy_from(&[1, 1]);
        }

        let tx_blocks = blocks.keys().copied().collect();

        self.name2file.borrow_mut().insert(
            filename.into(),
            IdbFile::Main(IdbPageFile {
                file_size: blocks.len() * page_size,
                block_size: page_size,
                blocks,
                tx_blocks,
                sync_notified: false,
            }),
        );

        self.send_task_with_notify(IdbCommitOp::Sync(filename.into()))
    }

    fn export_db(&self, name: &str) -> Result<Vec<u8>> {
        let name2file = self.name2file.borrow();

        match name2file.get(name) {
            Some(IdbFile::Main(file)) => {
                let file_size = file.file_size;
                let mut ret = vec![0; file_size];
                for (&offset, buffer) in &file.blocks {
                    if offset >= file_size {
                        continue;
                    }
                    buffer.copy_to(&mut ret[offset..offset + file.block_size]);
                }
                Ok(ret)
            }
            Some(IdbFile::Temp(_)) => Err(RelaxedIdbError::Generic(
                "Does not support dumping temporary files".into(),
            )),
            None => Err(RelaxedIdbError::Generic(
                "The file to be exported does not exist".into(),
            )),
        }
    }

    fn delete_db(&self, name: &str) -> Result<WaitCommit> {
        self.name2file.borrow_mut().remove(name);
        self.send_task_with_notify(IdbCommitOp::Delete(name.into()))
    }

    fn clear_all(&self) -> Result<WaitCommit> {
        std::mem::take(&mut *self.name2file.borrow_mut());
        self.send_task_with_notify(IdbCommitOp::Clear)
    }

    fn exists(&self, file: &str) -> bool {
        self.name2file.borrow().contains_key(file)
    }

    fn check_file(&self, file: &str) -> Result<()> {
        match self.failed_files.borrow().get(file) {
            Some(error) => Err(RelaxedIdbError::Generic(error.clone())),
            None => Ok(()),
        }
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

    // already drop
    #[allow(clippy::await_holding_refcell_ref)]
    async fn sync_db_impl(&self, file: &str) -> Result<()> {
        let mut name2file = self.name2file.borrow_mut();
        let Some(idb_file) = name2file.get_mut(file) else {
            return Ok(());
        };

        let IdbFile::Main(idb_blocks) = idb_file else {
            return Ok(());
        };

        idb_blocks.sync_notified = false;

        let file_size = idb_blocks.file_size;
        let mut truncated_offset = idb_blocks.file_size;
        while idb_blocks.blocks.remove(&truncated_offset).is_some() {
            truncated_offset += idb_blocks.block_size;
        }

        let tx_blocks = std::mem::take(&mut idb_blocks.tx_blocks);
        if tx_blocks.is_empty() && file_size == truncated_offset {
            // no need to put or delete
            return Ok(());
        }

        let path = JsValue::from(file);

        let transaction = self
            .idb
            .transaction("blocks")
            .with_mode(TransactionMode::Readwrite)
            .build()?;

        let store = transaction.object_store("blocks")?;

        for offset in tx_blocks {
            if let Some(buffer) = idb_blocks.blocks.get(&offset) {
                store.put(&set_block(&path, offset, buffer)).build()?;
            }
        }
        store.delete(key_range(file, file_size)).build()?;

        // The `RefMut` from `name2file` is explicitly dropped here to avoid holding the borrow across an `.await` point.
        drop(name2file);

        transaction.commit().await?;

        Ok(())
    }

    async fn commit_loop(&self, mut rx: UnboundedReceiver<IdbCommit>) {
        while let Some(commit) = rx.recv().await {
            let IdbCommit { op, notify } = commit;
            let ret = match op {
                IdbCommitOp::Sync(file) => {
                    let ret = match self.check_file(&file) {
                        Ok(()) => self.sync_db_impl(&file).await,
                        Err(error) => Err(error),
                    };
                    if let Err(error) = &ret {
                        self.failed_files
                            .borrow_mut()
                            .entry(file)
                            .or_insert_with(|| error.to_string());
                    }
                    ret
                }
                // This is a FIFO queue barrier, not another write. Unlike a
                // dummy-file deletion, it observes prior failures of this file.
                IdbCommitOp::Flush(file) => self.check_file(&file),
                IdbCommitOp::Delete(file) => {
                    let ret = self.delete_db_impl(&file).await;
                    if ret.is_ok() {
                        self.failed_files.borrow_mut().remove(&file);
                    } else if let Err(error) = &ret {
                        self.failed_files
                            .borrow_mut()
                            .entry(file)
                            .or_insert_with(|| error.to_string());
                    }
                    ret
                }
                IdbCommitOp::Clear => {
                    let ret = clear_impl(&self.idb).await;
                    if ret.is_ok() {
                        self.failed_files.borrow_mut().clear();
                    }
                    ret
                }
            };
            if let Some(notify) = notify {
                // An unsuccessful send would be one where the corresponding receiver
                // has already been deallocated.
                let _ = notify.send(ret);
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

struct RelaxedIdbStore;

impl VfsStore<IdbFile, RelaxedIdb> for RelaxedIdbStore {
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
        let idb_file = match pool.name2file.borrow_mut().remove(file) {
            Some(file) => file,
            None => {
                return Err(VfsError::new(
                    SQLITE_IOERR_DELETE,
                    format!("{file} not found"),
                ))
            }
        };
        // temp db never put into indexed db, no need to delete
        if let IdbFile::Main(_) = &idb_file {
            if pool.send_task(IdbCommitOp::Delete(file.into())).is_err() {
                return Err(VfsError::new(
                    SQLITE_IOERR_DELETE,
                    format!("failed to send delete task, file: {file}"),
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

struct RelaxedIdbIoMethods;

impl SQLiteIoMethods for RelaxedIdbIoMethods {
    type File = IdbFile;
    type AppData = RelaxedIdb;
    type Store = RelaxedIdbStore;

    const VERSION: ::std::os::raw::c_int = 1;

    unsafe extern "C" fn xFileControl(
        pFile: *mut sqlite3_file,
        op: ::std::os::raw::c_int,
        pArg: *mut ::std::os::raw::c_void,
    ) -> ::std::os::raw::c_int {
        let vfs_file = SQLiteVfsFile::from_file(pFile);
        let pool = Self::Store::app_data(vfs_file.vfs);
        let name = vfs_file.name();

        let mut name2file = pool.name2file.borrow_mut();
        let file = check_option!(name2file.get_mut(name));

        let IdbFile::Main(file) = file else {
            return SQLITE_NOTFOUND;
        };

        match op {
            SQLITE_FCNTL_PRAGMA => {
                let pArg = pArg as *mut *mut c_char;
                let name = *pArg.add(1);
                let value = *pArg.add(2);

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
                        "relaxed-idb vfs only supports synchronous=off".into(),
                    ));
                };
            }
            SQLITE_FCNTL_SYNC | SQLITE_FCNTL_COMMIT_PHASETWO => {
                if !file.sync_notified {
                    if pool.send_task(IdbCommitOp::Sync(name.into())).is_err() {
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

struct RelaxedIdbVfs<C>(PhantomData<C>);

impl<C> SQLiteVfs<RelaxedIdbIoMethods> for RelaxedIdbVfs<C>
where
    C: OsCallback,
{
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

/// A future that resolves when a pending IndexedDB commit operation is complete.
pub struct WaitCommit(tokio::sync::oneshot::Receiver<Result<()>>);

impl Future for WaitCommit {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(ret) => Poll::Ready(ret.unwrap_or_else(|_| {
                Err(RelaxedIdbError::Generic(
                    "Waiting for notify failure".into(),
                ))
            })),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RelaxedIdbError {
    #[error(transparent)]
    Vfs(#[from] RegisterVfsError),
    #[error(transparent)]
    ImportDb(#[from] ImportDbError),
    #[error(transparent)]
    OpenDb(#[from] indexed_db_futures::error::OpenDbError),
    #[error(transparent)]
    IndexedDb(#[from] indexed_db_futures::error::Error),
    #[error("Generic error: {0}")]
    Generic(String),
}

/// Select which dbs to preload into memory.
pub enum Preload {
    /// Preload all databases
    All,
    /// Specify the path to load the database
    Paths(Vec<String>),
    /// Not preloaded, can be manually loaded later via `RelaxedIdbUtil`
    None,
}

/// Build `RelaxedIdbCfg`
pub struct RelaxedIdbCfgBuilder(RelaxedIdbCfg);

impl RelaxedIdbCfgBuilder {
    pub fn new() -> Self {
        Self(RelaxedIdbCfg::default())
    }

    /// The SQLite VFS name under which this pool's VFS is registered.
    pub fn vfs_name(mut self, name: &str) -> Self {
        self.0.vfs_name = name.into();
        self
    }

    /// Delete all files on initialization.
    pub fn clear_on_init(mut self, set: bool) -> Self {
        self.0.clear_on_init = set;
        self
    }

    /// Select which dbs to preload into memory.
    pub fn preload(mut self, preload: Preload) -> Self {
        self.0.preload = preload;
        self
    }

    /// Build `RelaxedIdbCfg`.
    pub fn build(self) -> RelaxedIdbCfg {
        self.0
    }
}

impl Default for RelaxedIdbCfgBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// `RelaxedIdb` options
pub struct RelaxedIdbCfg {
    /// The SQLite VFS name under which this pool's VFS is registered.
    pub vfs_name: String,
    /// Delete all files on initialization.
    pub clear_on_init: bool,
    /// Select which dbs to preload into memory.
    pub preload: Preload,
}

impl Default for RelaxedIdbCfg {
    fn default() -> Self {
        Self {
            vfs_name: "relaxed-idb".into(),
            clear_on_init: false,
            preload: Preload::All,
        }
    }
}

/// RelaxedIdbVfs management tool.
pub struct RelaxedIdbUtil {
    pool: &'static VfsAppData<RelaxedIdb>,
}

impl RelaxedIdbUtil {
    /// Wait for queued commits and report any earlier persistence failure of
    /// this file. Failure is sticky until the file is explicitly deleted.
    pub fn flush_db(&self, filename: &str) -> Result<WaitCommit> {
        self.pool
            .send_task_with_notify(IdbCommitOp::Flush(filename.into()))
    }

    /// Preload the db.
    ///
    /// Because indexed db reading data is an asynchronous operation,
    /// the db must be preloaded into memory before opening the sqlite db.
    pub async fn preload_db(&self, preload: Vec<String>) -> Result<()> {
        self.pool.preload_db(preload).await
    }

    /// Refresh one closed file from IndexedDB, including removal if absent.
    ///
    /// The host must hold its exclusive physical ownership lock, have closed
    /// or irrevocably retired every SQLite connection to this file, and have
    /// awaited prior commits.
    /// This reads storage without writing it or disturbing unrelated files.
    pub async fn reload_db(&self, filename: &str) -> Result<()> {
        self.pool.reload_db(filename).await
    }

    /// Import the database.
    ///
    /// If the database is imported with WAL mode enabled,
    /// it will be forced to write back to legacy mode, see
    /// <https://sqlite.org/forum/forumpost/67882c5b04>
    ///
    /// If the imported database is encrypted, use `import_db_unchecked` instead.
    pub fn import_db(&self, filename: &str, bytes: &[u8]) -> Result<WaitCommit> {
        self.pool.import_db(filename, bytes)
    }

    /// `import_db` without checking, can be used to import encrypted database.
    pub fn import_db_unchecked(
        &self,
        filename: &str,
        bytes: &[u8],
        page_size: usize,
    ) -> Result<WaitCommit> {
        self.pool
            .import_db_unchecked(filename, bytes, page_size, false)
    }

    /// Export the database.
    pub fn export_db(&self, filename: &str) -> Result<Vec<u8>> {
        self.pool.export_db(filename)
    }

    /// Delete the specified database, make sure that the database is closed.
    pub fn delete_db(&self, filename: &str) -> Result<WaitCommit> {
        self.pool.delete_db(filename)
    }

    /// Delete all database, make sure that all database is closed.
    pub fn clear_all(&self) -> Result<WaitCommit> {
        self.pool.clear_all()
    }

    /// Does the database exists.
    pub fn exists(&self, filename: &str) -> bool {
        self.pool.exists(filename)
    }

    /// List all files.
    pub fn list(&self) -> Vec<String> {
        self.pool.name2file.borrow().keys().cloned().collect()
    }

    /// Number of files.
    pub fn count(&self) -> usize {
        self.pool.name2file.borrow().len()
    }
}

/// Register `relaxed-idb` vfs and return a management tool which can be used
/// to perform basic administration of the file pool.
///
/// If the vfs corresponding to `options.vfs_name` has been registered,
/// only return a management tool without register.
pub async fn install<C: OsCallback>(
    options: &RelaxedIdbCfg,
    default_vfs: bool,
) -> Result<RelaxedIdbUtil> {
    static REGISTER_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = REGISTER_GUARD.lock().await;

    let pool = if let Some(vfs) = registered_vfs(&options.vfs_name)? {
        unsafe { RelaxedIdbStore::app_data(vfs) }
    } else {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let pool = RelaxedIdb::new(options, tx).await?;
        let vfs = register_vfs::<RelaxedIdbIoMethods, RelaxedIdbVfs<C>>(
            &options.vfs_name,
            pool,
            default_vfs,
        )?;

        let app_data = unsafe { RelaxedIdbStore::app_data(vfs) };
        wasm_bindgen_futures::spawn_local(app_data.commit_loop(rx));
        app_data
    };

    Ok(RelaxedIdbUtil { pool })
}

#[cfg(test)]
mod tests {
    use super::{IdbFile, RelaxedIdb, RelaxedIdbCfgBuilder, RelaxedIdbStore};
    use rsqlite_vfs::{test_suite::test_vfs_store, VfsAppData};
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    async fn test_relaxed_idb_vfs_store() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        test_vfs_store::<RelaxedIdb, IdbFile, RelaxedIdbStore>(VfsAppData::new(
            RelaxedIdb::new(
                &RelaxedIdbCfgBuilder::new()
                    .vfs_name("test_relaxed_idb_suite")
                    .build(),
                tx,
            )
            .await
            .unwrap(),
        ))
        .unwrap();

        wasm_bindgen_futures::spawn_local(async move { while let Some(_) = rx.recv().await {} });
    }
}
