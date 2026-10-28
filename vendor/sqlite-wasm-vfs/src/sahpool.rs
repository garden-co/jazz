//! opfs-sahpool vfs implementation, ported from sqlite-wasm.
//!
//! See [`opfs-sahpool`](https://sqlite.org/wasm/doc/trunk/persistence.md#vfs-opfs-sahpool) for details.
//!
//! ```rust
//! use sqlite_wasm_rs as ffi;
//! use sqlite_wasm_vfs::sahpool::{install as install_opfs_sahpool, OpfsSAHPoolCfg};
//!
//! async fn open_db() {
//!     // install opfs-sahpool persistent vfs and set as default vfs
//!     install_opfs_sahpool::<ffi::WasmOsCallback>(&OpfsSAHPoolCfg::default(), true)
//!         .await
//!         .unwrap();
//!
//!     // open with opfs-sahpool vfs
//!     let mut db = std::ptr::null_mut();
//!     let ret = unsafe {
//!         ffi::sqlite3_open_v2(
//!             c"opfs-sahpool.db".as_ptr().cast(),
//!             &mut db as *mut _,
//!             ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
//!             std::ptr::null()
//!         )
//!     };
//!     assert_eq!(ffi::SQLITE_OK, ret);
//! }
//! ```
//!
//! The VFS is based on
//! [`FileSystemSyncAccessHandle`](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle)
//! read and write, and you can install the
//! [`opfs-explorer`](https://chromewebstore.google.com/detail/opfs-explorer/acndjpgkpaclldomagafnognkcgjignd)
//! plugin to browse files.

use rsqlite_vfs::{
    check_import_db,
    ffi::{
        sqlite3_file, sqlite3_filename, sqlite3_vfs, sqlite3_vfs_register, sqlite3_vfs_unregister,
        SQLITE_CANTOPEN, SQLITE_ERROR, SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN, SQLITE_IOERR,
        SQLITE_IOERR_DELETE, SQLITE_OK, SQLITE_OPEN_DELETEONCLOSE, SQLITE_OPEN_MAIN_DB,
        SQLITE_OPEN_MAIN_JOURNAL, SQLITE_OPEN_SUPER_JOURNAL, SQLITE_OPEN_WAL,
    },
    register_vfs, registered_vfs, ImportDbError, OsCallback, RegisterVfsError, SQLiteIoMethods,
    SQLiteVfs, SQLiteVfsFile, VfsAppData, VfsError, VfsFile, VfsResult, VfsStore,
};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::{
    cell::{Cell, RefCell},
    marker::PhantomData,
};

use js_sys::{Array, DataView, IteratorNext, Reflect, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemReadWriteOptions, FileSystemSyncAccessHandle,
    WorkerGlobalScope,
};

const SECTOR_SIZE: usize = 4096;
const HEADER_MAX_FILENAME_SIZE: usize = 512;
const HEADER_FLAGS_SIZE: usize = 4;
const HEADER_CORPUS_SIZE: usize = HEADER_MAX_FILENAME_SIZE + HEADER_FLAGS_SIZE;
const HEADER_OFFSET_FLAGS: usize = HEADER_MAX_FILENAME_SIZE;
const HEADER_OFFSET_DATA: usize = SECTOR_SIZE;

const PERSISTENT_FILE_TYPES: i32 =
    SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_MAIN_JOURNAL | SQLITE_OPEN_SUPER_JOURNAL | SQLITE_OPEN_WAL;

type Result<T, E = OpfsSAHError> = std::result::Result<T, E>;

fn read_write_options(at: f64) -> FileSystemReadWriteOptions {
    let options = FileSystemReadWriteOptions::new();
    options.set_at(at);
    options
}

struct SyncAccessFile {
    handle: FileSystemSyncAccessHandle,
    opaque: String,
}

struct OpfsSAHPool {
    /// Directory handle to the `.opaque` subdirectory within the VFS root.
    /// This directory holds the actual files, which have randomly-generated names.
    dh_opaque: FileSystemDirectoryHandle,
    /// A reusable buffer for reading and writing file headers.
    header_buffer: Uint8Array,
    /// A `DataView` for accessing the binary data in `header_buffer`.
    header_buffer_view: DataView,
    /// A pool of available `SyncAccessHandle`s that are not currently associated with a database file.
    available_files: RefCell<Vec<SyncAccessFile>>,
    /// Maps the user-facing database filenames to their underlying `SyncAccessFile`.
    map_filename_to_file: RefCell<HashMap<String, SyncAccessFile>>,
    /// A flag to indicate whether the VFS is currently paused.
    is_paused: Cell<bool>,
    /// A set of filenames for all currently open database connections.
    open_files: RefCell<HashSet<String>>,
    /// A tuple holding the raw pointer to the `sqlite3_vfs` struct and whether it was registered as the default.
    vfs: Cell<(*mut sqlite3_vfs, bool)>,
    random: fn(&mut [u8]),
}

impl OpfsSAHPool {
    async fn new<C: OsCallback>(options: &OpfsSAHPoolCfg) -> Result<OpfsSAHPool> {
        const OPAQUE_DIR_NAME: &str = ".opaque";

        let vfs_dir = &options.directory;
        let capacity = options.initial_capacity;
        let clear_files = options.clear_on_init;

        let create_option = FileSystemGetDirectoryOptions::new();
        create_option.set_create(true);

        let mut handle: FileSystemDirectoryHandle = JsFuture::from(
            js_sys::global()
                .dyn_into::<WorkerGlobalScope>()
                .map_err(|_| OpfsSAHError::NotSupported)?
                .navigator()
                .storage()
                .get_directory(),
        )
        .await
        .map_err(OpfsSAHError::GetDirHandle)?
        .into();

        for dir in vfs_dir.split('/').filter(|x| !x.is_empty()) {
            let next =
                JsFuture::from(handle.get_directory_handle_with_options(dir, &create_option))
                    .await
                    .map_err(OpfsSAHError::GetDirHandle)?
                    .into();
            handle = next;
        }

        let dh_opaque = JsFuture::from(
            handle.get_directory_handle_with_options(OPAQUE_DIR_NAME, &create_option),
        )
        .await
        .map_err(OpfsSAHError::GetDirHandle)?
        .into();

        let ap_body = Uint8Array::new_with_length(HEADER_CORPUS_SIZE as _);
        let dv_body = DataView::new(
            &ap_body.buffer(),
            ap_body.byte_offset() as usize,
            (ap_body.byte_length() - ap_body.byte_offset()) as usize,
        );

        let pool = Self {
            dh_opaque,
            header_buffer: ap_body,
            header_buffer_view: dv_body,
            map_filename_to_file: RefCell::new(HashMap::new()),
            available_files: RefCell::new(Vec::new()),
            is_paused: Cell::new(false),
            open_files: RefCell::new(HashSet::new()),
            vfs: Cell::new((std::ptr::null_mut(), false)),
            random: C::random,
        };

        pool.acquire_access_handles(clear_files).await?;
        pool.reserve_minimum_capacity(capacity).await?;

        Ok(pool)
    }

    async fn add_capacity(&self, n: u32) -> Result<u32> {
        for _ in 0..n {
            let opaque = rsqlite_vfs::random_name(self.random);
            let handle: FileSystemFileHandle =
                JsFuture::from(self.dh_opaque.get_file_handle_with_options(&opaque, &{
                    let options = FileSystemGetFileOptions::new();
                    options.set_create(true);
                    options
                }))
                .await
                .map_err(OpfsSAHError::GetFileHandle)?
                .into();
            let sah: FileSystemSyncAccessHandle =
                JsFuture::from(handle.create_sync_access_handle())
                    .await
                    .map_err(OpfsSAHError::CreateSyncAccessHandle)?
                    .into();
            let file = SyncAccessFile {
                handle: sah,
                opaque,
            };
            self.set_associated_filename(&file.handle, None, 0)?;
            self.available_files.borrow_mut().push(file);
        }
        Ok(self.get_capacity())
    }

    async fn reserve_minimum_capacity(&self, min: u32) -> Result<()> {
        self.add_capacity(min.saturating_sub(self.get_capacity()))
            .await?;
        Ok(())
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn reduce_capacity(&self, n: u32) -> Result<u32> {
        let mut available_files = self.available_files.borrow_mut();
        let available_length = available_files.len();
        let max_reduce = available_length.min(n as usize);
        let files = available_files.split_off(available_length - max_reduce);
        // The `RefMut` from `name2file` is explicitly dropped here to avoid holding the borrow across an `.await` point.
        drop(available_files);

        for file in files {
            file.handle.close();
            JsFuture::from(self.dh_opaque.remove_entry(&file.opaque))
                .await
                .map_err(OpfsSAHError::RemoveEntity)?;
        }

        Ok(max_reduce as u32)
    }

    fn get_capacity(&self) -> u32 {
        (self.map_filename_to_file.borrow().len() + self.available_files.borrow().len()) as u32
    }

    fn get_file_count(&self) -> u32 {
        self.map_filename_to_file.borrow().len() as u32
    }

    fn get_filenames(&self) -> Vec<String> {
        self.map_filename_to_file.borrow().keys().cloned().collect()
    }

    fn get_associated_filename(&self, sah: &FileSystemSyncAccessHandle) -> Result<Option<String>> {
        sah.read_with_buffer_source_and_options(&self.header_buffer, &read_write_options(0.0))
            .map_err(OpfsSAHError::Read)?;
        let flags = self.header_buffer_view.get_uint32(HEADER_OFFSET_FLAGS);
        if self.header_buffer.get_index(0) != 0
            && ((flags & SQLITE_OPEN_DELETEONCLOSE as u32 != 0)
                || (flags & PERSISTENT_FILE_TYPES as u32) == 0)
        {
            return Ok(None);
        }

        let name_length = self
            .header_buffer
            .to_vec()
            .iter()
            .position(|&x| x == 0)
            .unwrap_or_default();
        if name_length == 0 {
            sah.truncate_with_u32(HEADER_OFFSET_DATA as u32)
                .map_err(OpfsSAHError::Truncate)?;
            return Ok(None);
        }
        // set_associated_filename ensures that it is utf8
        let filename =
            String::from_utf8(self.header_buffer.subarray(0, name_length as u32).to_vec()).unwrap();
        Ok(Some(filename))
    }

    fn set_associated_filename(
        &self,
        sah: &FileSystemSyncAccessHandle,
        filename: Option<&str>,
        flags: i32,
    ) -> Result<()> {
        self.header_buffer_view
            .set_uint32(HEADER_OFFSET_FLAGS, flags as u32);

        if let Some(filename) = filename {
            if filename.is_empty() {
                return Err(OpfsSAHError::Generic("Filename is empty".into()));
            }
            if HEADER_MAX_FILENAME_SIZE <= filename.len() + 1 {
                return Err(OpfsSAHError::Generic(format!(
                    "Filename too long: {filename}"
                )));
            }
            self.header_buffer
                .subarray(0, filename.len() as u32)
                .copy_from(filename.as_bytes());
            self.header_buffer
                .fill(0, filename.len() as u32, HEADER_MAX_FILENAME_SIZE as u32);
        } else {
            self.header_buffer
                .fill(0, 0, HEADER_MAX_FILENAME_SIZE as u32);
            sah.truncate_with_u32(HEADER_OFFSET_DATA as u32)
                .map_err(OpfsSAHError::Truncate)?;
        }

        sah.write_with_js_u8_array_and_options(&self.header_buffer, &read_write_options(0.0))
            .map_err(OpfsSAHError::Write)?;

        Ok(())
    }

    async fn acquire_access_handles(&self, clear_files: bool) -> Result<()> {
        let iter = self.dh_opaque.entries();
        while let Ok(future) = iter.next() {
            let next: IteratorNext = JsFuture::from(future)
                .await
                .map_err(OpfsSAHError::IterHandle)?
                .into();
            if next.done() {
                break;
            }
            let array: Array = next.value().into();
            let opaque = array
                .get(0)
                .as_string()
                .ok_or_else(|| OpfsSAHError::Generic("Failed to get file's opaque name".into()))?;
            let value = array.get(1);
            let kind = Reflect::get(&value, &JsValue::from("kind"))
                .map_err(OpfsSAHError::Reflect)?
                .as_string();
            if kind.as_deref() == Some("file") {
                let handle = FileSystemFileHandle::from(value);
                let sah = JsFuture::from(handle.create_sync_access_handle())
                    .await
                    .map_err(OpfsSAHError::CreateSyncAccessHandle)?;
                let sah = FileSystemSyncAccessHandle::from(sah);
                let file = SyncAccessFile {
                    handle: sah,
                    opaque,
                };
                let clear_file = |file: SyncAccessFile| -> Result<()> {
                    self.set_associated_filename(&file.handle, None, 0)?;
                    self.available_files.borrow_mut().push(file);
                    Ok(())
                };
                if clear_files {
                    clear_file(file)?;
                } else if let Some(filename) = self.get_associated_filename(&file.handle)? {
                    self.map_filename_to_file
                        .borrow_mut()
                        .insert(filename, file);
                } else {
                    clear_file(file)?;
                }
            }
        }

        Ok(())
    }

    fn release_access_handles(&self) {
        for file in std::mem::take(&mut *self.available_files.borrow_mut())
            .into_iter()
            .chain(std::mem::take(&mut *self.map_filename_to_file.borrow_mut()).into_values())
        {
            file.handle.close();
        }
    }

    fn delete_file(&self, filename: &str) -> Result<bool> {
        let mut map_filename_to_file = self.map_filename_to_file.borrow_mut();
        let mut available_files = self.available_files.borrow_mut();

        if let Some(file) = map_filename_to_file.remove(filename) {
            available_files.push(file);
            let Some(file) = available_files.last() else {
                unreachable!();
            };
            self.set_associated_filename(&file.handle, None, 0)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn has_filename(&self, filename: &str) -> bool {
        self.map_filename_to_file.borrow().contains_key(filename)
    }

    fn with_file<E, R, F: Fn(&SyncAccessFile) -> Result<R, E>>(
        &self,
        filename: &str,
        f: F,
    ) -> Option<Result<R, E>> {
        self.map_filename_to_file.borrow().get(filename).map(f)
    }

    fn with_file_mut<E, R, F: Fn(&mut SyncAccessFile) -> Result<R, E>>(
        &self,
        filename: &str,
        f: F,
    ) -> Option<Result<R, E>> {
        self.map_filename_to_file
            .borrow_mut()
            .get_mut(filename)
            .map(f)
    }

    fn with_new_file<E, F: Fn(&SyncAccessFile) -> Result<(), E>>(
        &self,
        filename: &str,
        flags: i32,
        f: F,
    ) -> Result<Result<(), E>> {
        let mut map_filename_to_file = self.map_filename_to_file.borrow_mut();
        let mut available_files = self.available_files.borrow_mut();
        if map_filename_to_file.contains_key(filename) {
            return Err(OpfsSAHError::Generic(format!(
                "{filename} file already exists"
            )));
        }
        let file = available_files
            .pop()
            .ok_or_else(|| OpfsSAHError::Generic("No files available in the pool".into()))?;
        map_filename_to_file.insert(filename.into(), file);

        let Some(file) = map_filename_to_file.get(filename) else {
            unreachable!();
        };
        self.set_associated_filename(&file.handle, Some(filename), flags)?;
        Ok(f(file))
    }

    fn pause_vfs(&self) -> Result<()> {
        if self.is_paused.get() {
            return Ok(());
        }

        if !self.open_files.borrow().is_empty() {
            return Err(OpfsSAHError::Generic(
                "Cannot pause: files may be in use".to_string(),
            ));
        }

        let (vfs, _) = self.vfs.get();
        if !vfs.is_null() {
            unsafe {
                sqlite3_vfs_unregister(vfs);
            }
        }
        self.release_access_handles();

        self.is_paused.set(true);

        Ok(())
    }

    async fn unpause_vfs(&self) -> Result<()> {
        if !self.is_paused.get() {
            return Ok(());
        }

        self.acquire_access_handles(false).await?;

        let (vfs, make_default) = self.vfs.get();
        if vfs.is_null() {
            return Err(OpfsSAHError::Generic(
                "VFS pointer is null. Did you forget to install?".to_string(),
            ));
        }

        match unsafe { sqlite3_vfs_register(vfs, i32::from(make_default)) } {
            SQLITE_OK => {
                self.is_paused.set(false);
                Ok(())
            }
            error_code => Err(OpfsSAHError::Generic(format!(
                "Failed to register VFS (SQLite error code: {error_code})"
            ))),
        }
    }

    fn export_db(&self, filename: &str) -> Result<Vec<u8>> {
        let files = self.map_filename_to_file.borrow();
        let file = files
            .get(filename)
            .ok_or_else(|| OpfsSAHError::Generic(format!("File not found: {filename}")))?;

        let sah = &file.handle;
        let actual_size = (sah.get_size().map_err(OpfsSAHError::GetSize)?
            - HEADER_OFFSET_DATA as f64)
            .max(0.0) as usize;

        let mut data = vec![0; actual_size];
        if actual_size > 0 {
            let read = sah
                .read_with_u8_array_and_options(
                    &mut data,
                    &read_write_options(HEADER_OFFSET_DATA as f64),
                )
                .map_err(OpfsSAHError::Read)?;
            if read != actual_size as f64 {
                return Err(OpfsSAHError::Generic(format!(
                    "Expected to read {actual_size} bytes but read {read}.",
                )));
            }
        }
        Ok(data)
    }

    fn import_db(&self, filename: &str, bytes: &[u8]) -> Result<()> {
        check_import_db(bytes)?;
        self.import_db_unchecked(filename, bytes, true)
    }

    fn import_db_unchecked(&self, filename: &str, bytes: &[u8], clear_wal: bool) -> Result<()> {
        self.with_new_file(filename, SQLITE_OPEN_MAIN_DB, |file| {
            let sah = &file.handle;
            let length = bytes.len() as f64;
            let written = sah
                .write_with_u8_array_and_options(
                    bytes,
                    &read_write_options(HEADER_OFFSET_DATA as f64),
                )
                .map_err(OpfsSAHError::Write)?;

            if written != length {
                return Err(OpfsSAHError::Generic(format!(
                    "Expected to write {length} bytes but wrote {written}.",
                )));
            }

            if clear_wal {
                // forced to write back to legacy mode
                sah.write_with_u8_array_and_options(
                    &[1, 1],
                    &read_write_options((HEADER_OFFSET_DATA + 18) as f64),
                )
                .map_err(OpfsSAHError::Write)?;
            }

            Ok(())
        })?
    }
}

impl VfsFile for SyncAccessFile {
    fn read(&self, buf: &mut [u8], offset: usize) -> VfsResult<bool> {
        let n_read = self
            .handle
            .read_with_u8_array_and_options(
                buf,
                &read_write_options((HEADER_OFFSET_DATA + offset) as f64),
            )
            .map_err(OpfsSAHError::Read)
            .map_err(|err| err.vfs_err(SQLITE_IOERR))?;

        if (n_read as usize) < buf.len() {
            buf[n_read as usize..].fill(0);
            return Ok(false);
        }

        Ok(true)
    }

    fn write(&mut self, buf: &[u8], offset: usize) -> VfsResult<()> {
        let n_write = self
            .handle
            .write_with_u8_array_and_options(
                buf,
                &read_write_options((HEADER_OFFSET_DATA + offset) as f64),
            )
            .map_err(OpfsSAHError::Write)
            .map_err(|err| err.vfs_err(SQLITE_IOERR))?;

        if buf.len() != n_write as usize {
            return Err(VfsError::new(SQLITE_ERROR, "failed to write file".into()));
        }

        Ok(())
    }

    fn truncate(&mut self, size: usize) -> VfsResult<()> {
        self.handle
            .truncate_with_f64((HEADER_OFFSET_DATA + size) as f64)
            .map_err(OpfsSAHError::Truncate)
            .map_err(|err| err.vfs_err(SQLITE_IOERR))
    }

    fn flush(&mut self) -> VfsResult<()> {
        FileSystemSyncAccessHandle::flush(&self.handle)
            .map_err(OpfsSAHError::Flush)
            .map_err(|err| err.vfs_err(SQLITE_IOERR))
    }

    fn size(&self) -> VfsResult<usize> {
        Ok(self
            .handle
            .get_size()
            .map_err(OpfsSAHError::GetSize)
            .map_err(|err| err.vfs_err(SQLITE_IOERR))? as usize
            - HEADER_OFFSET_DATA)
    }
}

type SyncAccessHandleAppData = OpfsSAHPool;

struct SyncAccessHandleStore;

impl VfsStore<SyncAccessFile, SyncAccessHandleAppData> for SyncAccessHandleStore {
    fn add_file(vfs: *mut sqlite3_vfs, filename: &str, flags: i32) -> VfsResult<()> {
        let pool = unsafe { Self::app_data(vfs) };

        pool.with_new_file(filename, flags, |_| Ok(()))
            .map_err(|err| err.vfs_err(SQLITE_CANTOPEN))?
    }

    fn contains_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<bool> {
        let pool = unsafe { Self::app_data(vfs) };
        Ok(pool.has_filename(file))
    }

    fn delete_file(vfs: *mut sqlite3_vfs, file: &str) -> VfsResult<()> {
        let pool = unsafe { Self::app_data(vfs) };
        pool.delete_file(file)
            .map_err(|err| err.vfs_err(SQLITE_IOERR_DELETE))?;
        Ok(())
    }

    fn with_file<F: Fn(&SyncAccessFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let name = unsafe { vfs_file.name() };
        let pool = unsafe { Self::app_data(vfs_file.vfs) };
        pool.with_file(name, f)
            .ok_or_else(|| VfsError::new(SQLITE_IOERR, format!("{name} not found")))?
    }

    fn with_file_mut<F: Fn(&mut SyncAccessFile) -> VfsResult<i32>>(
        vfs_file: &SQLiteVfsFile,
        f: F,
    ) -> VfsResult<i32> {
        let name = unsafe { vfs_file.name() };
        let pool = unsafe { Self::app_data(vfs_file.vfs) };
        pool.with_file_mut(name, f)
            .ok_or_else(|| VfsError::new(SQLITE_IOERR, format!("{name} not found")))?
    }
}

struct SyncAccessHandleIoMethods;

impl SQLiteIoMethods for SyncAccessHandleIoMethods {
    type File = SyncAccessFile;
    type AppData = SyncAccessHandleAppData;
    type Store = SyncAccessHandleStore;

    const VERSION: ::std::os::raw::c_int = 1;

    unsafe extern "C" fn xSectorSize(_pFile: *mut sqlite3_file) -> ::std::os::raw::c_int {
        SECTOR_SIZE as i32
    }

    unsafe extern "C" fn xCheckReservedLock(
        _pFile: *mut sqlite3_file,
        pResOut: *mut ::std::os::raw::c_int,
    ) -> ::std::os::raw::c_int {
        *pResOut = 1;
        SQLITE_OK
    }

    unsafe extern "C" fn xDeviceCharacteristics(
        _pFile: *mut sqlite3_file,
    ) -> ::std::os::raw::c_int {
        SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN
    }

    unsafe extern "C" fn xClose(pFile: *mut sqlite3_file) -> ::std::os::raw::c_int {
        let vfs_file = SQLiteVfsFile::from_file(pFile);
        // The VFS file handle will be dropped, so we must clone the filename to use it after the drop.
        let file = vfs_file.name().to_string();
        let app_data = SyncAccessHandleStore::app_data(vfs_file.vfs);
        let ret = Self::xCloseImpl(pFile);
        if ret == SQLITE_OK {
            let exist = app_data.open_files.borrow_mut().remove(&file);
            debug_assert!(exist, "DB closed without open");
        }
        ret
    }
}

struct SyncAccessHandleVfs<C>(PhantomData<C>);

impl<C> SQLiteVfs<SyncAccessHandleIoMethods> for SyncAccessHandleVfs<C>
where
    C: OsCallback,
{
    const VERSION: ::std::os::raw::c_int = 2;
    const MAX_PATH_SIZE: ::std::os::raw::c_int = HEADER_MAX_FILENAME_SIZE as _;

    unsafe extern "C" fn xOpen(
        pVfs: *mut sqlite3_vfs,
        zName: sqlite3_filename,
        pFile: *mut sqlite3_file,
        flags: ::std::os::raw::c_int,
        pOutFlags: *mut ::std::os::raw::c_int,
    ) -> ::std::os::raw::c_int {
        let ret = Self::xOpenImpl(pVfs, zName, pFile, flags, pOutFlags);
        if ret == SQLITE_OK {
            let app_data = SyncAccessHandleStore::app_data(pVfs);

            // At this point, SQLite has allocated the pFile structure for us.
            let vfs_file = SQLiteVfsFile::from_file(pFile);
            app_data
                .open_files
                .borrow_mut()
                .insert(vfs_file.name().into());
        }
        ret
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

/// Build `OpfsSAHPoolCfg`
pub struct OpfsSAHPoolCfgBuilder(OpfsSAHPoolCfg);

impl OpfsSAHPoolCfgBuilder {
    pub fn new() -> Self {
        Self(OpfsSAHPoolCfg::default())
    }

    /// The SQLite VFS name under which this pool's VFS is registered.
    pub fn vfs_name(mut self, name: &str) -> Self {
        self.0.vfs_name = name.into();
        self
    }

    /// Specifies the OPFS directory name in which to store metadata for the `vfs_name`
    pub fn directory(mut self, directory: &str) -> Self {
        self.0.directory = directory.into();
        self
    }

    /// If truthy, contents and filename mapping are removed from each SAH
    /// as it is acquired during initalization of the VFS, leaving the VFS's
    /// storage in a pristine state. Use this only for databases which need not
    /// survive a page reload.
    pub fn clear_on_init(mut self, set: bool) -> Self {
        self.0.clear_on_init = set;
        self
    }

    /// Specifies the default capacity of the VFS, i.e. the number of files
    /// it may contain.
    pub fn initial_capacity(mut self, cap: u32) -> Self {
        self.0.initial_capacity = cap;
        self
    }

    /// Build `OpfsSAHPoolCfg`.
    pub fn build(self) -> OpfsSAHPoolCfg {
        self.0
    }
}

impl Default for OpfsSAHPoolCfgBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// `OpfsSAHPool` options
pub struct OpfsSAHPoolCfg {
    /// The SQLite VFS name under which this pool's VFS is registered.
    pub vfs_name: String,
    /// Specifies the OPFS directory name in which to store metadata for the `vfs_name`.
    pub directory: String,
    /// If truthy, contents and filename mapping are removed from each SAH
    /// as it is acquired during initalization of the VFS, leaving the VFS's
    /// storage in a pristine state. Use this only for databases which need not
    /// survive a page reload.
    pub clear_on_init: bool,
    /// Specifies the default capacity of the VFS, i.e. the number of files
    /// it may contain.
    pub initial_capacity: u32,
}

impl Default for OpfsSAHPoolCfg {
    fn default() -> Self {
        Self {
            vfs_name: "opfs-sahpool".into(),
            directory: ".opfs-sahpool".into(),
            clear_on_init: false,
            initial_capacity: 6,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum OpfsSAHError {
    #[error(transparent)]
    Vfs(#[from] RegisterVfsError),
    #[error(transparent)]
    ImportDb(#[from] ImportDbError),
    #[error("This vfs is only available in dedicated worker")]
    NotSupported,
    #[error("An error occurred while getting the directory handle")]
    GetDirHandle(JsValue),
    #[error("An error occurred while getting the file handle")]
    GetFileHandle(JsValue),
    #[error("An error occurred while creating sync access handle")]
    CreateSyncAccessHandle(JsValue),
    #[error("An error occurred while iterating")]
    IterHandle(JsValue),
    #[error("An error occurred while getting filename")]
    GetPath(JsValue),
    #[error("An error occurred while removing entity")]
    RemoveEntity(JsValue),
    #[error("An error occurred while getting size")]
    GetSize(JsValue),
    #[error("An error occurred while reading data")]
    Read(JsValue),
    #[error("An error occurred while writing data")]
    Write(JsValue),
    #[error("An error occurred while flushing data")]
    Flush(JsValue),
    #[error("An error occurred while truncating data")]
    Truncate(JsValue),
    #[error("An error occurred while getting data using reflect")]
    Reflect(JsValue),
    #[error("Generic error: {0}")]
    Generic(String),
}

impl OpfsSAHError {
    fn vfs_err(&self, code: i32) -> VfsError {
        VfsError::new(code, format!("{self}"))
    }
}

/// SAHPoolVfs management tool.
pub struct OpfsSAHPoolUtil {
    pool: &'static VfsAppData<SyncAccessHandleAppData>,
}

impl OpfsSAHPoolUtil {
    /// Returns the number of files currently contained in the SAH pool.
    pub fn get_capacity(&self) -> u32 {
        self.pool.get_capacity()
    }

    /// Adds n entries to the current pool.
    pub async fn add_capacity(&self, n: u32) -> Result<u32> {
        self.pool.add_capacity(n).await
    }

    /// Removes up to n entries from the pool, with the caveat that
    /// it can only remove currently-unused entries.
    pub async fn reduce_capacity(&self, n: u32) -> Result<u32> {
        self.pool.reduce_capacity(n).await
    }

    /// Removes up to n entries from the pool, with the caveat that it can only
    /// remove currently-unused entries.
    pub async fn reserve_minimum_capacity(&self, min: u32) -> Result<()> {
        self.pool.reserve_minimum_capacity(min).await
    }
}

impl OpfsSAHPoolUtil {
    /// Imports the contents of an SQLite database, provided as a byte array
    /// under the given name, overwriting any existing content.
    ///
    /// If the database is imported with WAL mode enabled,
    /// it will be forced to write back to legacy mode, see
    /// <https://sqlite.org/forum/forumpost/67882c5b04>.
    ///
    /// If the imported database is encrypted, use `import_db_unchecked` instead.
    pub fn import_db(&self, filename: &str, bytes: &[u8]) -> Result<()> {
        self.pool.import_db(filename, bytes)
    }

    /// `import_db` without checking, can be used to import encrypted database.
    pub fn import_db_unchecked(&self, filename: &str, bytes: &[u8]) -> Result<()> {
        self.pool.import_db_unchecked(filename, bytes, false)
    }

    /// Export the database.
    pub fn export_db(&self, filename: &str) -> Result<Vec<u8>> {
        self.pool.export_db(filename)
    }

    /// Delete the specified database, make sure that the database is closed.
    pub fn delete_db(&self, filename: &str) -> Result<bool> {
        self.pool.delete_file(filename)
    }

    /// Delete all database, make sure that all database is closed.
    pub async fn clear_all(&self) -> Result<()> {
        self.pool.release_access_handles();
        self.pool.acquire_access_handles(true).await?;
        Ok(())
    }

    /// Does the database exists.
    pub fn exists(&self, filename: &str) -> Result<bool> {
        Ok(self.pool.has_filename(filename))
    }

    /// List all files.
    pub fn list(&self) -> Vec<String> {
        self.pool.get_filenames()
    }

    /// Number of files.
    pub fn count(&self) -> u32 {
        self.pool.get_file_count()
    }

    /// "Pauses" this VFS by unregistering it from SQLite and
    /// relinquishing all open SAHs, leaving the associated files
    /// intact. If this instance is already paused, this is a
    /// no-op. Returns a Result.
    ///
    /// This method returns an error if SQLite has any opened file handles
    /// hosted by this VFS, as the alternative would be to invoke
    /// Undefined Behavior by closing file handles out from under the
    /// library. Similarly, automatically closing any database handles
    /// opened by this VFS would invoke Undefined Behavior in
    /// downstream code which is holding those pointers.
    ///
    /// If this method returns and error due to open file handles then it has
    /// no side effects. If the OPFS API returns an error while closing handles
    /// then the VFS is left in an undefined state.
    pub fn pause_vfs(&self) -> Result<()> {
        self.pool.pause_vfs()
    }

    /// "Unpauses" this VFS, reacquiring all SAH's and (if successful)
    /// re-registering it with SQLite. This is a no-op if the VFS is
    /// not currently paused.
    ///
    /// The returned a Result. See acquire_access_handles() for how it
    /// behaves if it returns an error due to SAH acquisition failure.
    pub async fn unpause_vfs(&self) -> Result<()> {
        self.pool.unpause_vfs().await
    }

    /// Check if VFS is paused.
    pub fn is_paused(&self) -> bool {
        self.pool.is_paused.get()
    }
}

/// Register `opfs-sahpool` vfs and return a management tool which can be used
/// to perform basic administration of the file pool.
///
/// If the vfs corresponding to `options.vfs_name` has been registered,
/// only return a management tool without register.
pub async fn install<C: OsCallback>(
    options: &OpfsSAHPoolCfg,
    default_vfs: bool,
) -> Result<OpfsSAHPoolUtil> {
    static REGISTER_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = REGISTER_GUARD.lock().await;

    let vfs = match registered_vfs(&options.vfs_name)? {
        Some(vfs) => vfs,
        None => register_vfs::<SyncAccessHandleIoMethods, SyncAccessHandleVfs<C>>(
            &options.vfs_name,
            OpfsSAHPool::new::<C>(options).await?,
            default_vfs,
        )?,
    };

    let pool = unsafe { SyncAccessHandleStore::app_data(vfs) };
    pool.vfs.set((vfs, default_vfs));

    Ok(OpfsSAHPoolUtil { pool })
}

#[cfg(test)]
mod tests {
    use super::{
        OpfsSAHPool, OpfsSAHPoolCfgBuilder, SyncAccessFile, SyncAccessHandleAppData,
        SyncAccessHandleStore,
    };
    use rsqlite_vfs::{test_suite::test_vfs_store, VfsAppData};
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    async fn test_opfs_vfs_store() {
        let data = OpfsSAHPool::new::<sqlite_wasm_rs::WasmOsCallback>(
            &OpfsSAHPoolCfgBuilder::new()
                .directory("test_opfs_suite")
                .build(),
        )
        .await
        .unwrap();

        test_vfs_store::<SyncAccessHandleAppData, SyncAccessFile, SyncAccessHandleStore>(
            VfsAppData::new(data),
        )
        .unwrap();
    }
}
