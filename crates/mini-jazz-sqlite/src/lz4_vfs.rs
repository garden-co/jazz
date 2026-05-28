use rusqlite::ffi;
use rusqlite::{Connection, OpenFlags};
use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::{self, MaybeUninit};
use std::os::raw::{c_char, c_int, c_void};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Once;

const VFS_NAME: &str = "mini_jazz_lz4";
const MAGIC: &[u8; 8] = b"MJLZ4V1\0";
const HEADER_LEN: u64 = 24;
const RECORD_HEADER_LEN: usize = 16;
const DEFAULT_PAGE_SIZE: usize = 4096;

static REGISTER: Once = Once::new();
static mut REGISTER_RESULT: c_int = ffi::SQLITE_OK;

pub(crate) fn open(path: PathBuf) -> crate::Result<Connection> {
    register()?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    Ok(Connection::open_with_flags_and_vfs(path, flags, VFS_NAME)?)
}

fn register() -> crate::Result<()> {
    REGISTER.call_once(|| unsafe {
        REGISTER_RESULT = register_inner();
    });
    let result = unsafe { REGISTER_RESULT };
    if result == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(crate::Error::new(format!(
            "failed to register lz4 sqlite vfs: {result}"
        )))
    }
}

unsafe fn register_inner() -> c_int {
    let default_vfs = ffi::sqlite3_vfs_find(ptr::null());
    if default_vfs.is_null() {
        return ffi::SQLITE_ERROR;
    }
    let name = CString::new(VFS_NAME).expect("static vfs name");
    let default = &*default_vfs;
    let vfs = Box::new(ffi::sqlite3_vfs {
        iVersion: default.iVersion,
        szOsFile: mem::size_of::<Lz4File>() as c_int,
        mxPathname: default.mxPathname,
        pNext: ptr::null_mut(),
        zName: name.into_raw(),
        pAppData: default_vfs.cast(),
        xOpen: Some(vfs_open),
        xDelete: Some(vfs_delete),
        xAccess: Some(vfs_access),
        xFullPathname: Some(vfs_full_pathname),
        xDlOpen: default.xDlOpen,
        xDlError: default.xDlError,
        xDlSym: default.xDlSym,
        xDlClose: default.xDlClose,
        xRandomness: default.xRandomness,
        xSleep: default.xSleep,
        xCurrentTime: default.xCurrentTime,
        xGetLastError: default.xGetLastError,
        xCurrentTimeInt64: default.xCurrentTimeInt64,
        xSetSystemCall: default.xSetSystemCall,
        xGetSystemCall: default.xGetSystemCall,
        xNextSystemCall: default.xNextSystemCall,
    });
    ffi::sqlite3_vfs_register(Box::into_raw(vfs), 0)
}

#[repr(C)]
struct Lz4File {
    base: ffi::sqlite3_file,
    kind: Lz4FileKind,
}

enum Lz4FileKind {
    Compressed(CompressedMainDb),
    Passthrough(PassthroughFile),
}

struct PassthroughFile {
    inner: Box<[MaybeUninit<u8>]>,
}

struct CompressedMainDb {
    file: File,
    logical_size: u64,
    page_size: usize,
    pages: BTreeMap<u64, Vec<u8>>,
}

unsafe extern "C" fn vfs_open(
    vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    out_file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    if z_name.is_null() {
        return passthrough_open(vfs, z_name, out_file, flags, out_flags);
    }
    let path = match c_path(z_name) {
        Some(path) => path,
        None => return ffi::SQLITE_CANTOPEN,
    };
    if flags & ffi::SQLITE_OPEN_MAIN_DB == 0 {
        return passthrough_open(vfs, z_name, out_file, flags, out_flags);
    }
    match CompressedMainDb::open(&path, flags) {
        Ok(db) => {
            ptr::write(
                out_file.cast::<Lz4File>(),
                Lz4File {
                    base: ffi::sqlite3_file {
                        pMethods: &COMPRESSED_IO_METHODS,
                    },
                    kind: Lz4FileKind::Compressed(db),
                },
            );
            if !out_flags.is_null() {
                *out_flags = flags;
            }
            ffi::SQLITE_OK
        }
        Err(_) => ffi::SQLITE_CANTOPEN,
    }
}

unsafe fn passthrough_open(
    vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    out_file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let default_vfs = (*vfs).pAppData.cast::<ffi::sqlite3_vfs>();
    let default = &*default_vfs;
    let mut inner = Box::<[u8]>::new_uninit_slice(default.szOsFile as usize);
    let result = (default.xOpen.expect("default vfs xOpen"))(
        default_vfs,
        z_name,
        inner.as_mut_ptr().cast::<ffi::sqlite3_file>(),
        flags,
        out_flags,
    );
    if result != ffi::SQLITE_OK {
        return result;
    }
    ptr::write(
        out_file.cast::<Lz4File>(),
        Lz4File {
            base: ffi::sqlite3_file {
                pMethods: &PASSTHROUGH_IO_METHODS,
            },
            kind: Lz4FileKind::Passthrough(PassthroughFile { inner }),
        },
    );
    ffi::SQLITE_OK
}

unsafe extern "C" fn vfs_delete(
    vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    let default_vfs = (*vfs).pAppData.cast::<ffi::sqlite3_vfs>();
    ((*default_vfs).xDelete.expect("default vfs xDelete"))(default_vfs, z_name, sync_dir)
}

unsafe extern "C" fn vfs_access(
    vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    let default_vfs = (*vfs).pAppData.cast::<ffi::sqlite3_vfs>();
    ((*default_vfs).xAccess.expect("default vfs xAccess"))(default_vfs, z_name, flags, out)
}

unsafe extern "C" fn vfs_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    out_len: c_int,
    out: *mut c_char,
) -> c_int {
    let default_vfs = (*vfs).pAppData.cast::<ffi::sqlite3_vfs>();
    ((*default_vfs)
        .xFullPathname
        .expect("default vfs xFullPathname"))(default_vfs, z_name, out_len, out)
}

static COMPRESSED_IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(compressed_close),
    xRead: Some(compressed_read),
    xWrite: Some(compressed_write),
    xTruncate: Some(compressed_truncate),
    xSync: Some(compressed_sync),
    xFileSize: Some(compressed_file_size),
    xLock: Some(compressed_lock),
    xUnlock: Some(compressed_unlock),
    xCheckReservedLock: Some(compressed_check_reserved_lock),
    xFileControl: Some(compressed_file_control),
    xSectorSize: Some(compressed_sector_size),
    xDeviceCharacteristics: Some(compressed_device_characteristics),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

static PASSTHROUGH_IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(passthrough_close),
    xRead: Some(passthrough_read),
    xWrite: Some(passthrough_write),
    xTruncate: Some(passthrough_truncate),
    xSync: Some(passthrough_sync),
    xFileSize: Some(passthrough_file_size),
    xLock: Some(passthrough_lock),
    xUnlock: Some(passthrough_unlock),
    xCheckReservedLock: Some(passthrough_check_reserved_lock),
    xFileControl: Some(passthrough_file_control),
    xSectorSize: Some(passthrough_sector_size),
    xDeviceCharacteristics: Some(passthrough_device_characteristics),
    xShmMap: Some(passthrough_shm_map),
    xShmLock: Some(passthrough_shm_lock),
    xShmBarrier: Some(passthrough_shm_barrier),
    xShmUnmap: Some(passthrough_shm_unmap),
    xFetch: Some(passthrough_fetch),
    xUnfetch: Some(passthrough_unfetch),
};

impl CompressedMainDb {
    fn open(path: &Path, flags: c_int) -> std::io::Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true);
        if flags & ffi::SQLITE_OPEN_READWRITE != 0 {
            options.write(true);
        }
        if flags & ffi::SQLITE_OPEN_CREATE != 0 {
            options.create(true);
        }
        let mut file = options.open(path)?;
        let metadata_len = file.metadata()?.len();
        if metadata_len < HEADER_LEN {
            file.set_len(0)?;
            write_header(&mut file, DEFAULT_PAGE_SIZE, 0)?;
            return Ok(Self {
                file,
                logical_size: 0,
                page_size: DEFAULT_PAGE_SIZE,
                pages: BTreeMap::new(),
            });
        }
        let mut header = [0; HEADER_LEN as usize];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header)?;
        if &header[0..8] != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "not a mini jazz lz4 sqlite file",
            ));
        }
        let page_size = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let logical_size = u64::from_le_bytes(header[12..20].try_into().unwrap());
        let mut pages = BTreeMap::new();
        file.seek(SeekFrom::Start(HEADER_LEN))?;
        loop {
            let mut record_header = [0; RECORD_HEADER_LEN];
            match file.read_exact(&mut record_header) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err),
            }
            let page_no = u64::from_le_bytes(record_header[0..8].try_into().unwrap());
            let original_len = u32::from_le_bytes(record_header[8..12].try_into().unwrap());
            let compressed_len = u32::from_le_bytes(record_header[12..16].try_into().unwrap());
            if original_len as usize != page_size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected compressed sqlite page size",
                ));
            }
            let mut compressed = vec![0; compressed_len as usize];
            file.read_exact(&mut compressed)?;
            let page = lz4_flex::decompress_size_prepended(&compressed).map_err(|err| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
            })?;
            if page.len() != page_size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "decoded sqlite page had wrong size",
                ));
            }
            pages.insert(page_no, page);
        }
        Ok(Self {
            file,
            logical_size,
            page_size,
            pages,
        })
    }

    fn read_at(&self, out: &mut [u8], offset: u64) -> c_int {
        out.fill(0);
        let end = offset.saturating_add(out.len() as u64);
        let short = end > self.logical_size;
        let mut copied = 0;
        while copied < out.len() {
            let absolute = offset + copied as u64;
            let page_no = absolute / self.page_size as u64;
            let page_offset = (absolute % self.page_size as u64) as usize;
            let len = (self.page_size - page_offset).min(out.len() - copied);
            if let Some(page) = self.pages.get(&page_no) {
                out[copied..copied + len].copy_from_slice(&page[page_offset..page_offset + len]);
            }
            copied += len;
        }
        if short {
            ffi::SQLITE_IOERR_SHORT_READ
        } else {
            ffi::SQLITE_OK
        }
    }

    fn write_at(&mut self, input: &[u8], offset: u64) -> std::io::Result<()> {
        let mut copied = 0;
        while copied < input.len() {
            let absolute = offset + copied as u64;
            let page_no = absolute / self.page_size as u64;
            let page_offset = (absolute % self.page_size as u64) as usize;
            let len = (self.page_size - page_offset).min(input.len() - copied);
            let mut page = self
                .pages
                .remove(&page_no)
                .unwrap_or_else(|| vec![0; self.page_size]);
            page[page_offset..page_offset + len].copy_from_slice(&input[copied..copied + len]);
            self.append_page_record(page_no, &page)?;
            self.pages.insert(page_no, page);
            copied += len;
        }
        self.logical_size = self.logical_size.max(offset + input.len() as u64);
        write_header(&mut self.file, self.page_size, self.logical_size)
    }

    fn truncate(&mut self, size: u64) -> std::io::Result<()> {
        self.logical_size = size;
        let keep_pages = if size == 0 {
            0
        } else {
            ((size - 1) / self.page_size as u64) + 1
        };
        self.pages.retain(|page_no, _| *page_no < keep_pages);
        if let Some(last_page) = keep_pages.checked_sub(1) {
            let used = (size % self.page_size as u64) as usize;
            if used != 0 {
                if let Some(page) = self.pages.get_mut(&last_page) {
                    page[used..].fill(0);
                }
            }
        }
        write_header(&mut self.file, self.page_size, self.logical_size)
    }

    fn compact(&mut self) -> std::io::Result<()> {
        self.file.set_len(0)?;
        write_header(&mut self.file, self.page_size, self.logical_size)?;
        let pages = self
            .pages
            .iter()
            .map(|(page_no, page)| (*page_no, page.clone()))
            .collect::<Vec<_>>();
        for (page_no, page) in pages {
            self.append_page_record(page_no, &page)?;
        }
        Ok(())
    }

    fn append_page_record(&mut self, page_no: u64, page: &[u8]) -> std::io::Result<()> {
        let compressed = lz4_flex::compress_prepend_size(page);
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&page_no.to_le_bytes())?;
        self.file.write_all(&(page.len() as u32).to_le_bytes())?;
        self.file
            .write_all(&(compressed.len() as u32).to_le_bytes())?;
        self.file.write_all(&compressed)?;
        Ok(())
    }
}

fn write_header(file: &mut File, page_size: usize, logical_size: u64) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(MAGIC)?;
    file.write_all(&(page_size as u32).to_le_bytes())?;
    file.write_all(&logical_size.to_le_bytes())?;
    file.write_all(&0_u32.to_le_bytes())?;
    Ok(())
}

unsafe fn wrapper<'a>(file: *mut ffi::sqlite3_file) -> &'a mut Lz4File {
    &mut *file.cast::<Lz4File>()
}

unsafe fn passthrough(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    match &mut wrapper(file).kind {
        Lz4FileKind::Passthrough(inner) => inner.inner.as_mut_ptr().cast::<ffi::sqlite3_file>(),
        Lz4FileKind::Compressed(_) => ptr::null_mut(),
    }
}

unsafe fn passthrough_methods(file: *mut ffi::sqlite3_file) -> *const ffi::sqlite3_io_methods {
    (*passthrough(file)).pMethods
}

unsafe extern "C" fn compressed_close(file: *mut ffi::sqlite3_file) -> c_int {
    ptr::drop_in_place(file.cast::<Lz4File>());
    ffi::SQLITE_OK
}

unsafe extern "C" fn compressed_read(
    file: *mut ffi::sqlite3_file,
    out: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let Lz4FileKind::Compressed(db) = &mut wrapper(file).kind else {
        return ffi::SQLITE_IOERR_READ;
    };
    let out = std::slice::from_raw_parts_mut(out.cast::<u8>(), amount as usize);
    db.read_at(out, offset as u64)
}

unsafe extern "C" fn compressed_write(
    file: *mut ffi::sqlite3_file,
    input: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let Lz4FileKind::Compressed(db) = &mut wrapper(file).kind else {
        return ffi::SQLITE_IOERR_WRITE;
    };
    let input = std::slice::from_raw_parts(input.cast::<u8>(), amount as usize);
    match db.write_at(input, offset as u64) {
        Ok(()) => ffi::SQLITE_OK,
        Err(_) => ffi::SQLITE_IOERR_WRITE,
    }
}

unsafe extern "C" fn compressed_truncate(
    file: *mut ffi::sqlite3_file,
    size: ffi::sqlite3_int64,
) -> c_int {
    let Lz4FileKind::Compressed(db) = &mut wrapper(file).kind else {
        return ffi::SQLITE_IOERR_TRUNCATE;
    };
    match db.truncate(size as u64) {
        Ok(()) => ffi::SQLITE_OK,
        Err(_) => ffi::SQLITE_IOERR_TRUNCATE,
    }
}

unsafe extern "C" fn compressed_sync(file: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    let Lz4FileKind::Compressed(db) = &mut wrapper(file).kind else {
        return ffi::SQLITE_IOERR_FSYNC;
    };
    match db.compact().and_then(|()| db.file.sync_all()) {
        Ok(()) => ffi::SQLITE_OK,
        Err(_) => ffi::SQLITE_IOERR_FSYNC,
    }
}

unsafe extern "C" fn compressed_file_size(
    file: *mut ffi::sqlite3_file,
    out: *mut ffi::sqlite3_int64,
) -> c_int {
    let Lz4FileKind::Compressed(db) = &mut wrapper(file).kind else {
        return ffi::SQLITE_IOERR_FSTAT;
    };
    *out = db.logical_size as ffi::sqlite3_int64;
    ffi::SQLITE_OK
}

unsafe extern "C" fn compressed_lock(_file: *mut ffi::sqlite3_file, _lock: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn compressed_unlock(_file: *mut ffi::sqlite3_file, _lock: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn compressed_check_reserved_lock(
    _file: *mut ffi::sqlite3_file,
    out: *mut c_int,
) -> c_int {
    *out = 0;
    ffi::SQLITE_OK
}

unsafe extern "C" fn compressed_file_control(
    _file: *mut ffi::sqlite3_file,
    _op: c_int,
    _arg: *mut c_void,
) -> c_int {
    ffi::SQLITE_NOTFOUND
}

unsafe extern "C" fn compressed_sector_size(_file: *mut ffi::sqlite3_file) -> c_int {
    DEFAULT_PAGE_SIZE as c_int
}

unsafe extern "C" fn compressed_device_characteristics(_file: *mut ffi::sqlite3_file) -> c_int {
    0
}

unsafe extern "C" fn passthrough_close(file: *mut ffi::sqlite3_file) -> c_int {
    let inner = passthrough(file);
    let result = ((*passthrough_methods(file)).xClose.expect("inner xClose"))(inner);
    ptr::drop_in_place(file.cast::<Lz4File>());
    result
}

unsafe extern "C" fn passthrough_read(
    file: *mut ffi::sqlite3_file,
    out: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    ((*passthrough_methods(file)).xRead.expect("inner xRead"))(
        passthrough(file),
        out,
        amount,
        offset,
    )
}

unsafe extern "C" fn passthrough_write(
    file: *mut ffi::sqlite3_file,
    input: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    ((*passthrough_methods(file)).xWrite.expect("inner xWrite"))(
        passthrough(file),
        input,
        amount,
        offset,
    )
}

unsafe extern "C" fn passthrough_truncate(
    file: *mut ffi::sqlite3_file,
    size: ffi::sqlite3_int64,
) -> c_int {
    ((*passthrough_methods(file))
        .xTruncate
        .expect("inner xTruncate"))(passthrough(file), size)
}

unsafe extern "C" fn passthrough_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    ((*passthrough_methods(file)).xSync.expect("inner xSync"))(passthrough(file), flags)
}

unsafe extern "C" fn passthrough_file_size(
    file: *mut ffi::sqlite3_file,
    out: *mut ffi::sqlite3_int64,
) -> c_int {
    ((*passthrough_methods(file))
        .xFileSize
        .expect("inner xFileSize"))(passthrough(file), out)
}

unsafe extern "C" fn passthrough_lock(file: *mut ffi::sqlite3_file, lock: c_int) -> c_int {
    ((*passthrough_methods(file)).xLock.expect("inner xLock"))(passthrough(file), lock)
}

unsafe extern "C" fn passthrough_unlock(file: *mut ffi::sqlite3_file, lock: c_int) -> c_int {
    ((*passthrough_methods(file)).xUnlock.expect("inner xUnlock"))(passthrough(file), lock)
}

unsafe extern "C" fn passthrough_check_reserved_lock(
    file: *mut ffi::sqlite3_file,
    out: *mut c_int,
) -> c_int {
    ((*passthrough_methods(file))
        .xCheckReservedLock
        .expect("inner xCheckReservedLock"))(passthrough(file), out)
}

unsafe extern "C" fn passthrough_file_control(
    file: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    ((*passthrough_methods(file))
        .xFileControl
        .expect("inner xFileControl"))(passthrough(file), op, arg)
}

unsafe extern "C" fn passthrough_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    ((*passthrough_methods(file))
        .xSectorSize
        .expect("inner xSectorSize"))(passthrough(file))
}

unsafe extern "C" fn passthrough_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    ((*passthrough_methods(file))
        .xDeviceCharacteristics
        .expect("inner xDeviceCharacteristics"))(passthrough(file))
}

unsafe extern "C" fn passthrough_shm_map(
    file: *mut ffi::sqlite3_file,
    page: c_int,
    page_size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    match (*passthrough_methods(file)).xShmMap {
        Some(method) => method(passthrough(file), page, page_size, extend, out),
        None => ffi::SQLITE_IOERR_SHMMAP,
    }
}

unsafe extern "C" fn passthrough_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    match (*passthrough_methods(file)).xShmLock {
        Some(method) => method(passthrough(file), offset, n, flags),
        None => ffi::SQLITE_IOERR_SHMLOCK,
    }
}

unsafe extern "C" fn passthrough_shm_barrier(file: *mut ffi::sqlite3_file) {
    if let Some(method) = (*passthrough_methods(file)).xShmBarrier {
        method(passthrough(file));
    }
}

unsafe extern "C" fn passthrough_shm_unmap(
    file: *mut ffi::sqlite3_file,
    delete_flag: c_int,
) -> c_int {
    match (*passthrough_methods(file)).xShmUnmap {
        Some(method) => method(passthrough(file), delete_flag),
        None => ffi::SQLITE_OK,
    }
}

unsafe extern "C" fn passthrough_fetch(
    file: *mut ffi::sqlite3_file,
    offset: ffi::sqlite3_int64,
    amount: c_int,
    out: *mut *mut c_void,
) -> c_int {
    match (*passthrough_methods(file)).xFetch {
        Some(method) => method(passthrough(file), offset, amount, out),
        None => {
            *out = ptr::null_mut();
            ffi::SQLITE_OK
        }
    }
}

unsafe extern "C" fn passthrough_unfetch(
    file: *mut ffi::sqlite3_file,
    offset: ffi::sqlite3_int64,
    pointer: *mut c_void,
) -> c_int {
    match (*passthrough_methods(file)).xUnfetch {
        Some(method) => method(passthrough(file), offset, pointer),
        None => ffi::SQLITE_OK,
    }
}

unsafe fn c_path(name: *const c_char) -> Option<PathBuf> {
    let bytes = CStr::from_ptr(name).to_bytes();
    if bytes.is_empty() {
        return None;
    }
    Some(PathBuf::from(String::from_utf8_lossy(bytes).into_owned()))
}
