use std::cell::RefCell;
use std::rc::Rc;

use crate::BTreeError;

#[cfg(target_arch = "wasm32")]
use serde::Serialize;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsCast;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;

pub trait SyncFile {
    fn len(&self) -> Result<u64, BTreeError>;
    fn is_empty(&self) -> Result<bool, BTreeError> {
        self.len().map(|len| len == 0)
    }
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError>;
    fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError>;
    fn truncate(&self, len: u64) -> Result<(), BTreeError>;
    fn flush(&self) -> Result<(), BTreeError>;
}

#[derive(Clone, Default, Debug)]
pub struct MemoryFile {
    inner: Rc<RefCell<Vec<u8>>>,
}

impl MemoryFile {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SyncFile for MemoryFile {
    fn len(&self) -> Result<u64, BTreeError> {
        Ok(self.inner.borrow().len() as u64)
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
        let offset = usize::try_from(offset)
            .map_err(|_| BTreeError::Io("offset does not fit in usize".to_string()))?;
        let end = offset
            .checked_add(buf.len())
            .ok_or_else(|| BTreeError::Io("read overflow".to_string()))?;

        let data = self.inner.borrow();
        if end > data.len() {
            return Err(BTreeError::Io(format!(
                "unexpected eof: read {}..{} from {}",
                offset,
                end,
                data.len()
            )));
        }
        buf.copy_from_slice(&data[offset..end]);
        Ok(())
    }

    fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
        let offset = usize::try_from(offset)
            .map_err(|_| BTreeError::Io("offset does not fit in usize".to_string()))?;
        let end = offset
            .checked_add(buf.len())
            .ok_or_else(|| BTreeError::Io("write overflow".to_string()))?;

        let mut data = self.inner.borrow_mut();
        if end > data.len() {
            data.resize(end, 0);
        }
        data[offset..end].copy_from_slice(buf);
        Ok(())
    }

    fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        let len = usize::try_from(len)
            .map_err(|_| BTreeError::Io("truncate length does not fit in usize".to_string()))?;
        let mut data = self.inner.borrow_mut();
        data.resize(len, 0);
        Ok(())
    }

    fn flush(&self) -> Result<(), BTreeError> {
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct StdFile {
    inner: Rc<RefCell<std::fs::File>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl StdFile {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, BTreeError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| BTreeError::Io(e.to_string()))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|e| BTreeError::Io(e.to_string()))?;
        Ok(Self {
            inner: Rc::new(RefCell::new(file)),
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SyncFile for StdFile {
    fn len(&self) -> Result<u64, BTreeError> {
        let file = self.inner.borrow();
        file.metadata()
            .map(|m| m.len())
            .map_err(|e| BTreeError::Io(e.to_string()))
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let file = self.inner.borrow();
            file.read_exact_at(buf, offset)
                .map_err(|e| BTreeError::Io(e.to_string()))
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let file = self.inner.borrow();
            let read = file
                .seek_read(buf, offset)
                .map_err(|e| BTreeError::Io(e.to_string()))?;
            if read != buf.len() {
                return Err(BTreeError::Io(format!(
                    "unexpected eof: read {} of {} bytes",
                    read,
                    buf.len()
                )));
            }
            Ok(())
        }
    }

    fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let file = self.inner.borrow();
            file.write_all_at(buf, offset)
                .map_err(|e| BTreeError::Io(e.to_string()))
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let file = self.inner.borrow();
            let written = file
                .seek_write(buf, offset)
                .map_err(|e| BTreeError::Io(e.to_string()))?;
            if written != buf.len() {
                return Err(BTreeError::Io(format!(
                    "short write: wrote {} of {} bytes",
                    written,
                    buf.len()
                )));
            }
            Ok(())
        }
    }

    fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        let file = self.inner.borrow();
        file.set_len(len).map_err(|e| BTreeError::Io(e.to_string()))
    }

    fn flush(&self) -> Result<(), BTreeError> {
        let file = self.inner.borrow();
        file.sync_all().map_err(|e| BTreeError::Io(e.to_string()))
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Debug)]
pub struct OpfsFile {
    inner: Rc<OpfsFileInner>,
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug)]
struct OpfsFileInner {
    handle: web_sys::FileSystemSyncAccessHandle,
    read_options: web_sys::FileSystemReadWriteOptions,
    write_options: web_sys::FileSystemReadWriteOptions,
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub(crate) struct OpfsIoCounters {
    pub read_calls: u64,
    pub read_bytes: u64,
    pub write_calls: u64,
    pub write_bytes: u64,
    pub len_calls: u64,
    pub truncate_calls: u64,
    pub flush_calls: u64,
}

#[cfg(target_arch = "wasm32")]
impl OpfsIoCounters {
    pub(crate) fn delta_since(self, before: Self) -> Self {
        Self {
            read_calls: self.read_calls.saturating_sub(before.read_calls),
            read_bytes: self.read_bytes.saturating_sub(before.read_bytes),
            write_calls: self.write_calls.saturating_sub(before.write_calls),
            write_bytes: self.write_bytes.saturating_sub(before.write_bytes),
            len_calls: self.len_calls.saturating_sub(before.len_calls),
            truncate_calls: self.truncate_calls.saturating_sub(before.truncate_calls),
            flush_calls: self.flush_calls.saturating_sub(before.flush_calls),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl Drop for OpfsFileInner {
    fn drop(&mut self) {
        self.handle.close();
    }
}

#[cfg(target_arch = "wasm32")]
thread_local! {
    static OPFS_IO_COUNTERS: std::cell::Cell<OpfsIoCounters> = const { std::cell::Cell::new(OpfsIoCounters {
        read_calls: 0,
        read_bytes: 0,
        write_calls: 0,
        write_bytes: 0,
        len_calls: 0,
        truncate_calls: 0,
        flush_calls: 0,
    }) };
}

#[cfg(target_arch = "wasm32")]
fn update_opfs_io_counters(f: impl FnOnce(&mut OpfsIoCounters)) {
    OPFS_IO_COUNTERS.with(|cell| {
        let mut counters = cell.get();
        f(&mut counters);
        cell.set(counters);
    });
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn opfs_io_counters_snapshot() -> OpfsIoCounters {
    OPFS_IO_COUNTERS.with(|cell| cell.get())
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn opfs_io_counters_reset() {
    OPFS_IO_COUNTERS.with(|cell| cell.set(OpfsIoCounters::default()));
}

#[cfg(target_arch = "wasm32")]
impl OpfsFile {
    pub async fn open(namespace: &str) -> Result<Self, BTreeError> {
        let global: web_sys::WorkerGlobalScope = js_sys::global().dyn_into().map_err(|_| {
            BTreeError::Io("OpfsFile::open must run in a dedicated worker".to_string())
        })?;
        let storage = global.navigator().storage();

        let root: web_sys::FileSystemDirectoryHandle = JsFuture::from(storage.get_directory())
            .await
            .map_err(map_js_error)?
            .dyn_into()
            .map_err(|_| BTreeError::Io("failed to cast OPFS root".to_string()))?;

        let opts = web_sys::FileSystemGetFileOptions::new();
        opts.set_create(true);
        let file: web_sys::FileSystemFileHandle =
            JsFuture::from(root.get_file_handle_with_options(&Self::file_name(namespace), &opts))
                .await
                .map_err(map_js_error)?
                .dyn_into()
                .map_err(|_| BTreeError::Io("failed to cast OPFS file handle".to_string()))?;

        // Retry with exponential backoff: on rapid page refresh the previous
        // worker may still hold the exclusive SyncAccessHandle.  The browser
        // releases it once the old worker is GC'd, typically within a few
        // hundred milliseconds.
        // Only retries on DOMExceptions (handle conflicts); other errors fail immediately.
        const MAX_RETRIES: u32 = 5;
        const BASE_DELAY_MS: u32 = 50; // 50, 100, 200, 400, 800 → ~1.5s total

        let mut last_err = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let delay = BASE_DELAY_MS * (1 << (attempt - 1));
                sleep_ms(delay).await;
            }
            match JsFuture::from(file.create_sync_access_handle()).await {
                Ok(val) => {
                    let handle: web_sys::FileSystemSyncAccessHandle =
                        val.dyn_into().map_err(|_| {
                            BTreeError::Io("failed to cast OPFS sync access handle".to_string())
                        })?;
                    if attempt > 0 {
                        tracing::info!(attempt, "acquired OPFS access handle after retry");
                    }

                    let read_options = web_sys::FileSystemReadWriteOptions::new();
                    let write_options = web_sys::FileSystemReadWriteOptions::new();
                    return Ok(Self {
                        inner: Rc::new(OpfsFileInner {
                            handle,
                            read_options,
                            write_options,
                        }),
                    });
                }
                Err(e) => {
                    if !is_retryable_handle_conflict(&e) {
                        return Err(map_js_error(e));
                    }
                    last_err = Some(e);
                }
            }
        }

        // All retries exhausted — return the last error.
        Err(map_js_error(last_err.unwrap()))
    }

    pub async fn destroy(namespace: &str) -> Result<(), BTreeError> {
        let global: web_sys::WorkerGlobalScope = js_sys::global().dyn_into().map_err(|_| {
            BTreeError::Io("OpfsFile::destroy must run in a dedicated worker".to_string())
        })?;
        let storage = global.navigator().storage();
        let root: web_sys::FileSystemDirectoryHandle = JsFuture::from(storage.get_directory())
            .await
            .map_err(map_js_error)?
            .dyn_into()
            .map_err(|_| BTreeError::Io("failed to cast OPFS root".to_string()))?;

        let name = Self::file_name(namespace);
        let remove_fn = js_sys::Reflect::get(&root, &"removeEntry".into()).map_err(map_js_error)?;
        let remove_fn: js_sys::Function = remove_fn
            .dyn_into()
            .map_err(|_| BTreeError::Io("OPFS removeEntry is unavailable".to_string()))?;
        let opts = js_sys::Object::new();
        let _ = js_sys::Reflect::set(&opts, &"recursive".into(), &false.into());
        let promise = remove_fn.call2(&root, &name.into(), &opts.into());
        if let Ok(promise) = promise {
            let promise: js_sys::Promise = promise
                .dyn_into()
                .map_err(|_| BTreeError::Io("failed to cast removeEntry promise".to_string()))?;
            let _ = JsFuture::from(promise).await;
        }

        Ok(())
    }

    fn file_name(namespace: &str) -> String {
        format!("{}.opfsbtree", namespace)
    }
}

#[cfg(target_arch = "wasm32")]
impl SyncFile for OpfsFile {
    fn len(&self) -> Result<u64, BTreeError> {
        update_opfs_io_counters(|stats| {
            stats.len_calls = stats.len_calls.saturating_add(1);
        });
        Ok(self.inner.handle.get_size().map_err(map_js_error)? as u64)
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
        self.inner.read_options.set_at(offset as f64);
        let read = self
            .inner
            .handle
            .read_with_u8_array_and_options(buf, &self.inner.read_options)
            .map_err(map_js_error)? as usize;
        if read != buf.len() {
            return Err(BTreeError::Io(format!(
                "unexpected eof: read {} of {} bytes",
                read,
                buf.len()
            )));
        }
        update_opfs_io_counters(|stats| {
            stats.read_calls = stats.read_calls.saturating_add(1);
            stats.read_bytes = stats.read_bytes.saturating_add(read as u64);
        });
        Ok(())
    }

    fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
        self.inner.write_options.set_at(offset as f64);
        let written = self
            .inner
            .handle
            .write_with_u8_array_and_options(buf, &self.inner.write_options)
            .map_err(map_js_error)? as usize;
        if written != buf.len() {
            return Err(BTreeError::Io(format!(
                "short write: wrote {} of {} bytes",
                written,
                buf.len()
            )));
        }
        update_opfs_io_counters(|stats| {
            stats.write_calls = stats.write_calls.saturating_add(1);
            stats.write_bytes = stats.write_bytes.saturating_add(written as u64);
        });
        Ok(())
    }

    fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        update_opfs_io_counters(|stats| {
            stats.truncate_calls = stats.truncate_calls.saturating_add(1);
        });
        truncate_handle(&self.inner.handle, len)
    }

    fn flush(&self) -> Result<(), BTreeError> {
        update_opfs_io_counters(|stats| {
            stats.flush_calls = stats.flush_calls.saturating_add(1);
        });
        self.inner.handle.flush().map_err(map_js_error)
    }
}

#[cfg(target_arch = "wasm32")]
fn truncate_handle(
    handle: &web_sys::FileSystemSyncAccessHandle,
    len: u64,
) -> Result<(), BTreeError> {
    let truncate = js_sys::Reflect::get(handle, &"truncate".into()).map_err(map_js_error)?;
    let truncate: js_sys::Function = truncate
        .dyn_into()
        .map_err(|_| BTreeError::Io("OPFS truncate is unavailable".to_string()))?;
    truncate
        .call1(handle, &wasm_bindgen::JsValue::from_f64(len as f64))
        .map_err(map_js_error)?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
async fn sleep_ms(ms: u32) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        let global = js_sys::global();
        let set_timeout = js_sys::Reflect::get(&global, &"setTimeout".into()).unwrap();
        let set_timeout: js_sys::Function = set_timeout.unchecked_into();
        let _ = set_timeout.call2(&global, &resolve, &wasm_bindgen::JsValue::from(ms));
    });
    let _ = JsFuture::from(promise).await;
}

/// Returns true if the JS error is a DOMException (has a `name` property),
/// meaning it's likely a retryable handle conflict. Non-DOMException errors
/// (e.g. quota, TypeError) fail immediately.
#[cfg(target_arch = "wasm32")]
fn is_retryable_handle_conflict(value: &wasm_bindgen::JsValue) -> bool {
    js_sys::Reflect::get(value, &"name".into())
        .ok()
        .and_then(|v| v.as_string())
        .is_some()
}

#[cfg(target_arch = "wasm32")]
fn map_js_error(value: wasm_bindgen::JsValue) -> BTreeError {
    if let Some(s) = value.as_string() {
        BTreeError::Io(s)
    } else {
        BTreeError::Io(format!("{value:?}"))
    }
}
