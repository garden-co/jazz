use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use crate::BTreeError;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsCast;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;

/// Async file I/O abstraction. Every method returns a future so the OPFS
/// backend can drive the *asynchronous* File System Access APIs
/// (`getFile()` / `Blob.arrayBuffer()` for reads, `createWritable()` for
/// writes) instead of the synchronous `FileSystemSyncAccessHandle`. The
/// in-memory and native backends complete immediately.
#[allow(async_fn_in_trait)]
pub trait AsyncFile {
    async fn len(&self) -> Result<u64, BTreeError>;
    async fn is_empty(&self) -> Result<bool, BTreeError> {
        Ok(self.len().await? == 0)
    }
    async fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError>;
    async fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError>;
    async fn truncate(&self, len: u64) -> Result<(), BTreeError>;
    async fn flush(&self) -> Result<(), BTreeError>;

    /// Open a companion file alongside this one, identified by `suffix`. The
    /// b-tree uses this to keep the WAL in a separate (small) file so each
    /// async `createWritable()` copy-on-write touches only the WAL, not the
    /// whole multi-megabyte home file. Reopening the same logical file must
    /// return a handle onto the same companion bytes.
    async fn open_sibling(&self, suffix: &str) -> Result<Self, BTreeError>
    where
        Self: Sized;
}

/// Minimal, dependency-free executor used by the native sync facades and the
/// native test suite. The native and in-memory `AsyncFile` impls never truly
/// pend (their bodies are synchronous), so the future is ready on the first
/// poll; the spin loop is purely defensive.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::hint::spin_loop(),
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct MemoryFile {
    inner: Rc<RefCell<Vec<u8>>>,
    // Companion files (e.g. the WAL) live in this shared slot so that a cloned
    // handle — which is how the in-memory backend models "reopen" — sees the
    // same companion bytes. Without sharing, a reopen would get a fresh, empty
    // WAL and lose un-checkpointed commits.
    siblings: Rc<RefCell<FxHashMap<String, MemoryFile>>>,
}

impl MemoryFile {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AsyncFile for MemoryFile {
    async fn len(&self) -> Result<u64, BTreeError> {
        Ok(self.inner.borrow().len() as u64)
    }

    async fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
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

    async fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
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

    async fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        let len = usize::try_from(len)
            .map_err(|_| BTreeError::Io("truncate length does not fit in usize".to_string()))?;
        let mut data = self.inner.borrow_mut();
        data.resize(len, 0);
        Ok(())
    }

    async fn flush(&self) -> Result<(), BTreeError> {
        Ok(())
    }

    async fn open_sibling(&self, suffix: &str) -> Result<Self, BTreeError> {
        let mut siblings = self.siblings.borrow_mut();
        let sibling = siblings.entry(suffix.to_string()).or_default();
        Ok(sibling.clone())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct StdFile {
    path: std::path::PathBuf,
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
            path: path.to_path_buf(),
            inner: Rc::new(RefCell::new(file)),
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl AsyncFile for StdFile {
    async fn len(&self) -> Result<u64, BTreeError> {
        let file = self.inner.borrow();
        file.metadata()
            .map(|m| m.len())
            .map_err(|e| BTreeError::Io(e.to_string()))
    }

    async fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
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

    async fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
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

    async fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        let file = self.inner.borrow();
        file.set_len(len).map_err(|e| BTreeError::Io(e.to_string()))
    }

    async fn flush(&self) -> Result<(), BTreeError> {
        let file = self.inner.borrow();
        file.sync_all().map_err(|e| BTreeError::Io(e.to_string()))
    }

    async fn open_sibling(&self, suffix: &str) -> Result<Self, BTreeError> {
        let mut sibling = self.path.clone().into_os_string();
        sibling.push(suffix);
        StdFile::open(std::path::PathBuf::from(sibling))
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
    handle: web_sys::FileSystemFileHandle,
    /// The OPFS entry name, so siblings (the WAL) can be derived by suffix.
    name: String,
    /// A single writable stream held open across a write batch. Reopening a
    /// fresh `createWritable()` per write would copy-on-write the whole file
    /// each time; instead we open once on the first write and `close()` (which
    /// publishes the changes) on the next `flush()` or read. This collapses a
    /// checkpoint's dozens of full-file copies into one.
    writable: RefCell<Option<web_sys::FileSystemWritableFileStream>>,
}

#[cfg(target_arch = "wasm32")]
impl OpfsFile {
    pub async fn open(namespace: &str) -> Result<Self, BTreeError> {
        Self::open_named(Self::file_name(namespace)).await
    }

    async fn open_named(name: String) -> Result<Self, BTreeError> {
        let root = opfs_root().await?;
        let opts = web_sys::FileSystemGetFileOptions::new();
        opts.set_create(true);
        let handle: web_sys::FileSystemFileHandle =
            JsFuture::from(root.get_file_handle_with_options(&name, &opts))
                .await
                .map_err(map_js_error)?
                .dyn_into()
                .map_err(|_| BTreeError::Io("failed to cast OPFS file handle".to_string()))?;

        Ok(Self {
            inner: Rc::new(OpfsFileInner {
                handle,
                name,
                writable: RefCell::new(None),
            }),
        })
    }

    pub async fn destroy(namespace: &str) -> Result<(), BTreeError> {
        let name = Self::file_name(namespace);
        // Remove the home file and its WAL sibling (see `open_sibling`).
        remove_entry(&format!("{}.wal", name)).await?;
        remove_entry(&name).await
    }

    fn file_name(namespace: &str) -> String {
        format!("{}.opfsbtree", namespace)
    }

    /// `getFile()` returns a fresh `File` (a `Blob`) snapshot of the current
    /// on-disk contents. Reads slice this blob and pull the bytes through
    /// `arrayBuffer()`.
    async fn current_file(&self) -> Result<web_sys::File, BTreeError> {
        JsFuture::from(self.inner.handle.get_file())
            .await
            .map_err(map_js_error)?
            .dyn_into()
            .map_err(|_| BTreeError::Io("failed to cast OPFS File".to_string()))
    }

    /// Opens a writable stream that keeps the existing file bytes. `keepExisting
    /// Data:true` makes the browser snapshot the file copy-on-write; changes are
    /// published on `close()`.
    async fn open_writable(&self) -> Result<web_sys::FileSystemWritableFileStream, BTreeError> {
        let opts = web_sys::FileSystemCreateWritableOptions::new();
        opts.set_keep_existing_data(true);
        JsFuture::from(self.inner.handle.create_writable_with_options(&opts))
            .await
            .map_err(map_js_error)?
            .dyn_into()
            .map_err(|_| BTreeError::Io("failed to cast OPFS writable stream".to_string()))
    }

    /// Take the held writable stream, opening a fresh one if none is held.
    async fn take_writable(&self) -> Result<web_sys::FileSystemWritableFileStream, BTreeError> {
        let existing = self.inner.writable.borrow_mut().take();
        match existing {
            Some(stream) => Ok(stream),
            None => self.open_writable().await,
        }
    }

    fn store_writable(&self, stream: web_sys::FileSystemWritableFileStream) {
        *self.inner.writable.borrow_mut() = Some(stream);
    }

    /// Close any held writable so its buffered writes become visible to the
    /// next `getFile()` read. This is the durability/visibility barrier.
    async fn commit_writable(&self) -> Result<(), BTreeError> {
        let existing = self.inner.writable.borrow_mut().take();
        if let Some(stream) = existing {
            JsFuture::from(stream.close()).await.map_err(map_js_error)?;
        }
        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
impl AsyncFile for OpfsFile {
    async fn len(&self) -> Result<u64, BTreeError> {
        self.commit_writable().await?;
        Ok(self.current_file().await?.size() as u64)
    }

    async fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
        if buf.is_empty() {
            return Ok(());
        }
        self.commit_writable().await?;
        let file = self.current_file().await?;
        let start = offset as f64;
        let end = start + buf.len() as f64;
        let blob: web_sys::Blob = file
            .slice_with_f64_and_f64(start, end)
            .map_err(map_js_error)?;
        let array_buffer = JsFuture::from(blob.array_buffer())
            .await
            .map_err(map_js_error)?;
        let bytes = js_sys::Uint8Array::new(&array_buffer);
        let read = bytes.length() as usize;
        if read != buf.len() {
            return Err(BTreeError::Io(format!(
                "unexpected eof: read {} of {} bytes",
                read,
                buf.len()
            )));
        }
        bytes.copy_to(buf);
        Ok(())
    }

    async fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
        let stream = self.take_writable().await?;
        let result: Result<(), BTreeError> = async {
            JsFuture::from(stream.seek_with_f64(offset as f64).map_err(map_js_error)?)
                .await
                .map_err(map_js_error)?;
            JsFuture::from(stream.write_with_u8_array(buf).map_err(map_js_error)?)
                .await
                .map_err(map_js_error)?;
            Ok(())
        }
        .await;
        if result.is_ok() {
            self.store_writable(stream);
        }
        result
    }

    async fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        let stream = self.take_writable().await?;
        let result: Result<(), BTreeError> = async {
            JsFuture::from(stream.truncate_with_f64(len as f64).map_err(map_js_error)?)
                .await
                .map_err(map_js_error)?;
            Ok(())
        }
        .await;
        if result.is_ok() {
            self.store_writable(stream);
        }
        result
    }

    async fn flush(&self) -> Result<(), BTreeError> {
        // Closing the held writable publishes its writes durably.
        self.commit_writable().await
    }

    async fn open_sibling(&self, suffix: &str) -> Result<Self, BTreeError> {
        Self::open_named(format!("{}{}", self.inner.name, suffix)).await
    }
}

#[cfg(target_arch = "wasm32")]
async fn remove_entry(name: &str) -> Result<(), BTreeError> {
    let root = opfs_root().await?;
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

#[cfg(target_arch = "wasm32")]
async fn opfs_root() -> Result<web_sys::FileSystemDirectoryHandle, BTreeError> {
    let global: web_sys::WorkerGlobalScope = js_sys::global()
        .dyn_into()
        .map_err(|_| BTreeError::Io("OpfsFile must run in a dedicated worker".to_string()))?;
    let storage = global.navigator().storage();
    JsFuture::from(storage.get_directory())
        .await
        .map_err(map_js_error)?
        .dyn_into()
        .map_err(|_| BTreeError::Io("failed to cast OPFS root".to_string()))
}

#[cfg(target_arch = "wasm32")]
fn map_js_error(value: wasm_bindgen::JsValue) -> BTreeError {
    if value.is_instance_of::<web_sys::DomException>() {
        let ex: web_sys::DomException = value.unchecked_into();
        if ex.name() == "SecurityError" {
            return BTreeError::SecurityError(ex.message().into());
        }
        return BTreeError::Io(format!("DOMException({}): {}", ex.name(), ex.message()));
    }
    if let Some(s) = value.as_string() {
        BTreeError::Io(s)
    } else {
        BTreeError::Io(format!("{value:?}"))
    }
}
