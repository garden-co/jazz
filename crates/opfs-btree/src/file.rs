use std::cell::RefCell;
use std::rc::Rc;

use crate::BTreeError;

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
}

#[cfg(target_arch = "wasm32")]
impl Drop for OpfsFileInner {
    fn drop(&mut self) {
        self.handle.close();
    }
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

        let handle: web_sys::FileSystemSyncAccessHandle =
            JsFuture::from(file.create_sync_access_handle())
                .await
                .map_err(map_js_error)?
                .dyn_into()
                .map_err(|_| {
                    BTreeError::Io("failed to cast OPFS sync access handle".to_string())
                })?;

        Ok(Self {
            inner: Rc::new(OpfsFileInner { handle }),
        })
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
        Ok(self.inner.handle.get_size().map_err(map_js_error)? as u64)
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
        let opts = web_sys::FileSystemReadWriteOptions::new();
        opts.set_at(offset as f64);
        let read = self
            .inner
            .handle
            .read_with_u8_array_and_options(buf, &opts)
            .map_err(map_js_error)? as usize;
        if read != buf.len() {
            return Err(BTreeError::Io(format!(
                "unexpected eof: read {} of {} bytes",
                read,
                buf.len()
            )));
        }
        Ok(())
    }

    fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
        let opts = web_sys::FileSystemReadWriteOptions::new();
        opts.set_at(offset as f64);
        let written = self
            .inner
            .handle
            .write_with_u8_array_and_options(buf, &opts)
            .map_err(map_js_error)? as usize;
        if written != buf.len() {
            return Err(BTreeError::Io(format!(
                "short write: wrote {} of {} bytes",
                written,
                buf.len()
            )));
        }
        Ok(())
    }

    fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        truncate_handle(&self.inner.handle, len)
    }

    fn flush(&self) -> Result<(), BTreeError> {
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
fn map_js_error(value: wasm_bindgen::JsValue) -> BTreeError {
    if let Some(s) = value.as_string() {
        BTreeError::Io(s)
    } else {
        BTreeError::Io(format!("{value:?}"))
    }
}
