use std::cell::RefCell;
use std::rc::Rc;

use crate::BTreeError;

pub trait SyncFile {
    fn len(&self) -> Result<u64, BTreeError>;
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
