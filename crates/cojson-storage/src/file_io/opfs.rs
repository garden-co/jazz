//! Origin Private File System (OPFS) file I/O implementation for browsers.
//!
//! This implementation uses the browser's OPFS API for persistent storage.
//! OPFS provides a sandboxed filesystem that is:
//! - Private to the origin (domain)
//! - Persistent across browser sessions
//! - Supports synchronous access via FileSystemSyncAccessHandle (in Workers)
//!
//! # Important Notes
//!
//! - FileSystemSyncAccessHandle is only available in Web Workers, not the main thread
//! - For main thread usage, async operations via FileSystemWritableFileStream are used
//! - OPFS has good browser support in Chrome, Edge, Firefox, and Safari
//!
//! # Example
//!
//! ```rust,ignore
//! use cojson_storage::file_io::{OpfsFileIO, FileIO, OpenOptions};
//!
//! // Initialize OPFS (async)
//! let fs = OpfsFileIO::new("my-app-storage").await?;
//!
//! // Open a file
//! let file = fs.open("data.bin", OpenOptions::new().read(true).write(true).create(true)).await?;
//! ```

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemSyncAccessHandle,
    FileSystemGetDirectoryOptions, FileSystemGetFileOptions,
    FileSystemReadWriteOptions,
};

use super::traits::{FileHandle, OpenOptions};

/// Error type for OPFS operations.
#[derive(Debug)]
pub enum OpfsError {
    /// JavaScript error from the browser API
    JsError(String),
    /// File not found
    NotFound(String),
    /// Permission denied
    PermissionDenied(String),
    /// Invalid operation
    InvalidOperation(String),
    /// Not in a Web Worker (SyncAccessHandle unavailable)
    NotInWorker,
}

impl std::fmt::Display for OpfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpfsError::JsError(msg) => write!(f, "JavaScript error: {}", msg),
            OpfsError::NotFound(path) => write!(f, "File not found: {}", path),
            OpfsError::PermissionDenied(msg) => write!(f, "Permission denied: {}", msg),
            OpfsError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            OpfsError::NotInWorker => write!(f, "Sync access requires Web Worker context"),
        }
    }
}

impl std::error::Error for OpfsError {}

impl From<JsValue> for OpfsError {
    fn from(err: JsValue) -> Self {
        let msg = err
            .as_string()
            .or_else(|| {
                js_sys::Reflect::get(&err, &"message".into())
                    .ok()
                    .and_then(|v| v.as_string())
            })
            .unwrap_or_else(|| format!("{:?}", err));
        OpfsError::JsError(msg)
    }
}

impl From<OpfsError> for io::Error {
    fn from(err: OpfsError) -> Self {
        match err {
            OpfsError::NotFound(path) => io::Error::new(io::ErrorKind::NotFound, path),
            OpfsError::PermissionDenied(msg) => {
                io::Error::new(io::ErrorKind::PermissionDenied, msg)
            }
            OpfsError::NotInWorker => {
                io::Error::new(io::ErrorKind::Unsupported, "Requires Web Worker context")
            }
            _ => io::Error::new(io::ErrorKind::Other, err.to_string()),
        }
    }
}

/// OPFS file handle using FileSystemSyncAccessHandle.
///
/// This handle provides synchronous read/write access to a file in OPFS.
/// Note: Only available in Web Worker contexts.
pub struct OpfsFileHandle {
    /// The synchronous access handle (only available in Workers)
    sync_handle: FileSystemSyncAccessHandle,
    /// Current file position
    position: RwLock<u64>,
    /// Whether the file is open for reading
    read: bool,
    /// Whether the file is open for writing
    write: bool,
}

impl OpfsFileHandle {
    /// Create a new OPFS file handle from a sync access handle.
    pub fn new(sync_handle: FileSystemSyncAccessHandle, read: bool, write: bool) -> Self {
        Self {
            sync_handle,
            position: RwLock::new(0),
            read,
            write,
        }
    }

    /// Get the underlying sync handle.
    pub fn sync_handle(&self) -> &FileSystemSyncAccessHandle {
        &self.sync_handle
    }
}

impl Drop for OpfsFileHandle {
    fn drop(&mut self) {
        // Close the sync handle when dropped
        self.sync_handle.close();
    }
}

impl Read for OpfsFileHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.read {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "File not opened for reading",
            ));
        }

        let mut pos = self.position.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "Lock poisoned")
        })?;

        // Create options with the current position
        let options = FileSystemReadWriteOptions::new();
        options.set_at(*pos as f64);

        // Read into a Uint8Array
        let js_array = js_sys::Uint8Array::new_with_length(buf.len() as u32);
        
        let bytes_read = self
            .sync_handle
            .read_with_buffer_source_and_options(&js_array, &options)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, OpfsError::from(e).to_string()))?;

        let bytes_read = bytes_read as usize;
        
        // Copy from Uint8Array to buffer
        js_array.slice(0, bytes_read as u32).copy_to(&mut buf[..bytes_read]);
        
        *pos += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl Write for OpfsFileHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.write {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "File not opened for writing",
            ));
        }

        let mut pos = self.position.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "Lock poisoned")
        })?;

        // Create options with the current position
        let options = FileSystemReadWriteOptions::new();
        options.set_at(*pos as f64);

        // Create Uint8Array from buffer
        let js_array = js_sys::Uint8Array::from(buf);

        let bytes_written = self
            .sync_handle
            .write_with_buffer_source_and_options(&js_array, &options)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, OpfsError::from(e).to_string()))?;

        let bytes_written = bytes_written as usize;
        *pos += bytes_written as u64;
        
        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sync_handle
            .flush()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, OpfsError::from(e).to_string()))
    }
}

impl Seek for OpfsFileHandle {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let mut position = self.position.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "Lock poisoned")
        })?;

        let size = self
            .sync_handle
            .get_size()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, OpfsError::from(e).to_string()))?
            as u64;

        let new_pos = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => size as i64 + n,
            SeekFrom::Current(n) => *position as i64 + n,
        };

        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Seek to negative position",
            ));
        }

        *position = new_pos as u64;
        Ok(*position)
    }
}

impl FileHandle for OpfsFileHandle {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if !self.read {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "File not opened for reading",
            ));
        }

        let options = FileSystemReadWriteOptions::new();
        options.set_at(offset as f64);

        let js_array = js_sys::Uint8Array::new_with_length(buf.len() as u32);
        
        let bytes_read = self
            .sync_handle
            .read_with_buffer_source_and_options(&js_array, &options)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, OpfsError::from(e).to_string()))?;

        let bytes_read = bytes_read as usize;
        js_array.slice(0, bytes_read as u32).copy_to(&mut buf[..bytes_read]);
        
        Ok(bytes_read)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        if !self.write {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "File not opened for writing",
            ));
        }

        let options = FileSystemReadWriteOptions::new();
        options.set_at(offset as f64);

        let js_array = js_sys::Uint8Array::from(buf);

        let bytes_written = self
            .sync_handle
            .write_with_buffer_source_and_options(&js_array, &options)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, OpfsError::from(e).to_string()))?;

        Ok(bytes_written as usize)
    }

    fn flush_all(&self) -> io::Result<()> {
        self.sync_handle
            .flush()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, OpfsError::from(e).to_string()))
    }

    fn len(&self) -> io::Result<u64> {
        self.sync_handle
            .get_size()
            .map(|s| s as u64)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, OpfsError::from(e).to_string()))
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.sync_handle
            .truncate_with_u53(len as f64)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, OpfsError::from(e).to_string()))
    }

    fn sync_all(&self) -> io::Result<()> {
        self.flush_all()
    }

    fn sync_data(&self) -> io::Result<()> {
        self.flush_all()
    }
}

/// OPFS (Origin Private File System) file I/O provider.
///
/// This provides file I/O using the browser's OPFS API, which is:
/// - Persistent across browser sessions
/// - Private to the origin
/// - Supports synchronous access in Web Workers
///
/// # Usage
///
/// ```rust,ignore
/// let fs = OpfsFileIO::new("my-database").await?;
/// let file = fs.open_async("data.bin", OpenOptions::new().read(true).write(true).create(true)).await?;
/// ```
pub struct OpfsFileIO {
    /// Root directory handle
    root: FileSystemDirectoryHandle,
    /// Database/storage name
    name: String,
    /// Directory handle cache
    dir_cache: RwLock<HashMap<String, FileSystemDirectoryHandle>>,
}

impl OpfsFileIO {
    /// Create a new OPFS file I/O provider.
    ///
    /// This creates or opens a directory in OPFS for storage.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the storage directory (e.g., "jazz-storage")
    pub async fn new(name: &str) -> Result<Self, OpfsError> {
        // Get the OPFS root directory
        let window = web_sys::window().ok_or(OpfsError::InvalidOperation(
            "No window object (not in browser?)".to_string(),
        ))?;

        let navigator = window.navigator();
        let storage = navigator.storage();

        let root_promise = storage.get_directory();
        let root: FileSystemDirectoryHandle = JsFuture::from(root_promise)
            .await?
            .dyn_into()
            .map_err(|_| OpfsError::JsError("Failed to get OPFS root".to_string()))?;

        // Create or get our storage directory
        let options = FileSystemGetDirectoryOptions::new();
        options.set_create(true);

        let storage_dir_promise = root.get_directory_handle_with_options(name, &options);
        let storage_dir: FileSystemDirectoryHandle = JsFuture::from(storage_dir_promise)
            .await?
            .dyn_into()
            .map_err(|_| OpfsError::JsError("Failed to create storage directory".to_string()))?;

        Ok(Self {
            root: storage_dir,
            name: name.to_string(),
            dir_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Get the storage name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if running in a Web Worker context.
    ///
    /// FileSystemSyncAccessHandle is only available in Workers.
    pub fn is_in_worker() -> bool {
        // Check if we're in a Worker by checking for WorkerGlobalScope
        js_sys::eval("typeof WorkerGlobalScope !== 'undefined' && self instanceof WorkerGlobalScope")
            .map(|v| v.as_bool().unwrap_or(false))
            .unwrap_or(false)
    }

    /// Get or create a directory handle for the given path.
    async fn get_directory(&self, path: &str) -> Result<FileSystemDirectoryHandle, OpfsError> {
        if path.is_empty() || path == "/" {
            return Ok(self.root.clone());
        }

        // Check cache first
        {
            let cache = self.dir_cache.read().unwrap();
            if let Some(handle) = cache.get(path) {
                return Ok(handle.clone());
            }
        }

        // Create directory path
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let mut current = self.root.clone();

        for part in &parts {
            let options = FileSystemGetDirectoryOptions::new();
            options.set_create(true);

            let promise = current.get_directory_handle_with_options(part, &options);
            current = JsFuture::from(promise)
                .await?
                .dyn_into()
                .map_err(|_| OpfsError::JsError(format!("Failed to get directory: {}", part)))?;
        }

        // Cache the result
        {
            let mut cache = self.dir_cache.write().unwrap();
            cache.insert(path.to_string(), current.clone());
        }

        Ok(current)
    }

    /// Get a file handle, optionally creating it.
    async fn get_file_handle(
        &self,
        path: &str,
        create: bool,
    ) -> Result<FileSystemFileHandle, OpfsError> {
        let (dir_path, file_name) = match path.rfind('/') {
            Some(idx) => (&path[..idx], &path[idx + 1..]),
            None => ("", path),
        };

        let dir = self.get_directory(dir_path).await?;

        let options = FileSystemGetFileOptions::new();
        options.set_create(create);

        let promise = dir.get_file_handle_with_options(file_name, &options);
        JsFuture::from(promise)
            .await
            .map_err(|e| {
                if !create {
                    OpfsError::NotFound(path.to_string())
                } else {
                    OpfsError::from(e)
                }
            })?
            .dyn_into()
            .map_err(|_| OpfsError::JsError(format!("Failed to get file handle: {}", path)))
    }

    /// Open a file asynchronously.
    ///
    /// This returns an OPFS file handle with synchronous access (requires Worker context).
    pub async fn open_async(&self, path: &str, options: OpenOptions) -> Result<OpfsFileHandle, OpfsError> {
        if !Self::is_in_worker() {
            return Err(OpfsError::NotInWorker);
        }

        // Handle create_new (must not exist)
        if options.create_new {
            // Try to get without creating - should fail
            if self.get_file_handle(path, false).await.is_ok() {
                return Err(OpfsError::JsError(format!(
                    "File already exists: {}",
                    path
                )));
            }
        }

        // Get or create the file
        let file_handle = self.get_file_handle(path, options.create || options.create_new).await?;

        // Get synchronous access handle
        let sync_promise = file_handle.create_sync_access_handle();
        let sync_handle: FileSystemSyncAccessHandle = JsFuture::from(sync_promise)
            .await?
            .dyn_into()
            .map_err(|_| OpfsError::JsError("Failed to create sync access handle".to_string()))?;

        // Handle truncate
        if options.truncate {
            sync_handle
                .truncate_with_u53(0.0)
                .map_err(|e| OpfsError::from(e))?;
        }

        Ok(OpfsFileHandle::new(sync_handle, options.read, options.write))
    }

    /// Check if a path exists.
    pub async fn exists_async(&self, path: &str) -> bool {
        self.get_file_handle(path, false).await.is_ok()
    }

    /// Remove a file.
    pub async fn remove_file_async(&self, path: &str) -> Result<(), OpfsError> {
        let (dir_path, file_name) = match path.rfind('/') {
            Some(idx) => (&path[..idx], &path[idx + 1..]),
            None => ("", path),
        };

        let dir = self.get_directory(dir_path).await?;
        
        let promise = dir.remove_entry(file_name);
        JsFuture::from(promise).await?;
        
        Ok(())
    }

    /// Create a directory.
    pub async fn create_dir_async(&self, path: &str) -> Result<(), OpfsError> {
        self.get_directory(path).await?;
        Ok(())
    }
}

// Note: We cannot implement the synchronous FileIO trait directly for OpfsFileIO
// because OPFS operations are async. Instead, we provide async methods that
// can be used in WASM contexts. The integration with the storage backend
// will need to handle the async boundary appropriately.
