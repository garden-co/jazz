//! File I/O trait definitions.
//!
//! These traits provide a platform-agnostic interface for file operations,
//! allowing BF-Tree and other storage backends to work across different
//! environments.

use std::io::{self, Read, Seek, Write};

/// Options for opening a file.
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    /// Open for reading
    pub read: bool,
    /// Open for writing
    pub write: bool,
    /// Create the file if it doesn't exist
    pub create: bool,
    /// Create a new file, failing if it exists
    pub create_new: bool,
    /// Truncate the file to zero length
    pub truncate: bool,
    /// Append to the file
    pub append: bool,
}

impl OpenOptions {
    /// Create new default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set read mode.
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Set write mode.
    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Set create mode.
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Set create_new mode.
    pub fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Set truncate mode.
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// Set append mode.
    pub fn append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }
}

/// A file handle for reading and writing.
///
/// This trait provides synchronous file operations that BF-Tree requires.
pub trait FileHandle: Read + Write + Seek + Send + Sync {
    /// Read bytes at a specific offset without changing the file position.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Write bytes at a specific offset without changing the file position.
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize>;

    /// Flush all buffered data to the underlying storage.
    fn flush_all(&self) -> io::Result<()>;

    /// Get the current length of the file.
    fn len(&self) -> io::Result<u64>;

    /// Check if the file is empty.
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Truncate or extend the file to the specified length.
    fn set_len(&self, len: u64) -> io::Result<()>;

    /// Sync all data and metadata to storage.
    fn sync_all(&self) -> io::Result<()>;

    /// Sync only data (not metadata) to storage.
    fn sync_data(&self) -> io::Result<()>;
}

/// Platform-agnostic file I/O provider.
///
/// This trait abstracts file system operations to support different platforms:
/// - Standard filesystem (Node.js, React Native)
/// - Origin Private File System (browsers via WASM)
/// - In-memory filesystem (Cloudflare Workers, testing)
pub trait FileIO: Send + Sync {
    /// The file handle type returned by this provider.
    type Handle: FileHandle;

    /// Open a file with the given options.
    fn open(&self, path: &str, options: OpenOptions) -> io::Result<Self::Handle>;

    /// Check if a file exists.
    fn exists(&self, path: &str) -> bool;

    /// Create a directory and all parent directories.
    fn create_dir_all(&self, path: &str) -> io::Result<()>;

    /// Remove a file.
    fn remove_file(&self, path: &str) -> io::Result<()>;

    /// Remove a directory and all its contents.
    fn remove_dir_all(&self, path: &str) -> io::Result<()>;

    /// Rename/move a file.
    fn rename(&self, from: &str, to: &str) -> io::Result<()>;

    /// List files in a directory.
    fn read_dir(&self, path: &str) -> io::Result<Vec<String>>;

    /// Get the root path of this file I/O provider.
    fn root_path(&self) -> &str;

    /// Sync all pending writes to storage.
    fn sync_all(&self) -> io::Result<()>;
}

/// Extension methods for FileHandle.
pub trait FileHandleExt: FileHandle {
    /// Read exactly `buf.len()` bytes from the file at the given offset.
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut total_read = 0;
        while total_read < buf.len() {
            let n = self.read_at(offset + total_read as u64, &mut buf[total_read..])?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected end of file",
                ));
            }
            total_read += n;
        }
        Ok(())
    }

    /// Write all bytes to the file at the given offset.
    fn write_all_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut total_written = 0;
        while total_written < buf.len() {
            let n = self.write_at(offset + total_written as u64, &buf[total_written..])?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write returned 0 bytes",
                ));
            }
            total_written += n;
        }
        Ok(())
    }
}

impl<T: FileHandle + ?Sized> FileHandleExt for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_options_builder() {
        let opts = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true);

        assert!(opts.read);
        assert!(opts.write);
        assert!(opts.create);
        assert!(!opts.truncate);
    }
}
