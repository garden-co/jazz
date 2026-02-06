//! In-memory file I/O implementation.
//!
//! This implementation is used for:
//! - Cloudflare Workers (where VFS /tmp doesn't persist across requests)
//! - Testing
//! - Any environment where persistent storage isn't available

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};

use super::traits::{FileHandle, FileIO, OpenOptions};

/// Shared state for in-memory files.
type FileData = Arc<RwLock<Vec<u8>>>;

/// In-memory file handle.
pub struct InMemoryHandle {
    data: FileData,
    position: RwLock<u64>,
    read: bool,
    write: bool,
}

impl InMemoryHandle {
    fn new(data: FileData, read: bool, write: bool) -> Self {
        Self {
            data,
            position: RwLock::new(0),
            read,
            write,
        }
    }
}

impl Read for InMemoryHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.read {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "file not opened for reading",
            ));
        }

        let data = self.data.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let mut pos = self.position.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let start = *pos as usize;
        if start >= data.len() {
            return Ok(0);
        }

        let end = (start + buf.len()).min(data.len());
        let len = end - start;
        buf[..len].copy_from_slice(&data[start..end]);
        *pos += len as u64;

        Ok(len)
    }
}

impl Write for InMemoryHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.write {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "file not opened for writing",
            ));
        }

        let mut data = self.data.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let mut pos = self.position.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let start = *pos as usize;
        let end = start + buf.len();

        // Extend if necessary
        if end > data.len() {
            data.resize(end, 0);
        }

        data[start..end].copy_from_slice(buf);
        *pos = end as u64;

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // No-op for in-memory
        Ok(())
    }
}

impl Seek for InMemoryHandle {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let data = self.data.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let mut position = self.position.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let new_pos = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => data.len() as i64 + n,
            SeekFrom::Current(n) => *position as i64 + n,
        };

        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }

        *position = new_pos as u64;
        Ok(*position)
    }
}

impl FileHandle for InMemoryHandle {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if !self.read {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "file not opened for reading",
            ));
        }

        let data = self.data.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let start = offset as usize;
        if start >= data.len() {
            return Ok(0);
        }

        let end = (start + buf.len()).min(data.len());
        let len = end - start;
        buf[..len].copy_from_slice(&data[start..end]);

        Ok(len)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        if !self.write {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "file not opened for writing",
            ));
        }

        let mut data = self.data.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let start = offset as usize;
        let end = start + buf.len();

        // Extend if necessary
        if end > data.len() {
            data.resize(end, 0);
        }

        data[start..end].copy_from_slice(buf);

        Ok(buf.len())
    }

    fn flush_all(&self) -> io::Result<()> {
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        let data = self.data.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        Ok(data.len() as u64)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let mut data = self.data.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        data.resize(len as usize, 0);
        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }
}

/// In-memory file I/O implementation.
///
/// This is useful for:
/// - Cloudflare Workers (VFS /tmp doesn't persist across requests)
/// - Testing storage implementations
/// - Any environment without persistent filesystem access
///
/// # Example
///
/// ```rust
/// use cojson_storage::file_io::{InMemoryFileIO, FileIO, OpenOptions};
///
/// let fs = InMemoryFileIO::new();
/// let mut file = fs.open("test.txt", OpenOptions::new().write(true).create(true)).unwrap();
/// ```
pub struct InMemoryFileIO {
    files: RwLock<HashMap<String, FileData>>,
    directories: RwLock<std::collections::HashSet<String>>,
}

impl InMemoryFileIO {
    /// Create a new in-memory file system.
    pub fn new() -> Self {
        Self {
            files: RwLock::new(HashMap::new()),
            directories: RwLock::new(std::collections::HashSet::new()),
        }
    }

    /// Get the total size of all files in memory.
    pub fn total_size(&self) -> usize {
        let files = self.files.read().unwrap();
        files
            .values()
            .map(|f| f.read().unwrap().len())
            .sum()
    }

    /// Get the number of files.
    pub fn file_count(&self) -> usize {
        self.files.read().unwrap().len()
    }

    /// Clear all files and directories.
    pub fn clear(&self) {
        self.files.write().unwrap().clear();
        self.directories.write().unwrap().clear();
    }
}

impl Default for InMemoryFileIO {
    fn default() -> Self {
        Self::new()
    }
}

impl FileIO for InMemoryFileIO {
    type Handle = InMemoryHandle;

    fn open(&self, path: &str, options: OpenOptions) -> io::Result<Self::Handle> {
        let mut files = self.files.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let data = if let Some(existing) = files.get(path) {
            if options.create_new {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "file already exists",
                ));
            }

            let data = if options.truncate {
                let new_data = Arc::new(RwLock::new(Vec::new()));
                files.insert(path.to_string(), Arc::clone(&new_data));
                new_data
            } else {
                Arc::clone(existing)
            };

            data
        } else {
            if !options.create && !options.create_new {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "file not found",
                ));
            }

            let data = Arc::new(RwLock::new(Vec::new()));
            files.insert(path.to_string(), Arc::clone(&data));
            data
        };

        Ok(InMemoryHandle::new(data, options.read, options.write))
    }

    fn exists(&self, path: &str) -> bool {
        let files = self.files.read().unwrap();
        let dirs = self.directories.read().unwrap();
        files.contains_key(path) || dirs.contains(path)
    }

    fn create_dir_all(&self, path: &str) -> io::Result<()> {
        let mut dirs = self.directories.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        // Create all parent directories
        let mut current = String::new();
        for part in path.split('/').filter(|p| !p.is_empty()) {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(part);
            dirs.insert(current.clone());
        }

        Ok(())
    }

    fn remove_file(&self, path: &str) -> io::Result<()> {
        let mut files = self.files.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        if files.remove(path).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file not found",
            ));
        }

        Ok(())
    }

    fn remove_dir_all(&self, path: &str) -> io::Result<()> {
        let mut files = self.files.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        let mut dirs = self.directories.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        // Remove all files under this directory
        let prefix = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };

        files.retain(|k, _| !k.starts_with(&prefix) && k != path);
        dirs.retain(|d| !d.starts_with(&prefix) && d != path);

        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut files = self.files.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        if let Some(data) = files.remove(from) {
            files.insert(to.to_string(), data);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file not found",
            ))
        }
    }

    fn read_dir(&self, path: &str) -> io::Result<Vec<String>> {
        let files = self.files.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;

        let prefix = if path.is_empty() || path == "/" {
            String::new()
        } else if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };

        let mut entries = std::collections::HashSet::new();

        for key in files.keys() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                // Get the first path component
                if let Some(idx) = rest.find('/') {
                    entries.insert(rest[..idx].to_string());
                } else {
                    entries.insert(rest.to_string());
                }
            }
        }

        Ok(entries.into_iter().collect())
    }

    fn root_path(&self) -> &str {
        ""
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn test_in_memory_file_io_create_and_write() {
        let fs = InMemoryFileIO::new();

        let mut handle = fs
            .open("test.txt", OpenOptions::new().write(true).create(true))
            .unwrap();

        handle.write_all(b"Hello, World!").unwrap();
        drop(handle);

        let mut handle = fs
            .open("test.txt", OpenOptions::new().read(true))
            .unwrap();

        let mut buf = String::new();
        handle.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "Hello, World!");
    }

    #[test]
    fn test_in_memory_file_io_read_at_write_at() {
        let fs = InMemoryFileIO::new();

        let handle = fs
            .open("test.txt", OpenOptions::new().read(true).write(true).create(true))
            .unwrap();

        handle.write_at(0, b"Hello").unwrap();
        handle.write_at(5, b", World!").unwrap();

        let mut buf = [0u8; 13];
        handle.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"Hello, World!");
    }

    #[test]
    fn test_in_memory_file_io_exists() {
        let fs = InMemoryFileIO::new();

        assert!(!fs.exists("test.txt"));

        fs.open("test.txt", OpenOptions::new().write(true).create(true))
            .unwrap();

        assert!(fs.exists("test.txt"));
    }

    #[test]
    fn test_in_memory_file_io_remove() {
        let fs = InMemoryFileIO::new();

        fs.open("test.txt", OpenOptions::new().write(true).create(true))
            .unwrap();

        assert!(fs.exists("test.txt"));
        fs.remove_file("test.txt").unwrap();
        assert!(!fs.exists("test.txt"));
    }

    #[test]
    fn test_in_memory_file_io_create_dir_all() {
        let fs = InMemoryFileIO::new();

        fs.create_dir_all("a/b/c").unwrap();

        assert!(fs.exists("a"));
        assert!(fs.exists("a/b"));
        assert!(fs.exists("a/b/c"));
    }

    #[test]
    fn test_in_memory_file_io_read_dir() {
        let fs = InMemoryFileIO::new();

        fs.open("dir/file1.txt", OpenOptions::new().write(true).create(true))
            .unwrap();
        fs.open("dir/file2.txt", OpenOptions::new().write(true).create(true))
            .unwrap();
        fs.open("dir/subdir/file3.txt", OpenOptions::new().write(true).create(true))
            .unwrap();

        let entries = fs.read_dir("dir").unwrap();
        assert_eq!(entries.len(), 3);
        assert!(entries.contains(&"file1.txt".to_string()));
        assert!(entries.contains(&"file2.txt".to_string()));
        assert!(entries.contains(&"subdir".to_string()));
    }

    #[test]
    fn test_in_memory_file_io_truncate() {
        let fs = InMemoryFileIO::new();

        let mut handle = fs
            .open("test.txt", OpenOptions::new().write(true).create(true))
            .unwrap();
        handle.write_all(b"Hello, World!").unwrap();
        drop(handle);

        let mut handle = fs
            .open("test.txt", OpenOptions::new().write(true).truncate(true))
            .unwrap();
        handle.write_all(b"Hi").unwrap();
        drop(handle);

        let mut handle = fs
            .open("test.txt", OpenOptions::new().read(true))
            .unwrap();
        let mut buf = String::new();
        handle.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "Hi");
    }

    #[test]
    fn test_in_memory_file_io_set_len() {
        let fs = InMemoryFileIO::new();

        let handle = fs
            .open("test.txt", OpenOptions::new().write(true).create(true))
            .unwrap();
        handle.write_at(0, b"Hello").unwrap();
        assert_eq!(handle.len().unwrap(), 5);

        handle.set_len(10).unwrap();
        assert_eq!(handle.len().unwrap(), 10);

        handle.set_len(3).unwrap();
        assert_eq!(handle.len().unwrap(), 3);
    }
}
