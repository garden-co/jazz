//! Standard filesystem I/O implementation.
//!
//! This implementation uses the standard library's filesystem APIs and is
//! suitable for Node.js and React Native environments.

use std::fs::{self, File, OpenOptions as StdOpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use super::traits::{FileHandle, FileIO, OpenOptions};

/// Standard filesystem file handle.
pub struct StdFileHandle {
    file: RwLock<File>,
}

impl StdFileHandle {
    fn new(file: File) -> Self {
        Self {
            file: RwLock::new(file),
        }
    }
}

impl Read for StdFileHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut file = self.file.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.read(buf)
    }
}

impl Write for StdFileHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file = self.file.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let file = self.file.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.sync_data()
    }
}

impl Seek for StdFileHandle {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let mut file = self.file.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.seek(pos)
    }
}

impl FileHandle for StdFileHandle {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let file = self.file.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.read_at(buf, offset)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let file = self.file.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.write_at(buf, offset)
    }

    fn flush_all(&self) -> io::Result<()> {
        let file = self.file.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.sync_all()
    }

    fn len(&self) -> io::Result<u64> {
        let file = self.file.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        Ok(file.metadata()?.len())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let file = self.file.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.set_len(len)
    }

    fn sync_all(&self) -> io::Result<()> {
        let file = self.file.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.sync_all()
    }

    fn sync_data(&self) -> io::Result<()> {
        let file = self.file.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        file.sync_data()
    }
}

/// Standard filesystem I/O implementation.
///
/// This uses the operating system's native filesystem and is suitable for
/// Node.js and React Native environments where persistent storage is available.
///
/// # Example
///
/// ```rust,ignore
/// use cojson_storage::file_io::{StdFileIO, FileIO, OpenOptions};
///
/// let fs = StdFileIO::new("/path/to/storage").unwrap();
/// let file = fs.open("data.bin", OpenOptions::new().read(true).write(true).create(true)).unwrap();
/// ```
pub struct StdFileIO {
    root: PathBuf,
}

impl StdFileIO {
    /// Create a new standard filesystem I/O provider.
    ///
    /// The root path will be created if it doesn't exist.
    pub fn new<P: AsRef<Path>>(root: P) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Get the full path for a relative path.
    fn full_path(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }
}

impl FileIO for StdFileIO {
    type Handle = StdFileHandle;

    fn open(&self, path: &str, options: OpenOptions) -> io::Result<Self::Handle> {
        let full_path = self.full_path(path);

        // Create parent directories if needed
        if options.create || options.create_new {
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent)?;
            }
        }

        let file = StdOpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .create_new(options.create_new)
            .truncate(options.truncate)
            .append(options.append)
            .open(full_path)?;

        Ok(StdFileHandle::new(file))
    }

    fn exists(&self, path: &str) -> bool {
        self.full_path(path).exists()
    }

    fn create_dir_all(&self, path: &str) -> io::Result<()> {
        fs::create_dir_all(self.full_path(path))
    }

    fn remove_file(&self, path: &str) -> io::Result<()> {
        fs::remove_file(self.full_path(path))
    }

    fn remove_dir_all(&self, path: &str) -> io::Result<()> {
        fs::remove_dir_all(self.full_path(path))
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        fs::rename(self.full_path(from), self.full_path(to))
    }

    fn read_dir(&self, path: &str) -> io::Result<Vec<String>> {
        let full_path = self.full_path(path);
        let mut entries = Vec::new();

        for entry in fs::read_dir(full_path)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                entries.push(name.to_string());
            }
        }

        Ok(entries)
    }

    fn root_path(&self) -> &str {
        self.root.to_str().unwrap_or("")
    }

    fn sync_all(&self) -> io::Result<()> {
        // Sync the root directory
        let dir = fs::File::open(&self.root)?;
        dir.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use tempfile::TempDir;

    #[test]
    fn test_std_file_io_create_and_write() {
        let tmp = TempDir::new().unwrap();
        let fs = StdFileIO::new(tmp.path()).unwrap();

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
    fn test_std_file_io_read_at_write_at() {
        let tmp = TempDir::new().unwrap();
        let fs = StdFileIO::new(tmp.path()).unwrap();

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
    fn test_std_file_io_exists() {
        let tmp = TempDir::new().unwrap();
        let fs = StdFileIO::new(tmp.path()).unwrap();

        assert!(!fs.exists("test.txt"));

        fs.open("test.txt", OpenOptions::new().write(true).create(true))
            .unwrap();

        assert!(fs.exists("test.txt"));
    }

    #[test]
    fn test_std_file_io_nested_directories() {
        let tmp = TempDir::new().unwrap();
        let fs = StdFileIO::new(tmp.path()).unwrap();

        // Creating a file in nested directories should auto-create parents
        fs.open(
            "a/b/c/test.txt",
            OpenOptions::new().write(true).create(true),
        )
        .unwrap();

        assert!(fs.exists("a/b/c/test.txt"));
    }

    #[test]
    fn test_std_file_io_rename() {
        let tmp = TempDir::new().unwrap();
        let fs = StdFileIO::new(tmp.path()).unwrap();

        let mut handle = fs
            .open("old.txt", OpenOptions::new().write(true).create(true))
            .unwrap();
        handle.write_all(b"content").unwrap();
        drop(handle);

        fs.rename("old.txt", "new.txt").unwrap();

        assert!(!fs.exists("old.txt"));
        assert!(fs.exists("new.txt"));
    }

    #[test]
    fn test_std_file_io_read_dir() {
        let tmp = TempDir::new().unwrap();
        let fs = StdFileIO::new(tmp.path()).unwrap();

        fs.open("file1.txt", OpenOptions::new().write(true).create(true))
            .unwrap();
        fs.open("file2.txt", OpenOptions::new().write(true).create(true))
            .unwrap();
        fs.create_dir_all("subdir").unwrap();

        let entries = fs.read_dir("").unwrap();
        assert!(entries.contains(&"file1.txt".to_string()));
        assert!(entries.contains(&"file2.txt".to_string()));
        assert!(entries.contains(&"subdir".to_string()));
    }
}
