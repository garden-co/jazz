//! Exclusive admission of Node SQLite roots. The upstream host retains the lock
//! across schema views and until the core retires its persistent owner.
use super::*;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

pub(super) enum NativeStorage {
    File { path: PathBuf, lock: Rc<File> },
}

/// `jazz.sqlite.native-root.v1`: one engine file under an exclusively owned
/// root. Existing non-SQLite roots are refused, never opened as empty stores.
pub(super) fn persistent_storage(
    root: &str,
) -> std::result::Result<NativeStorage, PersistentOpenError> {
    let root = std::path::Path::new(root);
    std::fs::create_dir_all(root).map_err(napi_error)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(".jazz-native-owner.lock"))
        .map_err(napi_error)?;
    lock.try_lock().map_err(|error| {
        napi::Error::from_reason(format!(
            "native database root is already open or cannot be locked: {error}"
        ))
    })?;
    // This is a dedicated root, not an arbitrary directory containing user
    // files. Stop at the first foreign entry (including a RocksDB CURRENT or
    // MANIFEST) before opening any SQLite database or installing owner metadata.
    for entry in std::fs::read_dir(root).map_err(napi_error)? {
        let entry = entry.map_err(napi_error)?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            napi::Error::from_reason("native database root contains an unknown storage entry")
        })?;
        let known_name = matches!(
            name,
            ".jazz-native-owner.lock"
                | ".jazz-account-owner"
                | "jazz.sqlite"
                | "jazz.sqlite-wal"
                | "jazz.sqlite-shm"
                | "jazz.sqlite-journal"
        ) || account_owner::is_temporary_marker_name(name);
        // A recognized basename must still be a regular file. Do not follow
        // symlinks into another root or mistake a directory for an engine file.
        if !known_name || !entry.file_type().map_err(napi_error)?.is_file() {
            return Err(PersistentOpenError::UnsupportedStorageCodecs(
                jazz::db::Error::new(
                    jazz::db::ErrorCode::UnsupportedStorageCodecs,
                    "native database root uses an unsupported storage layout; expected jazz.sqlite.native-root.v1",
                ),
            ));
        }
    }
    Ok(NativeStorage::File {
        path: root.join("jazz.sqlite"),
        lock: Rc::new(lock),
    })
}
