//! Scratch store for query state spilled out of memory.
//!
//! Groove moves join arrangements here when the server runs short of memory
//! (see [`groove::ivm::SpillStorage`]). Nothing in it outlives the process:
//! the directory is wiped when the store opens and removed when it drops,
//! writes skip the WAL, and nothing is ever synced, like a Postgres temp
//! file. It shares the process block cache and write buffer budget with the
//! durable stores, so spilling cannot grow RocksDB's own memory.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use groove::ivm::{SpillEntries, SpillStorage};
use rocksdb::{DB, Direction, IteratorMode, ReadOptions, WriteBatch, WriteOptions};

use crate::memory;

/// RocksDB-backed [`SpillStorage`] in a directory it owns.
pub struct RocksDbSpillStorage {
    path: PathBuf,
    /// Reports the store's memory while open; released before `db` closes.
    memory_registration: Option<memory::Registration>,
    db: Option<Arc<DB>>,
    write_options: WriteOptions,
}

impl RocksDbSpillStorage {
    /// Open an empty scratch store at `path`, discarding anything a previous
    /// process left there.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            std::fs::remove_dir_all(&path)
                .map_err(|error| format!("clearing spill directory {}: {error}", path.display()))?;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let shared_memory = memory::shared_memory();
        let mut options = crate::rocksdb_options(
            &shared_memory.block_cache,
            &shared_memory.write_buffer_manager,
        );
        options.create_if_missing(true);
        let db = Arc::new(DB::open(&options, &path).map_err(|error| error.to_string())?);
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        Ok(Self {
            path,
            memory_registration: Some(memory::Registration::new(&db)),
            db: Some(db),
            write_options,
        })
    }

    fn db(&self) -> &DB {
        self.db.as_ref().expect("spill store is open until dropped")
    }
}

impl SpillStorage for RocksDbSpillStorage {
    fn write(&self, entries: SpillEntries) -> Result<(), String> {
        let mut batch = WriteBatch::default();
        for (key, value) in entries {
            batch.put(key, value);
        }
        self.db()
            .write_opt(&batch, &self.write_options)
            .map_err(|error| error.to_string())
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<SpillEntries, String> {
        let mut options = ReadOptions::default();
        if let Some(upper) = prefix_successor(prefix) {
            options.set_iterate_upper_bound(upper);
        }
        let start = after.unwrap_or(prefix);
        let iterator = self
            .db()
            .iterator_opt(IteratorMode::From(start, Direction::Forward), options);
        let mut entries = Vec::new();
        for item in iterator {
            let (key, value) = item.map_err(|error| error.to_string())?;
            if !key.starts_with(prefix) {
                break;
            }
            if after.is_some_and(|after| key.as_ref() == after) {
                continue;
            }
            entries.push((key.into_vec(), value.into_vec()));
            if entries.len() == limit {
                break;
            }
        }
        Ok(entries)
    }

    fn delete_prefix(&self, prefix: &[u8]) -> Result<(), String> {
        let mut batch = WriteBatch::default();
        match prefix_successor(prefix) {
            Some(upper) => batch.delete_range(prefix, &upper),
            // No key bounds an all-`0xff` prefix from above; delete each key.
            None => {
                for item in self
                    .db()
                    .iterator(IteratorMode::From(prefix, Direction::Forward))
                {
                    batch.delete(item.map_err(|error| error.to_string())?.0);
                }
            }
        }
        self.db()
            .write_opt(&batch, &self.write_options)
            .map_err(|error| error.to_string())
    }
}

impl Drop for RocksDbSpillStorage {
    fn drop(&mut self) {
        // Leaving the registry first waits out a concurrent memory report, so
        // the database really closes here, before its files are removed.
        drop(self.memory_registration.take());
        drop(self.db.take());
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// The smallest key greater than every key starting with `prefix`, or `None`
/// when `prefix` is all `0xff`.
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.pop() {
        if last != 0xff {
            upper.push(last + 1);
            return Some(upper);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &[u8], value: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (key.to_vec(), value.to_vec())
    }

    #[test]
    fn scans_pages_within_a_prefix_and_deletes_it() {
        let dir = tempfile::tempdir().unwrap();
        let spill = RocksDbSpillStorage::open(dir.path().join("spill")).unwrap();
        spill
            .write(vec![
                entry(b"a\x01", b"1"),
                entry(b"a\x02", b"2"),
                entry(b"a\xff", b"3"),
                entry(b"b\x00", b"4"),
            ])
            .unwrap();

        let first = spill.scan_prefix(b"a", None, 2).unwrap();
        assert_eq!(first, vec![entry(b"a\x01", b"1"), entry(b"a\x02", b"2")]);
        let rest = spill.scan_prefix(b"a", Some(b"a\x02"), 2).unwrap();
        assert_eq!(rest, vec![entry(b"a\xff", b"3")]);

        spill.delete_prefix(b"a").unwrap();
        assert!(spill.scan_prefix(b"a", None, 10).unwrap().is_empty());
        assert_eq!(
            spill.scan_prefix(b"b", None, 10).unwrap(),
            vec![entry(b"b\x00", b"4")]
        );
    }

    #[test]
    fn opening_discards_old_contents_and_dropping_removes_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill");
        {
            let spill = RocksDbSpillStorage::open(&path).unwrap();
            spill.write(vec![entry(b"k", b"v")]).unwrap();
        }
        assert!(!path.exists());

        std::fs::create_dir_all(path.join("stale")).unwrap();
        let spill = RocksDbSpillStorage::open(&path).unwrap();
        assert!(spill.scan_prefix(b"", None, 10).unwrap().is_empty());
        assert!(!path.join("stale").exists());
    }
}
