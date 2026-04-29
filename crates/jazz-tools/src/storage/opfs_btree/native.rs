//! Native-only entry points (anything that touches `std::fs::Path` or
//! `opfs_btree::StdFile`). The whole module is cfg-gated to non-wasm32 in
//! `mod.rs`, so the body is unconditional here.

use std::path::Path;

use opfs_btree::StdFile;

use crate::storage::StorageError;

use super::{AnyFile, OpfsBTreeStorage, map_storage_err};

impl OpfsBTreeStorage {
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let file = StdFile::open(path).map_err(map_storage_err)?;
        Self::open_with_file(AnyFile::Std(file), cache_size_bytes)
    }
}
