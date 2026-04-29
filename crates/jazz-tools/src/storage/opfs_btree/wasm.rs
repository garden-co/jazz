//! WASM-only entry points (`OpfsFile`-based constructors). The whole module
//! is cfg-gated to wasm32 in `mod.rs`, so the body is unconditional here.

use opfs_btree::OpfsFile;

use crate::storage::StorageError;

use super::{AnyFile, OpfsBTreeStorage, map_storage_err};

impl OpfsBTreeStorage {
    pub fn with_opfs(file: OpfsFile, cache_size_bytes: usize) -> Result<Self, StorageError> {
        Self::open_with_file(AnyFile::Opfs(file), cache_size_bytes)
    }

    pub async fn open_opfs(namespace: &str, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let file = OpfsFile::open(namespace).await.map_err(map_storage_err)?;
        Self::with_opfs(file, cache_size_bytes)
    }

    pub async fn destroy_opfs(namespace: &str) -> Result<(), StorageError> {
        OpfsFile::destroy(namespace).await.map_err(map_storage_err)
    }
}
