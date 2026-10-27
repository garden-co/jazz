//! Where large-value node bytes live.
//!
//! A store's SQLite file always holds the chunk metadata: which nodes
//! exist, their hashes, the edges between them, reference counts, receipts
//! and upload leases. Those commit with the rows that reference them, so a
//! row and the tree it names are one transaction whatever holds the bytes.
//!
//! By default the bytes live there too, in `_jazz_lv_node`. A
//! [`ChunkStore`] given to [`crate::options::DbConfig::with_chunk_store`]
//! takes the bytes of settled leaf nodes out of the file: a new node is
//! written to SQLite with its row, and [`crate::db::Db::tick`] copies each
//! settled leaf to the chunk store and then drops its bytes from SQLite.
//! Branch nodes (a few hashes each), and leaves still being staged, pushed
//! or uploaded, stay in the file. A leaf that is reclaimed is deleted from
//! the chunk store after the deletion commits.
//!
//! The store is a plain immutable byte KV, keyed by the node's opaque
//! locator. It sees no schema, row or policy; every byte it returns is
//! verified against the hash the caller expects before Jazz uses it. Its
//! operations are asynchronous, so object stores reached over the network
//! (S3, R2 from a Durable Object) fit. A store that can also answer a read
//! without waiting (a local directory) says so through
//! [`ChunkStore::get_now`], and reads never wait on it; a read from a
//! store that cannot is demand that the next tick fetches, as a replica's
//! reads of chunks its upstream holds are.

use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
use std::pin::Pin;

use bytes::Bytes;
use jazz_types::large_values::chunks::ChunkRequest;

use crate::options::Error;
#[cfg(not(target_arch = "wasm32"))]
use crate::options::ErrorCode;

/// A node's key in a chunk store: its opaque locator, and the object hash
/// its bytes must have (which the store may keep, but need not check).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeKey {
    pub locator: [u8; 32],
    pub hash: [u8; 32],
}

impl NodeKey {
    /// The name the node is stored under: its locator in lowercase hex.
    pub fn object_name(&self) -> String {
        let mut name = String::with_capacity(64);
        for byte in self.locator {
            name.push_str(&format!("{byte:02x}"));
        }
        name
    }
}

impl From<&ChunkRequest> for NodeKey {
    fn from(request: &ChunkRequest) -> Self {
        Self {
            locator: *request.locator.as_bytes(),
            hash: request.object_hash,
        }
    }
}

/// The future a [`ChunkStore`] operation returns. Not `Send`: the database
/// is thread-affine, and so are the futures it awaits.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + 'a>>;

/// An immutable byte KV for large-value nodes, keyed by locator.
///
/// - `put` stores `bytes` under `node.locator`. A node's bytes never
///   change, so putting a key again (after a crash, or a retry) puts the
///   same bytes; it must succeed.
/// - `get` returns what `put` stored, or `None` if nothing is stored.
/// - `delete` removes the key; deleting an absent key succeeds.
///
/// Jazz puts a node before it drops the bytes from SQLite, and deletes it
/// only after its metadata is gone, so a store never has to be transactional
/// with the database. An operation that fails is retried on a later tick.
pub trait ChunkStore: Send + Sync {
    fn put<'a>(&'a self, node: &'a NodeKey, bytes: &'a [u8]) -> StoreFuture<'a, ()>;

    fn get<'a>(&'a self, node: &'a NodeKey) -> StoreFuture<'a, Option<Bytes>>;

    fn delete<'a>(&'a self, node: &'a NodeKey) -> StoreFuture<'a, ()>;

    /// `get` without waiting, for stores that can (`None` if this one
    /// cannot). Reads of a store that answers here never wait for a tick.
    fn get_now(&self, node: &NodeKey) -> Option<Result<Option<Bytes>, Error>> {
        let _ = node;
        None
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn io(what: &str, path: &Path, error: std::io::Error) -> Error {
    Error::new(
        ErrorCode::Storage,
        format!("chunk store: {what} {}: {error}", path.display()),
    )
}

/// Nodes as files in a directory, one per node under a two-hex-digit
/// fan-out directory (`ab/abcd…`). A write lands in a temporary file that
/// is synced and renamed into place, so a crash leaves either no file or
/// the whole node, never a torn one. Reads answer at once.
#[cfg(not(target_arch = "wasm32"))]
pub struct FileChunkStore {
    dir: PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileChunkStore {
    /// A store in `dir`, which is created if it does not exist.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| io("create", &dir, e))?;
        Ok(Self { dir })
    }

    fn path(&self, node: &NodeKey) -> PathBuf {
        let name = node.object_name();
        self.dir.join(&name[..2]).join(name)
    }

    fn put_now(&self, node: &NodeKey, bytes: &[u8]) -> Result<(), Error> {
        use std::io::Write;
        let path = self.path(node);
        let parent = path.parent().expect("a fan-out directory");
        std::fs::create_dir_all(parent).map_err(|e| io("create", parent, e))?;
        let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
        let mut file =
            std::fs::File::create(&temporary).map_err(|e| io("create", &temporary, e))?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|e| io("write", &temporary, e))?;
        drop(file);
        std::fs::rename(&temporary, &path).map_err(|e| io("rename", &temporary, e))?;
        // The rename is durable once the directory is.
        std::fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| io("sync", parent, e))
    }

    fn read(&self, node: &NodeKey) -> Result<Option<Bytes>, Error> {
        let path = self.path(node);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io("read", &path, error)),
        }
    }

    fn remove(&self, node: &NodeKey) -> Result<(), Error> {
        let path = self.path(node);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io("delete", &path, error)),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ChunkStore for FileChunkStore {
    fn put<'a>(&'a self, node: &'a NodeKey, bytes: &'a [u8]) -> StoreFuture<'a, ()> {
        Box::pin(std::future::ready(self.put_now(node, bytes)))
    }

    fn get<'a>(&'a self, node: &'a NodeKey) -> StoreFuture<'a, Option<Bytes>> {
        Box::pin(std::future::ready(self.read(node)))
    }

    fn delete<'a>(&'a self, node: &'a NodeKey) -> StoreFuture<'a, ()> {
        Box::pin(std::future::ready(self.remove(node)))
    }

    fn get_now(&self, node: &NodeKey) -> Option<Result<Option<Bytes>, Error>> {
        Some(self.read(node))
    }
}
