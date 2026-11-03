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
//! The store is a plain immutable byte KV, keyed by the node's hash: one
//! store per database, since a reclaimed node is deleted from it. It sees no schema, row or policy; every byte it returns is
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

/// A node's key in a chunk store: the object hash its bytes have, which
/// the store may check but need not.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeKey {
    pub hash: [u8; 32],
}

impl NodeKey {
    /// The name the node is stored under: its hash in lowercase hex.
    pub fn object_name(&self) -> String {
        let mut name = String::with_capacity(64);
        for byte in self.hash {
            name.push_str(&format!("{byte:02x}"));
        }
        name
    }
}

impl From<&ChunkRequest> for NodeKey {
    fn from(request: &ChunkRequest) -> Self {
        Self {
            hash: request.object_hash,
        }
    }
}

/// The future a [`ChunkStore`] operation returns. Not `Send`: the database
/// is thread-affine, and so are the futures it awaits.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + 'a>>;

/// An immutable byte KV for large-value nodes, keyed by node hash.
///
/// - `put` stores `bytes` under `node.hash`. A node's bytes never
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
    /// A store in `dir`, which is created if it does not exist. Temporary
    /// files left by writes a crash interrupted are removed.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| io("create", &dir, e))?;
        let store = Self { dir };
        store.sweep_temporaries()?;
        Ok(store)
    }

    /// Remove `<hash>.<pid>.tmp` files from the fan-out directories. Only
    /// one process opens a store, so none of them is a write in flight.
    fn sweep_temporaries(&self) -> Result<(), Error> {
        let fan_outs = std::fs::read_dir(&self.dir).map_err(|e| io("list", &self.dir, e))?;
        for fan_out in fan_outs {
            let fan_out = fan_out.map_err(|e| io("list", &self.dir, e))?.path();
            let is_fan_out = fan_out.is_dir()
                && fan_out
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.len() == 2 && name.bytes().all(|b| b.is_ascii_hexdigit())
                    });
            if !is_fan_out {
                continue;
            }
            for entry in std::fs::read_dir(&fan_out).map_err(|e| io("list", &fan_out, e))? {
                let path = entry.map_err(|e| io("list", &fan_out, e))?.path();
                if path.extension().is_some_and(|ext| ext == "tmp") {
                    match std::fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(io("delete", &path, error)),
                    }
                }
            }
        }
        Ok(())
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

/// Operations on a chunk store at once.
const CONCURRENCY: usize = 16;

/// One batch of chunk-store work: nodes reads wait on, settled leaves to
/// move out of SQLite and reclaimed ones to delete. [`crate::db::Db::tick`]
/// runs each batch itself, unless the store was opened
/// [`crate::options::DbConfig::with_host_chunk_io`]: then the host takes it
/// with [`crate::db::Db::take_chunk_io`], runs it with [`ChunkIo::run`]
/// wherever it likes and hands the result to
/// [`crate::db::Db::finish_chunk_io`]. One batch is out at a time, so a
/// delete never overtakes the put it undoes; until its result is back, no
/// further batch is handed out.
///
/// It is plain data (`Send`): the puts' marks are committed before it is
/// handed out, so the host may run it on another thread, and a host that
/// loses it (a crash) loses nothing the store does not redo on reopen.
#[derive(Debug, Default)]
pub struct ChunkIo {
    pub(crate) fetch: Vec<ChunkRequest>,
    pub(crate) puts: Vec<(NodeKey, Vec<u8>)>,
    pub(crate) deletes: Vec<NodeKey>,
}

/// What running a [`ChunkIo`] came to, for
/// [`crate::db::Db::finish_chunk_io`].
#[derive(Debug)]
pub struct ChunkIoDone {
    pub(crate) io: ChunkIo,
    pub(crate) fetched: Vec<Result<Option<Bytes>, Error>>,
    pub(crate) put: Vec<Result<(), Error>>,
    pub(crate) deleted: Vec<Result<(), Error>>,
}

impl ChunkIo {
    /// Whether the batch has nothing to do.
    pub fn is_empty(&self) -> bool {
        self.fetch.is_empty() && self.puts.is_empty() && self.deletes.is_empty()
    }

    /// Nodes to fetch, leaves to put and leaves to delete, in that order.
    pub fn counts(&self) -> (usize, usize, usize) {
        (self.fetch.len(), self.puts.len(), self.deletes.len())
    }

    /// Run the batch against `store`, up to 16 operations of each kind at
    /// once. A
    /// batch never holds a node in two lists, so the order among them does
    /// not matter.
    pub async fn run(self, store: &dyn ChunkStore) -> ChunkIoDone {
        use futures::StreamExt;
        let keys: Vec<NodeKey> = self.fetch.iter().map(NodeKey::from).collect();
        let fetched = futures::stream::iter(keys.iter().map(|key| store.get(key)))
            .buffered(CONCURRENCY)
            .collect::<Vec<_>>();
        let put = futures::stream::iter(self.puts.iter().map(|(key, bytes)| store.put(key, bytes)))
            .buffered(CONCURRENCY)
            .collect::<Vec<_>>();
        let deleted = futures::stream::iter(self.deletes.iter().map(|key| store.delete(key)))
            .buffered(CONCURRENCY)
            .collect::<Vec<_>>();
        let (fetched, put, deleted) = futures::join!(fetched, put, deleted);
        ChunkIoDone {
            io: self,
            fetched,
            put,
            deleted,
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn opening_a_file_store_removes_interrupted_writes() {
        let dir = std::env::temp_dir().join(format!("jazz-chunk-sweep-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = FileChunkStore::new(&dir).unwrap();
        let node = NodeKey { hash: [0xab; 32] };
        store.put_now(&node, b"kept").unwrap();
        let interrupted = store.path(&node).with_extension("4242.tmp");
        std::fs::write(&interrupted, b"torn").unwrap();
        std::fs::write(dir.join("owner"), b"not a fan-out\n").unwrap();

        let store = FileChunkStore::new(&dir).unwrap();
        assert!(!interrupted.exists());
        assert!(dir.join("owner").exists());
        assert_eq!(store.read(&node).unwrap().as_deref(), Some(&b"kept"[..]));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
