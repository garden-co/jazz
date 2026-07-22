//! Sync bridge over a natively-async ordered-KV backend.
//!
//! [`AsyncOrderedKvStorage`] is the async counterpart of the crate's
//! [`OrderedKvStorage`] seam. The async backend lives on a dedicated worker
//! thread that owns its own tokio runtime; [`SyncBridgeStorage`] forwards each
//! sync call as an owned request over a channel and blocks on the reply. The
//! engine and its frontends stay synchronous and never touch a reactor: every
//! future they observe resolves immediately because all real async work has
//! already completed on the worker thread.
//!
//! Scans cross the channel as materialized `Vec<KeyValue>` rather than through
//! the borrowed [`ScanVisitor`] callback; the bridge replays the vector through
//! the caller's visitor on the calling thread.

use std::future::Future;
use std::sync::mpsc::SyncSender;
use std::thread::JoinHandle;

use super::{
    ColumnFamilyName, Error, Key, KeyValue, OrderedKvStorage, OwnedWriteOperation,
    ReopenableStorage, ScanVisitor, Value, WriteOperation,
};

/// Natively-async ordered KV backend. Implementations live entirely on the
/// bridge worker thread and need not be `Send`/`Sync`; only request and reply
/// payloads cross threads.
#[allow(async_fn_in_trait)]
pub trait AsyncOrderedKvStorage: Sized + 'static {
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    async fn set(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), Error>;
    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), Error>;
    /// Ascending byte-lexicographic order, start inclusive, end exclusive.
    async fn scan_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Vec<KeyValue>, Error>;
    async fn scan_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Vec<KeyValue>, Error>;
    /// Atomic batch; must reject unknown column families with
    /// [`Error::ColumnFamilyNotFound`] before writing anything.
    async fn write_many(&self, operations: Vec<OwnedWriteOperation>) -> Result<(), Error>;
    async fn close(&self) -> Result<(), Error>;
    /// Reopen with an expanded column-family set, consuming self like
    /// [`ReopenableStorage::reopen`].
    async fn reopen(self, column_families: Vec<String>) -> Result<Self, Error>;
    fn column_family_names(&self) -> Option<Vec<String>>;
}

enum Request {
    Get {
        cf: String,
        key: Vec<u8>,
        reply: SyncSender<Result<Option<Vec<u8>>, Error>>,
    },
    Set {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
        reply: SyncSender<Result<(), Error>>,
    },
    Delete {
        cf: String,
        key: Vec<u8>,
        reply: SyncSender<Result<(), Error>>,
    },
    ScanRange {
        cf: String,
        start: Vec<u8>,
        end: Vec<u8>,
        reply: SyncSender<Result<Vec<KeyValue>, Error>>,
    },
    ScanPrefix {
        cf: String,
        prefix: Vec<u8>,
        reply: SyncSender<Result<Vec<KeyValue>, Error>>,
    },
    WriteMany {
        operations: Vec<OwnedWriteOperation>,
        reply: SyncSender<Result<(), Error>>,
    },
    ColumnFamilyNames {
        reply: SyncSender<Option<Vec<String>>>,
    },
    Close {
        reply: SyncSender<Result<(), Error>>,
    },
    Reopen {
        column_families: Vec<String>,
        reply: SyncSender<Result<(), Error>>,
    },
    Shutdown,
}

/// Sync [`OrderedKvStorage`] facade over an [`AsyncOrderedKvStorage`] running
/// on a dedicated worker thread.
pub struct SyncBridgeStorage {
    request_tx: tokio::sync::mpsc::UnboundedSender<Request>,
    worker: Option<JoinHandle<()>>,
}

impl SyncBridgeStorage {
    /// Spawn the worker thread, run `open` on its runtime, and return once the
    /// backend is ready. The backend is created on and never leaves the worker
    /// thread, so neither it nor the open future needs `Send`.
    pub fn open<A, F, Fut>(open: F) -> Result<Self, Error>
    where
        A: AsyncOrderedKvStorage,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<A, Error>>,
    {
        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel();
        let (startup_tx, startup_rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("groove-async-storage".into())
            .spawn(move || {
                // One tokio worker thread besides this blocked one so backend
                // background tasks keep making progress mid-request.
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = startup_tx.send(Err(Error::AsyncBridge(format!(
                            "failed to build bridge runtime: {err}"
                        ))));
                        return;
                    }
                };
                runtime.block_on(async move {
                    match open().await {
                        Ok(storage) => {
                            let _ = startup_tx.send(Ok(()));
                            worker_loop(storage, request_rx).await;
                        }
                        Err(err) => {
                            let _ = startup_tx.send(Err(err));
                        }
                    }
                });
            })
            .map_err(|err| {
                Error::AsyncBridge(format!("failed to spawn bridge worker thread: {err}"))
            })?;
        match startup_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                request_tx,
                worker: Some(worker),
            }),
            Ok(Err(err)) => {
                let _ = worker.join();
                Err(err)
            }
            Err(_) => {
                let _ = worker.join();
                Err(Error::AsyncBridge(
                    "bridge worker exited during startup".into(),
                ))
            }
        }
    }

    fn roundtrip<T>(&self, build: impl FnOnce(SyncSender<T>) -> Request) -> Result<T, Error> {
        let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
        self.request_tx
            .send(build(reply_tx))
            .map_err(|_| Error::AsyncBridge("bridge worker unavailable".into()))?;
        reply_rx
            .recv()
            .map_err(|_| Error::AsyncBridge("bridge worker dropped reply".into()))
    }
}

fn storage_closed() -> Error {
    Error::AsyncBridge("storage closed".into())
}

async fn worker_loop<A: AsyncOrderedKvStorage>(
    storage: A,
    mut requests: tokio::sync::mpsc::UnboundedReceiver<Request>,
) {
    let mut storage = Some(storage);
    while let Some(request) = requests.recv().await {
        match request {
            Request::Get { cf, key, reply } => {
                let result = match storage.as_ref() {
                    Some(storage) => storage.get(&cf, &key).await,
                    None => Err(storage_closed()),
                };
                let _ = reply.send(result);
            }
            Request::Set {
                cf,
                key,
                value,
                reply,
            } => {
                let result = match storage.as_ref() {
                    Some(storage) => storage.set(&cf, &key, &value).await,
                    None => Err(storage_closed()),
                };
                let _ = reply.send(result);
            }
            Request::Delete { cf, key, reply } => {
                let result = match storage.as_ref() {
                    Some(storage) => storage.delete(&cf, &key).await,
                    None => Err(storage_closed()),
                };
                let _ = reply.send(result);
            }
            Request::ScanRange {
                cf,
                start,
                end,
                reply,
            } => {
                let result = match storage.as_ref() {
                    Some(storage) => storage.scan_range(&cf, &start, &end).await,
                    None => Err(storage_closed()),
                };
                let _ = reply.send(result);
            }
            Request::ScanPrefix { cf, prefix, reply } => {
                let result = match storage.as_ref() {
                    Some(storage) => storage.scan_prefix(&cf, &prefix).await,
                    None => Err(storage_closed()),
                };
                let _ = reply.send(result);
            }
            Request::WriteMany { operations, reply } => {
                let result = match storage.as_ref() {
                    Some(storage) => storage.write_many(operations).await,
                    None => Err(storage_closed()),
                };
                let _ = reply.send(result);
            }
            Request::ColumnFamilyNames { reply } => {
                let _ = reply.send(
                    storage
                        .as_ref()
                        .and_then(AsyncOrderedKvStorage::column_family_names),
                );
            }
            Request::Close { reply } => {
                let result = match storage.take() {
                    Some(storage) => storage.close().await,
                    None => Ok(()),
                };
                let _ = reply.send(result);
            }
            Request::Reopen {
                column_families,
                reply,
            } => {
                let result = match storage.take() {
                    Some(current) => match current.reopen(column_families).await {
                        Ok(reopened) => {
                            storage = Some(reopened);
                            Ok(())
                        }
                        Err(err) => Err(err),
                    },
                    None => Err(storage_closed()),
                };
                let _ = reply.send(result);
            }
            Request::Shutdown => break,
        }
    }
    if let Some(storage) = storage.take() {
        let _ = storage.close().await;
    }
}

impl OrderedKvStorage for SyncBridgeStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        self.roundtrip(|reply| Request::Get {
            cf: cf.to_owned(),
            key: key.to_vec(),
            reply,
        })?
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        self.roundtrip(|reply| Request::Set {
            cf: cf.to_owned(),
            key: key.to_vec(),
            value: value.to_vec(),
            reply,
        })?
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        self.roundtrip(|reply| Request::Delete {
            cf: cf.to_owned(),
            key: key.to_vec(),
            reply,
        })?
    }

    fn close(&self) -> Result<(), Error> {
        self.roundtrip(|reply| Request::Close { reply })?
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let entries = self.roundtrip(|reply| Request::ScanRange {
            cf: cf.to_owned(),
            start: start.to_vec(),
            end: end.to_vec(),
            reply,
        })??;
        for (key, value) in entries {
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let entries = self.roundtrip(|reply| Request::ScanPrefix {
            cf: cf.to_owned(),
            prefix: prefix.to_vec(),
            reply,
        })??;
        for (key, value) in entries {
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        let operations = operations.iter().map(owned_write_operation).collect();
        self.roundtrip(|reply| Request::WriteMany { operations, reply })?
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.roundtrip(|reply| Request::ColumnFamilyNames { reply })
            .ok()
            .flatten()
    }
}

impl ReopenableStorage for SyncBridgeStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error> {
        let column_families = column_families
            .iter()
            .map(|cf| (*cf).to_owned())
            .collect::<Vec<_>>();
        self.roundtrip(|reply| Request::Reopen {
            column_families,
            reply,
        })??;
        Ok(self)
    }
}

impl Drop for SyncBridgeStorage {
    fn drop(&mut self) {
        let _ = self.request_tx.send(Request::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn owned_write_operation(operation: &WriteOperation<'_>) -> OwnedWriteOperation {
    match operation {
        WriteOperation::Set { cf, key, value } => OwnedWriteOperation::Set {
            cf: (*cf).to_owned(),
            key: key.to_vec(),
            value: value.to_vec(),
        },
        WriteOperation::Delete { cf, key } => OwnedWriteOperation::Delete {
            cf: (*cf).to_owned(),
            key: key.to_vec(),
        },
        WriteOperation::Delta { cf, key, delta } => OwnedWriteOperation::Delta {
            cf: (*cf).to_owned(),
            key: key.to_vec(),
            delta: (*delta).clone(),
        },
    }
}
