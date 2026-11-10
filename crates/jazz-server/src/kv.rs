//! The server's own administrative state in the app's store file (see
//! [`jazz::host`]): plain SQL for host tables with columns of their own
//! (the account registry), and a small ordered key-value store over one
//! host table. The schema catalogue has typed tables of its own (see
//! [`jazz::catalogue`]).
//!
//! Writes run on the store's thread, on the one connection the runtime's
//! rows use too, and wait for it (at most the one runtime job or tick slice
//! in progress). The store commits with `synchronous = FULL`, so a returned
//! write is durable and `flush` has nothing left to do. Reads see what was
//! committed when they begin. Native storage uses a separate reader; worker
//! storage queues reads on the same owner as writes and core operations.

use std::sync::{Arc, mpsc};

#[cfg(feature = "native")]
pub(crate) use jazz::host::HostReader as NativeReader;
#[cfg(not(feature = "native"))]
pub(crate) enum NativeReader {}
use NativeReader as HostReader;
use jazz::host::ConnectionSql;
use jazz::host::HostTables;
#[cfg(test)]
use jazz::host::{HostPair, HostTable};

/// Errors are reported as text: callers wrap them in their own error types.
pub(crate) type KvResult<T> = Result<T, String>;

#[cfg(test)]
pub(crate) struct HostKv {
    table: HostTable,
    reader: Option<Arc<HostReader>>,
    store: crate::runtime::HostSender,
}

#[cfg(test)]
impl HostKv {
    pub(crate) fn new(
        table: HostTable,
        reader: Option<Arc<HostReader>>,
        store: crate::runtime::HostSender,
    ) -> Self {
        Self {
            table,
            reader,
            store,
        }
    }

    /// Run `f` on the store's thread and wait for its answer.
    fn call<T: Send + 'static>(
        &self,
        f: impl for<'a> FnOnce(
            crate::runtime::HostAccess<'a>,
            HostTable,
        )
            -> futures::future::LocalBoxFuture<'a, Result<T, jazz::db::Error>>
        + Send
        + 'static,
    ) -> KvResult<T> {
        let (reply, answer) = mpsc::sync_channel(1);
        let table = self.table;
        self.store.send(Box::new(move |tables| {
            Box::pin(async move {
                let _ = reply.send(f(tables, table).await.map_err(|error| error.to_string()));
            })
        }))?;
        answer
            .recv()
            .map_err(|_| "server store closed before answering".to_owned())?
    }

    pub(crate) fn get(&self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        match &self.reader {
            #[cfg(feature = "native")]
            Some(reader) => reader
                .get(self.table, key)
                .map_err(|error| error.to_string()),
            _ => {
                let key = key.to_vec();
                self.call(move |tables, table| {
                    Box::pin(async move { tables.host_get(table, &key).await })
                })
            }
        }
    }

    /// Write every pair in one transaction: all or none.
    pub(crate) fn write_many(&self, pairs: &[HostPair]) -> KvResult<()> {
        let pairs = pairs.to_vec();
        self.call(move |tables, table| {
            Box::pin(async move { tables.host_put_many(table, &pairs).await })
        })
    }

    /// Every pair whose key starts with `prefix`, in key order.
    pub(crate) fn prefix(&self, prefix: &[u8]) -> KvResult<Vec<HostPair>> {
        match &self.reader {
            #[cfg(feature = "native")]
            Some(reader) => reader
                .prefix(self.table, prefix)
                .map_err(|error| error.to_string()),
            _ => {
                let prefix = prefix.to_vec();
                self.call(move |tables, table| {
                    Box::pin(async move { tables.host_prefix(table, &prefix).await })
                })
            }
        }
    }
}

/// SQL on the host tables a server shapes itself. Writes run like
/// [`HostKv`] writes: on the store's thread, in one savepoint, durable when
/// they return. Reads run on the read-only connection.
pub(crate) struct HostSql {
    reader: Option<Arc<HostReader>>,
    store: crate::runtime::HostSender,
    /// Tests: the next write commits and then reports an error, the
    /// ambiguous outcome a failed flush has.
    #[cfg(test)]
    fail_next_write: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Tests: the next read fails.
    #[cfg(test)]
    fail_next_read: std::sync::atomic::AtomicBool,
}

impl HostSql {
    pub(crate) fn new(reader: Option<Arc<HostReader>>, store: crate::runtime::HostSender) -> Self {
        Self {
            reader,
            store,
            #[cfg(test)]
            fail_next_write: Default::default(),
            #[cfg(test)]
            fail_next_read: Default::default(),
        }
    }

    /// Run `f` on the read-only connection: it sees what was committed
    /// when it began.
    pub(crate) fn read<T: Send + 'static>(
        &self,
        f: impl FnOnce(ConnectionSql<'_>) -> Result<T, jazz::db::Error> + Send + 'static,
    ) -> KvResult<T> {
        #[cfg(test)]
        if self
            .fail_next_read
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err("injected read failure".to_owned());
        }
        match &self.reader {
            #[cfg(feature = "native")]
            Some(reader) => reader.with_sql(f).map_err(|error| error.to_string()),
            // Run between owner jobs, after the prior transaction completed.
            _ => self.call(false, f),
        }
    }

    /// Run `f` on the store's thread in one savepoint and wait: what it
    /// wrote is durable when this returns `Ok`, and nothing is written if
    /// `f` fails.
    pub(crate) fn write<T: Send + 'static>(
        &self,
        f: impl FnOnce(ConnectionSql<'_>) -> Result<T, jazz::db::Error> + Send + 'static,
    ) -> KvResult<T> {
        self.call(true, f)
    }

    fn call<T: Send + 'static>(
        &self,
        write: bool,
        f: impl FnOnce(ConnectionSql<'_>) -> Result<T, jazz::db::Error> + Send + 'static,
    ) -> KvResult<T> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.store.send(Box::new(move |tables| {
            Box::pin(async move {
                let _ = reply.send(
                    tables
                        .with_sql(write, f)
                        .await
                        .map_err(|error| error.to_string()),
                );
            })
        }))?;
        let written = answer
            .recv()
            .map_err(|_| "server store closed before answering".to_owned())??;
        #[cfg(test)]
        if write
            && self
                .fail_next_write
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err("injected failure after the write committed".to_owned());
        }
        Ok(written)
    }

    /// Tests: fail the next read.
    #[cfg(test)]
    pub(crate) fn fail_next_read(&self) {
        self.fail_next_read
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Tests: the switch that fails the next write after it commits.
    #[cfg(test)]
    pub(crate) fn failpoint(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        std::sync::Arc::clone(&self.fail_next_write)
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::{AppStore, StorageConfig};
    use jazz::host::HostTable;

    const A: HostTable = HostTable::new("test_a");
    const B: HostTable = HostTable::new("test_b");

    #[test]
    fn writes_are_atomic_ordered_and_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let storage = StorageConfig::data_dir(dir.path());
        {
            let store = AppStore::open(&storage).unwrap();
            let kv = store.kv(A).unwrap();
            kv.write_many(&[
                (b"a:2".to_vec(), b"two".to_vec()),
                (b"a:1".to_vec(), b"one".to_vec()),
            ])
            .unwrap();
            kv.write_many(&[(b"b:1".to_vec(), b"b".to_vec())]).unwrap();
            store
                .kv(B)
                .unwrap()
                .write_many(&[(b"a:1".to_vec(), b"other".to_vec())])
                .unwrap();
            store.close().unwrap();
            assert!(
                kv.write_many(&[(b"c".to_vec(), b"c".to_vec())]).is_err(),
                "a closed store takes no writes"
            );
        }
        // One file per app.
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| !name.ends_with("-wal") && !name.ends_with("-shm"))
            .collect();
        assert_eq!(files, vec!["jazz.sqlite".to_owned()]);
        let store = AppStore::open(&storage).unwrap();
        let kv = store.kv(A).unwrap();
        assert_eq!(
            kv.prefix(b"a:").unwrap(),
            vec![
                (b"a:1".to_vec(), b"one".to_vec()),
                (b"a:2".to_vec(), b"two".to_vec())
            ]
        );
        assert_eq!(
            kv.prefix(b"b:").unwrap(),
            vec![(b"b:1".to_vec(), b"b".to_vec())]
        );
        assert_eq!(kv.prefix(b"c").unwrap(), vec![]);
        assert_eq!(
            store.kv(B).unwrap().prefix(b"a:").unwrap(),
            vec![(b"a:1".to_vec(), b"other".to_vec())]
        );
    }
}
