//! The server catalogue's durable home: the store's own catalogue tables
//! (see [`jazz::catalogue`]), beside the admissions they describe.

use std::sync::Arc;

use jazz::catalogue::CatalogueRows;
use jazz::host::HostReader;

pub(crate) type CatalogueStorageResult<T> = Result<T, CatalogueStorageError>;

#[derive(Debug, Clone)]
pub(crate) struct CatalogueStorageError(String);

impl std::fmt::Display for CatalogueStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CatalogueStorageError {}

/// Reads see what was committed when they begin, on a connection of their
/// own; writes run on the store's thread, on the connection the runtime's
/// rows use too, and wait for it.
pub(crate) struct CatalogueStorage {
    reader: Arc<HostReader>,
    #[cfg_attr(not(any(test, feature = "embedded-server")), allow(dead_code))]
    store: crate::runtime::HostSender,
}

impl CatalogueStorage {
    pub(crate) fn new(reader: Arc<HostReader>, store: crate::runtime::HostSender) -> Self {
        Self { reader, store }
    }

    pub(crate) fn read(&self) -> CatalogueStorageResult<CatalogueRows> {
        self.reader
            .catalogue()
            .map_err(|error| CatalogueStorageError(format!("read catalogue: {error}")))
    }

    /// Write `put` in one commit: all or none. A deploy instead writes its
    /// rows in the transaction that admits them (`Db::apply_catalogue`).
    #[cfg(any(test, feature = "embedded-server"))]
    pub(crate) fn put(&self, put: jazz::catalogue::CataloguePut) -> CatalogueStorageResult<()> {
        let (reply, answer) = std::sync::mpsc::sync_channel(1);
        self.store
            .send(Box::new(move |tables| {
                let _ = reply.send(
                    tables
                        .catalogue_put(&put)
                        .map_err(|error| error.to_string()),
                );
            }))
            .map_err(CatalogueStorageError)?;
        answer
            .recv()
            .map_err(|_| "server store closed before answering".to_owned())
            .and_then(|written| written)
            .map_err(|error| CatalogueStorageError(format!("write catalogue: {error}")))
    }
}
