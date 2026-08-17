//! Reusable public Jazz test infrastructure.
//!
//! The featureless base intentionally contains only transport helpers that can
//! support Jazz's direct engine contracts without enabling server, storage, or
//! compression capabilities. Public client/server scenarios use the default
//! `scenarios` feature.

pub mod duplex_transport;

#[cfg(feature = "scenarios")]
mod permissions;
#[cfg(feature = "scenarios")]
mod scenarios;
#[cfg(feature = "scenarios")]
pub use scenarios::*;

/// Connect an integration-test client through the native adapter owned by the
/// test process, never by the semantic Jazz crate.
#[cfg(feature = "scenarios")]
pub async fn connect(
    context: jazz::tools::AppContext,
) -> jazz::tools::Result<jazz::tools::JazzClient> {
    jazz::tools::JazzClient::connect_with_native_transport(
        context,
        std::sync::Arc::new(jazz_native_transport::NativeWebSocketConnector),
    )
    .await
}

#[cfg(feature = "scenarios")]
pub fn native_connector()
-> std::sync::Arc<dyn jazz::tools::native_transport_connector::NativeTransportConnector> {
    std::sync::Arc::new(jazz_native_transport::NativeWebSocketConnector)
}

#[cfg(feature = "rocksdb")]
pub fn persistent_storage_factory() -> std::sync::Arc<dyn groove::storage::StorageFactory> {
    std::sync::Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory)
}
