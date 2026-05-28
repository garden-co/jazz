mod branch;
mod effective;
mod error;
mod lz4_vfs;
pub mod persisted_rope;
mod policy;
mod projection;
mod query;
mod query_predicate;
mod read_set;
mod rows;
mod runtime;
mod schema;
mod stats;
mod storage;
mod subscription;
pub mod sync;
mod tx;
mod types;
mod users;

pub use error::{Error, Result};
pub use runtime::Runtime;
pub use schema::SchemaDef;
pub use storage::Storage;
pub use subscription::RowsSubscription;
pub use types::{
    ApplyBundleProfile, QueryExportProfile, RejectionInfo, RowDiff, RowView, StorageStats,
    TransactionInfo,
};

pub fn compact_lz4_storage(path: impl AsRef<std::path::Path>) -> Result<()> {
    lz4_vfs::compact_cold_pages(path.as_ref()).map_err(|err| Error::new(err.to_string()))
}
