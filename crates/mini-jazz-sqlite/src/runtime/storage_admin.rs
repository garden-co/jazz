use super::Runtime;
use crate::{projection, rows::row_num, stats, storage, types::StorageStats, Result};

impl Runtime {
    pub fn storage_admin(&self) -> RuntimeStorageAdmin<'_> {
        RuntimeStorageAdmin { runtime: self }
    }
}

pub struct RuntimeStorageAdmin<'a> {
    runtime: &'a Runtime,
}

impl RuntimeStorageAdmin<'_> {
    pub fn clear_current_projection(&self) -> Result<()> {
        projection::clear(&self.runtime.conn, &self.runtime.schema)
    }

    pub fn rebuild_current_projection(&self) -> Result<()> {
        projection::rebuild(
            &self.runtime.conn,
            &self.runtime.schema,
            self.runtime.node_num,
        )
    }

    pub fn physical_row_num_for(&self, row_id: &str) -> Result<i64> {
        row_num(&self.runtime.conn, row_id)
    }

    pub fn storage_stats(&self) -> Result<StorageStats> {
        stats::collect(&self.runtime.conn, &self.runtime.schema)
    }

    pub fn storage_format_version(&self) -> Result<i64> {
        storage::storage_version(&self.runtime.conn)
    }

    pub fn local_policy_fingerprint(&self) -> String {
        self.runtime.schema.policy_fingerprint()
    }
}
