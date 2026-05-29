use super::Runtime;
use crate::{projection, rows::row_num, stats, storage, types::StorageStats, Result};

impl Runtime {
    pub fn clear_current_projection_for_test(&mut self) -> Result<()> {
        projection::clear(&self.conn, &self.schema)
    }

    pub fn rebuild_current_projection(&mut self) -> Result<()> {
        projection::rebuild(&self.conn, &self.schema, self.node_num)
    }

    pub fn physical_row_num_for(&self, row_id: &str) -> Result<i64> {
        row_num(&self.conn, row_id)
    }

    pub fn storage_stats(&self) -> Result<StorageStats> {
        stats::collect(&self.conn, &self.schema)
    }

    pub fn storage_format_version(&self) -> Result<i64> {
        storage::storage_version(&self.conn)
    }

    pub fn local_policy_fingerprint(&self) -> String {
        self.schema.policy_fingerprint()
    }
}
