use super::Runtime;
use crate::{branch, projection, time::now_ms, types::BranchInfo, Result};
use rusqlite::params;

#[cfg(test)]
use crate::{schema::SchemaDef, Storage};

impl Runtime {
    pub fn create_branch(&mut self, branch_id: &str, base_global_epoch: Option<i64>) -> Result<()> {
        branch::ensure(&self.conn, branch_id, base_global_epoch, now_ms())?;
        Ok(())
    }

    pub fn create_branch_from_branches(
        &mut self,
        branch_id: &str,
        source_branch_ids: &[&str],
    ) -> Result<()> {
        self.create_branch_from_branches_at_base(branch_id, None, source_branch_ids)
    }

    pub fn create_branch_from_branches_at_base(
        &mut self,
        branch_id: &str,
        base_global_epoch: Option<i64>,
        source_branch_ids: &[&str],
    ) -> Result<()> {
        let branch_num = branch::ensure(&self.conn, branch_id, base_global_epoch, now_ms())?;
        for source_branch_id in source_branch_ids {
            branch::add_source(&self.conn, branch_num, source_branch_id)?;
        }
        Ok(())
    }

    pub fn add_branch_source(&mut self, branch_id: &str, source_branch_id: &str) -> Result<()> {
        let branch_num = branch::checkout(&self.conn, branch_id)?;
        branch::add_source(&self.conn, branch_num, source_branch_id)
    }

    pub fn remove_branch_source(&mut self, branch_id: &str, source_branch_id: &str) -> Result<()> {
        let branch_num = branch::checkout(&self.conn, branch_id)?;
        branch::remove_source(&self.conn, branch_num, source_branch_id)?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)
    }

    pub fn checkout_branch(&mut self, branch_id: &str) -> Result<()> {
        self.branch_num = branch::checkout(&self.conn, branch_id)?;
        Ok(())
    }

    pub(crate) fn query_in_branch<T>(
        &mut self,
        branch_id: &str,
        query: impl FnOnce(&mut Runtime) -> Result<T>,
    ) -> Result<T> {
        let previous_branch_id = branch::id_for_num(&self.conn, self.branch_num)?;
        self.checkout_branch(branch_id)?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| query(self)));
        let restore_result = self.checkout_branch(&previous_branch_id);
        match (result, restore_result) {
            (Ok(Ok(value)), Ok(())) => Ok(value),
            (Ok(Err(error)), _) => Err(error),
            (Ok(Ok(_)), Err(error)) => Err(error),
            (Err(payload), _) => std::panic::resume_unwind(payload),
        }
    }

    pub fn branches(&self) -> Result<Vec<BranchInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT branch_num, branch_id, base_global_epoch
             FROM jazz_branch
             ORDER BY branch_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })?;
        let mut branches = Vec::new();
        for row in rows {
            let (branch_num, id, base_global_epoch) = row?;
            let mut source_stmt = self.conn.prepare(
                "SELECT source.branch_id
                 FROM jazz_branch_source branch_source
                 JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
                 WHERE branch_source.branch_num = ?
                 ORDER BY source.branch_id",
            )?;
            let source_branch_ids = source_stmt
                .query_map(params![branch_num], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            branches.push(BranchInfo {
                id,
                base_global_epoch,
                source_branch_ids,
            });
        }
        Ok(branches)
    }

    pub fn branch_backing_rows(&self) -> Result<Vec<BranchInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT branch_id, base_global_epoch, source_branch_ids_json
             FROM jazz_branch_backing
             ORDER BY branch_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut branches = Vec::new();
        for row in rows {
            let (id, base_global_epoch, source_branch_ids_json) = row?;
            let source_branch_ids = serde_json::from_str::<Vec<String>>(&source_branch_ids_json)
                .map_err(|err| crate::Error::new(err.to_string()))?;
            branches.push(BranchInfo {
                id,
                base_global_epoch,
                source_branch_ids,
            });
        }
        Ok(branches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_in_branch_restores_checkout_after_panic() {
        let mut runtime = Runtime::open_trusted_with_schema(
            Storage::Memory,
            "trusted",
            SchemaDef::new().table("docs", |table| {
                table.text("title");
            }),
        )
        .unwrap();
        let main_branch_num = runtime.branch_num;
        runtime.create_branch("draft", None).unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<()> = runtime.query_in_branch("draft", |_| panic!("boom"));
        }));

        assert!(result.is_err());
        assert_eq!(runtime.branch_num, main_branch_num);
    }
}
