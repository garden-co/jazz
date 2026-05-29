use crate::schema::{FieldKind, PolicyDef, SchemaDef, TableDef};
use crate::{policy, tx, Result};
use rusqlite::{params_from_iter, types::Value as SqlValue, Connection};

const MAX_EFFECTIVE_POLICY_RECURSION_DEPTH: usize = 64;

pub(crate) struct ReadVisibility<'a> {
    pub(crate) conn: &'a Connection,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) branch_num: i64,
    pub(crate) user: &'a str,
    pub(crate) bypass_policy: bool,
}

impl ReadVisibility<'_> {
    pub(crate) fn current_policy_sql(&self, table: &TableDef, alias: &str) -> Result<String> {
        if self.bypass_policy {
            Ok("1 = 1".to_owned())
        } else {
            policy::branch_read_policy_sql_for_alias(
                self.schema,
                table,
                alias,
                self.user,
                self.branch_num,
            )
        }
    }

    pub(crate) fn snapshot_policy_sql(
        &self,
        table: &TableDef,
        alias: &str,
        base_epoch: i64,
    ) -> Result<String> {
        if self.bypass_policy {
            Ok("1 = 1".to_owned())
        } else {
            policy::branch_snapshot_read_policy_sql_for_alias(
                self.schema,
                table,
                alias,
                self.user,
                self.branch_num,
                base_epoch,
            )
        }
    }

    pub(crate) fn base_snapshot_row_nums_visible_in_branch(
        &self,
        table_name: &str,
        base_epoch: i64,
        row_nums: Option<&[i64]>,
    ) -> Result<Vec<i64>> {
        if matches!(row_nums, Some([])) {
            return Ok(Vec::new());
        }
        if let Some(row_nums) = row_nums {
            if row_nums.len() > crate::SQL_VARIABLE_CHUNK_SIZE {
                let mut visible_row_nums = Vec::new();
                for chunk in row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
                    visible_row_nums.extend(self.base_snapshot_row_nums_visible_in_branch(
                        table_name,
                        base_epoch,
                        Some(chunk),
                    )?);
                }
                visible_row_nums.sort();
                visible_row_nums.dedup();
                return Ok(visible_row_nums);
            }
        }
        let table = self.schema.table_def(table_name)?;
        let snapshot_policy_sql = self.snapshot_policy_sql(table, "h", base_epoch)?;
        let effective_branch_policy_sql =
            self.base_snapshot_effective_policy_sql(table, "h", base_epoch)?;
        let row_filter = row_filter_sql("h", row_nums);
        let sql = format!(
            "SELECT h.row_num
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE {row_filter}
               AND h.j_branch_num = 1
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND h.op != 3
               AND {snapshot_policy_sql}
               AND {effective_branch_policy_sql}
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = 1
                   AND newer_tx.outcome != ?
                   AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?
                   AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
               )
               AND NOT EXISTS (
                 SELECT 1
                 FROM {current_table} branch_current
                 WHERE branch_current.row_num = h.row_num
                   AND branch_current.j_branch_num = ?
               )
             ORDER BY h.row_num",
            crate::schema::history_table(table_name),
            history_table = crate::schema::history_table(table_name),
            current_table = crate::schema::current_table(table_name),
        );
        let mut params = row_nums
            .unwrap_or(&[])
            .iter()
            .copied()
            .map(SqlValue::Integer)
            .collect::<Vec<_>>();
        params.extend([
            SqlValue::Integer(tx::OUTCOME_REJECTED),
            SqlValue::Integer(base_epoch),
            SqlValue::Integer(tx::OUTCOME_REJECTED),
            SqlValue::Integer(base_epoch),
            SqlValue::Integer(self.branch_num),
        ]);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| row.get::<_, i64>(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn base_snapshot_effective_policy_sql(
        &self,
        table: &TableDef,
        alias: &str,
        base_epoch: i64,
    ) -> Result<String> {
        if self.bypass_policy {
            Ok("1 = 1".to_owned())
        } else {
            self.lower_effective_branch_policy(table, alias, &table.read_policy, base_epoch, 0)
        }
    }

    fn lower_effective_branch_policy(
        &self,
        table: &TableDef,
        alias: &str,
        policy: &PolicyDef,
        base_epoch: i64,
        depth: usize,
    ) -> Result<String> {
        if depth > MAX_EFFECTIVE_POLICY_RECURSION_DEPTH {
            return Err(crate::Error::new(
                "effective branch policy recursion depth exceeded",
            ));
        }
        match policy {
            PolicyDef::AllowAll => Ok("1 = 1".to_owned()),
            PolicyDef::CreatedByUser => Ok(format!(
                "{alias}.j_created_by = (SELECT user_num FROM jazz_user WHERE user_id = '{}')",
                self.user.replace('\'', "''")
            )),
            PolicyDef::RefReadable { field } => {
                let field = table
                    .fields
                    .iter()
                    .find(|candidate| candidate.name == *field)
                    .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
                let FieldKind::Ref {
                    table: ref_table_name,
                } = &field.kind
                else {
                    return Err(crate::Error::new(format!(
                        "policy field {} is not a ref",
                        field.name
                    )));
                };
                let ref_table = self.schema.table_def(ref_table_name)?;
                let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
                let current_alias = format!("effective_policy_current_{depth}");
                let current_tx_alias = format!("effective_policy_current_tx_{depth}");
                let snapshot_alias = format!("effective_policy_snapshot_{depth}");
                let snapshot_tx_alias = format!("effective_policy_snapshot_tx_{depth}");
                let newer_alias = format!("effective_policy_newer_{depth}");
                let newer_tx_alias = format!("effective_policy_newer_tx_{depth}");
                let current_policy = self.lower_effective_branch_policy(
                    ref_table,
                    &current_alias,
                    &ref_table.read_policy,
                    base_epoch,
                    depth + 1,
                )?;
                let snapshot_policy = self.lower_effective_branch_policy(
                    ref_table,
                    &snapshot_alias,
                    &ref_table.read_policy,
                    base_epoch,
                    depth + 1,
                )?;
                Ok(format!(
                    "(
                       EXISTS (
                         SELECT 1
                         FROM {} {current_alias}
                         JOIN jazz_tx {current_tx_alias}
                           ON {current_tx_alias}.tx_num = {current_alias}.visible_tx_num
                         WHERE {current_alias}.row_num = {alias}.{ref_column}
                           AND {current_alias}.j_branch_num = {}
                           AND {current_alias}.is_deleted = 0
                           AND {current_tx_alias}.outcome != {}
                           AND {current_policy}
                       )
                       OR (
                         NOT EXISTS (
                           SELECT 1
                           FROM {} shadow
                           WHERE shadow.row_num = {alias}.{ref_column}
                             AND shadow.j_branch_num = {}
                         )
                         AND EXISTS (
                           SELECT 1
                           FROM {} {snapshot_alias}
                           JOIN jazz_tx {snapshot_tx_alias}
                             ON {snapshot_tx_alias}.tx_num = {snapshot_alias}.tx_num
                           WHERE {snapshot_alias}.row_num = {alias}.{ref_column}
                             AND {snapshot_alias}.j_branch_num = 1
                             AND {snapshot_alias}.op != 3
                             AND {snapshot_tx_alias}.outcome != {}
                             AND {snapshot_tx_alias}.global_epoch IS NOT NULL
                             AND {snapshot_tx_alias}.global_epoch <= {base_epoch}
                             AND NOT EXISTS (
                               SELECT 1
                               FROM {} {newer_alias}
                               JOIN jazz_tx {newer_tx_alias}
                                 ON {newer_tx_alias}.tx_num = {newer_alias}.tx_num
                               WHERE {newer_alias}.row_num = {snapshot_alias}.row_num
                                 AND {newer_alias}.j_branch_num = 1
                                 AND {newer_tx_alias}.outcome != {}
                                 AND {newer_tx_alias}.global_epoch IS NOT NULL
                                 AND {newer_tx_alias}.global_epoch <= {base_epoch}
                                 AND ({newer_tx_alias}.global_epoch > {snapshot_tx_alias}.global_epoch OR ({newer_tx_alias}.global_epoch = {snapshot_tx_alias}.global_epoch AND {newer_tx_alias}.tx_num > {snapshot_tx_alias}.tx_num))
                             )
                             AND {snapshot_policy}
                         )
                       )
                     )",
                    crate::schema::current_table(ref_table_name),
                    self.branch_num,
                    tx::OUTCOME_REJECTED,
                    crate::schema::current_table(ref_table_name),
                    self.branch_num,
                    crate::schema::history_table(ref_table_name),
                    tx::OUTCOME_REJECTED,
                    crate::schema::history_table(ref_table_name),
                    tx::OUTCOME_REJECTED,
                ))
            }
            PolicyDef::BranchFieldEquals { .. } => {
                policy::branch_snapshot_read_policy_sql_for_alias(
                    self.schema,
                    table,
                    alias,
                    self.user,
                    self.branch_num,
                    base_epoch,
                )
            }
        }
    }
}

fn row_filter_sql(alias: &str, row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!(
            "{alias}.row_num IN ({})",
            (0..row_nums.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}
