use crate::schema::{FieldDef, FieldKind, PolicyDef, SchemaDef, TableDef};
use crate::{branch, tx, users, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

const MAX_POLICY_RECURSION_DEPTH: usize = 64;

#[derive(Clone, Copy)]
enum PolicyReadScope<'a> {
    CurrentMain,
    Branch {
        branch_num: i64,
        branch_context: Option<BranchPolicyContext<'a>>,
    },
}

#[derive(Clone, Copy)]
struct BranchPolicyContext<'a> {
    branch_num: i64,
    branch_table: &'a str,
}

pub(crate) struct WriteCheck<'a> {
    pub(crate) db: &'a Connection,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) table: &'a TableDef,
    pub(crate) row_num: i64,
    pub(crate) branch_num: i64,
    pub(crate) values: &'a BTreeMap<String, JsonValue>,
    pub(crate) user: &'a str,
}

pub(crate) fn write_allowed(check: WriteCheck<'_>) -> Result<bool> {
    if let Some((branch_table, branch_policy)) = single_branch_policy(check.table, check.branch_num)
    {
        let Some(write_policy) = branch_policy.write_policy.as_ref() else {
            return Ok(false);
        };
        let context = BranchPolicyContext {
            branch_num: check.branch_num,
            branch_table,
        };
        if !branch_backing_row_is_visible(check.db, check.schema, check.user, context)? {
            return Ok(false);
        }
        return write_policy_allowed(&check, write_policy, Some(context));
    }
    write_policy_allowed(&check, &check.table.write_policy, None)
}

fn write_policy_allowed(
    check: &WriteCheck<'_>,
    policy: &PolicyDef,
    branch_context: Option<BranchPolicyContext<'_>>,
) -> Result<bool> {
    match policy {
        PolicyDef::AllowAll => Ok(true),
        PolicyDef::CreatedByUser => {
            let user_num = users::ensure_user(check.db, check.user)?;
            let count: i64 = check.db.query_row(
                &format!(
                    "SELECT COUNT(*)
                     FROM {} current
                     WHERE current.row_num = ?
                       AND current.j_branch_num = ?
                       AND current.j_created_by = ?",
                    crate::schema::current_table(&check.table.name)
                ),
                params![check.row_num, check.branch_num, user_num],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        }
        PolicyDef::RefReadable { field } => {
            let field_def = check
                .table
                .fields
                .iter()
                .find(|candidate| candidate.name == *field)
                .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
            let FieldKind::Ref {
                table: ref_table_name,
            } = &field_def.kind
            else {
                return Err(crate::Error::new(format!(
                    "policy field {} is not a ref",
                    field_def.name
                )));
            };
            let ref_id = check
                .values
                .get(field)
                .or_else(|| check.values.get(&field_def.storage_name))
                .and_then(JsonValue::as_str)
                .ok_or_else(|| crate::Error::new(format!("expected ref id for {field}")))?;
            let ref_row_num = crate::rows::row_num(check.db, ref_id)?;
            let ref_table = check.schema.table_def(ref_table_name)?;
            if check.branch_num != 1 {
                if let Some(base_epoch) = branch::base_global_epoch(check.db, check.branch_num)? {
                    if !branch_current_row_exists(
                        check.db,
                        ref_table_name,
                        ref_row_num,
                        check.branch_num,
                    )? {
                        return snapshot_write_ref_allowed(
                            check.db,
                            check.schema,
                            ref_table,
                            ref_row_num,
                            check.user,
                            base_epoch,
                        );
                    }
                }
            }
            let policy_sql = lower_policy(
                check.schema,
                ref_table,
                "current",
                &ref_table.read_policy,
                check.user,
                PolicyReadScope::Branch {
                    branch_num: check.branch_num,
                    branch_context: None,
                },
                0,
            )?;
            let count: i64 = check.db.query_row(
                &format!(
                    "SELECT COUNT(*)
                     FROM {} current
                     JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                     WHERE current.row_num = ?
                       AND {}
                       AND current.is_deleted = 0
                       AND tx.outcome != {}
                       AND {policy_sql}",
                    crate::schema::current_table(ref_table_name),
                    effective_branch_sql("current", ref_table_name, check.branch_num),
                    tx::OUTCOME_REJECTED,
                ),
                params![ref_row_num],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        }
        PolicyDef::BranchFieldEquals {
            field,
            branch_field,
        } => {
            let Some(context) = branch_context else {
                return Err(crate::Error::new(
                    "branch field policy requires branch context",
                ));
            };
            let field_def = check
                .table
                .fields
                .iter()
                .find(|candidate| candidate.name == *field)
                .ok_or_else(|| crate::Error::new(format!("unknown branch policy field {field}")))?;
            let branch_table = check.schema.table_def(context.branch_table)?;
            let branch_field_def = branch_table
                .fields
                .iter()
                .find(|candidate| candidate.name == *branch_field)
                .ok_or_else(|| {
                    crate::Error::new(format!(
                        "unknown branch policy field {}.{branch_field}",
                        branch_table.name
                    ))
                })?;
            let Some(value) = check
                .values
                .get(field)
                .or_else(|| check.values.get(&field_def.storage_name))
            else {
                return Ok(false);
            };
            let value = crate::schema::field_sql_value(field_def, value, |ref_table, row_id| {
                crate::rows::row_num(check.db, row_id).map_err(|err| {
                    crate::Error::new(format!(
                        "failed to resolve ref {ref_table}.{row_id} for branch policy: {err}"
                    ))
                })
            })?;
            let Some(branch_value) = branch_backing_field_value(
                check.db,
                check.schema,
                check.user,
                context,
                branch_field_def,
            )?
            else {
                return Ok(false);
            };
            Ok(value == branch_value)
        }
    }
}

fn branch_backing_row_is_visible(
    conn: &Connection,
    schema: &SchemaDef,
    user: &str,
    context: BranchPolicyContext<'_>,
) -> Result<bool> {
    let sql = branch_backing_row_visible_sql(schema, user, context, 0, None)?;
    let count: i64 = conn.query_row(&format!("SELECT COUNT(*) WHERE {sql}"), [], |row| {
        row.get(0)
    })?;
    Ok(count > 0)
}

fn branch_backing_field_value(
    conn: &Connection,
    schema: &SchemaDef,
    user: &str,
    context: BranchPolicyContext<'_>,
    branch_field: &FieldDef,
) -> Result<Option<rusqlite::types::Value>> {
    let branch_table = schema.table_def(context.branch_table)?;
    let backing_alias = "branch_policy_backing_value";
    let ids_alias = "branch_policy_backing_value_ids";
    let tx_alias = "branch_policy_backing_value_tx";
    let branch_alias = "branch_policy_backing_value_branch";
    let policy_sql = lower_policy(
        schema,
        branch_table,
        backing_alias,
        &branch_table.read_policy,
        user,
        PolicyReadScope::CurrentMain,
        0,
    )?;
    let sql = format!(
        "SELECT {backing_alias}.{branch_column}
         FROM {current_table} {backing_alias}
         JOIN jazz_row_id {ids_alias}
           ON {ids_alias}.row_num = {backing_alias}.row_num
         JOIN jazz_tx {tx_alias}
           ON {tx_alias}.tx_num = {backing_alias}.visible_tx_num
         JOIN jazz_branch {branch_alias}
           ON {branch_alias}.branch_num = ?
         WHERE {ids_alias}.row_id = {branch_alias}.branch_id
           AND {backing_alias}.j_branch_num = 1
           AND {backing_alias}.is_deleted = 0
           AND {tx_alias}.outcome != ?
           AND {policy_sql}
         LIMIT 1",
        branch_column = crate::schema::quote_ident(&crate::schema::storage_column(branch_field)),
        current_table = crate::schema::current_table(&branch_table.name),
    );
    conn.query_row(
        &sql,
        params![context.branch_num, tx::OUTCOME_REJECTED],
        |row| row.get::<_, rusqlite::types::Value>(0),
    )
    .optional()
    .map_err(Into::into)
}

fn branch_current_row_exists(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {}
             WHERE row_num = ?
               AND j_branch_num = ?",
            crate::schema::current_table(table_name)
        ),
        params![row_num, branch_num],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn snapshot_write_ref_allowed(
    conn: &Connection,
    schema: &SchemaDef,
    ref_table: &TableDef,
    ref_row_num: i64,
    user: &str,
    base_epoch: i64,
) -> Result<bool> {
    let policy_sql = snapshot_read_policy_sql_for_alias(schema, ref_table, "h", user, base_epoch)?;
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.row_num = ?
               AND h.j_branch_num = 1
               AND h.op != 3
               AND tx.outcome != {}
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND {policy_sql}
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = 1
                   AND newer_tx.outcome != {}
                   AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?
                   AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
               )",
            crate::schema::history_table(&ref_table.name),
            tx::OUTCOME_REJECTED,
            tx::OUTCOME_REJECTED,
            history_table = crate::schema::history_table(&ref_table.name),
            policy_sql = policy_sql,
        ),
        params![ref_row_num, base_epoch, base_epoch],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn effective_branch_sql(alias: &str, table_name: &str, branch_num: i64) -> String {
    if branch_num == 1 {
        return format!("{alias}.j_branch_num = 1");
    }
    format!(
        "({alias}.j_branch_num = {branch_num}
          OR (
            {alias}.j_branch_num = 1
            AND NOT EXISTS (
              SELECT 1
              FROM {} branch_shadow
              WHERE branch_shadow.row_num = {alias}.row_num
                AND branch_shadow.j_branch_num = {branch_num}
            )
          ))",
        crate::schema::current_table(table_name)
    )
}

pub(crate) fn branch_read_policy_sql_for_alias(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    user: &str,
    branch_num: i64,
) -> Result<String> {
    let scope = if branch_num == 1 {
        PolicyReadScope::CurrentMain
    } else {
        PolicyReadScope::Branch {
            branch_num,
            branch_context: None,
        }
    };
    if let Some((branch_table, branch_policy)) = single_branch_policy(table, branch_num) {
        let Some(read_policy) = branch_policy.read_policy.as_ref() else {
            return Ok("0 = 1".to_owned());
        };
        let context = BranchPolicyContext {
            branch_num,
            branch_table,
        };
        let backing_sql = branch_backing_row_visible_sql(schema, user, context, 0, None)?;
        let policy_sql = lower_policy(
            schema,
            table,
            alias,
            read_policy,
            user,
            PolicyReadScope::Branch {
                branch_num,
                branch_context: Some(context),
            },
            0,
        )?;
        return Ok(format!("({backing_sql}) AND ({policy_sql})"));
    }
    lower_policy(schema, table, alias, &table.read_policy, user, scope, 0)
}

pub(crate) fn snapshot_read_policy_sql_for_alias(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    user: &str,
    base_epoch: i64,
) -> Result<String> {
    SnapshotPolicyLowering {
        schema,
        user,
        base_epoch,
        branch_context: None,
    }
    .lower(table, alias, &table.read_policy, 0)
}

pub(crate) fn branch_snapshot_read_policy_sql_for_alias(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    user: &str,
    branch_num: i64,
    base_epoch: i64,
) -> Result<String> {
    if let Some((branch_table, branch_policy)) = single_branch_policy(table, branch_num) {
        let Some(read_policy) = branch_policy.read_policy.as_ref() else {
            return Ok("0 = 1".to_owned());
        };
        let context = BranchPolicyContext {
            branch_num,
            branch_table,
        };
        let backing_sql = branch_backing_row_visible_sql(schema, user, context, 0, None)?;
        let policy_sql = SnapshotPolicyLowering {
            schema,
            user,
            base_epoch,
            branch_context: Some(context),
        }
        .lower(table, alias, read_policy, 0)?;
        return Ok(format!("({backing_sql}) AND ({policy_sql})"));
    }
    snapshot_read_policy_sql_for_alias(schema, table, alias, user, base_epoch)
}

fn lower_policy(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    policy: &PolicyDef,
    user: &str,
    scope: PolicyReadScope<'_>,
    depth: usize,
) -> Result<String> {
    if depth > MAX_POLICY_RECURSION_DEPTH {
        return Err(crate::Error::new("policy recursion depth exceeded"));
    }
    match policy {
        PolicyDef::AllowAll => Ok("1 = 1".to_owned()),
        PolicyDef::CreatedByUser => Ok(format!(
            "{alias}.j_created_by = (SELECT user_num FROM jazz_user WHERE user_id = '{}')",
            user.replace('\'', "''")
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
            let ref_table = schema.table_def(ref_table_name)?;
            let parent_alias = format!("policy_parent_{depth}");
            let parent_tx_alias = format!("policy_parent_tx_{depth}");
            let parent_policy = lower_policy(
                schema,
                ref_table,
                &parent_alias,
                &ref_table.read_policy,
                user,
                scope,
                depth + 1,
            )?;
            let branch_filter = match scope {
                PolicyReadScope::CurrentMain => String::new(),
                PolicyReadScope::Branch { branch_num, .. } => {
                    format!(
                        "AND {}",
                        effective_branch_sql(&parent_alias, &ref_table.name, branch_num)
                    )
                }
            };
            Ok(format!(
                "EXISTS (
                   SELECT 1
                   FROM {} {parent_alias}
                   JOIN jazz_tx {parent_tx_alias}
                     ON {parent_tx_alias}.tx_num = {parent_alias}.visible_tx_num
                   WHERE {parent_alias}.row_num = {alias}.{}
                     {branch_filter}
                     AND {parent_alias}.is_deleted = 0
                     AND {parent_tx_alias}.outcome != {}
                     AND {parent_policy}
                 )",
                crate::schema::current_table(&ref_table.name),
                crate::schema::quote_ident(&crate::schema::storage_column(field)),
                tx::OUTCOME_REJECTED,
            ))
        }
        PolicyDef::BranchFieldEquals {
            field,
            branch_field,
        } => {
            let PolicyReadScope::Branch {
                branch_context: Some(context),
                ..
            } = scope
            else {
                return Err(crate::Error::new(
                    "branch field policy requires branch context",
                ));
            };
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == *field)
                .ok_or_else(|| crate::Error::new(format!("unknown branch policy field {field}")))?;
            let branch_table = schema.table_def(context.branch_table)?;
            let branch_field = branch_table
                .fields
                .iter()
                .find(|candidate| candidate.name == *branch_field)
                .ok_or_else(|| {
                    crate::Error::new(format!(
                        "unknown branch policy field {}.{branch_field}",
                        branch_table.name
                    ))
                })?;
            branch_backing_row_visible_sql(
                schema,
                user,
                context,
                depth,
                Some((alias, field, branch_field)),
            )
        }
    }
}

fn single_branch_policy(
    table: &TableDef,
    branch_num: i64,
) -> Option<(&str, &crate::schema::BranchPolicyDef)> {
    if branch_num == 1 {
        return None;
    }
    table
        .branch_policies
        .iter()
        .next()
        .map(|(branch_table, policy)| (branch_table.as_str(), policy))
}

fn branch_backing_row_visible_sql(
    schema: &SchemaDef,
    user: &str,
    context: BranchPolicyContext<'_>,
    depth: usize,
    field_match: Option<(&str, &FieldDef, &FieldDef)>,
) -> Result<String> {
    let branch_table = schema.table_def(context.branch_table)?;
    let backing_alias = format!("branch_policy_backing_{depth}");
    let ids_alias = format!("branch_policy_backing_ids_{depth}");
    let tx_alias = format!("branch_policy_backing_tx_{depth}");
    let branch_alias = format!("branch_policy_branch_{depth}");
    let backing_policy_sql = lower_policy(
        schema,
        branch_table,
        &backing_alias,
        &branch_table.read_policy,
        user,
        PolicyReadScope::CurrentMain,
        depth + 1,
    )?;
    let field_match_sql = field_match
        .map(|(row_alias, row_field, branch_field)| {
            format!(
                "AND {row_alias}.{row_column} = {backing_alias}.{branch_column}",
                row_column = crate::schema::quote_ident(&crate::schema::storage_column(row_field)),
                branch_column =
                    crate::schema::quote_ident(&crate::schema::storage_column(branch_field)),
            )
        })
        .unwrap_or_default();
    Ok(format!(
        "EXISTS (
           SELECT 1
           FROM {current_table} {backing_alias}
           JOIN jazz_row_id {ids_alias}
             ON {ids_alias}.row_num = {backing_alias}.row_num
           JOIN jazz_tx {tx_alias}
             ON {tx_alias}.tx_num = {backing_alias}.visible_tx_num
           JOIN jazz_branch {branch_alias}
             ON {branch_alias}.branch_num = {branch_num}
           WHERE {ids_alias}.row_id = {branch_alias}.branch_id
             AND {backing_alias}.j_branch_num = 1
             AND {backing_alias}.is_deleted = 0
             AND {tx_alias}.outcome != {rejected}
             {field_match_sql}
             AND {backing_policy_sql}
         )",
        current_table = crate::schema::current_table(&branch_table.name),
        branch_num = context.branch_num,
        rejected = tx::OUTCOME_REJECTED,
    ))
}

struct SnapshotPolicyLowering<'a> {
    schema: &'a SchemaDef,
    user: &'a str,
    base_epoch: i64,
    branch_context: Option<BranchPolicyContext<'a>>,
}

impl SnapshotPolicyLowering<'_> {
    fn lower(
        &self,
        table: &TableDef,
        alias: &str,
        policy: &PolicyDef,
        depth: usize,
    ) -> Result<String> {
        if depth > MAX_POLICY_RECURSION_DEPTH {
            return Err(crate::Error::new(
                "snapshot policy recursion depth exceeded",
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
                let parent_alias = format!("snapshot_policy_parent_{depth}");
                let parent_tx_alias = format!("snapshot_policy_parent_tx_{depth}");
                let newer_alias = format!("snapshot_policy_newer_{depth}");
                let newer_tx_alias = format!("snapshot_policy_newer_tx_{depth}");
                let parent_policy =
                    self.lower(ref_table, &parent_alias, &ref_table.read_policy, depth + 1)?;
                Ok(format!(
                "EXISTS (
                   SELECT 1
                   FROM {} {parent_alias}
                   JOIN jazz_tx {parent_tx_alias}
                     ON {parent_tx_alias}.tx_num = {parent_alias}.tx_num
                   WHERE {parent_alias}.row_num = {alias}.{}
                     AND {parent_alias}.j_branch_num = 1
                     AND {parent_alias}.op != 3
                     AND {parent_tx_alias}.outcome != {}
                     AND {parent_tx_alias}.global_epoch IS NOT NULL
                     AND {parent_tx_alias}.global_epoch <= {}
                     AND NOT EXISTS (
                       SELECT 1
                       FROM {} {newer_alias}
                       JOIN jazz_tx {newer_tx_alias}
                         ON {newer_tx_alias}.tx_num = {newer_alias}.tx_num
                       WHERE {newer_alias}.row_num = {parent_alias}.row_num
                         AND {newer_alias}.j_branch_num = 1
                         AND {newer_tx_alias}.outcome != {}
                         AND {newer_tx_alias}.global_epoch IS NOT NULL
                         AND {newer_tx_alias}.global_epoch <= {}
                         AND ({newer_tx_alias}.global_epoch > {parent_tx_alias}.global_epoch OR ({newer_tx_alias}.global_epoch = {parent_tx_alias}.global_epoch AND {newer_tx_alias}.tx_num > {parent_tx_alias}.tx_num))
                     )
                     AND {parent_policy}
                 )",
                crate::schema::history_table(&ref_table.name),
                crate::schema::quote_ident(&crate::schema::storage_column(field)),
                tx::OUTCOME_REJECTED,
                self.base_epoch,
                crate::schema::history_table(&ref_table.name),
                tx::OUTCOME_REJECTED,
                self.base_epoch,
            ))
            }
            PolicyDef::BranchFieldEquals {
                field,
                branch_field,
            } => {
                let Some(context) = self.branch_context else {
                    return Err(crate::Error::new(
                        "branch field snapshot policy requires branch context",
                    ));
                };
                let field = table
                    .fields
                    .iter()
                    .find(|candidate| candidate.name == *field)
                    .ok_or_else(|| {
                        crate::Error::new(format!("unknown branch policy field {field}"))
                    })?;
                let branch_table = self.schema.table_def(context.branch_table)?;
                let branch_field = branch_table
                    .fields
                    .iter()
                    .find(|candidate| candidate.name == *branch_field)
                    .ok_or_else(|| {
                        crate::Error::new(format!(
                            "unknown branch policy field {}.{branch_field}",
                            branch_table.name
                        ))
                    })?;
                branch_backing_row_visible_sql(
                    self.schema,
                    self.user,
                    context,
                    depth,
                    Some((alias, field, branch_field)),
                )
            }
        }
    }
}
