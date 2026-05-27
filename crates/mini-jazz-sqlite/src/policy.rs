use crate::schema::{FieldKind, PolicyDef, SchemaDef, TableDef};
use crate::{branch, tx, users, Result};
use rusqlite::{params, Connection};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

const MAX_POLICY_RECURSION_DEPTH: usize = 64;

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
    match &check.table.write_policy {
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
        PolicyDef::RowIdEqualsUser => {
            Ok(crate::rows::public_row_id(check.db, check.row_num)? == check.user)
        }
        PolicyDef::UserRefEqualsSession { field } => {
            let field_def = check
                .table
                .fields
                .iter()
                .find(|candidate| candidate.name == *field)
                .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
            let FieldKind::Ref { table } = &field_def.kind else {
                return Err(crate::Error::new(format!(
                    "policy field {} is not a ref",
                    field_def.name
                )));
            };
            if table != "users" {
                return Err(crate::Error::new(format!(
                    "policy field {} must reference users",
                    field_def.name
                )));
            }
            Ok(check
                .values
                .get(field)
                .or_else(|| check.values.get(&field_def.storage_name))
                .and_then(JsonValue::as_str)
                == Some(check.user))
        }
        PolicyDef::GroupMember => {
            let policy_sql = current_group_member_policy_sql(
                "current.row_num",
                check.user,
                Some(check.branch_num),
                0,
            );
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
                    crate::schema::current_table(&check.table.name),
                    effective_branch_sql("current", &check.table.name, check.branch_num),
                    tx::OUTCOME_REJECTED,
                ),
                params![check.row_num],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        }
        PolicyDef::ProjectMember => {
            let policy_sql = current_project_member_policy_sql(
                "current.row_num",
                check.user,
                Some(check.branch_num),
                0,
            );
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
                    crate::schema::current_table(&check.table.name),
                    effective_branch_sql("current", &check.table.name, check.branch_num),
                    tx::OUTCOME_REJECTED,
                ),
                params![check.row_num],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        }
        PolicyDef::ProjectRefMember { field } => {
            let field_def = check
                .table
                .fields
                .iter()
                .find(|candidate| candidate.name == *field)
                .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
            let FieldKind::Ref { table } = &field_def.kind else {
                return Err(crate::Error::new(format!(
                    "policy field {} is not a ref",
                    field_def.name
                )));
            };
            if table != "projects" {
                return Err(crate::Error::new(format!(
                    "policy field {} must reference projects",
                    field_def.name
                )));
            }
            let project_id = check
                .values
                .get(field)
                .or_else(|| check.values.get(&field_def.storage_name))
                .and_then(JsonValue::as_str)
                .ok_or_else(|| crate::Error::new(format!("expected ref id for {field}")))?;
            let project_row_num = crate::rows::row_num(check.db, project_id)?;
            let policy_sql = current_project_member_policy_sql(
                "current.row_num",
                check.user,
                Some(check.branch_num),
                0,
            );
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
                    crate::schema::current_table("projects"),
                    effective_branch_sql("current", "projects", check.branch_num),
                    tx::OUTCOME_REJECTED,
                ),
                params![project_row_num],
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
                Some(check.branch_num),
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
    }
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

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn user_row_num_subquery(user: &str) -> String {
    format!(
        "SELECT row_num FROM jazz_row_id WHERE row_id = {}",
        sql_literal(user)
    )
}

fn row_id_equals_user_sql(alias: &str, user: &str) -> String {
    format!("{alias}.row_num = ({})", user_row_num_subquery(user))
}

fn current_user_ref_equals_session_sql(
    table: &TableDef,
    alias: &str,
    field_name: &str,
    user: &str,
) -> Result<String> {
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field_name}")))?;
    let FieldKind::Ref { table } = &field.kind else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    if table != "users" {
        return Err(crate::Error::new(format!(
            "policy field {} must reference users",
            field.name
        )));
    }
    Ok(format!(
        "{alias}.{} = ({})",
        crate::schema::quote_ident(&crate::schema::storage_column(field)),
        user_row_num_subquery(user)
    ))
}

fn current_project_ref_member_sql(
    table: &TableDef,
    alias: &str,
    field_name: &str,
    user: &str,
    branch_num: Option<i64>,
    depth: usize,
) -> Result<String> {
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field_name}")))?;
    let FieldKind::Ref { table } = &field.kind else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    if table != "projects" {
        return Err(crate::Error::new(format!(
            "policy field {} must reference projects",
            field.name
        )));
    }
    Ok(current_project_member_policy_sql(
        &format!(
            "{alias}.{}",
            crate::schema::quote_ident(&crate::schema::storage_column(field))
        ),
        user,
        branch_num,
        depth,
    ))
}

fn current_group_member_policy_sql(
    group_row_expr: &str,
    user: &str,
    branch_num: Option<i64>,
    depth: usize,
) -> String {
    let member_alias = format!("policy_group_member_{depth}");
    let member_tx_alias = format!("policy_group_member_tx_{depth}");
    let branch_filter = branch_num
        .map(|branch_num| {
            format!(
                "AND {}",
                effective_branch_sql(&member_alias, "group_members", branch_num)
            )
        })
        .unwrap_or_default();
    format!(
        "EXISTS (
           SELECT 1
           FROM {} {member_alias}
           JOIN jazz_tx {member_tx_alias}
             ON {member_tx_alias}.tx_num = {member_alias}.visible_tx_num
           WHERE {member_alias}.group_row_num = {group_row_expr}
             AND {member_alias}.user_row_num = ({})
             {branch_filter}
             AND {member_alias}.is_deleted = 0
             AND {member_tx_alias}.outcome != {}
         )",
        crate::schema::current_table("group_members"),
        user_row_num_subquery(user),
        tx::OUTCOME_REJECTED,
    )
}

fn current_project_member_policy_sql(
    project_row_expr: &str,
    user: &str,
    branch_num: Option<i64>,
    depth: usize,
) -> String {
    let project_member_alias = format!("policy_project_member_{depth}");
    let project_member_tx_alias = format!("policy_project_member_tx_{depth}");
    let group_member_alias = format!("policy_project_group_member_{depth}");
    let group_member_tx_alias = format!("policy_project_group_member_tx_{depth}");
    let group_ids_alias = format!("policy_project_group_ids_{depth}");
    let project_member_branch_filter = branch_num
        .map(|branch_num| {
            format!(
                "AND {}",
                effective_branch_sql(&project_member_alias, "project_members", branch_num)
            )
        })
        .unwrap_or_default();
    let group_member_branch_filter = branch_num
        .map(|branch_num| {
            format!(
                "AND {}",
                effective_branch_sql(&group_member_alias, "group_members", branch_num)
            )
        })
        .unwrap_or_default();
    format!(
        "EXISTS (
           SELECT 1
           FROM {} {project_member_alias}
           JOIN jazz_tx {project_member_tx_alias}
             ON {project_member_tx_alias}.tx_num = {project_member_alias}.visible_tx_num
           WHERE {project_member_alias}.project_row_num = {project_row_expr}
             {project_member_branch_filter}
             AND {project_member_alias}.is_deleted = 0
             AND {project_member_tx_alias}.outcome != {}
             AND (
               {project_member_alias}.member = {}
               OR EXISTS (
                 SELECT 1
                 FROM {} {group_member_alias}
                 JOIN jazz_tx {group_member_tx_alias}
                   ON {group_member_tx_alias}.tx_num = {group_member_alias}.visible_tx_num
                 JOIN jazz_row_id {group_ids_alias}
                   ON {group_ids_alias}.row_num = {group_member_alias}.group_row_num
                 WHERE {group_member_alias}.user_row_num = ({})
                   {group_member_branch_filter}
                   AND {group_member_alias}.is_deleted = 0
                   AND {group_member_tx_alias}.outcome != {}
                   AND {project_member_alias}.member = ('group:' || {group_ids_alias}.row_id)
               )
             )
         )",
        crate::schema::current_table("project_members"),
        tx::OUTCOME_REJECTED,
        sql_literal(&format!("user:{user}")),
        crate::schema::current_table("group_members"),
        user_row_num_subquery(user),
        tx::OUTCOME_REJECTED,
    )
}

pub(crate) fn branch_read_policy_sql_for_alias(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    user: &str,
    branch_num: i64,
) -> Result<String> {
    lower_policy(
        schema,
        table,
        alias,
        &table.read_policy,
        user,
        Some(branch_num),
        0,
    )
}

pub(crate) fn snapshot_read_policy_sql_for_alias(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    user: &str,
    base_epoch: i64,
) -> Result<String> {
    lower_snapshot_policy(
        schema,
        table,
        alias,
        &table.read_policy,
        user,
        base_epoch,
        0,
    )
}

fn lower_policy(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    policy: &PolicyDef,
    user: &str,
    branch_num: Option<i64>,
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
        PolicyDef::RowIdEqualsUser => Ok(row_id_equals_user_sql(alias, user)),
        PolicyDef::UserRefEqualsSession { field } => {
            current_user_ref_equals_session_sql(table, alias, field, user)
        }
        PolicyDef::GroupMember => Ok(current_group_member_policy_sql(
            &format!("{alias}.row_num"),
            user,
            branch_num,
            depth,
        )),
        PolicyDef::ProjectMember => Ok(current_project_member_policy_sql(
            &format!("{alias}.row_num"),
            user,
            branch_num,
            depth,
        )),
        PolicyDef::ProjectRefMember { field } => {
            current_project_ref_member_sql(table, alias, field, user, branch_num, depth)
        }
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
                branch_num,
                depth + 1,
            )?;
            let branch_filter = branch_num
                .map(|branch_num| {
                    format!(
                        "AND {}",
                        effective_branch_sql(&parent_alias, &ref_table.name, branch_num)
                    )
                })
                .unwrap_or_default();
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
    }
}

fn lower_snapshot_policy(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    policy: &PolicyDef,
    user: &str,
    base_epoch: i64,
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
            user.replace('\'', "''")
        )),
        PolicyDef::RowIdEqualsUser => Ok(row_id_equals_user_sql(alias, user)),
        PolicyDef::UserRefEqualsSession { field } => {
            current_user_ref_equals_session_sql(table, alias, field, user)
        }
        PolicyDef::GroupMember => Ok(current_group_member_policy_sql(
            &format!("{alias}.row_num"),
            user,
            None,
            depth,
        )),
        PolicyDef::ProjectMember => Ok(current_project_member_policy_sql(
            &format!("{alias}.row_num"),
            user,
            None,
            depth,
        )),
        PolicyDef::ProjectRefMember { field } => {
            current_project_ref_member_sql(table, alias, field, user, None, depth)
        }
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
            let parent_alias = format!("snapshot_policy_parent_{depth}");
            let parent_tx_alias = format!("snapshot_policy_parent_tx_{depth}");
            let newer_alias = format!("snapshot_policy_newer_{depth}");
            let newer_tx_alias = format!("snapshot_policy_newer_tx_{depth}");
            let parent_policy = lower_snapshot_policy(
                schema,
                ref_table,
                &parent_alias,
                &ref_table.read_policy,
                user,
                base_epoch,
                depth + 1,
            )?;
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
                     AND {parent_tx_alias}.global_epoch <= {base_epoch}
                     AND NOT EXISTS (
                       SELECT 1
                       FROM {} {newer_alias}
                       JOIN jazz_tx {newer_tx_alias}
                         ON {newer_tx_alias}.tx_num = {newer_alias}.tx_num
                       WHERE {newer_alias}.row_num = {parent_alias}.row_num
                         AND {newer_alias}.j_branch_num = 1
                         AND {newer_tx_alias}.outcome != {}
                         AND {newer_tx_alias}.global_epoch IS NOT NULL
                         AND {newer_tx_alias}.global_epoch <= {base_epoch}
                         AND ({newer_tx_alias}.global_epoch > {parent_tx_alias}.global_epoch OR ({newer_tx_alias}.global_epoch = {parent_tx_alias}.global_epoch AND {newer_tx_alias}.tx_num > {parent_tx_alias}.tx_num))
                     )
                     AND {parent_policy}
                 )",
                crate::schema::history_table(&ref_table.name),
                crate::schema::quote_ident(&crate::schema::storage_column(field)),
                tx::OUTCOME_REJECTED,
                crate::schema::history_table(&ref_table.name),
                tx::OUTCOME_REJECTED,
            ))
        }
    }
}
