use crate::schema::{
    CmpOp, FieldKind, Operation, PolicyDef, PolicyValue, SchemaDef, TableDef,
    OUTER_ROW_SESSION_PREFIX,
};
use crate::sync::history_op;
use crate::{branch, tx, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

const MAX_POLICY_RECURSION_DEPTH: usize = 64;

pub(crate) struct WriteCheck<'a> {
    pub(crate) db: &'a Connection,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) table: &'a TableDef,
    pub(crate) policy: &'a PolicyDef,
    pub(crate) row_num: i64,
    pub(crate) branch_num: i64,
    pub(crate) values: &'a BTreeMap<String, JsonValue>,
    pub(crate) user: &'a str,
    pub(crate) created_by: Option<&'a str>,
}

pub(crate) fn write_allowed(check: WriteCheck<'_>) -> Result<bool> {
    write_allowed_for_policy(&check, check.policy)
}

fn write_allowed_for_policy(check: &WriteCheck<'_>, policy: &PolicyDef) -> Result<bool> {
    match policy {
        PolicyDef::True => Ok(true),
        PolicyDef::False => Ok(false),
        PolicyDef::Cmp { column, op, value } => cmp_write_allowed(check, column, op, value),
        PolicyDef::SessionCmp { .. }
        | PolicyDef::IsNull { .. }
        | PolicyDef::SessionIsNull { .. }
        | PolicyDef::IsNotNull { .. }
        | PolicyDef::SessionIsNotNull { .. }
        | PolicyDef::Contains { .. }
        | PolicyDef::SessionContains { .. }
        | PolicyDef::In { .. }
        | PolicyDef::InList { .. }
        | PolicyDef::SessionInList { .. }
        | PolicyDef::ExistsRel { .. } => current_row_matches_policy(check, policy),
        PolicyDef::Exists { table, condition } => exists_write_allowed(check, table, condition),
        PolicyDef::Inherits {
            operation,
            via_column,
            ..
        } => {
            ensure_select_operation(*operation)?;
            ref_readable_write_allowed(check, via_column)
        }
        PolicyDef::InheritsReferencing { .. } => current_row_matches_policy(check, policy),
        PolicyDef::And(children) => {
            for child in children {
                if !write_allowed_for_policy(check, child)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        PolicyDef::Or(children) => {
            for child in children {
                if write_allowed_for_policy(check, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        PolicyDef::Not(child) => Ok(!write_allowed_for_policy(check, child)?),
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

fn ensure_select_operation(operation: Operation) -> Result<()> {
    if operation != Operation::Select {
        return Err(crate::Error::new(
            "mini-sqlite policies only lower SELECT inheritance today",
        ));
    }
    Ok(())
}

fn cmp_write_allowed(
    check: &WriteCheck<'_>,
    column: &str,
    op: &CmpOp,
    value: &PolicyValue,
) -> Result<bool> {
    if column == "$id" {
        let left = JsonValue::String(crate::rows::public_row_id(check.db, check.row_num)?);
        let right = resolve_policy_value_for_json(check, value)?;
        return Ok(compare_json_values(&left, op, &right));
    }
    if column == "$createdBy" {
        let left = current_or_pending_created_by(check)?
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null);
        let right = resolve_policy_value_for_json(check, value)?;
        return Ok(compare_json_values(&left, op, &right));
    }
    let field = check
        .table
        .fields
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| crate::Error::new(format!("unknown policy column {column}")))?;
    let left = check
        .values
        .get(column)
        .or_else(|| check.values.get(&field.storage_name))
        .cloned()
        .unwrap_or(JsonValue::Null);
    let right = match &field.kind {
        FieldKind::Ref { table } => resolve_policy_value_for_ref(check, table, value)?,
        FieldKind::Text | FieldKind::Bool => resolve_policy_value_for_json(check, value)?,
    };
    Ok(compare_json_values(&left, op, &right))
}

fn current_or_pending_created_by(check: &WriteCheck<'_>) -> Result<Option<String>> {
    if let Some(created_by) = check.created_by {
        return Ok(Some(created_by.to_owned()));
    }
    check
        .db
        .query_row(
            &format!(
                "SELECT user.user_id
                 FROM {} current
                 JOIN jazz_user user ON user.user_num = current.j_created_by
                 WHERE current.row_num = ?
                   AND current.j_branch_num = ?",
                crate::schema::current_table(&check.table.name)
            ),
            params![check.row_num, check.branch_num],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(Into::into)
}

fn resolve_policy_value_for_ref(
    check: &WriteCheck<'_>,
    ref_table: &str,
    value: &PolicyValue,
) -> Result<JsonValue> {
    match value {
        PolicyValue::Literal(value) => Ok(value.clone()),
        PolicyValue::SessionRef(path) if outer_row_ref_column(path).is_some() => {
            resolve_outer_policy_value_for_json(check, path)
        }
        PolicyValue::SessionRef(path) if is_session_user_id(path) && ref_table == "users" => {
            Ok(JsonValue::String(check.user.to_owned()))
        }
        PolicyValue::SessionRef(path) => Err(crate::Error::new(format!(
            "unsupported policy session ref {}",
            path.join(".")
        ))),
    }
}

fn resolve_policy_value_for_json(check: &WriteCheck<'_>, value: &PolicyValue) -> Result<JsonValue> {
    match value {
        PolicyValue::Literal(value) => Ok(value.clone()),
        PolicyValue::SessionRef(path) if outer_row_ref_column(path).is_some() => {
            resolve_outer_policy_value_for_json(check, path)
        }
        PolicyValue::SessionRef(path) if is_session_user_id(path) => {
            Ok(JsonValue::String(check.user.to_owned()))
        }
        PolicyValue::SessionRef(path) => Err(crate::Error::new(format!(
            "unsupported policy session ref {}",
            path.join(".")
        ))),
    }
}

fn resolve_outer_policy_value_for_json(
    check: &WriteCheck<'_>,
    path: &[String],
) -> Result<JsonValue> {
    let column = outer_row_ref_column(path).expect("outer row ref checked by caller");
    if column == "$id" {
        return Ok(JsonValue::String(crate::rows::public_row_id(
            check.db,
            check.row_num,
        )?));
    }
    if column == "$createdBy" {
        return Ok(current_or_pending_created_by(check)?
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null));
    }
    let field = check
        .table
        .fields
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| crate::Error::new(format!("unknown outer row policy column {column}")))?;
    Ok(check
        .values
        .get(column)
        .or_else(|| check.values.get(&field.storage_name))
        .cloned()
        .unwrap_or(JsonValue::Null))
}

fn compare_json_values(left: &JsonValue, op: &CmpOp, right: &JsonValue) -> bool {
    match op {
        CmpOp::Eq => left == right,
        CmpOp::Ne => left != right,
        CmpOp::Lt => json_ord(left, right).is_some_and(|ord| ord.is_lt()),
        CmpOp::Le => json_ord(left, right).is_some_and(|ord| ord.is_le()),
        CmpOp::Gt => json_ord(left, right).is_some_and(|ord| ord.is_gt()),
        CmpOp::Ge => json_ord(left, right).is_some_and(|ord| ord.is_ge()),
    }
}

fn json_ord(left: &JsonValue, right: &JsonValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (JsonValue::String(left), JsonValue::String(right)) => Some(left.cmp(right)),
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            left.as_f64()?.partial_cmp(&right.as_f64()?)
        }
        (JsonValue::Bool(left), JsonValue::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn ref_readable_write_allowed(check: &WriteCheck<'_>, field: &str) -> Result<bool> {
    let field_def = check
        .table
        .fields
        .iter()
        .find(|candidate| candidate.name == field)
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
    ref_row_read_allowed(check, ref_table_name, ref_id)
}

fn ref_row_read_allowed(
    check: &WriteCheck<'_>,
    ref_table_name: &str,
    ref_id: &str,
) -> Result<bool> {
    let ref_row_num = crate::rows::row_num(check.db, ref_id)?;
    let ref_table = check.schema.table_def(ref_table_name)?;
    if check.branch_num != 1 {
        if let Some(base_epoch) = branch::base_global_epoch(check.db, check.branch_num)? {
            if !branch_current_row_exists(check.db, ref_table_name, ref_row_num, check.branch_num)?
            {
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
        None,
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

fn current_row_matches_policy(check: &WriteCheck<'_>, policy: &PolicyDef) -> Result<bool> {
    let policy_sql = lower_policy(
        check.schema,
        check.table,
        "current",
        policy,
        check.user,
        Some(check.branch_num),
        0,
        None,
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
            crate::schema::current_table(&check.table.name),
            effective_branch_sql("current", &check.table.name, check.branch_num),
            tx::OUTCOME_REJECTED,
        ),
        params![check.row_num],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn exists_write_allowed(
    check: &WriteCheck<'_>,
    table_name: &str,
    condition: &PolicyDef,
) -> Result<bool> {
    let exists_table = check.schema.table_def(table_name)?;
    let exists_alias = "policy_exists_write";
    let exists_tx_alias = "policy_exists_write_tx";
    let condition_sql = lower_policy(
        check.schema,
        exists_table,
        exists_alias,
        condition,
        check.user,
        Some(check.branch_num),
        1,
        Some(PolicyOuterRow::Values {
            db: check.db,
            table: check.table,
            row_num: check.row_num,
            branch_num: check.branch_num,
            values: check.values,
            created_by: check.created_by,
        }),
    )?;
    let count: i64 = check.db.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {} {exists_alias}
             JOIN jazz_tx {exists_tx_alias}
               ON {exists_tx_alias}.tx_num = {exists_alias}.visible_tx_num
             WHERE {exists_alias}.is_deleted = 0
               AND {}
               AND {exists_tx_alias}.outcome != {}
               AND {condition_sql}",
            crate::schema::current_table(table_name),
            effective_branch_sql(exists_alias, table_name, check.branch_num),
            tx::OUTCOME_REJECTED,
        ),
        [],
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
    let delete_op = history_op::DELETE;
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.row_num = ?
               AND h.j_branch_num = 1
               AND h.op != {delete_op}
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

#[derive(Clone, Copy)]
enum PolicyOuterRow<'a> {
    Alias {
        table: &'a TableDef,
        alias: &'a str,
    },
    Values {
        db: &'a Connection,
        table: &'a TableDef,
        row_num: i64,
        branch_num: i64,
        values: &'a BTreeMap<String, JsonValue>,
        created_by: Option<&'a str>,
    },
}

pub(crate) fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn outer_row_ref_column(path: &[String]) -> Option<&str> {
    if path.len() == 2 && path[0] == OUTER_ROW_SESSION_PREFIX {
        Some(path[1].as_str())
    } else {
        None
    }
}

fn user_row_num_subquery(user: &str) -> String {
    format!(
        "SELECT row_num FROM jazz_row_id WHERE row_id = {}",
        sql_literal(user)
    )
}

fn current_cmp_sql(
    table: &TableDef,
    alias: &str,
    column: &str,
    op: &CmpOp,
    value: &PolicyValue,
    user: &str,
    outer_row: Option<PolicyOuterRow<'_>>,
) -> Result<String> {
    let left = policy_column_sql(table, alias, column)?;
    let right = policy_value_sql(table, column, value, user, outer_row)?;
    Ok(format!("{left} {} {right}", cmp_op_sql(op)))
}

fn policy_column_sql(table: &TableDef, alias: &str, column: &str) -> Result<String> {
    if column == "$id" {
        return Ok(format!(
            "(SELECT row_id FROM jazz_row_id WHERE row_num = {alias}.row_num)"
        ));
    }
    if column == "$createdBy" {
        return Ok(format!(
            "(SELECT user_id FROM jazz_user WHERE user_num = {alias}.j_created_by)"
        ));
    }
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| crate::Error::new(format!("unknown policy column {column}")))?;
    Ok(format!(
        "{alias}.{}",
        crate::schema::quote_ident(&crate::schema::storage_column(field))
    ))
}

fn policy_value_sql(
    table: &TableDef,
    column: &str,
    value: &PolicyValue,
    user: &str,
    outer_row: Option<PolicyOuterRow<'_>>,
) -> Result<String> {
    match value {
        PolicyValue::Literal(value) => {
            let Some(field) = table
                .fields
                .iter()
                .find(|candidate| candidate.name == column)
            else {
                return Ok(json_literal_sql(value));
            };
            match (&field.kind, value) {
                (FieldKind::Ref { .. }, JsonValue::String(row_id)) => Ok(format!(
                    "(SELECT row_num FROM jazz_row_id WHERE row_id = {})",
                    sql_literal(row_id)
                )),
                _ => Ok(json_literal_sql(value)),
            }
        }
        PolicyValue::SessionRef(path) if outer_row_ref_column(path).is_some() => {
            let outer_column = outer_row_ref_column(path).expect("outer row ref checked by guard");
            let Some(outer_row) = outer_row else {
                return Err(crate::Error::new(format!(
                    "outer row ref {} used without outer row context",
                    path.join(".")
                )));
            };
            outer_policy_value_sql(table, column, outer_row, outer_column)
        }
        PolicyValue::SessionRef(path) if is_session_user_id(path) => {
            let Some(field) = table
                .fields
                .iter()
                .find(|candidate| candidate.name == column)
            else {
                return Ok(sql_literal(user));
            };
            match &field.kind {
                FieldKind::Ref { table } if table == "users" => {
                    Ok(format!("({})", user_row_num_subquery(user)))
                }
                _ => Ok(sql_literal(user)),
            }
        }
        PolicyValue::SessionRef(path) => Err(crate::Error::new(format!(
            "unsupported policy session ref {}",
            path.join(".")
        ))),
    }
}

fn outer_policy_value_sql(
    table: &TableDef,
    column: &str,
    outer_row: PolicyOuterRow<'_>,
    outer_column: &str,
) -> Result<String> {
    match outer_row {
        PolicyOuterRow::Alias { table, alias } => policy_column_sql(table, alias, outer_column),
        PolicyOuterRow::Values {
            db,
            table: outer_table,
            row_num,
            branch_num,
            values,
            created_by,
        } => {
            let value = outer_policy_value_from_values(
                db,
                outer_table,
                row_num,
                branch_num,
                values,
                created_by,
                outer_column,
            )?;
            let Some(field) = table
                .fields
                .iter()
                .find(|candidate| candidate.name == column)
            else {
                return Ok(json_literal_sql(&value));
            };
            match (&field.kind, value) {
                (FieldKind::Ref { .. }, JsonValue::String(row_id)) => Ok(format!(
                    "(SELECT row_num FROM jazz_row_id WHERE row_id = {})",
                    sql_literal(&row_id)
                )),
                (_, value) => Ok(json_literal_sql(&value)),
            }
        }
    }
}

fn outer_policy_value_from_values(
    db: &Connection,
    table: &TableDef,
    row_num: i64,
    branch_num: i64,
    values: &BTreeMap<String, JsonValue>,
    created_by: Option<&str>,
    column: &str,
) -> Result<JsonValue> {
    if column == "$id" {
        return Ok(JsonValue::String(crate::rows::public_row_id(db, row_num)?));
    }
    if column == "$createdBy" {
        let created_by = match created_by {
            Some(created_by) => Some(created_by.to_owned()),
            None => current_created_by_for_row(db, table, row_num, branch_num)?,
        };
        return Ok(created_by.map(JsonValue::String).unwrap_or(JsonValue::Null));
    }
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| crate::Error::new(format!("unknown outer row policy column {column}")))?;
    Ok(values
        .get(column)
        .or_else(|| values.get(&field.storage_name))
        .cloned()
        .unwrap_or(JsonValue::Null))
}

fn current_created_by_for_row(
    db: &Connection,
    table: &TableDef,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<String>> {
    db.query_row(
        &format!(
            "SELECT user.user_id
             FROM {} current
             JOIN jazz_user user ON user.user_num = current.j_created_by
             WHERE current.row_num = ?
               AND current.j_branch_num = ?",
            crate::schema::current_table(&table.name)
        ),
        params![row_num, branch_num],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(Into::into)
}

fn is_session_user_id(path: &[String]) -> bool {
    path.len() == 1 && path[0] == "user_id"
}

fn cmp_op_sql(op: &CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "=",
        CmpOp::Ne => "!=",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
    }
}

fn json_literal_sql(value: &JsonValue) -> String {
    match value {
        JsonValue::String(value) => sql_literal(value),
        JsonValue::Bool(value) => {
            if *value {
                "1".to_owned()
            } else {
                "0".to_owned()
            }
        }
        JsonValue::Number(value) => value.to_string(),
        JsonValue::Null => "NULL".to_owned(),
        JsonValue::Array(_) | JsonValue::Object(_) => sql_literal(&value.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
fn current_exists_policy_sql(
    schema: &SchemaDef,
    outer_table: &TableDef,
    outer_alias: &str,
    table_name: &str,
    condition: &PolicyDef,
    user: &str,
    branch_num: Option<i64>,
    depth: usize,
) -> Result<String> {
    let exists_table = schema.table_def(table_name)?;
    let exists_alias = format!("policy_exists_{depth}");
    let exists_tx_alias = format!("policy_exists_tx_{depth}");
    let condition_sql = lower_policy(
        schema,
        exists_table,
        &exists_alias,
        condition,
        user,
        branch_num,
        depth + 1,
        Some(PolicyOuterRow::Alias {
            table: outer_table,
            alias: outer_alias,
        }),
    )?;
    let branch_filter = branch_num
        .map(|branch_num| {
            format!(
                "AND {}",
                effective_branch_sql(&exists_alias, table_name, branch_num)
            )
        })
        .unwrap_or_default();
    Ok(format!(
        "EXISTS (
           SELECT 1
           FROM {} {exists_alias}
           JOIN jazz_tx {exists_tx_alias}
             ON {exists_tx_alias}.tx_num = {exists_alias}.visible_tx_num
           WHERE {exists_alias}.is_deleted = 0
             {branch_filter}
             AND {exists_tx_alias}.outcome != {}
             AND {condition_sql}
         )",
        crate::schema::current_table(table_name),
        tx::OUTCOME_REJECTED,
    ))
}

fn current_group_member_policy_sql(
    group_row_expr: &str,
    user: &str,
    branch_num: Option<i64>,
    depth: usize,
) -> String {
    let reachable_alias = format!("policy_reachable_group_{depth}");
    let reachable_cte = current_reachable_group_rows_cte(&reachable_alias, user, branch_num, depth);
    format!(
        "EXISTS (
           {reachable_cte}
           SELECT 1
           FROM {reachable_alias}
           WHERE {reachable_alias}.row_num = {group_row_expr}
         )",
    )
}

fn current_reachable_group_rows_cte(
    cte_alias: &str,
    user: &str,
    branch_num: Option<i64>,
    depth: usize,
) -> String {
    let direct_alias = format!("{cte_alias}_direct_{depth}");
    let direct_tx_alias = format!("{cte_alias}_direct_tx_{depth}");
    let nested_alias = format!("{cte_alias}_nested_{depth}");
    let nested_tx_alias = format!("{cte_alias}_nested_tx_{depth}");
    let parent_alias = format!("{cte_alias}_parent_{depth}");
    let direct_branch_filter = branch_num
        .map(|branch_num| {
            format!(
                "AND {}",
                effective_branch_sql(&direct_alias, "group_members", branch_num)
            )
        })
        .unwrap_or_default();
    let nested_branch_filter = branch_num
        .map(|branch_num| {
            format!(
                "AND {}",
                effective_branch_sql(&nested_alias, "group_members", branch_num)
            )
        })
        .unwrap_or_default();
    format!(
        "WITH RECURSIVE {cte_alias}(row_num) AS (
           SELECT {direct_alias}.group_row_num
           FROM {} {direct_alias}
           JOIN jazz_tx {direct_tx_alias}
             ON {direct_tx_alias}.tx_num = {direct_alias}.visible_tx_num
           WHERE {direct_alias}.user_row_num = ({})
             {direct_branch_filter}
             AND {direct_alias}.is_deleted = 0
             AND {direct_tx_alias}.outcome != {}
           UNION
           SELECT {nested_alias}.group_row_num
           FROM {} {nested_alias}
           JOIN jazz_tx {nested_tx_alias}
             ON {nested_tx_alias}.tx_num = {nested_alias}.visible_tx_num
           JOIN {cte_alias} {parent_alias}
             ON 1 = 1
           WHERE {nested_alias}.member_group_row_num = {parent_alias}.row_num
             {nested_branch_filter}
             AND {nested_alias}.is_deleted = 0
             AND {nested_tx_alias}.outcome != {}
         )",
        crate::schema::current_table("group_members"),
        user_row_num_subquery(user),
        tx::OUTCOME_REJECTED,
        crate::schema::current_table("group_members"),
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
    let reachable_alias = format!("policy_project_reachable_group_{depth}");
    let reachable_cte = current_reachable_group_rows_cte(&reachable_alias, user, branch_num, depth);
    let project_member_branch_filter = branch_num
        .map(|branch_num| {
            format!(
                "AND {}",
                effective_branch_sql(&project_member_alias, "project_members", branch_num)
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
               {project_member_alias}.user_row_num = ({})
               OR EXISTS (
                 {reachable_cte}
                 SELECT 1
                 FROM {reachable_alias}
                 WHERE {project_member_alias}.group_row_num = {reachable_alias}.row_num
               )
             )
         )",
        crate::schema::current_table("project_members"),
        tx::OUTCOME_REJECTED,
        user_row_num_subquery(user),
    )
}

fn current_inherits_policy_sql(
    schema: &SchemaDef,
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
        None,
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

#[allow(clippy::too_many_arguments)]
fn current_inherits_referencing_policy_sql(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    source_table_name: &str,
    field_name: &str,
    user: &str,
    branch_num: Option<i64>,
    depth: usize,
) -> Result<String> {
    let source_table = schema.table_def(source_table_name)?;
    if source_table_name == "group_members"
        && field_name == "group"
        && source_table
            .read_policy
            .is_user_or_ref_readable("user", "member_group")
    {
        return Ok(current_group_member_policy_sql(
            &format!("{alias}.row_num"),
            user,
            branch_num,
            depth,
        ));
    }
    if source_table_name == "project_members"
        && field_name == "project"
        && source_table
            .read_policy
            .is_user_or_ref_readable("user", "group")
    {
        return Ok(current_project_member_policy_sql(
            &format!("{alias}.row_num"),
            user,
            branch_num,
            depth,
        ));
    }

    let field = source_table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
        .ok_or_else(|| {
            crate::Error::new(format!(
                "unknown policy ref {source_table_name}.{field_name}"
            ))
        })?;
    let FieldKind::Ref {
        table: ref_table_name,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {}.{} is not a ref",
            source_table_name, field.name
        )));
    };
    if ref_table_name != &table.name {
        return Err(crate::Error::new(format!(
            "policy field {}.{} must reference {}",
            source_table_name, field.name, table.name
        )));
    }
    let source_alias = format!("policy_ref_source_{depth}");
    let source_tx_alias = format!("policy_ref_source_tx_{depth}");
    let source_policy = lower_policy(
        schema,
        source_table,
        &source_alias,
        &source_table.read_policy,
        user,
        branch_num,
        depth + 1,
        None,
    )?;
    let branch_filter = branch_num
        .map(|branch_num| {
            format!(
                "AND {}",
                effective_branch_sql(&source_alias, source_table_name, branch_num)
            )
        })
        .unwrap_or_default();
    Ok(format!(
        "EXISTS (
           SELECT 1
           FROM {} {source_alias}
           JOIN jazz_tx {source_tx_alias}
             ON {source_tx_alias}.tx_num = {source_alias}.visible_tx_num
           WHERE {source_alias}.{} = {alias}.row_num
             {branch_filter}
             AND {source_alias}.is_deleted = 0
             AND {source_tx_alias}.outcome != {}
             AND {source_policy}
         )",
        crate::schema::current_table(source_table_name),
        crate::schema::quote_ident(&crate::schema::storage_column(field)),
        tx::OUTCOME_REJECTED,
    ))
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
        None,
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

#[allow(clippy::too_many_arguments)]
fn lower_policy(
    schema: &SchemaDef,
    table: &TableDef,
    alias: &str,
    policy: &PolicyDef,
    user: &str,
    branch_num: Option<i64>,
    depth: usize,
    outer_row: Option<PolicyOuterRow<'_>>,
) -> Result<String> {
    if depth > MAX_POLICY_RECURSION_DEPTH {
        return Err(crate::Error::new("policy recursion depth exceeded"));
    }
    match policy {
        PolicyDef::True => Ok("1 = 1".to_owned()),
        PolicyDef::False => Ok("0 = 1".to_owned()),
        PolicyDef::Cmp { column, op, value } => {
            current_cmp_sql(table, alias, column, op, value, user, outer_row)
        }
        PolicyDef::SessionCmp { .. }
        | PolicyDef::IsNull { .. }
        | PolicyDef::SessionIsNull { .. }
        | PolicyDef::IsNotNull { .. }
        | PolicyDef::SessionIsNotNull { .. }
        | PolicyDef::Contains { .. }
        | PolicyDef::SessionContains { .. }
        | PolicyDef::In { .. }
        | PolicyDef::InList { .. }
        | PolicyDef::SessionInList { .. }
        | PolicyDef::ExistsRel { .. } => Err(crate::Error::new(
            "policy expression is not lowerable by mini-sqlite yet",
        )),
        PolicyDef::Exists {
            table: exists_table,
            condition,
        } => current_exists_policy_sql(
            schema,
            table,
            alias,
            exists_table,
            condition,
            user,
            branch_num,
            depth,
        ),
        PolicyDef::Inherits {
            operation,
            via_column,
            ..
        } => {
            ensure_select_operation(*operation)?;
            current_inherits_policy_sql(schema, table, alias, via_column, user, branch_num, depth)
        }
        PolicyDef::InheritsReferencing {
            operation,
            source_table,
            via_column,
            ..
        } => {
            ensure_select_operation(*operation)?;
            current_inherits_referencing_policy_sql(
                schema,
                table,
                alias,
                source_table,
                via_column,
                user,
                branch_num,
                depth,
            )
        }
        PolicyDef::And(children) => {
            if children.is_empty() {
                return Ok("1 = 1".to_owned());
            }
            let parts = children
                .iter()
                .map(|child| {
                    lower_policy(
                        schema,
                        table,
                        alias,
                        child,
                        user,
                        branch_num,
                        depth + 1,
                        outer_row,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        PolicyDef::Or(children) => {
            if children.is_empty() {
                return Ok("0 = 1".to_owned());
            }
            let parts = children
                .iter()
                .map(|child| {
                    lower_policy(
                        schema,
                        table,
                        alias,
                        child,
                        user,
                        branch_num,
                        depth + 1,
                        outer_row,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        PolicyDef::Not(child) => Ok(format!(
            "NOT ({})",
            lower_policy(
                schema,
                table,
                alias,
                child,
                user,
                branch_num,
                depth + 1,
                outer_row,
            )?
        )),
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
        PolicyDef::True => Ok("1 = 1".to_owned()),
        PolicyDef::False => Ok("0 = 1".to_owned()),
        PolicyDef::Cmp { column, op, value } => {
            current_cmp_sql(table, alias, column, op, value, user, None)
        }
        PolicyDef::SessionCmp { .. }
        | PolicyDef::IsNull { .. }
        | PolicyDef::SessionIsNull { .. }
        | PolicyDef::IsNotNull { .. }
        | PolicyDef::SessionIsNotNull { .. }
        | PolicyDef::Contains { .. }
        | PolicyDef::SessionContains { .. }
        | PolicyDef::In { .. }
        | PolicyDef::InList { .. }
        | PolicyDef::SessionInList { .. }
        | PolicyDef::ExistsRel { .. } => Err(crate::Error::new(
            "policy expression is not lowerable by mini-sqlite yet",
        )),
        PolicyDef::Exists {
            table: exists_table,
            condition,
        } => current_exists_policy_sql(
            schema,
            table,
            alias,
            exists_table,
            condition,
            user,
            None,
            depth,
        ),
        PolicyDef::Inherits {
            operation,
            via_column: field,
            ..
        } => {
            ensure_select_operation(*operation)?;
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
            let delete_op = history_op::DELETE;
            Ok(format!(
                "EXISTS (
                   SELECT 1
                   FROM {} {parent_alias}
                   JOIN jazz_tx {parent_tx_alias}
                     ON {parent_tx_alias}.tx_num = {parent_alias}.tx_num
                   WHERE {parent_alias}.row_num = {alias}.{}
                     AND {parent_alias}.j_branch_num = 1
                     AND {parent_alias}.op != {delete_op}
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
        PolicyDef::InheritsReferencing {
            operation,
            source_table,
            via_column,
            ..
        } => {
            ensure_select_operation(*operation)?;
            current_inherits_referencing_policy_sql(
                schema,
                table,
                alias,
                source_table,
                via_column,
                user,
                None,
                depth,
            )
        }
        PolicyDef::And(children) => {
            if children.is_empty() {
                return Ok("1 = 1".to_owned());
            }
            let parts = children
                .iter()
                .map(|child| {
                    lower_snapshot_policy(schema, table, alias, child, user, base_epoch, depth + 1)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        PolicyDef::Or(children) => {
            if children.is_empty() {
                return Ok("0 = 1".to_owned());
            }
            let parts = children
                .iter()
                .map(|child| {
                    lower_snapshot_policy(schema, table, alias, child, user, base_epoch, depth + 1)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        PolicyDef::Not(child) => Ok(format!(
            "NOT ({})",
            lower_snapshot_policy(schema, table, alias, child, user, base_epoch, depth + 1)?
        )),
    }
}
