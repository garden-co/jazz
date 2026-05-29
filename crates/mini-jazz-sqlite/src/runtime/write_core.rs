use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriteOp {
    Create,
    Update,
    Delete,
}

impl WriteOp {
    pub(super) fn code(self) -> i64 {
        match self {
            Self::Create => 1,
            Self::Update => 2,
            Self::Delete => 3,
        }
    }

    fn is_create(self) -> bool {
        matches!(self, Self::Create)
    }
}

pub(super) struct InsertRowInTx<'a> {
    pub(super) db: &'a Connection,
    pub(super) schema: &'a SchemaDef,
    pub(super) table_name: &'a str,
    pub(super) id: &'a str,
    pub(super) values: &'a BTreeMap<String, JsonValue>,
    pub(super) tx_num: i64,
    pub(super) branch_num: i64,
    pub(super) now: i64,
    pub(super) user: &'a str,
    pub(super) bypass_policy: bool,
    pub(super) op: WriteOp,
    pub(super) base_values: Option<&'a BTreeMap<String, JsonValue>>,
}

pub(super) struct StageDeleteInTx<'a> {
    pub(super) db: &'a Connection,
    pub(super) schema: &'a SchemaDef,
    pub(super) table_name: &'a str,
    pub(super) id: &'a str,
    pub(super) visible_values: &'a BTreeMap<String, JsonValue>,
    pub(super) tx_num: i64,
    pub(super) branch_num: i64,
    pub(super) now: i64,
    pub(super) user: &'a str,
    pub(super) bypass_policy: bool,
    pub(super) read_set: DeleteReadSetMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeleteReadSetMode {
    AlreadyCoveredByWriteCall,
    RecordPreviousRow,
}

struct EffectiveWriteValues<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table_name: &'a str,
    id: &'a str,
    row_num: i64,
    branch_num: i64,
    patch_values: &'a BTreeMap<String, JsonValue>,
    op: WriteOp,
    base_values: Option<&'a BTreeMap<String, JsonValue>>,
}

fn effective_write_values(args: EffectiveWriteValues<'_>) -> Result<BTreeMap<String, JsonValue>> {
    let table = args.schema.table_def(args.table_name)?;
    if args.op.is_create() {
        let mut values = args.patch_values.clone();
        for field in &table.fields {
            if !values.contains_key(&field.name) {
                if let Some(default_value) = &field.default_value {
                    values.insert(field.name.clone(), default_value.clone());
                }
            }
        }
        return Ok(values);
    }
    let mut current = if let Some(base_values) = args.base_values {
        base_values.clone()
    } else {
        effective::row_values(
            args.db,
            args.schema,
            args.table_name,
            args.row_num,
            args.branch_num,
        )?
        .ok_or_else(|| crate::Error::new(format!("row {} is not visible", args.id)))?
    };
    current.extend(args.patch_values.clone());
    Ok(current)
}

pub(super) fn insert_row_in_tx(args: InsertRowInTx<'_>) -> Result<bool> {
    let table = args.schema.table_def(args.table_name)?;
    validate_write_fields(table, args.values)?;
    let (row_num, row_id_created) = ensure_row_id_with_status(args.db, args.id)?;
    if args.op.is_create() && !row_id_created {
        if row_id_used_by_other_table(args.db, args.schema, args.table_name, row_num)? {
            return Err(crate::Error::new(format!(
                "row id {} is already used by another table",
                args.id
            )));
        }
        if row_has_current_branch_value(args.db, args.table_name, row_num, args.branch_num)? {
            return Err(crate::Error::new(format!(
                "row id {} already exists in table {}",
                args.id, args.table_name
            )));
        }
    }
    let effective_values = effective_write_values(EffectiveWriteValues {
        db: args.db,
        schema: args.schema,
        table_name: args.table_name,
        id: args.id,
        row_num,
        branch_num: args.branch_num,
        patch_values: args.values,
        op: args.op,
        base_values: args.base_values,
    })?;
    if args.op.is_create() {
        if row_id_created {
            read_set::record_tx_absent_read(args.db, args.tx_num, args.table_name, row_num)?;
        } else {
            read_set::record_tx_create_read(
                args.db,
                args.tx_num,
                args.table_name,
                row_num,
                args.branch_num,
            )?;
        }
    } else {
        read_set::record_tx_read(
            args.db,
            args.tx_num,
            args.table_name,
            row_num,
            args.branch_num,
            2,
        )?;
    }
    policy_read_set::record_for_write(policy_read_set::WritePolicyReadSet {
        conn: args.db,
        schema: args.schema,
        table,
        policy: &table.write_policy,
        values: &effective_values,
        branch_num: args.branch_num,
        tx_num: args.tx_num,
    })?;
    let allowed = args.bypass_policy
        || local_write_allowed(LocalWriteCheck {
            db: args.db,
            schema: args.schema,
            table,
            row_num,
            branch_num: args.branch_num,
            values: &effective_values,
            user: args.user,
            op: args.op,
        })?;

    let mut columns = vec![
        "row_num".to_owned(),
        "tx_num".to_owned(),
        "j_branch_num".to_owned(),
        "op".to_owned(),
    ];
    let mut sql_values = vec![
        rusqlite::types::Value::Integer(row_num),
        rusqlite::types::Value::Integer(args.tx_num),
        rusqlite::types::Value::Integer(args.branch_num),
        rusqlite::types::Value::Integer(args.op.code()),
    ];

    for field in &table.fields {
        let value = effective_values
            .get(&field.name)
            .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
        columns.push(crate::schema::quote_ident(&crate::schema::storage_column(
            field,
        )));
        sql_values.push(crate::schema::field_sql_value(
            field,
            value,
            |ref_table, row_id| ensure_row_id(args.db, ref_table, row_id),
        )?);
    }
    columns.extend([
        "j_created_at".to_owned(),
        "j_updated_at".to_owned(),
        "j_created_by".to_owned(),
        "j_updated_by".to_owned(),
    ]);
    let (created_at, created_by) = if args.op.is_create() {
        (args.now, args.user.to_owned())
    } else {
        current_creation_metadata(args.db, &table.name, row_num, args.branch_num)?
            .unwrap_or((args.now, args.user.to_owned()))
    };
    let created_by_num = users::ensure_user(args.db, &created_by)?;
    let updated_by_num = users::ensure_user(args.db, args.user)?;
    sql_values.extend([
        rusqlite::types::Value::Integer(created_at),
        rusqlite::types::Value::Integer(args.now),
        rusqlite::types::Value::Integer(created_by_num),
        rusqlite::types::Value::Integer(updated_by_num),
    ]);
    insert_dynamic(
        args.db,
        &crate::schema::history_table(&table.name),
        &columns,
        &sql_values,
    )?;
    record_tx_write(args.db, args.tx_num, &table.name, row_num, args.op)?;

    if allowed {
        let mut current_columns = vec![
            "row_num".to_owned(),
            "j_branch_num".to_owned(),
            "visible_tx_num".to_owned(),
            "is_deleted".to_owned(),
        ];
        let mut current_values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(args.branch_num),
            rusqlite::types::Value::Integer(args.tx_num),
            rusqlite::types::Value::Integer(0),
        ];
        current_columns.extend(columns.iter().skip(4).cloned());
        current_values.extend(sql_values.iter().skip(4).cloned());
        insert_dynamic(
            args.db,
            &crate::schema::current_table(&table.name),
            &current_columns,
            &current_values,
        )?;
    }
    Ok(allowed)
}

pub(super) fn stage_delete_row_in_tx(args: StageDeleteInTx<'_>) -> Result<bool> {
    let table = args.schema.table_def(args.table_name)?;
    let row_num = row_num(args.db, args.id)?;
    if matches!(args.read_set, DeleteReadSetMode::RecordPreviousRow) {
        read_set::record_tx_read(
            args.db,
            args.tx_num,
            args.table_name,
            row_num,
            args.branch_num,
            2,
        )?;
    }
    policy_read_set::record_for_write(policy_read_set::WritePolicyReadSet {
        conn: args.db,
        schema: args.schema,
        table,
        policy: &table.write_policy,
        values: args.visible_values,
        branch_num: args.branch_num,
        tx_num: args.tx_num,
    })?;
    let allowed = args.bypass_policy
        || local_write_allowed(LocalWriteCheck {
            db: args.db,
            schema: args.schema,
            table,
            row_num,
            branch_num: args.branch_num,
            values: args.visible_values,
            user: args.user,
            op: WriteOp::Delete,
        })?;

    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut insert_columns = vec![
        "row_num".to_owned(),
        "tx_num".to_owned(),
        "j_branch_num".to_owned(),
        "op".to_owned(),
    ];
    insert_columns.extend(field_columns.iter().cloned());
    insert_columns.extend([
        "j_created_at".to_owned(),
        "j_updated_at".to_owned(),
        "j_created_by".to_owned(),
        "j_updated_by".to_owned(),
    ]);
    let mut select_columns = vec![
        "row_num".to_owned(),
        "?".to_owned(),
        "j_branch_num".to_owned(),
        "3".to_owned(),
    ];
    select_columns.extend(field_columns.iter().cloned());
    select_columns.extend([
        "j_created_at".to_owned(),
        "?".to_owned(),
        "j_created_by".to_owned(),
        "?".to_owned(),
    ]);
    let user_num = users::ensure_user(args.db, args.user)?;
    let inserted = args.db.execute(
        &format!(
            "INSERT OR IGNORE INTO {} ({})
             SELECT {}
             FROM {}
             WHERE row_num = ? AND j_branch_num = ?",
            crate::schema::history_table(&table.name),
            insert_columns.join(", "),
            select_columns.join(", "),
            crate::schema::current_table(&table.name),
        ),
        params![args.tx_num, args.now, user_num, row_num, args.branch_num],
    )?;
    if inserted == 0 {
        let mut values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(args.tx_num),
            rusqlite::types::Value::Integer(args.branch_num),
            rusqlite::types::Value::Integer(3),
        ];
        for field in &table.fields {
            let value = args
                .visible_values
                .get(&field.name)
                .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
            values.push(crate::schema::field_sql_value(
                field,
                value,
                |ref_table, row_id| ensure_row_id(args.db, ref_table, row_id),
            )?);
        }
        values.extend([
            rusqlite::types::Value::Integer(args.now),
            rusqlite::types::Value::Integer(args.now),
            rusqlite::types::Value::Integer(user_num),
            rusqlite::types::Value::Integer(user_num),
        ]);
        insert_dynamic(
            args.db,
            &crate::schema::history_table(&table.name),
            &insert_columns,
            &values,
        )?;
    }
    args.db.execute(
        &format!(
            "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
            crate::schema::current_table(&table.name)
        ),
        params![row_num, args.branch_num],
    )?;
    if args.branch_num != 1 {
        let mut current_columns = vec![
            "row_num".to_owned(),
            "j_branch_num".to_owned(),
            "visible_tx_num".to_owned(),
            "is_deleted".to_owned(),
        ];
        current_columns.extend(field_columns.iter().cloned());
        current_columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);
        let mut current_values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(args.branch_num),
            rusqlite::types::Value::Integer(args.tx_num),
            rusqlite::types::Value::Integer(1),
        ];
        for field in &table.fields {
            let value = args
                .visible_values
                .get(&field.name)
                .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
            current_values.push(crate::schema::field_sql_value(
                field,
                value,
                |ref_table, row_id| ensure_row_id(args.db, ref_table, row_id),
            )?);
        }
        current_values.extend([
            rusqlite::types::Value::Integer(args.now),
            rusqlite::types::Value::Integer(args.now),
            rusqlite::types::Value::Integer(user_num),
            rusqlite::types::Value::Integer(user_num),
        ]);
        insert_dynamic(
            args.db,
            &crate::schema::current_table(&table.name),
            &current_columns,
            &current_values,
        )?;
    }
    record_tx_write(args.db, args.tx_num, &table.name, row_num, WriteOp::Delete)?;
    Ok(allowed)
}

pub(super) fn row_has_current_branch_value(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {}
             WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
            crate::schema::current_table(table_name)
        ),
        params![row_num, branch_num],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub(super) fn row_id_used_by_other_table(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_num: i64,
) -> Result<bool> {
    for table in schema.tables() {
        if table.name == table_name {
            continue;
        }
        let history_sql = format!(
            "SELECT 1 FROM {} WHERE row_num = ? LIMIT 1",
            crate::schema::history_table(&table.name)
        );
        if conn
            .query_row(&history_sql, params![row_num], |_| Ok(()))
            .optional()?
            .is_some()
        {
            return Ok(true);
        }
        let current_sql = format!(
            "SELECT 1 FROM {} WHERE row_num = ? LIMIT 1",
            crate::schema::current_table(&table.name)
        );
        if conn
            .query_row(&current_sql, params![row_num], |_| Ok(()))
            .optional()?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_write_fields(
    table: &crate::schema::TableDef,
    values: &BTreeMap<String, JsonValue>,
) -> Result<()> {
    let schema_fields = table
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    for field_name in values.keys() {
        if !schema_fields.contains(field_name.as_str()) {
            return Err(crate::Error::new(format!(
                "unknown field {} on table {}",
                field_name, table.name
            )));
        }
    }
    Ok(())
}

pub(super) struct LocalWriteCheck<'a> {
    pub(super) db: &'a Connection,
    pub(super) schema: &'a SchemaDef,
    pub(super) table: &'a crate::schema::TableDef,
    pub(super) row_num: i64,
    pub(super) branch_num: i64,
    pub(super) values: &'a BTreeMap<String, JsonValue>,
    pub(super) user: &'a str,
    pub(super) op: WriteOp,
}

pub(super) fn local_write_allowed(check: LocalWriteCheck<'_>) -> Result<bool> {
    if check.op.is_create() && matches!(check.table.write_policy, PolicyDef::CreatedByUser) {
        return Ok(true);
    }
    policy::write_allowed(policy::WriteCheck {
        db: check.db,
        schema: check.schema,
        table: check.table,
        row_num: check.row_num,
        branch_num: check.branch_num,
        values: check.values,
        user: check.user,
    })
}

fn current_creation_metadata(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<(i64, String)>> {
    let metadata = conn
        .query_row(
            &format!(
                "SELECT j_created_at, j_created_by
             FROM {}
             WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
                crate::schema::current_table(table_name)
            ),
            params![row_num, branch_num],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    metadata
        .map(|(created_at, created_by_num)| {
            users::user_id(conn, created_by_num).map(|created_by| (created_at, created_by))
        })
        .transpose()
}

pub(super) fn exclusive_write_conflict_exists(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*)
         FROM jazz_tx_write writes
         JOIN jazz_tx tx ON tx.tx_num = writes.tx_num
         WHERE writes.table_num = ?
           AND writes.row_num = ?
           AND tx.conflict_mode = ?
           AND tx.outcome = ?",
        params![
            crate::schema::table_num(conn, table_name)?,
            row_num,
            tx::MODE_EXCLUSIVE,
            tx::OUTCOME_ACCEPTED
        ],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub(super) fn record_tx_write(
    conn: &Connection,
    tx_num: i64,
    table_name: &str,
    row_num: i64,
    op: WriteOp,
) -> Result<()> {
    let table_num = crate::schema::table_num(conn, table_name)?;
    record_tx_write_num(conn, tx_num, table_num, row_num, op.code())
}

pub(super) fn record_tx_write_num(
    conn: &Connection,
    tx_num: i64,
    table_num: i64,
    row_num: i64,
    op: i64,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT OR REPLACE INTO jazz_tx_write (tx_num, table_num, row_num, op)
         VALUES (?, ?, ?, ?)",
    )?;
    stmt.execute(params![tx_num, table_num, row_num, op])?;
    Ok(())
}
