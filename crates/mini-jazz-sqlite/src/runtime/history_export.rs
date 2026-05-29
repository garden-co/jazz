use super::*;

pub(super) fn export_txs(conn: &Connection) -> Result<Vec<TxRecord>> {
    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, node.node_id, tx.local_epoch, tx.global_epoch, tx.conflict_mode, tx.outcome, rejection.code, rejection.detail_json, tx.created_at, tx.metadata_json
         FROM jazz_tx tx
         JOIN jazz_node node ON node.node_num = tx.node_num
         LEFT JOIN jazz_tx_rejection rejection ON rejection.tx_num = tx.tx_num
         ORDER BY tx.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        let tx_id = row.get::<_, String>(0)?;
        let mut receipt_stmt = conn.prepare(
            "SELECT receipt.tier
             FROM jazz_tx_receipt receipt
             JOIN jazz_tx tx ON tx.tx_num = receipt.tx_num
             WHERE tx.tx_id = ?
             ORDER BY receipt.tier",
        )?;
        let receipt_tiers = receipt_stmt
            .query_map(params![tx_id], |row| row.get::<_, i64>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(TxRecord {
            tx_id,
            node_id: row.get(1)?,
            local_epoch: row.get(2)?,
            global_epoch: row.get(3)?,
            conflict_mode: row.get(4)?,
            outcome: row.get(5)?,
            auth_user: parse_tx_auth_user_for_sqlite(&row.get::<_, String>(9)?, 9)?,
            rejection_code: row.get(6)?,
            rejection_detail: row
                .get::<_, Option<String>>(7)?
                .map(|detail_json| parse_rejection_detail_for_sqlite(&detail_json, 7))
                .transpose()?
                .flatten(),
            receipt_tiers,
            created_at: row.get(8)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(super) fn export_txs_for_query_scope(
    conn: &Connection,
    _table_name: &str,
    history: &[HistoryRecord],
    reads: &[ReadRecord],
    extra_tx_ids: &[String],
) -> Result<Vec<TxRecord>> {
    let mut needed_tx_ids = history
        .iter()
        .map(|record| record.tx_id.as_str())
        .collect::<BTreeSet<_>>();
    for tx_id in extra_tx_ids {
        needed_tx_ids.insert(tx_id.as_str());
    }
    for record in reads {
        needed_tx_ids.insert(record.tx_id.as_str());
        if let Some(observed_tx_id) = &record.observed_tx_id {
            needed_tx_ids.insert(observed_tx_id.as_str());
        }
    }
    export_txs_by_ids(conn, needed_tx_ids)
}

pub(super) fn export_txs_by_ids(
    conn: &Connection,
    tx_ids: BTreeSet<&str>,
) -> Result<Vec<TxRecord>> {
    if tx_ids.is_empty() {
        return Ok(Vec::new());
    }
    let tx_ids = tx_ids.into_iter().collect::<Vec<_>>();

    let mut receipt_stmt = conn.prepare(
        "SELECT tx.tx_id, receipt.tier
         FROM jazz_tx tx
         JOIN jazz_tx_receipt receipt ON receipt.tx_num = tx.tx_num
         ORDER BY tx.tx_num, receipt.tier",
    )?;
    let receipt_rows = receipt_stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut receipt_tiers_by_tx = BTreeMap::<String, Vec<i64>>::new();
    for receipt_row in receipt_rows {
        let (tx_id, tier) = receipt_row?;
        if tx_ids.contains(&tx_id.as_str()) {
            receipt_tiers_by_tx.entry(tx_id).or_default().push(tier);
        }
    }

    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, node.node_id, tx.local_epoch, tx.global_epoch, tx.conflict_mode, tx.outcome, rejection.code, rejection.detail_json, tx.created_at, tx.metadata_json
         FROM jazz_tx tx
         JOIN jazz_node node ON node.node_num = tx.node_num
         LEFT JOIN jazz_tx_rejection rejection ON rejection.tx_num = tx.tx_num
         ORDER BY tx.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        let tx_id = row.get::<_, String>(0)?;
        let receipt_tiers = receipt_tiers_by_tx.get(&tx_id).cloned().unwrap_or_default();
        Ok(TxRecord {
            tx_id,
            node_id: row.get(1)?,
            local_epoch: row.get(2)?,
            global_epoch: row.get(3)?,
            conflict_mode: row.get(4)?,
            outcome: row.get(5)?,
            auth_user: parse_tx_auth_user_for_sqlite(&row.get::<_, String>(9)?, 9)?,
            rejection_code: row.get(6)?,
            rejection_detail: row
                .get::<_, Option<String>>(7)?
                .map(|detail_json| parse_rejection_detail_for_sqlite(&detail_json, 7))
                .transpose()?
                .flatten(),
            receipt_tiers,
            created_at: row.get(8)?,
        })
    })?;
    let mut tx_records = Vec::new();
    for record in records {
        let record = record?;
        if tx_ids.contains(&record.tx_id.as_str()) {
            tx_records.push(record);
        }
    }
    Ok(tx_records)
}

pub(super) fn parse_rejection_detail(detail_json: &str) -> Result<Option<JsonValue>> {
    let detail = serde_json::from_str::<JsonValue>(detail_json)
        .map_err(|err| crate::Error::new(format!("invalid rejection detail JSON: {err}")))?;
    if detail.is_null() {
        Ok(None)
    } else {
        Ok(Some(detail))
    }
}

pub(super) fn parse_tx_auth_user_for_sqlite(
    metadata_json: &str,
    column: usize,
) -> rusqlite::Result<Option<String>> {
    let metadata = serde_json::from_str::<JsonValue>(metadata_json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(err),
        )
    })?;
    Ok(metadata
        .get("auth_user")
        .and_then(JsonValue::as_str)
        .map(str::to_owned))
}

pub(super) fn parse_rejection_detail_for_sqlite(
    detail_json: &str,
    column: usize,
) -> rusqlite::Result<Option<JsonValue>> {
    let detail = serde_json::from_str::<JsonValue>(detail_json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(err),
        )
    })?;
    if detail.is_null() {
        Ok(None)
    } else {
        Ok(Some(detail))
    }
}

pub(super) fn export_reads_for_history(
    conn: &Connection,
    history: &[HistoryRecord],
) -> Result<Vec<ReadRecord>> {
    let mut tx_ids = history
        .iter()
        .map(|record| record.tx_id.clone())
        .collect::<Vec<_>>();
    tx_ids.sort();
    tx_ids.dedup();
    if tx_ids.is_empty() {
        return Ok(Vec::new());
    }
    if tx_ids.len() > crate::SQL_VARIABLE_CHUNK_SIZE {
        return export_reads_for_history_with_temp_scope(conn, history);
    }
    let candidate_read_count = count_read_rows_for_tx_ids(conn, &tx_ids)?;
    if candidate_read_count <= (history.len() * 4).max(256) {
        return export_reads_for_history_simple(conn, history, &tx_ids);
    }
    export_reads_for_history_with_temp_scope(conn, history)
}

pub(super) fn export_reads_for_history_simple(
    conn: &Connection,
    history: &[HistoryRecord],
    tx_ids: &[String],
) -> Result<Vec<ReadRecord>> {
    let history_keys = history
        .iter()
        .map(|record| {
            (
                record.tx_id.as_str(),
                record.table.as_str(),
                record.row_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    let mut stmt = conn.prepare(&format!(
        "SELECT tx.tx_id, tables.table_name, ids.row_id, reads.reason, observed.tx_id
         FROM jazz_tx_read reads
         JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
         JOIN jazz_table tables ON tables.table_num = reads.table_num
         LEFT JOIN jazz_tx observed ON observed.tx_num = reads.observed_tx_num
         JOIN jazz_row_id ids ON ids.row_num = reads.row_num
         WHERE tx.tx_id IN ({placeholders})
         ORDER BY tx.tx_num, tables.table_name, ids.row_id, reads.reason",
        placeholders = (0..tx_ids.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", "),
    ))?;
    let records = stmt.query_map(params_from_iter(tx_ids.iter()), |row| {
        Ok(ReadRecord {
            tx_id: row.get(0)?,
            table: row.get(1)?,
            row_id: row.get(2)?,
            reason: row.get(3)?,
            observed_tx_id: row.get(4)?,
        })
    })?;
    let records = records
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|record| {
            record.reason != read_set::REASON_ABSENT
                || history_keys.contains(&(
                    record.tx_id.as_str(),
                    record.table.as_str(),
                    record.row_id.as_str(),
                ))
        })
        .collect();
    Ok(records)
}

pub(super) fn count_read_rows_for_tx_ids(conn: &Connection, tx_ids: &[String]) -> Result<usize> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM jazz_tx_read reads
             JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
             WHERE tx.tx_id IN ({placeholders})",
            placeholders = (0..tx_ids.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", "),
        ),
        params_from_iter(tx_ids.iter()),
        |row| row.get(0),
    )?;
    Ok(count as usize)
}

pub(super) fn export_reads_for_history_with_temp_scope(
    conn: &Connection,
    history: &[HistoryRecord],
) -> Result<Vec<ReadRecord>> {
    if history.is_empty() {
        return Ok(Vec::new());
    }
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS jazz_export_tx_scope (
           tx_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         CREATE TEMP TABLE IF NOT EXISTS jazz_export_history_scope (
           tx_id TEXT NOT NULL,
           table_name TEXT NOT NULL,
           row_id TEXT NOT NULL,
           PRIMARY KEY (tx_id, table_name, row_id)
         ) WITHOUT ROWID;
         DELETE FROM jazz_export_tx_scope;
         DELETE FROM jazz_export_history_scope;",
    )?;
    {
        let mut tx_stmt =
            conn.prepare("INSERT OR IGNORE INTO jazz_export_tx_scope (tx_id) VALUES (?)")?;
        let mut scope_stmt = conn.prepare(
            "INSERT OR IGNORE INTO jazz_export_history_scope (tx_id, table_name, row_id)
             VALUES (?, ?, ?)",
        )?;
        for record in history {
            tx_stmt.execute(params![record.tx_id])?;
            scope_stmt.execute(params![record.tx_id, record.table, record.row_id])?;
        }
    }
    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, tables.table_name, ids.row_id, reads.reason, observed.tx_id
         FROM jazz_export_tx_scope tx_scope
         JOIN jazz_tx tx ON tx.tx_id = tx_scope.tx_id
         JOIN jazz_tx_read reads ON reads.tx_num = tx.tx_num
         JOIN jazz_table tables ON tables.table_num = reads.table_num
         LEFT JOIN jazz_tx observed ON observed.tx_num = reads.observed_tx_num
         JOIN jazz_row_id ids ON ids.row_num = reads.row_num
         LEFT JOIN jazz_export_history_scope history_scope
           ON history_scope.tx_id = tx.tx_id
          AND history_scope.table_name = tables.table_name
          AND history_scope.row_id = ids.row_id
         WHERE reads.reason != ?
            OR history_scope.tx_id IS NOT NULL
         ORDER BY tx.tx_num, tables.table_name, ids.row_id, reads.reason",
    )?;
    let records = stmt.query_map(params![read_set::REASON_ABSENT], |row| {
        Ok(ReadRecord {
            tx_id: row.get(0)?,
            table: row.get(1)?,
            row_id: row.get(2)?,
            reason: row.get(3)?,
            observed_tx_id: row.get(4)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(super) fn export_branch_records_for_history(
    conn: &Connection,
    history: &[HistoryRecord],
) -> Result<Vec<BranchRecord>> {
    let mut branch_ids = history
        .iter()
        .map(|record| record.branch_id.clone())
        .collect::<Vec<_>>();
    branch_ids.sort();
    branch_ids.dedup();

    let mut records = Vec::new();
    for branch_id in branch_ids {
        let (branch_num, base_global_epoch, source_version) = conn.query_row(
            "SELECT branch_num, base_global_epoch, source_version FROM jazz_branch WHERE branch_id = ?",
            params![branch_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, i64>(2)?)),
        )?;
        let mut stmt = conn.prepare(
            "SELECT source.branch_id
             FROM jazz_branch_source branch_source
             JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
             WHERE branch_source.branch_num = ?
             ORDER BY source.branch_id",
        )?;
        let source_branch_ids = stmt
            .query_map(params![branch_num], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        records.push(BranchRecord {
            branch_id,
            base_global_epoch,
            source_branch_ids,
            source_version,
        });
    }
    Ok(records)
}

pub(super) fn include_branch_record(
    conn: &Connection,
    records: &mut Vec<BranchRecord>,
    branch_num: i64,
) -> Result<()> {
    let (branch_id, base_global_epoch, source_version) = conn.query_row(
        "SELECT branch_id, base_global_epoch, source_version FROM jazz_branch WHERE branch_num = ?",
        params![branch_num],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    )?;
    let mut stmt = conn.prepare(
        "SELECT source.branch_num, source.branch_id
         FROM jazz_branch_source branch_source
         JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
         WHERE branch_source.branch_num = ?
         ORDER BY source.branch_id",
    )?;
    let source_branches = stmt
        .query_map(params![branch_num], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let source_branch_ids = source_branches
        .iter()
        .map(|(_, branch_id)| branch_id.clone())
        .collect();
    if !records.iter().any(|record| record.branch_id == branch_id) {
        records.push(BranchRecord {
            branch_id,
            base_global_epoch,
            source_branch_ids,
            source_version,
        });
    }
    for (source_branch_num, _) in source_branches {
        include_branch_record(conn, records, source_branch_num)?;
    }
    Ok(())
}

pub(super) fn export_table_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    user: &str,
    bypass_policy: bool,
    branch_num: i64,
) -> Result<Vec<HistoryRecord>> {
    let branch_nums = branch::scope_nums(conn, branch_num)?;
    let visibility = ReadVisibility {
        conn,
        schema,
        branch_num,
        user,
        bypass_policy,
    };
    let mut records = export_visible_table_history(&visibility, table_name, &branch_nums, None)?;
    records.extend(export_deleted_table_history(
        conn,
        schema,
        table_name,
        &branch_nums,
    )?);
    records.extend(export_policy_dependency_history(
        &visibility,
        PolicyDependencyExport {
            table_name,
            policy: &schema.table_def(table_name)?.read_policy,
            branch_nums: &branch_nums,
            child_row_nums: None,
        },
    )?);
    records.extend(export_policy_dependency_history(
        &visibility,
        PolicyDependencyExport {
            table_name,
            policy: &schema.table_def(table_name)?.write_policy,
            branch_nums: &branch_nums,
            child_row_nums: None,
        },
    )?);
    if branch_num != 1 {
        if let Some(base_epoch) = branch::base_global_epoch(conn, branch_num)? {
            records.extend(export_main_base_snapshot_history(
                &visibility,
                table_name,
                base_epoch,
            )?);
        }
    }
    Ok(records)
}

pub(super) fn export_main_base_snapshot_history(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let row_nums =
        visibility.base_snapshot_row_nums_visible_in_branch(table_name, base_epoch, None)?;
    let mut records = export_history_versions_for_rows(
        conn,
        schema,
        table_name,
        Some(&row_nums),
        Some(base_epoch),
    )?;
    records.extend(export_snapshot_policy_dependency_history(
        visibility,
        table_name,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

pub(super) fn export_snapshot_policy_dependency_history(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
    child_row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(table_name)?;
    let PolicyDef::RefReadable { field } = &table.read_policy else {
        return Ok(Vec::new());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = visibility.snapshot_policy_sql(table, "h", base_epoch)?;
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let sql = format!(
        "SELECT DISTINCT h.{ref_column}
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {row_filter}
           AND h.j_branch_num = 1
           AND h.op != 3
           AND tx.outcome != {}
           AND tx.global_epoch IS NOT NULL
           AND tx.global_epoch <= {base_epoch}
           AND {policy_sql}
           AND NOT EXISTS (
             SELECT 1
             FROM {history_table} newer
             JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
             WHERE newer.row_num = h.row_num
               AND newer.j_branch_num = 1
               AND newer_tx.outcome != {}
               AND newer_tx.global_epoch IS NOT NULL
               AND newer_tx.global_epoch <= {base_epoch}
               AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
           )",
        crate::schema::history_table(table_name),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        row_filter = history_row_filter_sql("h", child_row_nums),
        history_table = crate::schema::history_table(table_name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut records = export_history_versions_for_rows(
        conn,
        schema,
        parent_table,
        Some(&row_nums),
        Some(base_epoch),
    )?;
    records.extend(export_snapshot_policy_dependency_history(
        visibility,
        parent_table,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

pub(super) fn export_snapshot_policy_dependency_history_for_query_scope(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
    child_scope: &query::LoweredQueryRowScope,
) -> Result<Vec<HistoryRecord>> {
    export_snapshot_policy_dependency_history_for_query_scope_at_depth(
        visibility,
        table_name,
        base_epoch,
        child_scope,
        0,
    )
}

pub(super) fn export_snapshot_policy_dependency_history_for_query_scope_at_depth(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
    child_scope: &query::LoweredQueryRowScope,
    depth: usize,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(table_name)?;
    let PolicyDef::RefReadable { field } = &table.read_policy else {
        return Ok(Vec::new());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = visibility.snapshot_policy_sql(table, "h", base_epoch)?;
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let child_scope_name = format!("snapshot_policy_child_scope_{depth}");
    let parent_scope_name = format!("snapshot_policy_parent_scope_{depth}");
    let mut ctes = child_scope.ctes.clone();
    ctes.push(format!(
        "{child_scope_name}(row_num) AS ({})",
        child_scope.select_sql
    ));
    ctes.push(format!(
        "{parent_scope_name}(row_num) AS (
           SELECT DISTINCT h.{ref_column}
           FROM {} h
           JOIN {child_scope_name} child_scope ON child_scope.row_num = h.row_num
           JOIN jazz_tx tx ON tx.tx_num = h.tx_num
           WHERE h.j_branch_num = 1
             AND h.op != 3
             AND tx.outcome != {}
             AND tx.global_epoch IS NOT NULL
             AND tx.global_epoch <= {base_epoch}
             AND {policy_sql}
             AND NOT EXISTS (
               SELECT 1
               FROM {history_table} newer
               JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
               WHERE newer.row_num = h.row_num
                 AND newer.j_branch_num = 1
                 AND newer_tx.outcome != {}
                 AND newer_tx.global_epoch IS NOT NULL
                 AND newer_tx.global_epoch <= {base_epoch}
                 AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
             )
         )",
        crate::schema::history_table(table_name),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        history_table = crate::schema::history_table(table_name),
    ));
    let parent_scope = query::LoweredQueryRowScope {
        ctes,
        select_sql: format!("SELECT row_num FROM {parent_scope_name} WHERE row_num IS NOT NULL"),
        params: child_scope.params.clone(),
    };
    let mut records = export_history_versions_for_query_scope(
        conn,
        schema,
        parent_table,
        &parent_scope,
        Some(base_epoch),
    )?;
    records.extend(
        export_snapshot_policy_dependency_history_for_query_scope_at_depth(
            visibility,
            parent_table,
            base_epoch,
            &parent_scope,
            depth + 1,
        )?,
    );
    Ok(records)
}

pub(super) struct PolicyDependencyExport<'a> {
    pub(super) table_name: &'a str,
    pub(super) policy: &'a PolicyDef,
    pub(super) branch_nums: &'a [i64],
    pub(super) child_row_nums: Option<&'a [i64]>,
}

pub(super) struct PolicyDependencyQueryScopeExport<'a> {
    pub(super) table_name: &'a str,
    pub(super) policy: &'a PolicyDef,
    pub(super) branch_nums: &'a [i64],
    pub(super) child_scope: &'a query::LoweredQueryRowScope,
}

pub(super) fn export_policy_dependency_history(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyExport<'_>,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(args.table_name)?;
    let branch_policy_records =
        export_branch_policy_dependency_history(visibility, table, args.branch_nums)?;
    let PolicyDef::RefReadable { field } = args.policy else {
        return Ok(branch_policy_records);
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = if args.child_row_nums.is_some() {
        "1 = 1".to_owned()
    } else {
        visibility.current_policy_sql(table, "current")?
    };
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let row_nums = if let Some(child_row_nums) = args.child_row_nums {
        scoped_policy_parent_row_nums(
            conn,
            args.table_name,
            &ref_column,
            args.branch_nums,
            child_row_nums,
        )?
    } else {
        let sql = format!(
            "SELECT DISTINCT current.{ref_column}
             FROM {} current
             JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
             WHERE current.is_deleted = 0
               AND {}
               AND current_tx.outcome != {}
               AND {policy_sql}",
            crate::schema::current_table(args.table_name),
            branch_filter_sql("current", args.branch_nums),
            tx::OUTCOME_REJECTED,
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, i64>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows
    };
    let mut records = if args.child_row_nums.is_some() {
        export_history_versions_for_rows(conn, schema, parent_table, Some(&row_nums), None)?
    } else {
        export_visible_table_history(visibility, parent_table, args.branch_nums, Some(&row_nums))?
    };
    records.extend(branch_policy_records);
    records.extend(export_policy_dependency_history(
        visibility,
        PolicyDependencyExport {
            table_name: parent_table,
            policy: &schema.table_def(parent_table)?.read_policy,
            branch_nums: args.branch_nums,
            child_row_nums: Some(&row_nums),
        },
    )?);
    Ok(records)
}

pub(super) fn export_policy_dependency_history_for_query_scope(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyQueryScopeExport<'_>,
) -> Result<Vec<HistoryRecord>> {
    export_policy_dependency_history_for_query_scope_at_depth(visibility, args, 0)
}

pub(super) fn export_policy_dependency_history_for_query_scope_at_depth(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyQueryScopeExport<'_>,
    depth: usize,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(args.table_name)?;
    let branch_policy_records =
        export_branch_policy_dependency_history(visibility, table, args.branch_nums)?;
    let PolicyDef::RefReadable { field } = args.policy else {
        return Ok(branch_policy_records);
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let child_scope_name = format!("policy_child_scope_{depth}");
    let parent_scope_name = format!("policy_parent_scope_{depth}");
    let mut ctes = args.child_scope.ctes.clone();
    ctes.push(format!(
        "{child_scope_name}(row_num) AS ({})",
        args.child_scope.select_sql
    ));
    ctes.push(format!(
        "{parent_scope_name}(row_num) AS (
           SELECT DISTINCT current.{ref_column}
           FROM {} current
           JOIN {child_scope_name} child_scope ON child_scope.row_num = current.row_num
           JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
           WHERE current.is_deleted = 0
             AND {}
             AND current_tx.outcome != {}
         )",
        crate::schema::current_table(args.table_name),
        branch_filter_sql("current", args.branch_nums),
        tx::OUTCOME_REJECTED,
    ));
    let parent_scope = query::LoweredQueryRowScope {
        ctes,
        select_sql: format!("SELECT row_num FROM {parent_scope_name} WHERE row_num IS NOT NULL"),
        params: args.child_scope.params.clone(),
    };
    let mut records =
        export_history_versions_for_query_scope(conn, schema, parent_table, &parent_scope, None)?;
    records.extend(branch_policy_records);
    records.extend(export_policy_dependency_history_for_query_scope_at_depth(
        visibility,
        PolicyDependencyQueryScopeExport {
            table_name: parent_table,
            policy: &schema.table_def(parent_table)?.read_policy,
            branch_nums: args.branch_nums,
            child_scope: &parent_scope,
        },
        depth + 1,
    )?);
    Ok(records)
}

pub(super) fn export_branch_policy_dependency_history(
    visibility: &ReadVisibility<'_>,
    table: &crate::schema::TableDef,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    if table.branch_policies.is_empty() || branch_nums.is_empty() {
        return Ok(Vec::new());
    }
    let conn = visibility.conn;
    let mut records = Vec::new();
    let main_visibility = ReadVisibility {
        conn: visibility.conn,
        schema: visibility.schema,
        branch_num: 1,
        user: visibility.user,
        bypass_policy: visibility.bypass_policy,
    };
    for branch_table_name in table.branch_policies.keys() {
        let row_nums = branch_backing_row_nums(conn, branch_nums)?;
        if row_nums.is_empty() {
            continue;
        }
        records.extend(export_visible_table_history(
            &main_visibility,
            branch_table_name,
            &[1],
            Some(&row_nums),
        )?);
    }
    dedupe_history_records(&mut records);
    Ok(records)
}

pub(super) fn branch_backing_row_nums(conn: &Connection, branch_nums: &[i64]) -> Result<Vec<i64>> {
    let branch_nums = sorted_unique_row_nums(branch_nums);
    let mut row_nums = BTreeSet::new();
    for chunk in branch_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let placeholders = sql_placeholders(chunk.len());
        let mut stmt = conn.prepare(&format!(
            "SELECT ids.row_num
             FROM jazz_branch branch
             JOIN jazz_row_id ids ON ids.row_id = branch.branch_id
             WHERE branch.branch_num IN ({placeholders})
             ORDER BY ids.row_num"
        ))?;
        let rows = stmt.query_map(params_from_iter(chunk.iter()), |row| row.get::<_, i64>(0))?;
        for row_num in rows {
            row_nums.insert(row_num?);
        }
    }
    Ok(row_nums.into_iter().collect())
}

pub(super) fn scoped_policy_parent_row_nums(
    conn: &Connection,
    table_name: &str,
    ref_column: &str,
    branch_nums: &[i64],
    child_row_nums: &[i64],
) -> Result<Vec<i64>> {
    if child_row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let mut parent_row_nums = BTreeSet::new();
    let child_row_nums = sorted_unique_row_nums(child_row_nums);
    for child_chunk in child_row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let child_placeholders = sql_placeholders(child_chunk.len());
        let mut stmt = conn.prepare(&format!(
            "SELECT current.{ref_column}
             FROM {} current
             JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
             WHERE current.row_num IN ({child_placeholders})
               AND current.is_deleted = 0
               AND {}
               AND current_tx.outcome != ?",
            crate::schema::current_table(table_name),
            branch_filter_sql("current", branch_nums),
        ))?;
        let mut params = child_chunk
            .iter()
            .copied()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| row.get::<_, i64>(0))?;
        for row in rows {
            parent_row_nums.insert(row?);
        }
    }
    Ok(parent_row_nums.into_iter().collect())
}

pub(super) fn export_deleted_table_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    let sql = format!(
        "SELECT h.row_num
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE h.op = 3
           AND {}
           AND tx.outcome != {}
           AND NOT EXISTS (
             SELECT 1
             FROM {history_table} newer
             JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
             WHERE newer.row_num = h.row_num
               AND newer.j_branch_num = h.j_branch_num
               AND newer_tx.outcome != {}
               AND newer.tx_num > h.tx_num
           )",
        crate::schema::history_table(table_name),
        branch_filter_sql("h", branch_nums),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        history_table = crate::schema::history_table(table_name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

pub(super) fn export_deleted_recursive_descendant_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    parent_field: &str,
    branch_nums: &[i64],
    parent_row_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    if parent_row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let parent_row_nums = sorted_unique_row_nums(parent_row_nums);
    let table = schema.table_def(table_name)?;
    let field = table
        .fields
        .iter()
        .find(|field| field.name == parent_field)
        .ok_or_else(|| crate::Error::new(format!("unknown ref field {parent_field}")))?;
    let parent_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let mut row_nums = BTreeSet::new();
    for parent_chunk in parent_row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let sql = format!(
            "WITH RECURSIVE deleted_tree(row_num) AS (
               SELECT h.row_num
               FROM {history_table} h
               JOIN jazz_tx tx ON tx.tx_num = h.tx_num
               WHERE h.op = 3
                 AND {branch_filter}
                 AND h.{parent_column} IN ({parent_placeholders})
                 AND tx.outcome != {rejected}
                 AND NOT EXISTS (
                   SELECT 1
                   FROM {history_table} newer
                   JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                   WHERE newer.row_num = h.row_num
                     AND newer.j_branch_num = h.j_branch_num
                     AND newer_tx.outcome != {rejected}
                     AND newer.tx_num > h.tx_num
                 )
               UNION
               SELECT child.row_num
               FROM {history_table} child
               JOIN jazz_tx child_tx ON child_tx.tx_num = child.tx_num
               JOIN deleted_tree parent ON child.{parent_column} = parent.row_num
               WHERE child.op = 3
                 AND {child_branch_filter}
                 AND child_tx.outcome != {rejected}
                 AND NOT EXISTS (
                   SELECT 1
                   FROM {history_table} newer
                   JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                   WHERE newer.row_num = child.row_num
                     AND newer.j_branch_num = child.j_branch_num
                     AND newer_tx.outcome != {rejected}
                     AND newer.tx_num > child.tx_num
                 )
             )
             SELECT row_num FROM deleted_tree",
            history_table = crate::schema::history_table(table_name),
            branch_filter = branch_filter_sql("h", branch_nums),
            child_branch_filter = branch_filter_sql("child", branch_nums),
            rejected = tx::OUTCOME_REJECTED,
            parent_placeholders = sql_placeholders(parent_chunk.len()),
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(parent_chunk.iter()), |row| {
            row.get::<_, i64>(0)
        })?;
        for row in rows {
            row_nums.insert(row?);
        }
    }
    let row_nums = row_nums.into_iter().collect::<Vec<_>>();
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

pub(super) fn export_recursive_scope_repair_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    parent_field: &str,
    branch_nums: &[i64],
    parent_row_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    if parent_row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let parent_row_nums = sorted_unique_row_nums(parent_row_nums);
    let table = schema.table_def(table_name)?;
    let field = table
        .fields
        .iter()
        .find(|field| field.name == parent_field)
        .ok_or_else(|| crate::Error::new(format!("unknown ref field {parent_field}")))?;
    let parent_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let mut row_nums = BTreeSet::new();
    for parent_chunk in parent_row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let sql = format!(
            "WITH RECURSIVE historical_tree(row_num) AS (
               SELECT h.row_num
               FROM {history_table} h
               JOIN jazz_tx tx ON tx.tx_num = h.tx_num
               WHERE {branch_filter}
                 AND h.{parent_column} IN ({parent_placeholders})
                 AND tx.outcome != {rejected}
               UNION
               SELECT child.row_num
               FROM {history_table} child
               JOIN jazz_tx child_tx ON child_tx.tx_num = child.tx_num
               JOIN historical_tree parent ON child.{parent_column} = parent.row_num
               WHERE {child_branch_filter}
                 AND child_tx.outcome != {rejected}
             )
             SELECT DISTINCT row_num FROM historical_tree",
            history_table = crate::schema::history_table(table_name),
            branch_filter = branch_filter_sql("h", branch_nums),
            child_branch_filter = branch_filter_sql("child", branch_nums),
            rejected = tx::OUTCOME_REJECTED,
            parent_placeholders = sql_placeholders(parent_chunk.len()),
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(parent_chunk.iter()), |row| {
            row.get::<_, i64>(0)
        })?;
        for row in rows {
            row_nums.insert(row?);
        }
    }
    let row_nums = row_nums.into_iter().collect::<Vec<_>>();
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

pub(super) fn query_scope_repair_row_nums(
    conn: &Connection,
    table: &crate::schema::TableDef,
    field_name: &str,
    op: &str,
    value: &JsonValue,
) -> Result<Vec<i64>> {
    // Return the physical rows whose history can affect a query-scope refresh.
    // These are not necessarily current result rows.
    //
    // Predicate repair:
    //
    //   history rows ever matching predicate
    //        |
    //        v
    //   exported repair history
    //        |
    //        v
    //   receiver deletes stale current rows no longer justified by history
    //
    // `id` is special because the row id lives in `jazz_row_id`, not the user
    // table history. `$createdBy` is special because it is a system column on
    // history/current tables. User fields lower through `query_predicate`.
    if op == "eq_top_created_at_desc" || op == "eq_top_field_desc" {
        let observed_ids = observed_ids_from_query_value(value)?;
        if observed_ids.is_empty() {
            let value = value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top query expects eq value"))?;
            return query_scope_repair_row_nums(conn, table, field_name, "eq", value);
        }
        return observed_ids
            .into_iter()
            .map(|row_id| row_num(conn, &row_id))
            .collect();
    }
    if field_name == "id" {
        if op == "ne" {
            let excluded_id = value
                .as_str()
                .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
            let mut stmt = conn.prepare(&format!(
                "SELECT DISTINCT h.row_num
                 FROM {} h
                 JOIN jazz_row_id ids ON ids.row_num = h.row_num
                 WHERE ids.row_id != ?
                 ORDER BY h.row_num",
                crate::schema::history_table(&table.name)
            ))?;
            let rows = stmt.query_map(params![excluded_id], |row| row.get(0))?;
            return rows
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into);
        }
        return id_predicate_values(op, value)?
            .into_iter()
            .map(|row_id| ensure_row_id(conn, &table.name, &row_id))
            .collect();
    }
    if field_name == "$createdBy" {
        let Some(created_by) = value.as_str() else {
            return Err(crate::Error::new(
                "$createdBy predicate expects a string value",
            ));
        };
        let created_by_num = match users::user_num(conn, created_by) {
            Ok(user_num) => user_num,
            Err(_) if op == "eq" => return Ok(Vec::new()),
            Err(_) => -1,
        };
        let created_by_sql = match op {
            "eq" => "h.j_created_by = ?",
            "ne" => "h.j_created_by != ?",
            op => {
                return Err(crate::Error::new(format!(
                    "unsupported $createdBy predicate op {op}"
                )));
            }
        };
        let sql = format!(
            "SELECT DISTINCT h.row_num
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE {created_by_sql}
               AND tx.outcome != ?",
            crate::schema::history_table(&table.name),
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![created_by_num, tx::OUTCOME_REJECTED], |row| {
            row.get::<_, i64>(0)
        })?;
        return rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into);
    }
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
        .ok_or_else(|| crate::Error::new(format!("unknown query field {field_name}")))?;
    if op == "in" {
        let mut row_nums = Vec::new();
        for value in value
            .as_array()
            .ok_or_else(|| crate::Error::new("in predicate expects an array value"))?
        {
            row_nums.extend(query_scope_repair_row_nums(
                conn, table, field_name, "eq", value,
            )?);
        }
        row_nums.sort();
        row_nums.dedup();
        return Ok(row_nums);
    }
    let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let predicate_sql = query_predicate::sql(field, &format!("h.{predicate_column}"), op)?;
    let predicate_value = query_predicate::value(field, op, value, conn)?;
    let sql = format!(
        "SELECT DISTINCT h.row_num
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {predicate_sql}
           AND tx.outcome != ?",
        crate::schema::history_table(&table.name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![predicate_value, tx::OUTCOME_REJECTED], |row| {
        row.get::<_, i64>(0)
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(super) fn query_scope_rejected_tx_ids(
    conn: &Connection,
    table: &crate::schema::TableDef,
    field_name: &str,
    op: &str,
    value: &JsonValue,
) -> Result<Vec<String>> {
    if op == "eq_top_created_at_desc" || op == "eq_top_field_desc" {
        let observed_ids = observed_ids_from_query_value(value)?;
        if observed_ids.is_empty() {
            let value = value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top query expects eq value"))?;
            return query_scope_rejected_tx_ids(conn, table, field_name, "eq", value);
        }
        let row_nums = observed_ids
            .into_iter()
            .map(|row_id| row_num(conn, &row_id))
            .collect::<Result<Vec<_>>>()?;
        return rejected_tx_ids_for_row_nums(conn, &table.name, &row_nums);
    }
    if op == "in" {
        let mut tx_ids = Vec::new();
        for value in value
            .as_array()
            .ok_or_else(|| crate::Error::new("in predicate expects an array value"))?
        {
            tx_ids.extend(query_scope_rejected_tx_ids(
                conn, table, field_name, "eq", value,
            )?);
        }
        tx_ids.sort();
        tx_ids.dedup();
        return Ok(tx_ids);
    }
    if field_name == "id" {
        if op == "ne" {
            let excluded_id = value
                .as_str()
                .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
            let sql = format!(
                "SELECT DISTINCT tx.tx_id
                 FROM {} h
                 JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                 JOIN jazz_row_id ids ON ids.row_num = h.row_num
                 WHERE ids.row_id != ?
                   AND tx.outcome = ?
                 ORDER BY tx.tx_num",
                crate::schema::history_table(&table.name),
            );
            let mut stmt = conn.prepare(&sql)?;
            let tx_ids = stmt.query_map(params![excluded_id, tx::OUTCOME_REJECTED], |row| {
                row.get::<_, String>(0)
            })?;
            return tx_ids
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into);
        }
        let row_nums = id_predicate_values(op, value)?
            .into_iter()
            .map(|row_id| ensure_row_id(conn, &table.name, &row_id))
            .collect::<Result<Vec<_>>>()?;
        return rejected_tx_ids_for_row_nums(conn, &table.name, &row_nums);
    }
    if field_name == "$createdBy" {
        let created_by = value
            .as_str()
            .ok_or_else(|| crate::Error::new("$createdBy predicate expects a string value"))?;
        let created_by_num = match users::user_num(conn, created_by) {
            Ok(user_num) => user_num,
            Err(_) if op == "eq" => return Ok(Vec::new()),
            Err(_) => -1,
        };
        let created_by_sql = match op {
            "eq" => "h.j_created_by = ?",
            "ne" => "h.j_created_by != ?",
            op => {
                return Err(crate::Error::new(format!(
                    "unsupported $createdBy predicate op {op}"
                )));
            }
        };
        let sql = format!(
            "SELECT DISTINCT tx.tx_id
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE {created_by_sql}
               AND tx.outcome = ?
             ORDER BY tx.tx_num",
            crate::schema::history_table(&table.name),
        );
        let mut stmt = conn.prepare(&sql)?;
        let tx_ids = stmt.query_map(params![created_by_num, tx::OUTCOME_REJECTED], |row| {
            row.get::<_, String>(0)
        })?;
        return tx_ids
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into);
    }
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
        .ok_or_else(|| crate::Error::new(format!("unknown query field {field_name}")))?;
    let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let predicate_sql = query_predicate::sql(field, &format!("h.{predicate_column}"), op)?;
    let predicate_value = query_predicate::value(field, op, value, conn)?;
    let sql = format!(
        "SELECT DISTINCT tx.tx_id
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {predicate_sql}
           AND tx.outcome = ?
         ORDER BY tx.tx_num",
        crate::schema::history_table(&table.name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let tx_ids = stmt.query_map(params![predicate_value, tx::OUTCOME_REJECTED], |row| {
        row.get::<_, String>(0)
    })?;
    tx_ids
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(super) fn rejected_tx_ids_for_row_nums(
    conn: &Connection,
    table_name: &str,
    row_nums: &[i64],
) -> Result<Vec<String>> {
    if row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT DISTINCT tx.tx_id
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {}
           AND tx.outcome = ?
         ORDER BY tx.tx_num",
        crate::schema::history_table(table_name),
        history_row_filter_sql("h", Some(row_nums)),
    );
    let mut stmt = conn.prepare(&sql)?;
    let tx_ids = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| row.get::<_, String>(0))?;
    tx_ids
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(super) fn query_scope_repair_row_nums_for_read(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    query_read: &QueryReadRecord,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<i64>> {
    // Dispatch from the serialized query-read shape used in bundles to the row
    // collection shape used by export. Built queries are stored opaquely, so
    // they need a small adapter before repair candidates can be collected.
    if query_read.op == "query" {
        let query = built_query_from_read(query_read)?;
        return query_scope_repair_row_nums_for_built_query(
            conn,
            schema,
            table,
            &query,
            branch_num,
            user,
            bypass_policy,
        );
    }
    query_scope_repair_row_nums(
        conn,
        table,
        &query_read.field,
        &query_read.op,
        &query_read.value,
    )
}

pub(super) fn query_scope_repair_row_nums_for_built_query(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    built_query: &BuiltQuery,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<i64>> {
    // Built-query repair row collection mirrors `apply_built_query_scope_repair`:
    //
    //   built query
    //       |
    //       +-- one predicate ------------------------+
    //       |                                         v
    //       +-- eq + createdAt desc + limit --> predicate repair rows
    //       |
    //       +-- other SQL-lowered shape ------> SQL-lowered history scope
    //
    // Generic built-query repair asks SQLite for rows whose history matched the
    // query conditions. Export then sends those row histories so peers learn
    // about rows that left a multi-filter or custom-ordered query.
    if built_query.table != table.name {
        return Err(crate::Error::new(
            "query read table does not match descriptor",
        ));
    }
    let context = query::QueryContext {
        conn,
        schema,
        branch_num,
        user,
        bypass_policy,
        read_tier: ReadTier::Local,
    };
    context.repair_row_nums_for_built_query(built_query)
}

pub(super) fn query_scope_rejected_tx_ids_for_read(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    query_read: &QueryReadRecord,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<String>> {
    if query_read.op == "query" {
        let query = built_query_from_read(query_read)?;
        return query_scope_rejected_tx_ids_for_built_query(
            conn,
            schema,
            table,
            &query,
            branch_num,
            user,
            bypass_policy,
        );
    }
    query_scope_rejected_tx_ids(
        conn,
        table,
        &query_read.field,
        &query_read.op,
        &query_read.value,
    )
}

pub(super) fn query_scope_rejected_tx_ids_for_built_query(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    built_query: &BuiltQuery,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<String>> {
    if built_query.table != table.name {
        return Err(crate::Error::new(
            "query read table does not match descriptor",
        ));
    }
    let context = query::QueryContext {
        conn,
        schema,
        branch_num,
        user,
        bypass_policy,
        read_tier: ReadTier::Local,
    };
    let row_nums = context.repair_row_nums_for_built_query(built_query)?;
    rejected_tx_ids_for_row_nums(conn, &built_query.table, &row_nums)
}

pub(super) enum BuiltQueryRepairScope<'a> {
    Predicate(&'a QueryCondition),
    Generic,
}

pub(super) fn built_query_repair_scope(query: &BuiltQuery) -> Result<BuiltQueryRepairScope<'_>> {
    if query.conditions.len() == 1 && query.offset.unwrap_or(0) == 0 {
        let condition = &query.conditions[0];
        match (query.order_by.as_slice(), query.limit) {
            ([], None) if legacy_predicate_repair_supports(condition) => {
                return Ok(BuiltQueryRepairScope::Predicate(condition));
            }
            _ => {}
        }
    }
    Ok(BuiltQueryRepairScope::Generic)
}

pub(super) fn legacy_predicate_repair_supports(condition: &QueryCondition) -> bool {
    match condition.column.as_str() {
        "id" => matches!(
            condition.op,
            QueryConditionOp::Eq | QueryConditionOp::Ne | QueryConditionOp::In
        ),
        "$createdBy" => matches!(condition.op, QueryConditionOp::Eq | QueryConditionOp::Ne),
        "$createdAt" | "$updatedAt" => false,
        _ => !query_condition_value_contains_null(&condition.value),
    }
}

pub(super) fn query_condition_value_contains_null(value: &JsonValue) -> bool {
    value.is_null()
        || value
            .as_array()
            .is_some_and(|values| values.iter().any(JsonValue::is_null))
}

pub(super) fn built_query_repair_keep_query(query: &BuiltQuery) -> Result<BuiltQuery> {
    let offset = query.offset.unwrap_or(0);
    if offset == 0 {
        return Ok(query.clone());
    }

    let mut keep_query = query.clone();
    keep_query.offset = None;
    keep_query.limit = query
        .limit
        .map(|limit| {
            offset
                .checked_add(limit)
                .ok_or_else(|| crate::Error::new("query limit plus offset is too large"))
        })
        .transpose()?;
    Ok(keep_query)
}

pub(super) fn delete_current_rows_outside_keep_set(
    db: &Connection,
    table_name: &str,
    branch_num: i64,
    scope_row_nums: &[i64],
    keep_row_nums: &[i64],
) -> Result<()> {
    if scope_row_nums.is_empty() {
        return Ok(());
    }

    // Generic window repair is a contraction pass:
    //
    //   rows matching query filters, without LIMIT/OFFSET
    //             |
    //             v
    //   +------------------+        +----------------------+
    //   | scope row nums   |  minus | rows to keep locally |
    //   +------------------+        +----------------------+
    //             |
    //             v
    //   DELETE stale current rows from the observed branch
    //
    // For offset queries, "keep" is the exported support window
    // [0, offset + limit). Those prefix rows must stay local so SQLite can
    // still evaluate the original OFFSET query correctly after the refresh.
    let keep_row_nums = keep_row_nums.iter().copied().collect::<BTreeSet<_>>();
    let delete_row_nums = scope_row_nums
        .iter()
        .copied()
        .filter(|row_num| !keep_row_nums.contains(row_num))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for delete_chunk in delete_row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
        let sql = format!(
            "DELETE FROM {}
             WHERE j_branch_num = ?
               AND is_deleted = 0
               AND row_num IN ({})",
            crate::schema::current_table(table_name),
            sql_placeholders(delete_chunk.len()),
        );
        let mut params = Vec::with_capacity(1 + delete_chunk.len());
        params.push(rusqlite::types::Value::Integer(branch_num));
        params.extend(
            delete_chunk
                .iter()
                .copied()
                .map(rusqlite::types::Value::Integer),
        );
        db.execute(&sql, params_from_iter(params.iter()))?;
    }
    Ok(())
}

pub(super) fn id_predicate_values(op: &str, value: &JsonValue) -> Result<Vec<String>> {
    match op {
        "eq" => value
            .as_str()
            .map(|row_id| vec![row_id.to_owned()])
            .ok_or_else(|| crate::Error::new("id equality expects a string value")),
        "in" => value
            .as_array()
            .ok_or_else(|| crate::Error::new("id in expects an array value"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| crate::Error::new("id in expects string values"))
            })
            .collect(),
        _ => Err(crate::Error::new(format!("unsupported id predicate {op}"))),
    }
}

pub(super) fn dedupe_history_records(records: &mut Vec<HistoryRecord>) {
    let mut seen = BTreeSet::new();
    records.retain(|record| {
        seen.insert((
            record.table.clone(),
            record.row_id.clone(),
            record.branch_id.clone(),
            record.tx_id.clone(),
            record.op,
        ))
    });
}

pub(super) fn export_visible_table_history(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    branch_nums: &[i64],
    row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    if let Some(row_nums) = row_nums {
        if row_nums.is_empty() {
            return Ok(Vec::new());
        }
        if row_nums.len() > crate::SQL_VARIABLE_CHUNK_SIZE {
            let row_nums = sorted_unique_row_nums(row_nums);
            let mut records = Vec::new();
            for row_chunk in row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
                records.extend(export_visible_table_history(
                    visibility,
                    table_name,
                    branch_nums,
                    Some(row_chunk),
                )?);
            }
            return Ok(records);
        }
    }
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(table_name)?;
    let policy_sql = visibility.current_policy_sql(table, "current")?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
        "h.op".to_owned(),
    ];
    select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
    select_columns.extend([
        "h.j_created_at".to_owned(),
        "h.j_updated_at".to_owned(),
        format!(
            "{} AS j_created_by",
            users::user_id_expr("h", "j_created_by")
        ),
        format!(
            "{} AS j_updated_by",
            users::user_id_expr("h", "j_updated_by")
        ),
    ]);
    let sql = format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {row_filter}
           AND EXISTS (
           SELECT 1
           FROM {} current
           JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
           WHERE current.row_num = h.row_num
             AND current.j_branch_num = h.j_branch_num
             AND current.is_deleted = 0
             AND {}
             AND current_tx.outcome != {}
             AND {policy_sql}
         )
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        crate::schema::current_table(table_name),
        branch_filter_sql("current", branch_nums),
        tx::OUTCOME_REJECTED,
        row_filter = row_filter_sql(row_nums),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut records = Vec::new();
    let mut rows = match row_nums {
        Some(row_nums) => stmt.query(params_from_iter(row_nums.iter()))?,
        None => stmt.query([])?,
    };
    let mut public_row_id_cache = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json_cached(conn, field, &row[idx + 4], &mut public_row_id_cache)?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

pub(super) fn export_history_versions_for_rows(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_rows_with_branch_filter(
        conn,
        schema,
        table_name,
        row_nums,
        max_global_epoch,
        None,
    )
}

pub(super) fn export_history_versions_for_rows_in_branches(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_rows_with_branch_filter(
        conn,
        schema,
        table_name,
        row_nums,
        max_global_epoch,
        Some(branch_nums),
    )
}

pub(super) fn export_history_versions_for_query_scope(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_scope: &query::LoweredQueryRowScope,
    max_global_epoch: Option<i64>,
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_query_scope_with_branch_filter(
        conn,
        schema,
        table_name,
        row_scope,
        max_global_epoch,
        None,
    )
}

pub(super) fn export_history_versions_for_query_scope_in_branches(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_scope: &query::LoweredQueryRowScope,
    max_global_epoch: Option<i64>,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_query_scope_with_branch_filter(
        conn,
        schema,
        table_name,
        row_scope,
        max_global_epoch,
        Some(branch_nums),
    )
}

pub(super) fn export_history_versions_for_query_scope_with_branch_filter(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_scope: &query::LoweredQueryRowScope,
    max_global_epoch: Option<i64>,
    branch_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
        "h.op".to_owned(),
    ];
    select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
    select_columns.extend([
        "h.j_created_at".to_owned(),
        "h.j_updated_at".to_owned(),
        format!(
            "{} AS j_created_by",
            users::user_id_expr("h", "j_created_by")
        ),
        format!(
            "{} AS j_updated_by",
            users::user_id_expr("h", "j_updated_by")
        ),
    ]);
    let sql = format!(
        "{}
         SELECT {}
         FROM {} h
         JOIN query_scope scope ON scope.row_num = h.row_num
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {branch_filter}
           AND {epoch_filter}
         ORDER BY h.row_num, h.tx_num",
        row_scope.with_scope_cte("query_scope"),
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        branch_filter = history_branch_filter_sql("h", branch_nums),
        epoch_filter = history_epoch_filter_sql(max_global_epoch),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut rows = stmt.query(params_from_iter(row_scope.params.iter()))?;
    let mut records = Vec::new();
    let mut public_row_id_cache = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json_cached(conn, field, &row[idx + 4], &mut public_row_id_cache)?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

pub(super) fn export_history_versions_for_rows_with_branch_filter(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
    branch_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    if let Some(row_nums) = row_nums {
        if row_nums.is_empty() {
            return Ok(Vec::new());
        }
        if row_nums.len() > crate::SQL_VARIABLE_CHUNK_SIZE {
            let row_nums = sorted_unique_row_nums(row_nums);
            let mut records = Vec::new();
            for row_chunk in row_nums.chunks(crate::SQL_VARIABLE_CHUNK_SIZE) {
                records.extend(export_history_versions_for_rows_with_branch_filter(
                    conn,
                    schema,
                    table_name,
                    Some(row_chunk),
                    max_global_epoch,
                    branch_nums,
                )?);
            }
            return Ok(records);
        }
    }
    let table = schema.table_def(table_name)?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
        "h.op".to_owned(),
    ];
    select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
    select_columns.extend([
        "h.j_created_at".to_owned(),
        "h.j_updated_at".to_owned(),
        format!(
            "{} AS j_created_by",
            users::user_id_expr("h", "j_created_by")
        ),
        format!(
            "{} AS j_updated_by",
            users::user_id_expr("h", "j_updated_by")
        ),
    ]);
    let sql = format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {row_filter}
           AND {branch_filter}
           AND {epoch_filter}
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        row_filter = row_filter_sql(row_nums),
        branch_filter = history_branch_filter_sql("h", branch_nums),
        epoch_filter = history_epoch_filter_sql(max_global_epoch),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut rows = match row_nums {
        Some(row_nums) => stmt.query(params_from_iter(row_nums.iter()))?,
        None => stmt.query([])?,
    };
    let mut records = Vec::new();
    let mut public_row_id_cache = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json_cached(conn, field, &row[idx + 4], &mut public_row_id_cache)?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

pub(super) fn history_epoch_filter_sql(max_global_epoch: Option<i64>) -> String {
    match max_global_epoch {
        Some(epoch) => format!("tx.global_epoch IS NOT NULL AND tx.global_epoch <= {epoch}"),
        None => "1 = 1".to_owned(),
    }
}

pub(super) fn row_filter_sql(row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!("h.row_num IN ({})", sql_placeholders(row_nums.len())),
        None => "1 = 1".to_owned(),
    }
}

pub(super) fn history_row_filter_sql(alias: &str, row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!(
            "{alias}.row_num IN ({})",
            row_nums
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}

pub(super) fn history_branch_filter_sql(alias: &str, branch_nums: Option<&[i64]>) -> String {
    match branch_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(branch_nums) => format!(
            "{alias}.j_branch_num IN ({})",
            branch_nums
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}

pub(super) fn branch_filter_sql(alias: &str, branch_nums: &[i64]) -> String {
    if branch_nums.is_empty() {
        return "0 = 1".to_owned();
    }
    format!(
        "{alias}.j_branch_num IN ({})",
        branch_nums
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub(super) fn sorted_unique_row_nums(row_nums: &[i64]) -> Vec<i64> {
    let mut row_nums = row_nums.to_vec();
    row_nums.sort();
    row_nums.dedup();
    row_nums
}

pub(super) fn sql_placeholders(count: usize) -> String {
    (0..count).map(|_| "?").collect::<Vec<_>>().join(", ")
}

pub(super) fn sql_value_to_json(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
) -> Result<JsonValue> {
    let mut public_row_id_cache = BTreeMap::new();
    sql_value_to_json_cached(conn, field, value, &mut public_row_id_cache)
}

pub(super) fn sql_value_to_json_cached(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
    public_row_id_cache: &mut BTreeMap<i64, String>,
) -> Result<JsonValue> {
    match (&field.kind, value) {
        (_, rusqlite::types::Value::Null) if field.nullable => Ok(JsonValue::Null),
        (FieldKind::Text, rusqlite::types::Value::Text(value)) => {
            Ok(JsonValue::String(value.clone()))
        }
        (FieldKind::Bool, rusqlite::types::Value::Integer(value)) => {
            Ok(JsonValue::Bool(*value != 0))
        }
        (FieldKind::Ref { .. }, rusqlite::types::Value::Integer(row_num)) => Ok(JsonValue::String(
            cached_public_row_id(conn, public_row_id_cache, *row_num)?,
        )),
        _ => Err(crate::Error::new(format!(
            "unexpected SQL value for field {}",
            field.name
        ))),
    }
}

pub(super) fn cached_public_row_id(
    conn: &Connection,
    cache: &mut BTreeMap<i64, String>,
    row_num: i64,
) -> Result<String> {
    if let Some(row_id) = cache.get(&row_num) {
        return Ok(row_id.clone());
    }
    let row_id = public_row_id(conn, row_num)?;
    cache.insert(row_num, row_id.clone());
    Ok(row_id)
}

pub(super) fn text_value(value: &rusqlite::types::Value, name: &str) -> Result<String> {
    match value {
        rusqlite::types::Value::Text(value) => Ok(value.clone()),
        _ => Err(crate::Error::new(format!("expected text {name}"))),
    }
}

pub(super) fn integer_value(value: &rusqlite::types::Value, name: &str) -> Result<i64> {
    match value {
        rusqlite::types::Value::Integer(value) => Ok(*value),
        _ => Err(crate::Error::new(format!("expected integer {name}"))),
    }
}

pub(super) fn scoped_policy_fingerprint(
    schema: &SchemaDef,
    history: &[HistoryRecord],
    query_reads: &[QueryReadRecord],
) -> String {
    let mut tables = BTreeSet::new();
    for record in history {
        tables.insert(record.table.clone());
    }
    for query_read in query_reads {
        tables.insert(query_read.table.clone());
    }
    schema.policy_fingerprint_for_tables(tables.iter())
}

pub(super) fn make_bundle(
    schema: &SchemaDef,
    branches: Vec<BranchRecord>,
    txs: Vec<TxRecord>,
    reads: Vec<ReadRecord>,
    query_reads: Vec<QueryReadRecord>,
    history: Vec<HistoryRecord>,
) -> Bundle {
    Bundle {
        protocol_version: BUNDLE_PROTOCOL_VERSION,
        schema_fingerprint: schema.compatibility_fingerprint(),
        policy_fingerprint: scoped_policy_fingerprint(schema, &history, &query_reads),
        branches,
        txs,
        reads,
        query_reads,
        history,
    }
}
