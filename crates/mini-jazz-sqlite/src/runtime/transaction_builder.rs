use super::*;
use crate::transaction::{snapshot_result, StagedRowChange, TransactionSnapshot};

pub struct TransactionBuilder<'a> {
    pub(super) runtime: &'a mut Runtime,
    pub(super) mutations: Vec<Mutation>,
    pub(super) mode: TransactionMode,
    pub(super) start_snapshot: std::result::Result<TransactionSnapshot, String>,
}

pub(super) enum TransactionMode {
    Mergeable,
    Exclusive { global_epoch: Option<i64> },
}

pub(super) enum Mutation {
    Row {
        table: String,
        id: String,
        values: BTreeMap<String, JsonValue>,
        op: i64,
    },
    DeleteRow {
        table: String,
        id: String,
    },
}

impl Mutation {
    fn staged_row_change<'a>(&'a self, author: &'a str) -> StagedRowChange<'a> {
        match self {
            Self::Row {
                table, id, values, ..
            } => StagedRowChange::Upsert {
                table,
                id,
                values,
                author,
            },
            Self::DeleteRow { table, id } => StagedRowChange::Delete { table, id },
        }
    }
}

fn normalize_mutations(mutations: Vec<Mutation>) -> Vec<Mutation> {
    let mut normalized: Vec<Mutation> = Vec::new();
    for mutation in mutations {
        let (table, id) = match &mutation {
            Mutation::Row { table, id, .. } | Mutation::DeleteRow { table, id } => {
                (table.as_str(), id.as_str())
            }
        };
        let Some(existing) = normalized.iter_mut().find(|existing| match existing {
            Mutation::Row {
                table: existing_table,
                id: existing_id,
                ..
            }
            | Mutation::DeleteRow {
                table: existing_table,
                id: existing_id,
            } => existing_table == table && existing_id == id,
        }) else {
            normalized.push(mutation);
            continue;
        };
        match (existing, mutation) {
            (
                Mutation::Row {
                    values: existing_values,
                    op: existing_op,
                    ..
                },
                Mutation::Row { values, op, .. },
            ) => {
                existing_values.extend(values);
                if *existing_op != 1 {
                    *existing_op = op;
                }
            }
            (existing_slot, later) => {
                *existing_slot = later;
            }
        }
    }
    normalized
}

impl TransactionBuilder<'_> {
    pub fn read_rows(&self, table_name: &str) -> Result<Vec<RowView>> {
        Ok(snapshot_result(&self.start_snapshot)?.read_rows(
            table_name,
            self.mutations
                .iter()
                .map(|mutation| mutation.staged_row_change(self.runtime.attribution_user())),
        ))
    }

    pub fn exclusive(mut self) -> Self {
        self.mode = TransactionMode::Exclusive { global_epoch: None };
        self
    }

    pub fn exclusive_at_global(mut self, global_epoch: i64) -> Self {
        self.mode = TransactionMode::Exclusive {
            global_epoch: Some(global_epoch),
        };
        self
    }

    pub fn insert_row(
        mut self,
        table: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Self {
        self.mutations.push(Mutation::Row {
            table: table.to_owned(),
            id: id.to_owned(),
            values,
            op: 1,
        });
        self
    }

    pub fn update_row(
        mut self,
        table: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Self {
        self.mutations.push(Mutation::Row {
            table: table.to_owned(),
            id: id.to_owned(),
            values,
            op: 2,
        });
        self
    }

    pub fn upsert_row(
        mut self,
        table: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Self {
        let op = match self.runtime.row_has_current_branch_value(table, id) {
            Ok(true) => 2,
            Ok(false) | Err(_) => 1,
        };
        self.mutations.push(Mutation::Row {
            table: table.to_owned(),
            id: id.to_owned(),
            values,
            op,
        });
        self
    }

    pub fn delete_row(mut self, table: &str, id: &str) -> Self {
        self.mutations.push(Mutation::DeleteRow {
            table: table.to_owned(),
            id: id.to_owned(),
        });
        self
    }

    pub fn commit(self) -> Result<String> {
        let mutations = normalize_mutations(self.mutations);
        if mutations.is_empty() {
            return Ok(String::new());
        }
        let user = self.runtime.attribution_user().to_owned();
        let bypass_policy = self.runtime.bypasses_policy();
        let mut delete_snapshots = BTreeMap::new();
        for mutation in &mutations {
            let Mutation::DeleteRow { table, id } = mutation else {
                continue;
            };
            let visible_row = self
                .runtime
                .read_rows(table)?
                .into_iter()
                .find(|row| row.id == *id)
                .ok_or_else(|| crate::Error::new(format!("row {id} is not visible")))?;
            delete_snapshots.insert((table.clone(), id.clone()), visible_row);
        }
        let (conflict_mode, outcome, global_epoch) = match self.mode {
            TransactionMode::Mergeable => (tx::MODE_MERGEABLE, tx::OUTCOME_PENDING, None),
            TransactionMode::Exclusive {
                global_epoch: Some(global_epoch),
            } => (tx::MODE_EXCLUSIVE, tx::OUTCOME_ACCEPTED, Some(global_epoch)),
            TransactionMode::Exclusive { global_epoch: None } => {
                return Err(crate::Error::new(
                    "exclusive transactions require global acceptance",
                ));
            }
        };
        if conflict_mode == tx::MODE_EXCLUSIVE {
            for mutation in &mutations {
                let (table, id): (&str, &str) = match mutation {
                    Mutation::Row { table, id, .. } | Mutation::DeleteRow { table, id } => {
                        (table.as_str(), id.as_str())
                    }
                };
                let row_num = ensure_row_id(&self.runtime.conn, table, id)?;
                if exclusive_write_conflict_exists(&self.runtime.conn, table, row_num)? {
                    return Err(crate::Error::new("exclusive conflict"));
                }
            }
        }
        let db = self.runtime.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx_with_options(
            &db,
            self.runtime.node_num,
            &self.runtime.node_id,
            now,
            conflict_mode,
            outcome,
            global_epoch,
        )?;
        let mut allowed = true;
        for mutation in mutations {
            match mutation {
                Mutation::Row {
                    table,
                    id,
                    values,
                    op,
                } => {
                    let base_values =
                        snapshot_result(&self.start_snapshot)?.base_values(&table, &id)?;
                    allowed &= insert_row_in_tx(InsertRowInTx {
                        db: &db,
                        schema: &self.runtime.schema,
                        table_name: &table,
                        id: &id,
                        values: &values,
                        tx_num,
                        branch_num: self.runtime.branch_num,
                        now,
                        user: &user,
                        bypass_policy,
                        op,
                        base_values,
                    })?;
                }
                Mutation::DeleteRow { table, id } => {
                    let table_def = self.runtime.schema.table_def(&table)?;
                    let row_num = row_num(&db, &id)?;
                    read_set::record_tx_read(
                        &db,
                        tx_num,
                        &table,
                        row_num,
                        self.runtime.branch_num,
                        2,
                    )?;
                    let visible_row = delete_snapshots
                        .get(&(table.clone(), id.clone()))
                        .ok_or_else(|| {
                            crate::Error::new(format!("missing delete snapshot {id}"))
                        })?;
                    policy_read_set::record_for_write(policy_read_set::WritePolicyReadSet {
                        conn: &db,
                        schema: &self.runtime.schema,
                        table: table_def,
                        policy: &table_def.write_policy,
                        values: &visible_row.values,
                        branch_num: self.runtime.branch_num,
                        tx_num,
                    })?;
                    allowed &= bypass_policy
                        || local_write_allowed(LocalWriteCheck {
                            db: &db,
                            schema: &self.runtime.schema,
                            table: table_def,
                            row_num,
                            branch_num: self.runtime.branch_num,
                            values: &visible_row.values,
                            user: &user,
                            op: 3,
                        })?;
                    let field_columns = table_def
                        .fields
                        .iter()
                        .map(|field| {
                            crate::schema::quote_ident(&crate::schema::storage_column(field))
                        })
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
                    let user_num = users::ensure_user(&db, &user)?;
                    let inserted = db.execute(
                        &format!(
                            "INSERT OR IGNORE INTO {} ({})
                             SELECT {}
                             FROM {}
                             WHERE row_num = ? AND j_branch_num = ?",
                            crate::schema::history_table(&table),
                            insert_columns.join(", "),
                            select_columns.join(", "),
                            crate::schema::current_table(&table),
                        ),
                        params![tx_num, now, user_num, row_num, self.runtime.branch_num],
                    )?;
                    if inserted == 0 {
                        let mut values = vec![
                            rusqlite::types::Value::Integer(row_num),
                            rusqlite::types::Value::Integer(tx_num),
                            rusqlite::types::Value::Integer(self.runtime.branch_num),
                            rusqlite::types::Value::Integer(3),
                        ];
                        for field in &table_def.fields {
                            let value = visible_row.values.get(&field.name).ok_or_else(|| {
                                crate::Error::new(format!("missing field {}", field.name))
                            })?;
                            values.push(crate::schema::field_sql_value(
                                field,
                                value,
                                |ref_table, row_id| ensure_row_id(&db, ref_table, row_id),
                            )?);
                        }
                        values.extend([
                            rusqlite::types::Value::Integer(now),
                            rusqlite::types::Value::Integer(now),
                            rusqlite::types::Value::Integer(user_num),
                            rusqlite::types::Value::Integer(user_num),
                        ]);
                        insert_dynamic(
                            &db,
                            &crate::schema::history_table(&table),
                            &insert_columns,
                            &values,
                        )?;
                    }
                    db.execute(
                        &format!(
                            "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
                            crate::schema::current_table(&table)
                        ),
                        params![row_num, self.runtime.branch_num],
                    )?;
                    if self.runtime.branch_num != 1 {
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
                            rusqlite::types::Value::Integer(self.runtime.branch_num),
                            rusqlite::types::Value::Integer(tx_num),
                            rusqlite::types::Value::Integer(1),
                        ];
                        for field in &table_def.fields {
                            let value = visible_row.values.get(&field.name).ok_or_else(|| {
                                crate::Error::new(format!("missing field {}", field.name))
                            })?;
                            current_values.push(crate::schema::field_sql_value(
                                field,
                                value,
                                |ref_table, row_id| ensure_row_id(&db, ref_table, row_id),
                            )?);
                        }
                        current_values.extend([
                            rusqlite::types::Value::Integer(now),
                            rusqlite::types::Value::Integer(now),
                            rusqlite::types::Value::Integer(user_num),
                            rusqlite::types::Value::Integer(user_num),
                        ]);
                        insert_dynamic(
                            &db,
                            &crate::schema::current_table(&table),
                            &current_columns,
                            &current_values,
                        )?;
                    }
                    record_tx_write(&db, tx_num, &table, row_num, 3)?;
                }
            }
        }
        if !allowed {
            tx::reject(&db, &tx_id, "policy_denied")?;
            projection::rebuild(&db, &self.runtime.schema, self.runtime.node_num)?;
        }
        db.commit()?;
        Ok(tx_id)
    }
}
