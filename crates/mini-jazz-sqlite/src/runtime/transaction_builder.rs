use super::write_core::{
    exclusive_write_conflict_exists, insert_row_in_tx, stage_delete_row_in_tx, DeleteReadSetMode,
    InsertRowInTx, StageDeleteInTx, WriteOp,
};
use super::Runtime;
use crate::rows::ensure_row_id;
use crate::time::now_ms;
use crate::transaction::{snapshot_result, StagedRowChange, TransactionSnapshot};
use crate::types::RowView;
use crate::{projection, tx, Result};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

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
        op: WriteOp,
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
                if *existing_op != WriteOp::Create {
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

impl Runtime {
    pub fn transaction(&mut self) -> TransactionBuilder<'_> {
        let start_snapshot = self
            .schema
            .tables()
            .map(|table| {
                self.read_rows(&table.name)
                    .map(|rows| (table.name.clone(), rows))
                    .map_err(|error| error.to_string())
            })
            .collect::<std::result::Result<BTreeMap<_, _>, _>>()
            .map(TransactionSnapshot::new);
        TransactionBuilder {
            runtime: self,
            mutations: Vec::new(),
            mode: TransactionMode::Mergeable,
            start_snapshot,
        }
    }
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
            op: WriteOp::Create,
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
            op: WriteOp::Update,
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
            Ok(true) => WriteOp::Update,
            Ok(false) | Err(_) => WriteOp::Create,
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
                    let visible_row = delete_snapshots
                        .get(&(table.clone(), id.clone()))
                        .ok_or_else(|| {
                            crate::Error::new(format!("missing delete snapshot {id}"))
                        })?;
                    allowed &= stage_delete_row_in_tx(StageDeleteInTx {
                        db: &db,
                        schema: &self.runtime.schema,
                        table_name: &table,
                        id: &id,
                        visible_values: &visible_row.values,
                        tx_num,
                        branch_num: self.runtime.branch_num,
                        now,
                        user: &user,
                        bypass_policy,
                        read_set: DeleteReadSetMode::RecordPreviousRow,
                    })?;
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
