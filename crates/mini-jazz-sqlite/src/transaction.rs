use crate::types::RowView;
use crate::{Error, Result};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(crate) struct TransactionSnapshot {
    rows_by_table: BTreeMap<String, Vec<RowView>>,
}

#[derive(Clone, Debug)]
pub(crate) enum StagedRowChange<'a> {
    Upsert {
        table: &'a str,
        id: &'a str,
        values: &'a BTreeMap<String, JsonValue>,
        author: &'a str,
    },
    Delete {
        table: &'a str,
        id: &'a str,
    },
}

impl TransactionSnapshot {
    pub(crate) fn new(rows_by_table: BTreeMap<String, Vec<RowView>>) -> Self {
        Self { rows_by_table }
    }

    pub(crate) fn read_rows<'a>(
        &self,
        table_name: &str,
        staged_changes: impl IntoIterator<Item = StagedRowChange<'a>>,
    ) -> Vec<RowView> {
        let mut rows_by_id = self
            .rows_by_table
            .get(table_name)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|row| (row.id.clone(), row))
            .collect::<BTreeMap<_, _>>();

        for change in staged_changes {
            match change {
                StagedRowChange::Upsert {
                    table,
                    id,
                    values,
                    author,
                } if table == table_name => {
                    let row = rows_by_id.entry(id.to_owned()).or_insert_with(|| RowView {
                        table: table.to_owned(),
                        id: id.to_owned(),
                        values: BTreeMap::new(),
                        created_at: 0,
                        created_by: author.to_owned(),
                        tx_id: String::new(),
                        conflict_count: 0,
                    });
                    row.values.extend(values.clone());
                }
                StagedRowChange::Delete { table, id } if table == table_name => {
                    rows_by_id.remove(id);
                }
                _ => {}
            }
        }

        rows_by_id.into_values().collect()
    }

    pub(crate) fn base_values(
        &self,
        table_name: &str,
        id: &str,
    ) -> Option<&BTreeMap<String, JsonValue>> {
        self.rows_by_table
            .get(table_name)?
            .iter()
            .find(|row| row.id == id)
            .map(|row| &row.values)
    }
}

pub(crate) fn snapshot_result(
    snapshot: &std::result::Result<TransactionSnapshot, String>,
) -> Result<&TransactionSnapshot> {
    snapshot.as_ref().map_err(|error| Error::new(error.clone()))
}
