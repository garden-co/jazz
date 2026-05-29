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
        let mut rows = self
            .rows_by_table
            .get(table_name)
            .cloned()
            .unwrap_or_default();

        for change in staged_changes {
            match change {
                StagedRowChange::Upsert {
                    table,
                    id,
                    values,
                    author,
                } if table == table_name => {
                    let matching_indexes = rows
                        .iter()
                        .enumerate()
                        .filter_map(|(index, row)| (row.id == id).then_some(index))
                        .collect::<Vec<_>>();
                    if matching_indexes.is_empty() {
                        rows.push(RowView {
                            table: table.to_owned(),
                            id: id.to_owned(),
                            values: BTreeMap::new(),
                            created_at: 0,
                            created_by: author.to_owned(),
                            tx_id: String::new(),
                            conflict_count: 0,
                        });
                        let last = rows.len() - 1;
                        rows[last].values.extend(values.clone());
                    } else {
                        for index in matching_indexes {
                            rows[index].values.extend(values.clone());
                        }
                    }
                }
                StagedRowChange::Delete { table, id } if table == table_name => {
                    rows.retain(|row| row.id != id);
                }
                _ => {}
            }
        }

        rows
    }

    pub(crate) fn base_values(
        &self,
        table_name: &str,
        id: &str,
    ) -> Result<Option<&BTreeMap<String, JsonValue>>> {
        let matches = self
            .rows_by_table
            .get(table_name)
            .into_iter()
            .flatten()
            .filter(|row| row.id == id)
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            return Err(Error::new("ambiguous branch row"));
        }
        Ok(matches.first().map(|row| &row.values))
    }
}

pub(crate) fn snapshot_result(
    snapshot: &std::result::Result<TransactionSnapshot, String>,
) -> Result<&TransactionSnapshot> {
    snapshot.as_ref().map_err(|error| Error::new(error.clone()))
}
