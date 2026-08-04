//! Durable physical identity catalogue metadata.
//!
//! These mappings are shadow metadata until shared table storage starts using
//! physical identities. Keeping its serialization here lets that later cutover
//! reuse the catalogue state without changing current reads or writes.

use super::*;
use crate::ids::{PhysicalColumnId, PhysicalTableId};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct SchemaPhysicalMapping {
    pub(super) tables: BTreeMap<String, TablePhysicalMapping>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct TablePhysicalMapping {
    pub(super) table_id: PhysicalTableId,
    pub(super) columns: BTreeMap<String, PhysicalColumnId>,
}

pub(super) fn physical_column_epoch_is_compatible(
    source_table: &TableSchema,
    source_column_name: &str,
    target_table: &TableSchema,
    target_column_name: &str,
) -> bool {
    let Some(source_column) = source_table
        .columns
        .iter()
        .find(|column| column.name == source_column_name)
    else {
        return false;
    };
    let Some(target_column) = target_table
        .columns
        .iter()
        .find(|column| column.name == target_column_name)
    else {
        return false;
    };

    source_column.column_type == target_column.column_type
        && source_column.large_value == target_column.large_value
        && source_column.text_merge_spec == target_column.text_merge_spec
        && source_table.merge_strategy(source_column_name)
            == target_table.merge_strategy(target_column_name)
}
