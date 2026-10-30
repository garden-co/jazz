//! Read-only export hook for the one-time alpha.59 -> SQLite migration.
//!
//! `dev/migrate-alpha59/prepare.sh` copies this file into an alpha.59
//! checkout as `jazz_node::node::migrate_export`, so it can read the node's
//! private catalogue and history. It never writes. Nothing in released Jazz
//! contains it.
#![allow(missing_docs)]

use std::collections::BTreeMap;

use groove::records::{OwnedRecord, Value};

use super::codec::{VersionLayer, VersionRow};
use super::{Error, NodeState};
use crate::ids::{AuthorSubject, RowUuid, SchemaVersionId};
use crate::model::branch::BranchKey;
use crate::protocol::{MigrationLens, SchemaVersion};
use crate::schema::{LargeValueSemanticKind, TableSchema};
use crate::time::{GlobalTime, TxTime};
use crate::tx::{DeletionEvent, Fate, TxId, TxKind};

/// One stored row version, with the transaction that wrote it.
#[derive(Clone, Debug)]
pub struct ExportedVersion {
    /// Logical table name in `schema`.
    pub table: String,
    /// Schema version the version was written under.
    pub schema: SchemaVersionId,
    pub branch: BranchKey,
    pub row: RowUuid,
    pub tx: TxId,
    pub made_by: AuthorSubject,
    pub kind: TxKind,
    pub fate: Fate,
    pub global_time: Option<GlobalTime>,
    pub created_by: AuthorSubject,
    pub created_at: TxTime,
    pub updated_by: AuthorSubject,
    pub updated_at: TxTime,
    /// `None` for a content version; a delete or restore otherwise.
    pub deletion: Option<DeletionEvent>,
    /// Content cells by column name, large values read back whole. Absent
    /// cells are null.
    pub cells: BTreeMap<String, Value>,
    pub user_metadata_json: Option<String>,
}

/// The node's runtime catalogue: every schema version and lens it admitted.
#[derive(Clone, Debug)]
pub struct ExportedCatalogue {
    pub node: crate::ids::NodeUuid,
    pub local_schema: SchemaVersionId,
    pub active_schema: SchemaVersionId,
    pub schemas: Vec<SchemaVersion>,
    pub lenses: Vec<MigrationLens>,
}

impl<S> NodeState<S>
where
    S: groove::storage::OrderedKvStorage + 'static,
{
    /// The admitted schemas and lenses.
    pub fn migrate_export_catalogue(&self) -> ExportedCatalogue {
        ExportedCatalogue {
            node: self.node_uuid,
            local_schema: self.catalogue.local_schema_version_id,
            active_schema: self.catalogue.active_schema.schema,
            schemas: self.catalogue.catalogue_schemas.values().cloned().collect(),
            lenses: self.catalogue.catalogue_lenses.values().cloned().collect(),
        }
    }

    /// Every stored version of every table, in no particular order. Rejected
    /// versions are already gone (rejection cleanup removes them).
    pub async fn migrate_export_versions(&mut self) -> Result<Vec<ExportedVersion>, Error> {
        let mut sources: Vec<String> = Vec::new();
        let tables: Vec<String> = self
            .catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.keys().cloned())
            .collect();
        for table in &tables {
            for layer in [VersionLayer::Content, VersionLayer::Deletion] {
                for source in self.version_storage_sources_for_layer(table, layer)? {
                    if !sources.contains(&source) {
                        sources.push(source);
                    }
                }
            }
        }
        let mut versions = Vec::new();
        let mut transactions = BTreeMap::new();
        for source in sources {
            let raws: Vec<OwnedRecord> = self
                .database
                .primary_key_scan_raw(&source, &[])
                .await?
                .into_iter()
                .map(|raw| raw.owned_record())
                .collect();
            for record in raws {
                let version = self.decode_history_owned_record("", &source, record)?;
                versions.push(
                    self.migrate_export_version(&version, &mut transactions)
                        .await?,
                );
            }
        }
        Ok(versions)
    }

    async fn migrate_export_version(
        &mut self,
        version: &VersionRow,
        transactions: &mut BTreeMap<TxId, super::codec::StoredTransaction>,
    ) -> Result<ExportedVersion, Error> {
        let tx_id = self.version_tx_id(version)?;
        if !transactions.contains_key(&tx_id) {
            let tx = self
                .query_transaction(tx_id)
                .await?
                .ok_or(Error::MissingTransaction(tx_id))?;
            transactions.insert(tx_id, tx);
        }
        let tx = &transactions[&tx_id];
        let schema = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "history schema version alias must exist",
            ))?;
        let table = self.table_in_schema_ref(version.table(), schema)?.clone();
        let mut cells = version.cells(&table)?;
        for (name, value) in cells.iter_mut() {
            let kind = column_kind(&table, name);
            *value = self.migrate_inline_large(value, kind).await?;
        }
        Ok(ExportedVersion {
            table: version.table().to_owned(),
            schema,
            branch: version.branch_key().clone(),
            row: version.row_uuid(),
            tx: tx_id,
            made_by: tx.tx.made_by,
            kind: tx.tx.kind,
            fate: tx.fate.clone(),
            global_time: tx.global_time,
            created_by: version.created_by(),
            created_at: version.created_at(),
            updated_by: version.updated_by(),
            updated_at: version.updated_at(),
            deletion: version.deletion(),
            cells,
            user_metadata_json: tx.tx.user_metadata_json.clone(),
        })
    }

    /// Replace every large-value descriptor in `value` by its bytes.
    fn migrate_inline_large<'a>(
        &'a self,
        value: &'a Value,
        kind: LargeValueSemanticKind,
    ) -> futures::future::LocalBoxFuture<'a, Result<Value, Error>> {
        Box::pin(async move {
            Ok(match value {
                Value::Large(value_ref) => {
                    let bytes = self
                        .database
                        .read_large_value_range(value_ref, 0..value_ref.byte_length)
                        .await?;
                    match kind {
                        LargeValueSemanticKind::Bytes => Value::Bytes(bytes),
                        _ => Value::String(String::from_utf8(bytes).map_err(|_| {
                            Error::InvalidStoredValue("large text value is not UTF-8")
                        })?),
                    }
                }
                Value::Nullable(Some(inner)) => Value::Nullable(Some(Box::new(
                    self.migrate_inline_large(inner, kind).await?,
                ))),
                Value::Array(values) => {
                    let mut out = Vec::with_capacity(values.len());
                    for value in values {
                        out.push(self.migrate_inline_large(value, kind).await?);
                    }
                    Value::Array(out)
                }
                Value::Tuple(values) => {
                    let mut out = Vec::with_capacity(values.len());
                    for value in values {
                        out.push(self.migrate_inline_large(value, kind).await?);
                    }
                    Value::Tuple(out)
                }
                other => other.clone(),
            })
        })
    }
}

fn column_kind(table: &TableSchema, name: &str) -> LargeValueSemanticKind {
    table
        .columns
        .iter()
        .find(|column| column.name == name)
        .map(|column| column.large_value_kind)
        .unwrap_or(LargeValueSemanticKind::NotLarge)
}
