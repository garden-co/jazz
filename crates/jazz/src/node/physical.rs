//! Durable physical identity metadata and Groove history-table lowering.

use super::*;
use crate::ids::{PhysicalColumnId, PhysicalTableId};
use crate::schema::MERGE_HEADS_TABLE;
use groove::schema::{
    ColumnSchema as GrooveColumnSchema, IndexSchema as GrooveIndexSchema,
    TableSchema as GrooveTableSchema,
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct SchemaPhysicalMapping {
    pub(super) tables: BTreeMap<String, TablePhysicalMapping>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct TablePhysicalMapping {
    pub(super) table_id: PhysicalTableId,
    pub(super) columns: BTreeMap<String, PhysicalColumnId>,
}

#[derive(Clone, Debug)]
pub(super) struct PhysicalHistoryBinding {
    pub(super) storage_table: String,
    pub(super) descriptor: records::RecordDescriptor,
}

pub(super) fn physical_history_table_name(table_id: PhysicalTableId) -> String {
    format!("jazz_physical_{}_history", table_id.0)
}

pub(super) fn physical_register_table_name(table_id: PhysicalTableId) -> String {
    format!("jazz_physical_{}_register", table_id.0)
}

pub(super) fn physical_global_current_table_name(table_id: PhysicalTableId) -> String {
    format!("jazz_physical_{}_global_current", table_id.0)
}

pub(super) fn physical_register_global_current_table_name(table_id: PhysicalTableId) -> String {
    format!("jazz_physical_{}_register_global_current", table_id.0)
}

pub(super) fn physical_ahead_current_table_name(table_id: PhysicalTableId) -> String {
    format!("jazz_physical_{}_ahead_current", table_id.0)
}

pub(super) fn physical_register_ahead_current_table_name(table_id: PhysicalTableId) -> String {
    format!("jazz_physical_{}_register_ahead_current", table_id.0)
}

pub(super) fn physical_rejected_versions_table_name(table_id: PhysicalTableId) -> String {
    format!("jazz_physical_{}_rejected_versions", table_id.0)
}

pub(super) fn physical_branch_history_table_name(
    table_id: PhysicalTableId,
    branch_id: BranchId,
) -> String {
    format!(
        "jazz_physical_{}_branch_{}_history",
        table_id.0,
        branch_id.0.simple()
    )
}

pub(super) fn physical_branch_register_table_name(
    table_id: PhysicalTableId,
    branch_id: BranchId,
) -> String {
    format!(
        "jazz_physical_{}_branch_{}_register",
        table_id.0,
        branch_id.0.simple()
    )
}

pub(super) fn physical_branch_version_storage_table_name(
    table_id: PhysicalTableId,
    layer: VersionLayer,
    branch_id: BranchId,
) -> String {
    match layer {
        VersionLayer::Content => physical_branch_history_table_name(table_id, branch_id),
        VersionLayer::Deletion => physical_branch_register_table_name(table_id, branch_id),
    }
}

pub(super) fn physical_current_index_name(column_id: PhysicalColumnId) -> String {
    format!("by_physical_user_{}", column_id.0)
}

pub(super) fn physical_user_column_field(column_id: PhysicalColumnId) -> String {
    format!("user_{}", column_id.0)
}

pub(super) fn physical_history_projection_target(
    schema_alias: SchemaVersionAlias,
    logical_table: &str,
) -> String {
    format!("schema_{}_{}_history", schema_alias.0, logical_table)
}

pub(super) fn physical_current_projection_target(
    schema_alias: SchemaVersionAlias,
    logical_table: &str,
) -> String {
    format!("schema_{}_{}_current", schema_alias.0, logical_table)
}

#[derive(Clone, Copy)]
pub(super) enum PhysicalCurrentClass {
    Global,
    Ahead,
}

#[derive(Clone, Copy)]
enum ContentProjectionShape {
    History,
    Current,
}

pub(super) fn allocate_provisional_physical_mapping(
    schema: &JazzSchema,
    next_table_id: &mut u64,
    next_column_id: &mut u64,
) -> Result<SchemaPhysicalMapping, Error> {
    let mut tables = BTreeMap::new();
    for table in &schema.tables {
        let table_id = PhysicalTableId(*next_table_id);
        *next_table_id = next_table_id
            .checked_add(1)
            .ok_or(Error::InvalidStoredValue("physical table id exhausted"))?;
        let mut columns = BTreeMap::new();
        for column in &table.columns {
            let column_id = PhysicalColumnId(*next_column_id);
            *next_column_id = next_column_id
                .checked_add(1)
                .ok_or(Error::InvalidStoredValue("physical column id exhausted"))?;
            columns.insert(column.name.clone(), column_id);
        }
        tables.insert(
            table.name.clone(),
            TablePhysicalMapping { table_id, columns },
        );
    }
    Ok(SchemaPhysicalMapping { tables })
}

pub(super) fn physical_history_binding(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    schema_version_aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    schema_version: SchemaVersionId,
    logical_table: &str,
) -> Result<PhysicalHistoryBinding, Error> {
    let schema = catalogue_schemas
        .get(&schema_version)
        .ok_or(Error::InvalidStoredValue("physical history schema missing"))?;
    let table = schema
        .schema
        .tables
        .iter()
        .find(|table| table.name == logical_table)
        .ok_or_else(|| Error::TableNotFound(logical_table.to_owned()))?;
    let mapping = physical_mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "physical history table mapping missing",
        ))?;
    let alias =
        schema_version_aliases
            .get(&schema_version)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical history schema alias missing",
            ))?;
    Ok(PhysicalHistoryBinding {
        storage_table: physical_history_table_name(mapping.table_id),
        descriptor: physical_history_descriptor(table, mapping, alias)?,
    })
}

pub(super) fn physical_current_binding(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    schema_version: SchemaVersionId,
    logical_table: &str,
    class: PhysicalCurrentClass,
) -> Result<PhysicalHistoryBinding, Error> {
    let schema = catalogue_schemas
        .get(&schema_version)
        .ok_or(Error::InvalidStoredValue("physical current schema missing"))?;
    let table = schema
        .schema
        .tables
        .iter()
        .find(|table| table.name == logical_table)
        .ok_or_else(|| Error::TableNotFound(logical_table.to_owned()))?;
    let mapping = physical_mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "physical current table mapping missing",
        ))?;
    let storage_table = match class {
        PhysicalCurrentClass::Global => physical_global_current_table_name(mapping.table_id),
        PhysicalCurrentClass::Ahead => physical_ahead_current_table_name(mapping.table_id),
    };
    Ok(PhysicalHistoryBinding {
        storage_table,
        descriptor: physical_current_descriptor(table, mapping)?,
    })
}

pub(super) fn physical_rejected_version_binding(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    schema_version: SchemaVersionId,
    logical_table: &str,
) -> Result<PhysicalHistoryBinding, Error> {
    let schema = catalogue_schemas
        .get(&schema_version)
        .ok_or(Error::InvalidStoredValue(
            "physical rejected-version schema missing",
        ))?;
    let table = schema
        .schema
        .tables
        .iter()
        .find(|table| table.name == logical_table)
        .ok_or_else(|| Error::TableNotFound(logical_table.to_owned()))?;
    let mapping = physical_mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "physical rejected-version table mapping missing",
        ))?;
    Ok(PhysicalHistoryBinding {
        storage_table: physical_rejected_versions_table_name(mapping.table_id),
        descriptor: physical_rejected_version_descriptor(table, mapping)?,
    })
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn physical_table_id_for_schema(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
    ) -> Result<PhysicalTableId, Error> {
        self.table_in_schema(logical_table, schema_version)?;
        self.catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(logical_table))
            .map(|mapping| mapping.table_id)
            .ok_or(Error::InvalidStoredValue("physical table mapping missing"))
    }

    pub(super) fn physical_table_id_for_version(
        &self,
        version: &VersionRow,
    ) -> Result<PhysicalTableId, Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored row schema version alias missing",
            ))?;
        self.physical_table_id_for_schema(schema_version, version.table())
    }

    pub(super) fn physical_register_table_for_schema(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
    ) -> Result<String, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, logical_table)?;
        Ok(physical_register_table_name(table_id))
    }

    pub(super) fn physical_current_table_for_schema(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        layer: VersionLayer,
        class: PhysicalCurrentClass,
    ) -> Result<String, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, logical_table)?;
        Ok(match (class, layer) {
            (PhysicalCurrentClass::Global, VersionLayer::Content) => {
                physical_global_current_table_name(table_id)
            }
            (PhysicalCurrentClass::Global, VersionLayer::Deletion) => {
                physical_register_global_current_table_name(table_id)
            }
            (PhysicalCurrentClass::Ahead, VersionLayer::Content) => {
                physical_ahead_current_table_name(table_id)
            }
            (PhysicalCurrentClass::Ahead, VersionLayer::Deletion) => {
                physical_register_ahead_current_table_name(table_id)
            }
        })
    }

    pub(super) fn physical_current_source_graph(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        class: PhysicalCurrentClass,
    ) -> Result<GraphBuilder, Error> {
        let alias = self
            .catalogue
            .schema_version_aliases
            .get(&schema_version)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical current source schema alias missing",
            ))?;
        let binding = physical_current_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
            class,
        )?;
        Ok(GraphBuilder::variant_project(
            binding.storage_table,
            physical_current_projection_target(alias, logical_table),
        ))
    }

    pub(super) fn physical_current_source_scan_graph(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        class: PhysicalCurrentClass,
        scan: groove::ivm::StaticScanSpec,
    ) -> Result<GraphBuilder, Error> {
        let alias = self
            .catalogue
            .schema_version_aliases
            .get(&schema_version)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical current source schema alias missing",
            ))?;
        let binding = physical_current_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
            class,
        )?;
        Ok(GraphBuilder::variant_project_scan(
            binding.storage_table,
            physical_current_projection_target(alias, logical_table),
            scan,
        ))
    }

    pub(super) fn logical_table_for_physical_alias(
        &self,
        table_id: PhysicalTableId,
        alias: SchemaVersionAlias,
    ) -> Result<String, Error> {
        let schema_version =
            self.schema_version_for_alias(alias)
                .ok_or(Error::InvalidStoredValue(
                    "physical row schema version alias missing",
                ))?;
        self.catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| {
                mapping.tables.iter().find_map(|(logical_table, mapping)| {
                    (mapping.table_id == table_id).then(|| logical_table.clone())
                })
            })
            .ok_or(Error::InvalidStoredValue(
                "physical row logical table mapping missing",
            ))
    }

    pub(super) fn physical_table_ids(&self) -> BTreeSet<PhysicalTableId> {
        self.catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.values().map(|table| table.table_id))
            .collect()
    }

    pub(super) fn physical_history_source_graph(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
    ) -> Result<GraphBuilder, Error> {
        let alias = self
            .catalogue
            .schema_version_aliases
            .get(&schema_version)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical history source schema alias missing",
            ))?;
        let binding = physical_history_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.schema_version_aliases,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
        )?;
        Ok(GraphBuilder::variant_project(
            binding.storage_table,
            physical_history_projection_target(alias, logical_table),
        ))
    }

    pub(super) fn register_physical_history_variant_projections(&mut self) -> Result<(), Error> {
        let targets = self
            .catalogue
            .physical_mappings
            .iter()
            .flat_map(|(schema_version, mapping)| {
                mapping.tables.iter().map(|(logical_table, table)| {
                    (*schema_version, logical_table.clone(), table.clone())
                })
            })
            .collect::<Vec<_>>();
        for (target_schema, target_table_name, target_mapping) in targets {
            let target_alias = self
                .catalogue
                .schema_version_aliases
                .get(&target_schema)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical projection target schema alias missing",
                ))?;
            let target_table = self.table_in_schema(&target_table_name, target_schema)?;
            let storage_table = physical_history_table_name(target_mapping.table_id);
            let projection_target =
                physical_history_projection_target(target_alias, &target_table_name);
            self.database.define_variant_projection(
                &storage_table,
                &projection_target,
                target_table.history_storage_table().record_schema(),
            )?;

            let sources = self
                .catalogue
                .physical_mappings
                .iter()
                .flat_map(|(schema_version, mapping)| {
                    mapping
                        .tables
                        .iter()
                        .filter(|(_, table)| table.table_id == target_mapping.table_id)
                        .map(|(logical_table, table)| {
                            (*schema_version, logical_table.clone(), table.clone())
                        })
                })
                .collect::<Vec<_>>();
            for (source_schema, source_table_name, source_mapping) in sources {
                let source_alias = self
                    .catalogue
                    .schema_version_aliases
                    .get(&source_schema)
                    .copied()
                    .ok_or(Error::InvalidStoredValue(
                        "physical projection source schema alias missing",
                    ))?;
                let Some(fields) = self.physical_history_projection_case(
                    source_schema,
                    &source_table_name,
                    &source_mapping,
                    target_schema,
                    &target_table_name,
                )?
                else {
                    self.database.register_variant_projection_ignore_case(
                        &storage_table,
                        &projection_target,
                        source_alias.0,
                    )?;
                    continue;
                };
                self.database.register_variant_projection_case(
                    &storage_table,
                    &projection_target,
                    source_alias.0,
                    fields,
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn register_physical_current_variant_projections(&mut self) -> Result<(), Error> {
        let targets = self
            .catalogue
            .physical_mappings
            .iter()
            .flat_map(|(schema_version, mapping)| {
                mapping.tables.iter().map(|(logical_table, table)| {
                    (*schema_version, logical_table.clone(), table.clone())
                })
            })
            .collect::<Vec<_>>();
        for (target_schema, target_table_name, target_mapping) in targets {
            let target_alias = self
                .catalogue
                .schema_version_aliases
                .get(&target_schema)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical current projection target schema alias missing",
                ))?;
            let target_table = self.table_in_schema(&target_table_name, target_schema)?;
            let projection_target =
                physical_current_projection_target(target_alias, &target_table_name);
            let storage_tables = [
                physical_global_current_table_name(target_mapping.table_id),
                physical_ahead_current_table_name(target_mapping.table_id),
            ];
            for storage_table in &storage_tables {
                self.database.define_variant_projection(
                    storage_table,
                    &projection_target,
                    target_table.global_current_storage_tables()[0].record_schema(),
                )?;
            }

            let sources = self
                .catalogue
                .physical_mappings
                .iter()
                .flat_map(|(schema_version, mapping)| {
                    mapping
                        .tables
                        .iter()
                        .filter(|(_, table)| table.table_id == target_mapping.table_id)
                        .map(|(logical_table, table)| {
                            (*schema_version, logical_table.clone(), table.clone())
                        })
                })
                .collect::<Vec<_>>();
            for (source_schema, source_table_name, source_mapping) in sources {
                let source_alias = self
                    .catalogue
                    .schema_version_aliases
                    .get(&source_schema)
                    .copied()
                    .ok_or(Error::InvalidStoredValue(
                        "physical current projection source schema alias missing",
                    ))?;
                let fields = self.physical_current_projection_case(
                    source_schema,
                    &source_table_name,
                    &source_mapping,
                    target_schema,
                    &target_table_name,
                )?;
                for storage_table in &storage_tables {
                    if let Some(fields) = fields.clone() {
                        self.database.register_variant_projection_case(
                            storage_table,
                            &projection_target,
                            source_alias.0,
                            fields,
                        )?;
                    } else {
                        self.database.register_variant_projection_ignore_case(
                            storage_table,
                            &projection_target,
                            source_alias.0,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn synchronize_physical_version_tables(&mut self) -> Result<(), Error> {
        for desired in physical_version_storage_tables(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.schema_version_aliases,
            &self.catalogue.physical_mappings,
            &self.branches.branch_partitions,
        )? {
            let existing = match self.database.table_schema(&desired.name) {
                Ok(existing) => Some(existing.clone()),
                Err(GrooveDbError::TableNotFound(_)) => None,
                Err(error) => return Err(error.into()),
            };
            let Some(existing) = existing else {
                self.database.register_table(desired)?;
                continue;
            };
            let added_columns = desired
                .columns
                .iter()
                .filter(|column| {
                    existing
                        .columns
                        .iter()
                        .all(|candidate| candidate.name != column.name)
                })
                .cloned()
                .collect::<Vec<_>>();
            for schema_version in desired.schema_versions {
                if existing.schema_version(schema_version.version).is_some() {
                    continue;
                }
                self.database.register_table_schema_version_with_columns(
                    &desired.name,
                    added_columns.clone(),
                    schema_version,
                )?;
            }
            for index in desired.indices {
                if existing
                    .indices
                    .iter()
                    .any(|candidate| candidate.name == index.name)
                {
                    continue;
                }
                self.database.register_table_index(&desired.name, index)?;
            }
        }
        self.register_physical_history_variant_projections()?;
        self.register_physical_current_variant_projections()
    }

    // Retained while the legacy provisional-publication path is retired in the
    // following catalogue cleanup commit.
    #[allow(dead_code)]
    pub(super) fn discard_unmapped_physical_version_tables(
        &mut self,
        candidates: impl IntoIterator<Item = PhysicalTableId>,
    ) -> Result<(), Error> {
        let candidates = candidates.into_iter().collect::<BTreeSet<_>>();
        let live = self
            .catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.values().map(|table| table.table_id))
            .collect::<BTreeSet<_>>();
        let discarded = candidates
            .difference(&live)
            .copied()
            .collect::<BTreeSet<_>>();
        let discarded_branch_partitions = self
            .branches
            .branch_partitions
            .iter()
            .filter(|(table_id, _)| discarded.contains(table_id))
            .copied()
            .collect::<BTreeSet<_>>();
        let mut invalidated_rejections = BTreeMap::new();
        for table_id in &discarded {
            let table = physical_rejected_versions_table_name(*table_id);
            let rows = match self.database.primary_key_scan_raw(&table, &[]) {
                Ok(rows) => rows,
                Err(GrooveDbError::TableNotFound(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            for raw in rows {
                let record = raw.record();
                let time = TxTime(record.get_u64(RejectedVersionRowRecord::FIELD_TX_TIME_IDX)?);
                let alias =
                    NodeAlias(record.get_u64(RejectedVersionRowRecord::FIELD_TX_NODE_ID_IDX)?);
                let node = self.node_for_alias(alias).ok_or(Error::InvalidStoredValue(
                    "rejected transaction node alias missing",
                ))?;
                invalidated_rejections.insert(TxId::new(time, node), alias);
            }
        }

        for table_id in &discarded {
            let mut storage_tables = vec![
                (physical_history_table_name(*table_id), false),
                (physical_register_table_name(*table_id), false),
                (physical_global_current_table_name(*table_id), true),
                (physical_register_global_current_table_name(*table_id), true),
                (physical_ahead_current_table_name(*table_id), false),
                (physical_register_ahead_current_table_name(*table_id), false),
            ];
            for (_, branch_id) in discarded_branch_partitions
                .iter()
                .filter(|(candidate, _)| candidate == table_id)
            {
                storage_tables.push((
                    physical_branch_history_table_name(*table_id, *branch_id),
                    false,
                ));
                storage_tables.push((
                    physical_branch_register_table_name(*table_id, *branch_id),
                    false,
                ));
            }
            for (table, global_current) in storage_tables {
                let rows = match self.database.primary_key_scan_raw(&table, &[]) {
                    Ok(rows) => rows
                        .into_iter()
                        .map(|row| row.owned_record())
                        .collect::<Vec<_>>(),
                    Err(GrooveDbError::TableNotFound(_)) => continue,
                    Err(error) => return Err(error.into()),
                };
                if rows.is_empty() {
                    continue;
                }
                let mut batch = self.database.open_batch();
                for row in rows {
                    let record = row.borrowed();
                    let key = if global_current {
                        PrimaryKeyValue::Composite(vec![PrimaryKeyValue::Uuid(
                            record.get_uuid(GlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?,
                        )])
                    } else {
                        PrimaryKeyValue::Composite(vec![
                            PrimaryKeyValue::Uuid(
                                record.get_uuid(HistoryRowRecord::FIELD_ROW_UUID_IDX)?,
                            ),
                            PrimaryKeyValue::U64(
                                record.get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?,
                            ),
                            PrimaryKeyValue::U64(
                                record.get_u64(HistoryRowRecord::FIELD_TX_NODE_ID_IDX)?,
                            ),
                        ])
                    };
                    batch.delete(&table, key);
                }
                self.database.commit_batch(batch)?;
            }

            let rows = self
                .database
                .primary_key_scan_raw("jazz_global_changes", &[Value::U64(table_id.0)])?
                .into_iter()
                .map(|row| row.owned_record())
                .collect::<Vec<_>>();
            if !rows.is_empty() {
                let mut batch = self.database.open_batch();
                for row in rows {
                    batch.delete(
                        "jazz_global_changes",
                        global_change_primary_key_from_record(&row.borrowed())?,
                    );
                }
                self.database.commit_batch(batch)?;
            }

            let rows = self
                .database
                .primary_key_scan_raw(MERGE_HEADS_TABLE, &[Value::U64(table_id.0)])?
                .into_iter()
                .map(|row| row.owned_record())
                .collect::<Vec<_>>();
            if !rows.is_empty() {
                let mut batch = self.database.open_batch();
                for row in rows {
                    batch.delete(
                        MERGE_HEADS_TABLE,
                        PrimaryKeyValue::Composite(vec![
                            PrimaryKeyValue::U64(table_id.0),
                            PrimaryKeyValue::Uuid(row.borrowed().get_uuid(1)?),
                        ]),
                    );
                }
                self.database.commit_batch(batch)?;
            }
        }

        if !discarded_branch_partitions.is_empty() {
            let mut batch = self.database.open_batch();
            for (table_id, branch_id) in &discarded_branch_partitions {
                batch.delete(
                    "jazz_branch_partitions",
                    PrimaryKeyValue::Composite(vec![
                        PrimaryKeyValue::U64(table_id.0),
                        PrimaryKeyValue::Uuid(branch_id.0),
                    ]),
                );
            }
            self.database.commit_batch(batch)?;
            self.branches
                .branch_partitions
                .retain(|partition| !discarded_branch_partitions.contains(partition));
        }

        if !invalidated_rejections.is_empty() {
            let archive_table_ids = live.union(&discarded).copied().collect::<BTreeSet<_>>();
            let mut version_deletes = Vec::new();
            for (tx_id, alias) in &invalidated_rejections {
                for table_id in &archive_table_ids {
                    let table = physical_rejected_versions_table_name(*table_id);
                    let rows = match self.database.primary_key_scan_raw(
                        &table,
                        &[Value::U64(tx_id.time.0), Value::U64(alias.0)],
                    ) {
                        Ok(rows) => rows,
                        Err(GrooveDbError::TableNotFound(_)) => continue,
                        Err(error) => return Err(error.into()),
                    };
                    for raw in rows {
                        version_deletes.push((
                            table.clone(),
                            rejected_version_primary_key_from_record(&raw.record())?,
                        ));
                    }
                }
            }
            let mut batch = self.database.open_batch();
            for (tx_id, alias) in &invalidated_rejections {
                batch.delete(
                    "jazz_rejected_transactions",
                    rejected_transaction_primary_key(*alias, *tx_id),
                );
            }
            for (table, key) in version_deletes {
                batch.delete(table, key);
            }
            self.database.commit_batch(batch)?;
            for tx_id in invalidated_rejections.keys() {
                self.rejections.rejected_transactions.remove(tx_id);
            }
        }
        Ok(())
    }

    fn physical_history_projection_case(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        self.physical_content_projection_case(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            ContentProjectionShape::History,
        )
    }

    fn physical_current_projection_case(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        self.physical_content_projection_case(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            ContentProjectionShape::Current,
        )
    }

    fn physical_content_projection_case(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        shape: ContentProjectionShape,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        #[derive(Clone)]
        enum CellProjection {
            Field(String),
            Literal(Value),
        }

        let source_table = self.table_in_schema(source_table_name, source_schema)?;
        let target_table = self.table_in_schema(target_table_name, target_schema)?;
        let mut cells = source_table
            .columns
            .iter()
            .map(|column| {
                let column_id = source_mapping.columns.get(&column.name).copied().ok_or(
                    Error::InvalidStoredValue("physical projection column mapping missing"),
                )?;
                Ok((
                    column.name.clone(),
                    CellProjection::Field(physical_user_column_field(column_id)),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        if source_schema != target_schema || source_table_name != target_table_name {
            let mut path = None;
            for direction in [LensPathDirection::Forward, LensPathDirection::Reverse] {
                if let Some(candidate) = self.compiled_lens_path(
                    source_schema,
                    target_schema,
                    direction,
                    source_table_name,
                )? && candidate.target_table == target_table_name
                {
                    path = Some(candidate);
                    break;
                }
            }
            let Some(path) = path else {
                return Ok(None);
            };
            for op in path.ops {
                match op {
                    CompiledLensOp::Rename { from, to } => {
                        if let Some(value) = cells.remove(&from) {
                            cells.insert(to, value);
                        }
                    }
                    CompiledLensOp::Copy { from, to } => {
                        if let Some(value) = cells.get(&from).cloned() {
                            cells.insert(to, value);
                        }
                    }
                    CompiledLensOp::Add { column, default } => {
                        cells
                            .entry(column)
                            .or_insert(CellProjection::Literal(default));
                    }
                    CompiledLensOp::Drop { column } => {
                        cells.remove(&column);
                    }
                }
            }
        }

        let target_storage = match shape {
            ContentProjectionShape::History => target_table.history_storage_table(),
            ContentProjectionShape::Current => {
                target_table.global_current_storage_tables()[0].clone()
            }
        };
        let user_cells = match shape {
            ContentProjectionShape::History => HistoryRowRecord::USER_CELLS,
            ContentProjectionShape::Current => GlobalCurrentRowRecord::USER_CELLS,
        };
        let mut fields = target_storage
            .record_schema()
            .fields()
            .iter()
            .take(user_cells)
            .map(|field| {
                ProjectField::named(
                    field
                        .name
                        .clone()
                        .expect("Jazz history system fields are named"),
                )
            })
            .collect::<Vec<_>>();
        for column in &target_table.columns {
            let output = user_column_field(&column.name);
            let Some(projection) = cells.remove(&column.name) else {
                return Ok(None);
            };
            match projection {
                CellProjection::Field(source) => {
                    fields.push(ProjectField::renamed(source, output));
                }
                CellProjection::Literal(value) => fields.push(ProjectField::literal_typed(
                    output,
                    Value::Nullable(Some(Box::new(value))),
                    records::ValueType::Nullable(Box::new(column.column_type.clone())),
                )),
            }
        }
        fields.extend(
            target_storage
                .record_schema()
                .fields()
                .iter()
                .skip(user_cells + target_table.columns.len())
                .map(|field| {
                    ProjectField::named(
                        field
                            .name
                            .clone()
                            .expect("Jazz trailing storage fields are named"),
                    )
                }),
        );
        Ok(Some(fields))
    }

    pub(super) fn version_storage_table_for_row(
        &mut self,
        version: &VersionRow,
    ) -> Result<groove::Intern<String>, Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored row schema version alias missing",
            ))?;
        if version.layer() == VersionLayer::Deletion {
            return Ok(groove::Intern::new(
                self.physical_register_table_for_schema(schema_version, version.table())?,
            ));
        }
        Ok(groove::Intern::new(
            physical_history_binding(
                &self.catalogue.catalogue_schemas,
                &self.catalogue.schema_version_aliases,
                &self.catalogue.physical_mappings,
                schema_version,
                version.table(),
            )?
            .storage_table,
        ))
    }

    pub(super) fn version_storage_write_binding(
        &mut self,
        version: &VersionRow,
    ) -> Result<(groove::Intern<String>, groove::records::VersionedRecord), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored row schema version alias missing",
            ))?;
        if version.layer() == VersionLayer::Deletion {
            let table = self.version_storage_table_for_row(version)?;
            return Ok((table, version.groove_record()));
        }

        let binding = physical_history_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.schema_version_aliases,
            &self.catalogue.physical_mappings,
            schema_version,
            version.table(),
        )?;
        let record = OwnedRecord::new(version.record.raw().to_vec(), binding.descriptor);
        Ok((
            groove::Intern::new(binding.storage_table),
            groove::records::VersionedRecord::new(version.schema_version_alias().0, record),
        ))
    }

    pub(super) fn rejected_version_storage_write_binding(
        &self,
        version: &VersionRow,
        logical_record: &OwnedRecord,
    ) -> Result<(groove::Intern<String>, groove::records::VersionedRecord), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "rejected row schema version alias missing",
            ))?;
        let binding = physical_rejected_version_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            version.table(),
        )?;
        let record = OwnedRecord::new(logical_record.raw().to_vec(), binding.descriptor);
        Ok((
            groove::Intern::new(binding.storage_table),
            groove::records::VersionedRecord::new(version.schema_version_alias().0, record),
        ))
    }

    pub(super) fn branch_version_storage_write_binding(
        &mut self,
        version: &VersionRow,
        branch_id: BranchId,
    ) -> Result<(groove::Intern<String>, groove::records::VersionedRecord), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "branch row schema version alias missing",
            ))?;
        let table_id = self.physical_table_id_for_schema(schema_version, version.table())?;
        let (_, record) = self.version_storage_write_binding(version)?;
        Ok((
            groove::Intern::new(physical_branch_version_storage_table_name(
                table_id,
                version.layer(),
                branch_id,
            )),
            record,
        ))
    }
}

pub(super) fn physical_version_storage_tables(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    schema_version_aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    branch_partitions: &BTreeSet<(PhysicalTableId, BranchId)>,
) -> Result<Vec<GrooveTableSchema>, Error> {
    let mut lineages = BTreeMap::<
        PhysicalTableId,
        Vec<(SchemaVersionId, &TableSchema, &TablePhysicalMapping)>,
    >::new();
    for (schema_version, mapping) in physical_mappings {
        let schema = catalogue_schemas
            .get(schema_version)
            .ok_or(Error::InvalidStoredValue(
                "physical mapping schema payload missing",
            ))?;
        for (logical_table, table_mapping) in &mapping.tables {
            let table = schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == *logical_table)
                .ok_or(Error::InvalidStoredValue(
                    "physical mapping logical table missing",
                ))?;
            lineages.entry(table_mapping.table_id).or_default().push((
                *schema_version,
                table,
                table_mapping,
            ));
        }
    }

    let mut tables = Vec::with_capacity(lineages.len() * 7);
    for (table_id, variants) in lineages {
        let (_, template_table, _) = variants
            .first()
            .ok_or(Error::InvalidStoredValue("physical history lineage empty"))?;
        let template = template_table.history_storage_table();
        let system_columns = template
            .columns
            .iter()
            .take(HistoryRowRecord::USER_CELLS)
            .cloned()
            .collect::<Vec<_>>();
        let trailing_history_columns = template
            .columns
            .iter()
            .skip(HistoryRowRecord::USER_CELLS + template_table.columns.len())
            .cloned()
            .collect::<Vec<_>>();
        let mut physical_columns = BTreeMap::new();
        for (_, logical_table, mapping) in &variants {
            for column in &logical_table.columns {
                let column_id =
                    mapping
                        .columns
                        .get(&column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical history column mapping missing",
                        ))?;
                let storage_type = column.column_type.clone().nullable();
                if let Some(existing) = physical_columns.insert(column_id, storage_type.clone())
                    && existing != storage_type
                {
                    return Err(Error::InvalidStoredValue(
                        "physical history column type mismatch",
                    ));
                }
            }
        }
        let columns = system_columns
            .into_iter()
            .chain(physical_columns.iter().map(|(column_id, column_type)| {
                GrooveColumnSchema::new(physical_user_column_field(*column_id), column_type.clone())
            }))
            .chain(trailing_history_columns);
        let mut physical = GrooveTableSchema::new(physical_history_table_name(table_id), columns);
        physical.primary_key = template.primary_key.clone();
        physical.indices = template.indices.clone();
        let mut register = template_table.register_storage_table();
        register.name = physical_register_table_name(table_id);

        let logical_global_tables = template_table.global_current_storage_tables();
        let current_system_columns = logical_global_tables[0]
            .columns
            .iter()
            .take(GlobalCurrentRowRecord::USER_CELLS)
            .cloned()
            .collect::<Vec<_>>();
        let current_trailing_columns = logical_global_tables[0]
            .columns
            .iter()
            .skip(GlobalCurrentRowRecord::USER_CELLS + template_table.columns.len())
            .cloned()
            .collect::<Vec<_>>();
        let current_columns = || {
            current_system_columns
                .iter()
                .cloned()
                .chain(physical_columns.iter().map(|(column_id, column_type)| {
                    GrooveColumnSchema::new(
                        physical_user_column_field(*column_id),
                        column_type.clone(),
                    )
                }))
                .chain(current_trailing_columns.iter().cloned())
        };
        let mut physical_global = GrooveTableSchema::new(
            physical_global_current_table_name(table_id),
            current_columns(),
        );
        physical_global.primary_key = logical_global_tables[0].primary_key.clone();
        let indexed_columns = variants
            .iter()
            .flat_map(|(_, logical_table, mapping)| {
                logical_table
                    .global_current_indexed_columns()
                    .into_iter()
                    .filter_map(|column| mapping.columns.get(&column).copied())
            })
            .collect::<BTreeSet<_>>();
        for column_id in indexed_columns {
            physical_global = physical_global.with_index(GrooveIndexSchema::new(
                physical_current_index_name(column_id),
                [physical_user_column_field(column_id)],
            ));
        }
        let mut register_global = logical_global_tables[1].clone();
        register_global.name = physical_register_global_current_table_name(table_id);

        let logical_ahead_tables = template_table.ahead_current_storage_tables();
        let mut physical_ahead = GrooveTableSchema::new(
            physical_ahead_current_table_name(table_id),
            current_columns(),
        );
        physical_ahead.primary_key = logical_ahead_tables[0].primary_key.clone();
        physical_ahead.indices = logical_ahead_tables[0].indices.clone();
        let mut register_ahead = logical_ahead_tables[1].clone();
        register_ahead.name = physical_register_ahead_current_table_name(table_id);

        let rejected_template = template_table.rejected_versions_storage_table();
        let rejected_system_columns = rejected_template
            .columns
            .iter()
            .take(RejectedVersionRowRecord::USER_CELLS)
            .cloned();
        let rejected_columns = rejected_system_columns.chain(physical_columns.iter().map(
            |(column_id, column_type)| {
                GrooveColumnSchema::new(physical_user_column_field(*column_id), column_type.clone())
            },
        ));
        let mut rejected = GrooveTableSchema::new(
            physical_rejected_versions_table_name(table_id),
            rejected_columns,
        );
        rejected.primary_key = rejected_template.primary_key.clone();

        let mut layouts_by_alias = BTreeMap::new();
        let mut current_layouts_by_alias = BTreeMap::new();
        let mut rejected_layouts_by_alias = BTreeMap::new();
        for (schema_version, logical_table, mapping) in &variants {
            let alias = schema_version_aliases.get(&schema_version).copied().ok_or(
                Error::InvalidStoredValue("physical history schema alias missing"),
            )?;
            let fields = physical_history_field_names(logical_table, mapping)?;
            if layouts_by_alias.insert(alias, fields).is_some() {
                return Err(Error::InvalidStoredValue(
                    "schema maps multiple logical tables to one physical lineage",
                ));
            }
            let fields = physical_current_field_names(logical_table, mapping)?;
            if current_layouts_by_alias.insert(alias, fields).is_some() {
                return Err(Error::InvalidStoredValue(
                    "schema maps multiple logical tables to one physical lineage",
                ));
            }
            let fields = physical_rejected_version_field_names(logical_table, mapping)?;
            if rejected_layouts_by_alias.insert(alias, fields).is_some() {
                return Err(Error::InvalidStoredValue(
                    "schema maps multiple logical tables to one physical lineage",
                ));
            }
        }
        for (alias, fields) in layouts_by_alias {
            physical = physical.with_schema_version(alias.0, fields);
        }
        for (alias, fields) in current_layouts_by_alias {
            physical_global = physical_global.with_schema_version(alias.0, fields.clone());
            physical_ahead = physical_ahead.with_schema_version(alias.0, fields);
        }
        for (alias, fields) in rejected_layouts_by_alias {
            rejected = rejected.with_schema_version(alias.0, fields);
        }
        for (_, branch_id) in branch_partitions
            .iter()
            .filter(|(candidate, _)| *candidate == table_id)
        {
            let mut branch_history = physical.clone();
            branch_history.name = physical_branch_history_table_name(table_id, *branch_id);
            let mut branch_register = register.clone();
            branch_register.name = physical_branch_register_table_name(table_id, *branch_id);
            tables.push(branch_history);
            tables.push(branch_register);
        }
        tables.push(physical);
        tables.push(register);
        tables.push(physical_global);
        tables.push(register_global);
        tables.push(physical_ahead);
        tables.push(register_ahead);
        tables.push(rejected);
    }
    Ok(tables)
}

fn physical_history_descriptor(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
    _alias: SchemaVersionAlias,
) -> Result<records::RecordDescriptor, Error> {
    let logical_descriptor = table.history_storage_table().record_schema();
    let physical_names = physical_history_field_names(table, mapping)?;
    if logical_descriptor.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical history descriptor width mismatch",
        ));
    }
    Ok(records::RecordDescriptor::new(
        physical_names.into_iter().zip(
            logical_descriptor
                .fields()
                .iter()
                .map(|field| field.value_type.clone()),
        ),
    ))
}

fn physical_current_descriptor(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<records::RecordDescriptor, Error> {
    let logical_descriptor = table.global_current_storage_tables()[0].record_schema();
    let physical_names = physical_current_field_names(table, mapping)?;
    if logical_descriptor.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical current descriptor width mismatch",
        ));
    }
    Ok(records::RecordDescriptor::new(
        physical_names.into_iter().zip(
            logical_descriptor
                .fields()
                .iter()
                .map(|field| field.value_type.clone()),
        ),
    ))
}

fn physical_rejected_version_descriptor(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<records::RecordDescriptor, Error> {
    let logical_descriptor = table.rejected_versions_storage_table().record_schema();
    let physical_names = physical_rejected_version_field_names(table, mapping)?;
    if logical_descriptor.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical rejected-version descriptor width mismatch",
        ));
    }
    Ok(records::RecordDescriptor::new(
        physical_names.into_iter().zip(
            logical_descriptor
                .fields()
                .iter()
                .map(|field| field.value_type.clone()),
        ),
    ))
}

fn physical_history_field_names(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<Vec<String>, Error> {
    let logical_descriptor = table.history_storage_table().record_schema();
    let mut fields = logical_descriptor
        .fields()
        .iter()
        .take(HistoryRowRecord::USER_CELLS)
        .map(|field| {
            field.name.clone().ok_or(Error::InvalidStoredValue(
                "physical history system field unnamed",
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for column in &table.columns {
        let column_id =
            mapping
                .columns
                .get(&column.name)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical history column mapping missing",
                ))?;
        fields.push(physical_user_column_field(column_id));
    }
    fields.extend(
        logical_descriptor
            .fields()
            .iter()
            .skip(HistoryRowRecord::USER_CELLS + table.columns.len())
            .map(|field| {
                field.name.clone().ok_or(Error::InvalidStoredValue(
                    "physical history trailing field unnamed",
                ))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(fields)
}

fn physical_current_field_names(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<Vec<String>, Error> {
    let logical_descriptor = table.global_current_storage_tables()[0].record_schema();
    let mut fields = logical_descriptor
        .fields()
        .iter()
        .take(GlobalCurrentRowRecord::USER_CELLS)
        .map(|field| {
            field.name.clone().ok_or(Error::InvalidStoredValue(
                "physical current system field unnamed",
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for column in &table.columns {
        let column_id =
            mapping
                .columns
                .get(&column.name)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical current column mapping missing",
                ))?;
        fields.push(physical_user_column_field(column_id));
    }
    fields.extend(
        logical_descriptor
            .fields()
            .iter()
            .skip(GlobalCurrentRowRecord::USER_CELLS + table.columns.len())
            .map(|field| {
                field.name.clone().ok_or(Error::InvalidStoredValue(
                    "physical current trailing field unnamed",
                ))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(fields)
}

fn physical_rejected_version_field_names(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<Vec<String>, Error> {
    let logical_descriptor = table.rejected_versions_storage_table().record_schema();
    let mut fields = logical_descriptor
        .fields()
        .iter()
        .take(RejectedVersionRowRecord::USER_CELLS)
        .map(|field| {
            field.name.clone().ok_or(Error::InvalidStoredValue(
                "physical rejected-version system field unnamed",
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for column in &table.columns {
        let column_id =
            mapping
                .columns
                .get(&column.name)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical rejected-version column mapping missing",
                ))?;
        fields.push(physical_user_column_field(column_id));
    }
    Ok(fields)
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
