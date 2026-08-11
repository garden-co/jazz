//! Durable physical identity metadata and Groove history-table lowering.

use super::*;
use crate::ids::{PhysicalColumnId, PhysicalTableId};
use groove::schema::{
    ColumnSchema as GrooveColumnSchema, IndexSchema as GrooveIndexSchema,
    TableSchema as GrooveTableSchema, TableVariantField as GrooveTableVariantField,
};

/// Lower Jazz's durable schema alias into Groove's deliberately smaller,
/// table-local union-case space. A future user-declared top-level union will
/// allocate a distinct tag for each `(schema alias, user case)` pair here.
fn groove_variant_tag(alias: SchemaVersionAlias) -> Result<u32, Error> {
    u32::try_from(alias.0)
        .map_err(|_| Error::InvalidStoredValue("physical table variant tag exhausted"))
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct SchemaPhysicalMapping {
    pub(super) tables: BTreeMap<String, TablePhysicalMapping>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct TablePhysicalMapping {
    pub(super) table_id: PhysicalTableId,
    pub(super) columns: BTreeMap<String, PhysicalColumnId>,
    /// The one durable hidden Groove row case for this Jazz layout.
    #[serde(default)]
    pub(super) variant_cases: Vec<PhysicalVariantCase>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct PhysicalVariantCase {
    pub(super) tag: u32,
    /// Logical fields physically present in this dense case payload.
    pub(super) fields: BTreeSet<String>,
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
            TablePhysicalMapping {
                table_id,
                columns,
                variant_cases: Vec::new(),
            },
        );
    }
    Ok(SchemaPhysicalMapping { tables })
}

/// Allocate and retain the single hidden Groove row case for one Jazz layout.
/// Allocation consults the whole physical-table lineage; nested column enums
/// have their own registries and never multiply these row cases.
pub(super) fn allocate_physical_variant_cases(
    mappings: &mut BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    schema_version: SchemaVersionId,
    logical_table: &str,
    fields: BTreeSet<String>,
) -> Result<Vec<PhysicalVariantCase>, Error> {
    let target = mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "variant-case target physical mapping missing",
        ))?;
    let table_id = target.table_id;
    let target_columns = target.columns.keys().cloned().collect::<BTreeSet<_>>();
    if !fields.is_subset(&target_columns) {
        return Err(Error::InvalidStoredValue(
            "physical table variant contains an unknown field",
        ));
    }
    if let Some(existing) = target.variant_cases.first() {
        if target.variant_cases.len() != 1 || existing.fields != fields {
            return Err(Error::InvalidStoredValue(
                "physical table variant case definition changed",
            ));
        }
        return Ok(target.variant_cases.clone());
    }

    let mut used = BTreeMap::<u32, SchemaVersionId>::new();
    for (candidate_schema, mapping) in mappings.iter() {
        let Some((_, table)) = mapping
            .tables
            .iter()
            .find(|(_, table)| table.table_id == table_id)
        else {
            continue;
        };
        if table.variant_cases.is_empty() {
            // The target mapping is still provisional: its alias has never
            // been written as a row tag, and is replaced by the cases below.
            if *candidate_schema == schema_version {
                continue;
            }
            let alias = aliases
                .get(candidate_schema)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "variant-case schema alias missing",
                ))?;
            let tag = groove_variant_tag(alias)?;
            if used.insert(tag, *candidate_schema).is_some() {
                return Err(Error::InvalidStoredValue(
                    "physical table variant tag collision",
                ));
            }
        } else if table.variant_cases.len() != 1
            || used
                .insert(table.variant_cases[0].tag, *candidate_schema)
                .is_some()
        {
            return Err(Error::InvalidStoredValue(
                "physical table variant tag collision",
            ));
        }
    }
    let tag = groove_variant_tag(*aliases.get(&schema_version).ok_or(
        Error::InvalidStoredValue("variant-case schema alias missing"),
    )?)?;
    if used.contains_key(&tag) {
        return Err(Error::InvalidStoredValue(
            "physical table variant tag collision",
        ));
    }
    let allocated = vec![PhysicalVariantCase { tag, fields }];
    mappings
        .get_mut(&schema_version)
        .and_then(|mapping| mapping.tables.get_mut(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "variant-case target physical mapping missing",
        ))?
        .variant_cases = allocated.clone();
    Ok(allocated)
}

pub(super) fn validate_physical_variant_cases(
    mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
) -> Result<(), Error> {
    let mut by_table = BTreeMap::<PhysicalTableId, BTreeMap<u32, SchemaVersionId>>::new();
    for (schema_version, mapping) in mappings {
        for table in mapping.tables.values() {
            let tag = if table.variant_cases.is_empty() {
                groove_variant_tag(*aliases.get(schema_version).ok_or(
                    Error::InvalidStoredValue("variant-case schema alias missing"),
                )?)?
            } else {
                if table.variant_cases.len() != 1 {
                    return Err(Error::InvalidStoredValue(
                        "physical table layout has multiple row cases",
                    ));
                }
                table.variant_cases[0].tag
            };
            let tags = by_table.entry(table.table_id).or_default();
            if tags.insert(tag, *schema_version).is_some() {
                return Err(Error::InvalidStoredValue(
                    "physical table variant tag collision",
                ));
            }
        }
    }
    Ok(())
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
        Ok(GraphBuilder::variant_source(
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
        Ok(GraphBuilder::variant_source_scan(
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
        Ok(GraphBuilder::variant_source(
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
            let logical_output = target_table.history_storage_table().record_schema();
            let physical_names = physical_history_field_names(&target_table, &target_mapping)?;
            let output = widened_projection_descriptor(
                &logical_output,
                &physical_names,
                self.database.table_schema(&storage_table)?,
            )?;
            self.database
                .define_variant_projection(&storage_table, &projection_target, output)?;

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
                let cases = if source_mapping.variant_cases.is_empty() {
                    vec![(groove_variant_tag(source_alias)?, None)]
                } else {
                    source_mapping
                        .variant_cases
                        .iter()
                        .map(|case| (case.tag, Some(&case.fields)))
                        .collect()
                };
                for (tag, present) in cases {
                    let Some(fields) = self.physical_history_projection_case(
                        source_schema,
                        &source_table_name,
                        &source_mapping,
                        target_schema,
                        &target_table_name,
                        present,
                    )?
                    else {
                        self.database.register_variant_ignore_case(
                            &storage_table,
                            &projection_target,
                            tag,
                        )?;
                        continue;
                    };
                    self.database.register_variant_case(
                        &storage_table,
                        &projection_target,
                        tag,
                        fields,
                    )?;
                }
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
                let logical_output =
                    target_table.global_current_storage_tables()[0].record_schema();
                let physical_names = physical_current_field_names(&target_table, &target_mapping)?;
                let output = widened_projection_descriptor(
                    &logical_output,
                    &physical_names,
                    self.database.table_schema(storage_table)?,
                )?;
                self.database.define_variant_projection(
                    storage_table,
                    &projection_target,
                    output,
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
                let cases = if source_mapping.variant_cases.is_empty() {
                    vec![(groove_variant_tag(source_alias)?, None)]
                } else {
                    source_mapping
                        .variant_cases
                        .iter()
                        .map(|case| (case.tag, Some(&case.fields)))
                        .collect()
                };
                for (tag, present) in cases {
                    let fields = self.physical_current_projection_case(
                        source_schema,
                        &source_table_name,
                        &source_mapping,
                        target_schema,
                        &target_table_name,
                        present,
                    )?;
                    for storage_table in &storage_tables {
                        if let Some(fields) = fields.clone() {
                            self.database.register_variant_case(
                                storage_table,
                                &projection_target,
                                tag,
                                fields,
                            )?;
                        } else {
                            self.database.register_variant_ignore_case(
                                storage_table,
                                &projection_target,
                                tag,
                            )?;
                        }
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
            self.database
                .evolve_table_variant_registries(&desired.name, &desired.columns)?;
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
            for schema_version in desired.variants {
                if existing.variant(schema_version.tag).is_some() {
                    continue;
                }
                self.database.register_table_variant_with_columns(
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

    fn physical_history_projection_case(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        present: Option<&BTreeSet<String>>,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        self.physical_content_projection_case(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            ContentProjectionShape::History,
            present,
        )
    }

    fn physical_current_projection_case(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        present: Option<&BTreeSet<String>>,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        self.physical_content_projection_case(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            ContentProjectionShape::Current,
            present,
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
        present: Option<&BTreeSet<String>>,
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
            .filter(|column| present.is_none_or(|present| present.contains(&column.name)))
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
            let projection = match cells.remove(&column.name) {
                Some(projection) => projection,
                None if present.is_some() => CellProjection::Literal(Value::Nullable(None)),
                None => return Ok(None),
            };
            match projection {
                CellProjection::Field(source) => {
                    fields.push(ProjectField::renamed(source, output));
                }
                CellProjection::Literal(Value::Nullable(None)) => {
                    fields.push(ProjectField::literal_typed(
                        output,
                        Value::Nullable(None),
                        records::ValueType::Nullable(Box::new(column.column_type.clone())),
                    ));
                }
                CellProjection::Literal(value) => {
                    fields.push(ProjectField::literal_typed(
                        output,
                        Value::Nullable(Some(Box::new(value))),
                        records::ValueType::Nullable(Box::new(column.column_type.clone())),
                    ));
                }
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
    ) -> Result<(groove::Intern<String>, groove::records::VariantRecord), Error> {
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
        let source_table = self.table_in_schema(version.table(), schema_version)?;
        let source_mapping = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(version.table()))
            .ok_or(Error::InvalidStoredValue(
                "physical history table mapping missing",
            ))?;
        let descriptor = physical_write_descriptor(
            &source_table.history_storage_table().record_schema(),
            &physical_history_field_names(&source_table, source_mapping)?,
            self.database.table_schema(&binding.storage_table)?,
        )?;
        let record = OwnedRecord::new(version.record.raw().to_vec(), descriptor);
        Ok((
            groove::Intern::new(binding.storage_table),
            groove::records::VariantRecord::new(
                groove_variant_tag(version.schema_version_alias())?,
                record,
            ),
        ))
    }

    pub(super) fn rejected_version_storage_write_binding(
        &self,
        version: &VersionRow,
        logical_record: &OwnedRecord,
    ) -> Result<(groove::Intern<String>, groove::records::VariantRecord), Error> {
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
            groove::records::VariantRecord::new(
                groove_variant_tag(version.schema_version_alias())?,
                record,
            ),
        ))
    }

    pub(super) fn branch_version_storage_write_binding(
        &mut self,
        version: &VersionRow,
        branch_id: BranchId,
    ) -> Result<(groove::Intern<String>, groove::records::VariantRecord), Error> {
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

fn widened_projection_descriptor(
    logical: &records::RecordDescriptor,
    physical_names: &[String],
    physical_table: &GrooveTableSchema,
) -> Result<records::RecordDescriptor, Error> {
    if logical.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical projection descriptor width mismatch",
        ));
    }
    Ok(records::RecordDescriptor::new(
        logical
            .fields()
            .iter()
            .zip(physical_names)
            .map(|(field, name)| {
                let physical = physical_table
                    .columns
                    .iter()
                    .find(|column| column.name == *name)
                    .ok_or(Error::InvalidStoredValue(
                        "physical projection column missing",
                    ))?;
                Ok((
                    field.name.clone().ok_or(Error::InvalidStoredValue(
                        "physical projection logical field unnamed",
                    ))?,
                    widen_projection_value_type(&field.value_type, &physical.column_type),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?,
    ))
}

/// The write-side counterpart of `widened_projection_descriptor`: a physical
/// variant record must use the table's physical field names as well as its
/// widened value types. Keeping logical names here makes Groove correctly
/// reject the record as a descriptor mismatch, but too late to explain the
/// authored-to-physical enum boundary.
fn physical_write_descriptor(
    logical: &records::RecordDescriptor,
    physical_names: &[String],
    physical_table: &GrooveTableSchema,
) -> Result<records::RecordDescriptor, Error> {
    if logical.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical write descriptor width mismatch",
        ));
    }
    Ok(records::RecordDescriptor::new(
        logical
            .fields()
            .iter()
            .zip(physical_names)
            .map(|(logical, name)| {
                let physical = physical_table
                    .columns
                    .iter()
                    .find(|column| column.name == *name)
                    .ok_or(Error::InvalidStoredValue("physical write column missing"))?;
                Ok((
                    name.clone(),
                    widen_projection_value_type(&logical.value_type, &physical.column_type),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?,
    ))
}

fn widen_projection_value_type(
    logical: &records::ValueType,
    physical: &records::ValueType,
) -> records::ValueType {
    use records::ValueType;
    match (logical, physical) {
        (ValueType::EnumTag(_), ValueType::EnumTag(schema)) => ValueType::EnumTag(schema.clone()),
        (ValueType::Enum(_), ValueType::Enum(schema)) => ValueType::Enum(schema.clone()),
        (logical, ValueType::Nullable(physical)) if !matches!(logical, ValueType::Nullable(_)) => {
            widen_projection_value_type(logical, physical)
        }
        (ValueType::Nullable(logical), ValueType::Nullable(physical)) => {
            ValueType::Nullable(Box::new(widen_projection_value_type(logical, physical)))
        }
        (ValueType::Array(logical), ValueType::Array(physical)) => {
            ValueType::Array(Box::new(widen_projection_value_type(logical, physical)))
        }
        (ValueType::Tuple(logical), ValueType::Tuple(physical))
            if logical.len() == physical.len() =>
        {
            ValueType::Tuple(
                logical
                    .iter()
                    .zip(physical)
                    .map(|(logical, physical)| widen_projection_value_type(logical, physical))
                    .collect(),
            )
        }
        _ => logical.clone(),
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
                let storage_type = column
                    .column_type
                    .clone()
                    .nullable()
                    .rebind_variant_registries(&format!("physical-column/{}", column_id.0));
                if let Some(existing) = physical_columns.get_mut(&column_id) {
                    *existing = merge_physical_value_type(existing, &storage_type)?;
                } else {
                    physical_columns.insert(column_id, storage_type);
                }
            }
        }
        let columns = system_columns
            .into_iter()
            .chain(physical_columns.iter().map(|(column_id, column_type)| {
                GrooveColumnSchema::new(physical_user_column_field(*column_id), column_type.clone())
            }))
            .chain(trailing_history_columns);
        let mut physical = GrooveTableSchema::new_with_bound_registries(
            physical_history_table_name(table_id),
            columns,
        );
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
        let mut physical_global = GrooveTableSchema::new_with_bound_registries(
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
        let mut physical_ahead = GrooveTableSchema::new_with_bound_registries(
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
        let mut rejected = GrooveTableSchema::new_with_bound_registries(
            physical_rejected_versions_table_name(table_id),
            rejected_columns,
        );
        rejected.primary_key = rejected_template.primary_key.clone();

        let mut layouts_by_tag = BTreeMap::new();
        let mut current_layouts_by_tag = BTreeMap::new();
        let mut rejected_layouts_by_tag = BTreeMap::new();
        for (schema_version, logical_table, mapping) in &variants {
            let alias = schema_version_aliases.get(&schema_version).copied().ok_or(
                Error::InvalidStoredValue("physical history schema alias missing"),
            )?;
            let cases = if mapping.variant_cases.is_empty() {
                vec![(groove_variant_tag(alias)?, None)]
            } else {
                mapping
                    .variant_cases
                    .iter()
                    .map(|case| (case.tag, Some(&case.fields)))
                    .collect()
            };
            for (tag, fields) in cases {
                let history =
                    physical_history_field_names_for_case(logical_table, mapping, fields)?;
                if layouts_by_tag.insert(tag, history).is_some() {
                    return Err(Error::InvalidStoredValue(
                        "physical table variant tag collision",
                    ));
                }
                let current =
                    physical_current_field_names_for_case(logical_table, mapping, fields)?;
                if current_layouts_by_tag.insert(tag, current).is_some() {
                    return Err(Error::InvalidStoredValue(
                        "physical table variant tag collision",
                    ));
                }
                let rejected =
                    physical_rejected_version_field_names_for_case(logical_table, mapping, fields)?;
                if rejected_layouts_by_tag.insert(tag, rejected).is_some() {
                    return Err(Error::InvalidStoredValue(
                        "physical table variant tag collision",
                    ));
                }
            }
        }
        for (tag, fields) in layouts_by_tag {
            let payload = variant_payload_fields_for_names(&physical, &fields)?;
            physical = physical.with_variant_payload(tag, payload);
        }
        for (tag, fields) in current_layouts_by_tag {
            let global_payload = variant_payload_fields_for_names(&physical_global, &fields)?;
            physical_global = physical_global.with_variant_payload(tag, global_payload);
            let ahead_payload = variant_payload_fields_for_names(&physical_ahead, &fields)?;
            physical_ahead = physical_ahead.with_variant_payload(tag, ahead_payload);
        }
        for (tag, fields) in rejected_layouts_by_tag {
            let payload = variant_payload_fields_for_names(&rejected, &fields)?;
            rejected = rejected.with_variant_payload(tag, payload);
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

fn variant_payload_fields_for_names(
    table: &GrooveTableSchema,
    names: &[String],
) -> Result<Vec<GrooveTableVariantField>, Error> {
    names
        .iter()
        .map(|name| {
            let column = table
                .columns
                .iter()
                .find(|column| column.name == *name)
                .ok_or(Error::InvalidStoredValue(
                    "physical variant shared column missing",
                ))?;
            Ok(GrooveTableVariantField::shared(
                name.clone(),
                column.column_type.clone(),
                name.clone(),
            ))
        })
        .collect()
}

fn merge_physical_record_descriptor(
    existing: &records::RecordDescriptor,
    incoming: &records::RecordDescriptor,
) -> Result<records::RecordDescriptor, Error> {
    if existing.fields().len() != incoming.fields().len() {
        return Err(Error::InvalidStoredValue(
            "physical variant payload descriptor width changed",
        ));
    }
    let mut fields = Vec::with_capacity(existing.fields().len());
    for (left, right) in existing.fields().iter().zip(incoming.fields()) {
        if left.name != right.name {
            return Err(Error::InvalidStoredValue(
                "physical variant payload field identity changed",
            ));
        }
        fields.push((
            left.name.clone().ok_or(Error::InvalidStoredValue(
                "physical variant payload field unnamed",
            ))?,
            merge_physical_value_type(&left.value_type, &right.value_type)?,
        ));
    }
    Ok(records::RecordDescriptor::new(fields))
}

/// Merge two snapshots of one physical value occurrence. Registry identity,
/// rather than structural descriptor equality, is authoritative for enums and
/// enums; the older declaration must be an exact prefix of the newer one.
fn merge_physical_value_type(
    existing: &records::ValueType,
    incoming: &records::ValueType,
) -> Result<records::ValueType, Error> {
    use records::ValueType;
    match (existing, incoming) {
        (ValueType::EnumTag(left), ValueType::EnumTag(right))
            if left.registry_id() == right.registry_id() =>
        {
            let (shorter, longer) = if left.variants.len() <= right.variants.len() {
                (&left.variants, right)
            } else {
                (&right.variants, left)
            };
            if !longer.variants.starts_with(shorter) {
                return Err(Error::InvalidStoredValue(
                    "physical enum registry changed non-additively",
                ));
            }
            Ok(ValueType::EnumTag(longer.clone()))
        }
        (ValueType::Enum(left), ValueType::Enum(right))
            if left.registry_id == right.registry_id =>
        {
            let max_len = left.cases.len().max(right.cases.len());
            let mut cases = Vec::with_capacity(max_len);
            for index in 0..max_len {
                match (left.cases.get(index), right.cases.get(index)) {
                    (Some(a), Some(b)) => {
                        if a.name != b.name {
                            return Err(Error::InvalidStoredValue(
                                "physical enum registry changed non-additively",
                            ));
                        }
                        cases.push(records::EnumCase::new(
                            a.name.clone(),
                            merge_physical_record_descriptor(&a.payload, &b.payload)?,
                        ));
                    }
                    (Some(case), None) | (None, Some(case)) => cases.push(case.clone()),
                    (None, None) => unreachable!(),
                }
            }
            Ok(ValueType::Enum(Box::new(
                records::EnumSchema::new(right.name.clone(), cases)
                    .map_err(|_| Error::InvalidStoredValue("invalid physical enum registry"))?
                    .with_registry_id(left.registry_id),
            )))
        }
        (ValueType::Tuple(left), ValueType::Tuple(right)) if left.len() == right.len() => {
            Ok(ValueType::Tuple(
                left.iter()
                    .zip(right)
                    .map(|(a, b)| merge_physical_value_type(a, b))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (ValueType::Array(left), ValueType::Array(right)) => Ok(ValueType::Array(Box::new(
            merge_physical_value_type(left, right)?,
        ))),
        (ValueType::Nullable(left), ValueType::Nullable(right)) => Ok(ValueType::Nullable(
            Box::new(merge_physical_value_type(left, right)?),
        )),
        (ValueType::Record(left), ValueType::Record(right)) => Ok(ValueType::Record(Box::new(
            merge_physical_record_descriptor(left, right)?,
        ))),
        _ if existing == incoming => Ok(existing.clone()),
        _ => Err(Error::InvalidStoredValue(
            "physical history column type mismatch",
        )),
    }
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
    Ok(rebind_physical_user_registries(
        records::RecordDescriptor::new(
            physical_names.into_iter().zip(
                logical_descriptor
                    .fields()
                    .iter()
                    .map(|field| field.value_type.clone()),
            ),
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
    Ok(rebind_physical_user_registries(
        records::RecordDescriptor::new(
            physical_names.into_iter().zip(
                logical_descriptor
                    .fields()
                    .iter()
                    .map(|field| field.value_type.clone()),
            ),
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
    Ok(rebind_physical_user_registries(
        records::RecordDescriptor::new(
            physical_names.into_iter().zip(
                logical_descriptor
                    .fields()
                    .iter()
                    .map(|field| field.value_type.clone()),
            ),
        ),
    ))
}

fn rebind_physical_user_registries(
    descriptor: records::RecordDescriptor,
) -> records::RecordDescriptor {
    records::RecordDescriptor::new(descriptor.fields().iter().map(|field| {
        let name = field.name.clone().expect("physical descriptors are named");
        let value_type = name
            .strip_prefix("user_")
            .and_then(|id| id.parse::<u64>().ok())
            .map(|id| {
                field
                    .value_type
                    .clone()
                    .rebind_variant_registries(&format!("physical-column/{id}"))
            })
            .unwrap_or_else(|| field.value_type.clone());
        (name, value_type)
    }))
}

fn physical_history_field_names(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<Vec<String>, Error> {
    physical_history_field_names_for_case(table, mapping, None)
}

fn physical_history_field_names_for_case(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
    present: Option<&BTreeSet<String>>,
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
        if present.is_some_and(|present| !present.contains(&column.name)) {
            continue;
        }
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
    physical_current_field_names_for_case(table, mapping, None)
}

fn physical_current_field_names_for_case(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
    present: Option<&BTreeSet<String>>,
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
        if present.is_some_and(|present| !present.contains(&column.name)) {
            continue;
        }
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
    physical_rejected_version_field_names_for_case(table, mapping, None)
}

fn physical_rejected_version_field_names_for_case(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
    present: Option<&BTreeSet<String>>,
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
        if present.is_some_and(|present| !present.contains(&column.name)) {
            continue;
        }
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

    physical_value_epoch_is_compatible(&source_column.column_type, &target_column.column_type)
        && source_column.large_value == target_column.large_value
        && source_column.text_merge_spec == target_column.text_merge_spec
        && source_table.merge_strategy(source_column_name)
            == target_table.merge_strategy(target_column_name)
}

pub(super) fn physical_value_epoch_is_compatible(
    source: &records::ValueType,
    target: &records::ValueType,
) -> bool {
    use records::ValueType;
    match (source, target) {
        (ValueType::EnumTag(left), ValueType::EnumTag(right)) => {
            right.variants.starts_with(&left.variants)
        }
        (ValueType::Enum(left), ValueType::Enum(right)) => {
            right.cases.len() >= left.cases.len()
                && left.cases.iter().zip(&right.cases).all(|(a, b)| {
                    a.name == b.name && physical_record_epoch_is_compatible(&a.payload, &b.payload)
                })
        }
        (ValueType::Tuple(left), ValueType::Tuple(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(a, b)| physical_value_epoch_is_compatible(a, b))
        }
        (ValueType::Array(left), ValueType::Array(right))
        | (ValueType::Nullable(left), ValueType::Nullable(right)) => {
            physical_value_epoch_is_compatible(left, right)
        }
        (ValueType::Record(left), ValueType::Record(right)) => {
            physical_record_epoch_is_compatible(left, right)
        }
        _ => source == target,
    }
}

fn physical_record_epoch_is_compatible(
    source: &records::RecordDescriptor,
    target: &records::RecordDescriptor,
) -> bool {
    source.fields().len() == target.fields().len()
        && source.fields().iter().zip(target.fields()).all(|(a, b)| {
            a.name == b.name && physical_value_epoch_is_compatible(&a.value_type, &b.value_type)
        })
}

#[cfg(test)]
mod variant_case_tests {
    use super::*;

    fn schema(byte: u8) -> SchemaVersionId {
        SchemaVersionId(uuid::Uuid::from_bytes([byte; 16]))
    }

    fn mapping(table_id: u64, columns: &[(&str, u64)]) -> SchemaPhysicalMapping {
        SchemaPhysicalMapping {
            tables: BTreeMap::from([(
                "entries".to_owned(),
                TablePhysicalMapping {
                    table_id: PhysicalTableId(table_id),
                    columns: columns
                        .iter()
                        .map(|(name, id)| (name.to_string(), PhysicalColumnId(*id)))
                        .collect(),
                    variant_cases: Vec::new(),
                },
            )]),
        }
    }

    fn fields(edited: bool) -> BTreeSet<String> {
        let mut fields = BTreeSet::from(["id".to_owned(), "body".to_owned()]);
        if edited {
            fields.insert("edited".to_owned());
        }
        fields
    }

    #[test]
    fn schema_layout_cases_allocate_durably_without_collisions() {
        let v1 = schema(1);
        let v2 = schema(2);
        let aliases = BTreeMap::from([(v1, SchemaVersionAlias(1)), (v2, SchemaVersionAlias(2))]);
        let mut mappings =
            BTreeMap::from([(v1, mapping(7, &[("id", 1), ("body", 2), ("url", 3)]))]);

        let first =
            allocate_physical_variant_cases(&mut mappings, &aliases, v1, "entries", fields(false))
                .unwrap();
        mappings.insert(
            v2,
            mapping(7, &[("id", 1), ("body", 2), ("url", 3), ("edited", 4)]),
        );
        let second =
            allocate_physical_variant_cases(&mut mappings, &aliases, v2, "entries", fields(true))
                .unwrap();
        assert_eq!(first.iter().map(|case| case.tag).collect::<Vec<_>>(), [1]);
        assert_eq!(second.iter().map(|case| case.tag).collect::<Vec<_>>(), [2]);
        validate_physical_variant_cases(&mappings, &aliases).unwrap();

        // The mapping is the payload durably written in jazz_schema_versions;
        // a JSON round trip models close/reopen of the catalogue row.
        let encoded = serde_json::to_vec(&mappings).unwrap();
        let reopened: BTreeMap<SchemaVersionId, SchemaPhysicalMapping> =
            serde_json::from_slice(&encoded).unwrap();
        assert_eq!(reopened, mappings);
        validate_physical_variant_cases(&reopened, &aliases).unwrap();
    }

    #[test]
    fn reopen_validation_rejects_a_cross_layout_tag_collision() {
        let v1 = schema(1);
        let v2 = schema(2);
        let aliases = BTreeMap::from([(v1, SchemaVersionAlias(1)), (v2, SchemaVersionAlias(2))]);
        let mut first = mapping(7, &[("id", 1)]);
        first.tables.get_mut("entries").unwrap().variant_cases = vec![PhysicalVariantCase {
            tag: 9,
            fields: BTreeSet::from(["id".to_owned()]),
        }];
        let mut second = mapping(7, &[("id", 1)]);
        second.tables.get_mut("entries").unwrap().variant_cases = vec![PhysicalVariantCase {
            tag: 9,
            fields: BTreeSet::from(["id".to_owned()]),
        }];
        let mappings = BTreeMap::from([(v1, first), (v2, second)]);
        assert!(matches!(
            validate_physical_variant_cases(&mappings, &aliases),
            Err(Error::InvalidStoredValue(
                "physical table variant tag collision"
            ))
        ));
    }

    #[test]
    fn nested_enum_epoch_accepts_only_append_only_case_growth() {
        let value_type = |variants: &[&str]| {
            records::ValueType::EnumTag(
                records::ScalarEnumSchema::new("state", variants.iter().copied()).unwrap(),
            )
        };
        let old = value_type(&["new", "done"]);
        assert!(physical_value_epoch_is_compatible(
            &old,
            &value_type(&["new", "done", "archived"]),
        ));
        assert!(!physical_value_epoch_is_compatible(
            &old,
            &value_type(&["done", "new"]),
        ));
        assert!(!physical_value_epoch_is_compatible(
            &old,
            &value_type(&["new"]),
        ));
    }

    #[test]
    fn concurrent_scalar_enum_additions_preserve_schema_qualified_case_identity() {
        // This is deliberately an internal lowering test: the failure happens
        // before a public row can be decoded. Two concurrent authored schemas
        // both use ordinal 2, so accepting the raw tags as one physical tag
        // would alias `archived` and `snoozed`.
        let schema = |variants: &[&str]| {
            records::ValueType::EnumTag(
                records::ScalarEnumSchema::new("status", variants.iter().copied())
                    .unwrap()
                    .with_registry_id(91),
            )
        };
        let archived = schema(&["draft", "published", "archived"]);
        let snoozed = schema(&["draft", "published", "snoozed"]);

        let merged_ab = merge_physical_value_type(&archived, &snoozed)
            .expect("concurrent enum cases must coexist in one physical registry");
        let merged_ba = merge_physical_value_type(&snoozed, &archived)
            .expect("local arrival order must not choose semantic enum order");
        assert_eq!(merged_ab, merged_ba);

        // The implementation must additionally translate authored ordinal 2
        // through a schema-qualified identity at both storage boundaries. This
        // assertion only establishes the prerequisite: the physical registry
        // has distinct slots for the sibling introductions.
        let records::ValueType::EnumTag(registry) = merged_ab else {
            panic!("expected scalar enum registry");
        };
        assert_eq!(registry.variants.len(), 4);
    }
}
