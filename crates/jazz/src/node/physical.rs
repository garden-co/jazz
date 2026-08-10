//! Durable physical identity metadata and Groove history-table lowering.

use super::*;
use crate::ids::{PhysicalColumnId, PhysicalTableId};
use crate::schema::MERGE_HEADS_TABLE;
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
    /// Durable flat Groove cases for this Jazz layout. `user_case = None` is
    /// the ordinary non-union row. For a user top-level union, one entry is
    /// allocated per realizable `(schema layout, user case)` pair.
    #[serde(default)]
    pub(super) variant_cases: Vec<PhysicalVariantCase>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct PhysicalVariantCase {
    pub(super) user_case: Option<String>,
    pub(super) tag: u32,
    /// Logical fields physically present in this dense case payload.
    pub(super) fields: BTreeSet<String>,
    /// Case-local payload fields. Unlike `fields`, these names do not imply a
    /// table-wide identity and may reuse a name with a different type in
    /// another user case. `shared_column` opts a field into stable physical
    /// identity for keys/indices.
    #[serde(default)]
    pub(super) payload_fields: Vec<PhysicalVariantPayloadField>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct PhysicalVariantPayloadField {
    pub(super) name: String,
    pub(super) value_type: records::ValueType,
    pub(super) shared_column: Option<PhysicalColumnId>,
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

/// Allocate and retain the flat Groove cases for one Jazz table layout.
///
/// `cases` is `(portable user-case name, logical fields present)`. Passing one
/// `None` case represents an ordinary non-union table. Allocation consults all
/// schema mappings sharing the physical table lineage, so tags never collide
/// across layout evolution. The caller persists the containing
/// `SchemaPhysicalMapping` in the same catalogue batch as schema admission.
pub(super) fn allocate_physical_variant_cases(
    mappings: &mut BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    schema_version: SchemaVersionId,
    logical_table: &str,
    cases: impl IntoIterator<Item = (Option<String>, BTreeSet<String>)>,
) -> Result<Vec<PhysicalVariantCase>, Error> {
    let target = mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "variant-case target physical mapping missing",
        ))?;
    let table_id = target.table_id;
    let target_columns = target.columns.keys().cloned().collect::<BTreeSet<_>>();
    let existing_target = target
        .variant_cases
        .iter()
        .map(|case| (case.user_case.clone(), case.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut used = BTreeMap::<u32, (SchemaVersionId, Option<String>)>::new();
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
            if used.insert(tag, (*candidate_schema, None)).is_some() {
                return Err(Error::InvalidStoredValue(
                    "physical table variant tag collision",
                ));
            }
        } else {
            for case in &table.variant_cases {
                if used
                    .insert(case.tag, (*candidate_schema, case.user_case.clone()))
                    .is_some()
                {
                    return Err(Error::InvalidStoredValue(
                        "physical table variant tag collision",
                    ));
                }
            }
        }
    }

    let mut requested = BTreeMap::new();
    for (user_case, fields) in cases {
        if requested.insert(user_case, fields).is_some() {
            return Err(Error::InvalidStoredValue(
                "duplicate physical table variant case",
            ));
        }
    }
    if requested.is_empty() {
        return Err(Error::InvalidStoredValue(
            "physical table variant registry must not be empty",
        ));
    }
    if existing_target
        .keys()
        .any(|user_case| !requested.contains_key(user_case))
    {
        return Err(Error::InvalidStoredValue(
            "physical table variant case cannot be removed",
        ));
    }
    if existing_target.is_empty()
        && requested.len() == 1
        && let Some(fields) = requested.get(&None)
    {
        let tag = groove_variant_tag(*aliases.get(&schema_version).ok_or(
            Error::InvalidStoredValue("variant-case schema alias missing"),
        )?)?;
        if used.contains_key(&tag) {
            return Err(Error::InvalidStoredValue(
                "physical table variant tag collision",
            ));
        }
        let allocated = vec![PhysicalVariantCase {
            user_case: None,
            tag,
            fields: fields.clone(),
            payload_fields: Vec::new(),
        }];
        mappings
            .get_mut(&schema_version)
            .and_then(|mapping| mapping.tables.get_mut(logical_table))
            .ok_or(Error::InvalidStoredValue(
                "variant-case target physical mapping missing",
            ))?
            .variant_cases = allocated.clone();
        return Ok(allocated);
    }
    let mut next = used.keys().next_back().copied().unwrap_or(0);
    let mut allocated = Vec::with_capacity(requested.len());
    for (user_case, fields) in requested {
        if !fields.is_subset(&target_columns) {
            return Err(Error::InvalidStoredValue(
                "physical table variant contains an unknown field",
            ));
        }
        if let Some(existing) = existing_target.get(&user_case) {
            if existing.fields != fields {
                return Err(Error::InvalidStoredValue(
                    "physical table variant case definition changed",
                ));
            }
            allocated.push(existing.clone());
            continue;
        }
        next = next.checked_add(1).ok_or(Error::InvalidStoredValue(
            "physical table variant tag exhausted",
        ))?;
        allocated.push(PhysicalVariantCase {
            user_case,
            tag: next,
            fields,
            payload_fields: Vec::new(),
        });
    }
    allocated.sort_by_key(|case| case.tag);
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
    let mut by_table =
        BTreeMap::<PhysicalTableId, BTreeMap<u32, (SchemaVersionId, Option<String>)>>::new();
    for (schema_version, mapping) in mappings {
        for table in mapping.tables.values() {
            let cases = if table.variant_cases.is_empty() {
                vec![(
                    groove_variant_tag(*aliases.get(schema_version).ok_or(
                        Error::InvalidStoredValue("variant-case schema alias missing"),
                    )?)?,
                    None,
                )]
            } else {
                table
                    .variant_cases
                    .iter()
                    .map(|case| (case.tag, case.user_case.clone()))
                    .collect()
            };
            let tags = by_table.entry(table.table_id).or_default();
            for (tag, user_case) in cases {
                if tags.insert(tag, (*schema_version, user_case)).is_some() {
                    return Err(Error::InvalidStoredValue(
                        "physical table variant tag collision",
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn allocate_physical_payload_variant_cases(
    mappings: &mut BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    schema_version: SchemaVersionId,
    logical_table: &str,
    cases: impl IntoIterator<Item = (String, Vec<PhysicalVariantPayloadField>)>,
) -> Result<Vec<PhysicalVariantCase>, Error> {
    let mut requested = BTreeMap::new();
    for (user_case, payload_fields) in cases {
        if requested.insert(user_case, payload_fields).is_some() {
            return Err(Error::InvalidStoredValue(
                "duplicate physical table variant case",
            ));
        }
    }
    let mapping = mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "variant-case target physical mapping missing",
        ))?;
    let existing_payloads = mapping
        .variant_cases
        .iter()
        .filter_map(|case| {
            case.user_case
                .as_ref()
                .map(|user_case| (user_case.as_str(), &case.payload_fields))
        })
        .collect::<BTreeMap<_, _>>();
    for (user_case, payload_fields) in &requested {
        if let Some(existing) = existing_payloads.get(user_case.as_str())
            && *existing != payload_fields
        {
            return Err(Error::InvalidStoredValue(
                "physical table variant payload descriptor changed",
            ));
        }
    }
    let logical_by_physical = mapping
        .columns
        .iter()
        .map(|(logical, physical)| (*physical, logical.clone()))
        .collect::<BTreeMap<_, _>>();
    let allocation_input = requested
        .iter()
        .map(|(user_case, fields)| {
            let mut local_names = BTreeSet::new();
            let mut shared = BTreeSet::new();
            for field in fields {
                if !local_names.insert(field.name.as_str()) {
                    return Err(Error::InvalidStoredValue(
                        "duplicate case-local variant field",
                    ));
                }
                if let Some(column) = field.shared_column {
                    shared.insert(logical_by_physical.get(&column).cloned().ok_or(
                        Error::InvalidStoredValue("variant payload shared physical column missing"),
                    )?);
                }
            }
            Ok((Some(user_case.clone()), shared))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let mut allocated = allocate_physical_variant_cases(
        mappings,
        aliases,
        schema_version,
        logical_table,
        allocation_input,
    )?;
    for case in &mut allocated {
        case.payload_fields = requested
            .get(
                case.user_case
                    .as_deref()
                    .ok_or(Error::InvalidStoredValue("payload variant lost user case"))?,
            )
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "allocated payload variant definition missing",
            ))?;
    }
    mappings
        .get_mut(&schema_version)
        .and_then(|mapping| mapping.tables.get_mut(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "variant-case target physical mapping missing",
        ))?
        .variant_cases = allocated.clone();
    Ok(allocated)
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
        let record = OwnedRecord::new(version.record.raw().to_vec(), binding.descriptor);
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

        let mut layouts_by_tag = BTreeMap::new();
        let mut current_layouts_by_tag = BTreeMap::new();
        let mut rejected_layouts_by_tag = BTreeMap::new();
        let mut history_payloads_by_tag = BTreeMap::new();
        let mut current_payloads_by_tag = BTreeMap::new();
        let mut rejected_payloads_by_tag = BTreeMap::new();
        for (schema_version, logical_table, mapping) in &variants {
            let alias = schema_version_aliases.get(&schema_version).copied().ok_or(
                Error::InvalidStoredValue("physical history schema alias missing"),
            )?;
            let cases = if mapping.variant_cases.is_empty() {
                vec![(groove_variant_tag(alias)?, None, None)]
            } else {
                mapping
                    .variant_cases
                    .iter()
                    .map(|case| (case.tag, Some(&case.fields), Some(case)))
                    .collect()
            };
            for (tag, fields, case) in cases {
                if let Some(case) = case.filter(|case| !case.payload_fields.is_empty()) {
                    history_payloads_by_tag.insert(
                        tag,
                        physical_payload_variant_fields(
                            &physical,
                            HistoryRowRecord::USER_CELLS,
                            case,
                        )?,
                    );
                    current_payloads_by_tag.insert(
                        tag,
                        physical_payload_variant_fields(
                            &physical_global,
                            GlobalCurrentRowRecord::USER_CELLS,
                            case,
                        )?,
                    );
                    rejected_payloads_by_tag.insert(
                        tag,
                        physical_payload_variant_fields(
                            &rejected,
                            RejectedVersionRowRecord::USER_CELLS,
                            case,
                        )?,
                    );
                    continue;
                }
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
            physical = physical.with_variant(tag, fields);
        }
        for (tag, fields) in history_payloads_by_tag {
            physical = physical.with_variant_payload(tag, fields);
        }
        for (tag, fields) in current_layouts_by_tag {
            physical_global = physical_global.with_variant(tag, fields.clone());
            physical_ahead = physical_ahead.with_variant(tag, fields);
        }
        for (tag, fields) in current_payloads_by_tag {
            physical_global = physical_global.with_variant_payload(tag, fields.clone());
            physical_ahead = physical_ahead.with_variant_payload(tag, fields);
        }
        for (tag, fields) in rejected_layouts_by_tag {
            rejected = rejected.with_variant(tag, fields);
        }
        for (tag, fields) in rejected_payloads_by_tag {
            rejected = rejected.with_variant_payload(tag, fields);
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
    physical_history_field_names_for_case(table, mapping, None)
}

fn physical_payload_variant_fields(
    table: &GrooveTableSchema,
    system_fields: usize,
    case: &PhysicalVariantCase,
) -> Result<Vec<GrooveTableVariantField>, Error> {
    let mut fields = table
        .record_schema()
        .fields()
        .iter()
        .take(system_fields)
        .map(|field| {
            let name = field.name.clone().ok_or(Error::InvalidStoredValue(
                "physical variant system field unnamed",
            ))?;
            Ok(GrooveTableVariantField::shared(
                name.clone(),
                field.value_type.clone(),
                name,
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    fields.extend(case.payload_fields.iter().map(|field| {
        if let Some(column) = field.shared_column {
            GrooveTableVariantField::shared(
                field.name.clone(),
                field.value_type.clone(),
                physical_user_column_field(column),
            )
        } else {
            GrooveTableVariantField::local(field.name.clone(), field.value_type.clone())
        }
    }));
    Ok(fields)
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

    source_column.column_type == target_column.column_type
        && source_column.large_value == target_column.large_value
        && source_column.text_merge_spec == target_column.text_merge_spec
        && source_table.merge_strategy(source_column_name)
            == target_table.merge_strategy(target_column_name)
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

    fn cases(edited: bool) -> Vec<(Option<String>, BTreeSet<String>)> {
        let mut text = BTreeSet::from(["id".to_owned(), "body".to_owned()]);
        if edited {
            text.insert("edited".to_owned());
        }
        vec![
            (Some("text".to_owned()), text),
            (
                Some("image".to_owned()),
                BTreeSet::from(["id".to_owned(), "url".to_owned()]),
            ),
        ]
    }

    fn payload_cases() -> Vec<(String, Vec<PhysicalVariantPayloadField>)> {
        vec![
            (
                "text".to_owned(),
                vec![
                    PhysicalVariantPayloadField {
                        name: "id".to_owned(),
                        value_type: records::ValueType::U64,
                        shared_column: Some(PhysicalColumnId(1)),
                    },
                    PhysicalVariantPayloadField {
                        name: "value".to_owned(),
                        value_type: records::ValueType::String,
                        shared_column: None,
                    },
                ],
            ),
            (
                "metric".to_owned(),
                vec![
                    PhysicalVariantPayloadField {
                        name: "event_id".to_owned(),
                        value_type: records::ValueType::U64,
                        shared_column: Some(PhysicalColumnId(1)),
                    },
                    PhysicalVariantPayloadField {
                        name: "value".to_owned(),
                        value_type: records::ValueType::U64,
                        shared_column: None,
                    },
                ],
            ),
        ]
    }

    #[test]
    fn nested_layout_and_user_cases_allocate_durably_without_collisions() {
        let v1 = schema(1);
        let v2 = schema(2);
        let aliases = BTreeMap::from([(v1, SchemaVersionAlias(1)), (v2, SchemaVersionAlias(2))]);
        let mut mappings =
            BTreeMap::from([(v1, mapping(7, &[("id", 1), ("body", 2), ("url", 3)]))]);

        let first =
            allocate_physical_variant_cases(&mut mappings, &aliases, v1, "entries", cases(false))
                .unwrap();
        mappings.insert(
            v2,
            mapping(7, &[("id", 1), ("body", 2), ("url", 3), ("edited", 4)]),
        );
        let second =
            allocate_physical_variant_cases(&mut mappings, &aliases, v2, "entries", cases(true))
                .unwrap();
        assert_eq!(
            first.iter().map(|case| case.tag).collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            second.iter().map(|case| case.tag).collect::<Vec<_>>(),
            [3, 4]
        );
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
    fn case_local_same_name_different_types_survives_jazz_mapping_reopen() {
        let v1 = schema(1);
        let aliases = BTreeMap::from([(v1, SchemaVersionAlias(1))]);
        let mut mappings = BTreeMap::from([(v1, mapping(7, &[("id", 1)]))]);
        let allocated = allocate_physical_payload_variant_cases(
            &mut mappings,
            &aliases,
            v1,
            "entries",
            payload_cases(),
        )
        .unwrap();
        let text_value = &allocated
            .iter()
            .find(|case| case.user_case.as_deref() == Some("text"))
            .unwrap()
            .payload_fields[1];
        let metric_value = &allocated
            .iter()
            .find(|case| case.user_case.as_deref() == Some("metric"))
            .unwrap()
            .payload_fields[1];
        assert_eq!(text_value.name, metric_value.name);
        assert_eq!(text_value.value_type, records::ValueType::String);
        assert_eq!(metric_value.value_type, records::ValueType::U64);
        assert_eq!(text_value.shared_column, None);
        assert_eq!(metric_value.shared_column, None);
        let text_tag = allocated
            .iter()
            .find(|case| case.user_case.as_deref() == Some("text"))
            .unwrap()
            .tag;
        let metric_tag = allocated
            .iter()
            .find(|case| case.user_case.as_deref() == Some("metric"))
            .unwrap()
            .tag;

        let encoded = serde_json::to_vec(&mappings).unwrap();
        let mut reopened: BTreeMap<SchemaVersionId, SchemaPhysicalMapping> =
            serde_json::from_slice(&encoded).unwrap();
        assert_eq!(reopened, mappings);
        validate_physical_variant_cases(&reopened, &aliases).unwrap();

        // Model an already-persisted row before a second catalogue reopen. A
        // tag's complete dense payload descriptor is durable format, including
        // case-local fields that do not participate in shared identities.
        let text_descriptor = records::RecordDescriptor::new(
            allocated
                .iter()
                .find(|case| case.tag == text_tag)
                .unwrap()
                .payload_fields
                .iter()
                .map(|field| (field.name.clone(), field.value_type.clone())),
        );
        let stored = records::VariantRecord::create(
            text_tag.into(),
            text_descriptor,
            &[
                records::Value::U64(41),
                records::Value::String("old text".to_owned()),
            ],
        )
        .unwrap()
        .into_stored_bytes();

        let unchanged = reopened.clone();
        let mut changed_cases = payload_cases();
        changed_cases
            .iter_mut()
            .find(|(case, _)| case == "text")
            .unwrap()
            .1[1]
            .value_type = records::ValueType::U64;
        assert!(matches!(
            allocate_physical_payload_variant_cases(
                &mut reopened,
                &aliases,
                v1,
                "entries",
                changed_cases,
            ),
            Err(Error::InvalidStoredValue(
                "physical table variant payload descriptor changed"
            ))
        ));
        assert_eq!(reopened, unchanged);

        // Reopening again after the rejected admission keeps the original
        // descriptor, so the old row still decodes as text rather than U64.
        let encoded = serde_json::to_vec(&reopened).unwrap();
        let reopened: BTreeMap<SchemaVersionId, SchemaPhysicalMapping> =
            serde_json::from_slice(&encoded).unwrap();
        let persisted_text = reopened[&v1].tables["entries"]
            .variant_cases
            .iter()
            .find(|case| case.tag == text_tag)
            .unwrap();
        let descriptor = records::RecordDescriptor::new(
            persisted_text
                .payload_fields
                .iter()
                .map(|field| (field.name.clone(), field.value_type.clone())),
        );
        let (tag, payload) = records::split_variant_record(&stored).unwrap();
        assert_eq!(tag, text_tag);
        let old = records::OwnedRecord::new(payload.to_vec(), descriptor);
        assert_eq!(
            old.to_values().unwrap(),
            [
                records::Value::U64(41),
                records::Value::String("old text".to_owned()),
            ]
        );

        let jazz = JazzSchema::new([TableSchema::new(
            "entries",
            [crate::schema::ColumnSchema::new(
                "id",
                records::ValueType::U64,
            )],
        )]);
        let catalogue = BTreeMap::from([(v1, SchemaVersion::new(jazz))]);
        let lowered =
            physical_version_storage_tables(&catalogue, &aliases, &reopened, &BTreeSet::new())
                .unwrap();
        let history = lowered
            .iter()
            .find(|table| table.name == physical_history_table_name(PhysicalTableId(7)))
            .unwrap();
        let text = history.record_schema_for_variant(text_tag).unwrap();
        let metric = history.record_schema_for_variant(metric_tag).unwrap();
        let ty = |descriptor: &records::RecordDescriptor, name: &str| {
            descriptor
                .fields()
                .iter()
                .find(|field| field.name.as_deref() == Some(name))
                .unwrap()
                .value_type
                .clone()
        };
        assert_eq!(ty(&text, "value"), records::ValueType::String);
        assert_eq!(ty(&metric, "value"), records::ValueType::U64);
        assert_eq!(ty(&text, "id"), records::ValueType::U64);
        assert_eq!(ty(&metric, "event_id"), records::ValueType::U64);
    }

    #[test]
    fn variant_case_registry_rejects_duplicates_and_accidental_omission() {
        let v1 = schema(1);
        let aliases = BTreeMap::from([(v1, SchemaVersionAlias(1))]);
        let mut mappings =
            BTreeMap::from([(v1, mapping(7, &[("id", 1), ("body", 2), ("url", 3)]))]);

        let duplicate = vec![
            (
                Some("text".to_owned()),
                BTreeSet::from(["id".to_owned(), "body".to_owned()]),
            ),
            (
                Some("text".to_owned()),
                BTreeSet::from(["id".to_owned(), "body".to_owned()]),
            ),
        ];
        assert!(matches!(
            allocate_physical_variant_cases(&mut mappings, &aliases, v1, "entries", duplicate,),
            Err(Error::InvalidStoredValue(
                "duplicate physical table variant case"
            ))
        ));
        assert!(mappings[&v1].tables["entries"].variant_cases.is_empty());

        allocate_physical_variant_cases(&mut mappings, &aliases, v1, "entries", cases(false))
            .unwrap();
        let unchanged = mappings.clone();
        assert!(matches!(
            allocate_physical_variant_cases(
                &mut mappings,
                &aliases,
                v1,
                "entries",
                [(
                    Some("text".to_owned()),
                    BTreeSet::from(["id".to_owned(), "body".to_owned()]),
                )],
            ),
            Err(Error::InvalidStoredValue(
                "physical table variant case cannot be removed"
            ))
        ));
        assert_eq!(mappings, unchanged);

        let duplicate_payload = vec![payload_cases()[0].clone(), payload_cases()[0].clone()];
        assert!(matches!(
            allocate_physical_payload_variant_cases(
                &mut BTreeMap::from([(v1, mapping(8, &[("id", 1)]))]),
                &aliases,
                v1,
                "entries",
                duplicate_payload,
            ),
            Err(Error::InvalidStoredValue(
                "duplicate physical table variant case"
            ))
        ));
    }

    #[test]
    fn reopen_validation_rejects_a_cross_layout_tag_collision() {
        let v1 = schema(1);
        let v2 = schema(2);
        let aliases = BTreeMap::from([(v1, SchemaVersionAlias(1)), (v2, SchemaVersionAlias(2))]);
        let mut first = mapping(7, &[("id", 1)]);
        first.tables.get_mut("entries").unwrap().variant_cases = vec![PhysicalVariantCase {
            user_case: Some("text".to_owned()),
            tag: 9,
            fields: BTreeSet::from(["id".to_owned()]),
            payload_fields: Vec::new(),
        }];
        let mut second = mapping(7, &[("id", 1)]);
        second.tables.get_mut("entries").unwrap().variant_cases = vec![PhysicalVariantCase {
            user_case: Some("image".to_owned()),
            tag: 9,
            fields: BTreeSet::from(["id".to_owned()]),
            payload_fields: Vec::new(),
        }];
        let mappings = BTreeMap::from([(v1, first), (v2, second)]);
        assert!(matches!(
            validate_physical_variant_cases(&mappings, &aliases),
            Err(Error::InvalidStoredValue(
                "physical table variant tag collision"
            ))
        ));
    }
}
