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
    /// Per-physical-column semantic identities for compact scalar enum tags.
    /// This is durable catalogue state: local registry order is never inferred
    /// from receipt order.
    #[serde(default)]
    pub(super) scalar_enum_cases: BTreeMap<PhysicalColumnId, Vec<GlobalScalarEnumCaseId>>,
    /// The same durable identities for direct payload enums.  Payload layouts
    /// are resolved from the introducing schema; the opaque physical case tag
    /// never uses an authored ordinal or display name as identity.
    #[serde(default)]
    pub(super) payload_enum_cases: BTreeMap<PhysicalColumnId, Vec<GlobalScalarEnumCaseId>>,
    /// Recursive scalar enum occurrences below a direct user column.  The
    /// structural key is stable across array/nullable/tuple/record lowering;
    /// payload children are rooted under their parent case identity by the
    /// catalogue reconciler.
    #[serde(default)]
    pub(super) nested_scalar_enum_cases:
        BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<GlobalScalarEnumCaseId>>>,
    /// Recursive payload-enum occurrences have the same durable identity
    /// discipline as scalar occurrences.  Their children are rooted under
    /// the selected *global* parent case rather than an authored ordinal.
    #[serde(default)]
    pub(super) nested_payload_enum_cases:
        BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<GlobalScalarEnumCaseId>>>,
}

/// A source cell while lowering a migration-lens path into a physical current
/// winner projection.  The symbolic field survives rename/copy chains until
/// the projection is registered, whereas literal defaults are materialized at
/// that projection boundary.
#[derive(Clone)]
enum CurrentWinnerCellProjection {
    Field {
        name: String,
        column_id: PhysicalColumnId,
        column_type: records::ValueType,
    },
    Literal(Value),
    Null,
}

fn physical_mapping_has_enum_boundary(
    mapping: &TablePhysicalMapping,
    column_id: PhysicalColumnId,
) -> bool {
    mapping.scalar_enum_cases.contains_key(&column_id)
        || mapping.payload_enum_cases.contains_key(&column_id)
        || mapping
            .nested_scalar_enum_cases
            .get(&column_id)
            .is_some_and(|paths| !paths.is_empty())
        || mapping
            .nested_payload_enum_cases
            .get(&column_id)
            .is_some_and(|paths| !paths.is_empty())
}

fn value_type_has_enum_boundary(value_type: &records::ValueType) -> bool {
    use records::ValueType;
    match value_type {
        ValueType::EnumTag(_) | ValueType::Enum(_) => true,
        ValueType::Nullable(inner) | ValueType::Array(inner) => value_type_has_enum_boundary(inner),
        ValueType::Tuple(values) => values.iter().any(value_type_has_enum_boundary),
        ValueType::Record(record) => record
            .fields()
            .iter()
            .any(|field| value_type_has_enum_boundary(&field.value_type)),
        _ => false,
    }
}

/// Bootstrap mappings have no durable per-column registry entries yet. Their
/// physical tags still follow the validated authored layout, so initialize the
/// same recursive copy plan by ordinal and let durable mappings replace each
/// occurrence as soon as they are available.
fn bootstrap_copy_enum_remaps(
    source: &records::ValueType,
    target: &records::ValueType,
    path: &str,
    remaps: &mut EnumOccurrenceRemaps,
) -> Result<(), Error> {
    use records::ValueType;
    match (source, target) {
        (ValueType::EnumTag(source), ValueType::EnumTag(target)) => {
            remaps.scalar.entry(path.to_owned()).or_insert(
                source
                    .variants
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| {
                        target
                            .variants
                            .get(ordinal)
                            .map(|_| {
                                u8::try_from(ordinal).map_err(|_| {
                                    Error::InvalidStoredValue(
                                        "bootstrap copied scalar enum tag exhausted",
                                    )
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?,
            );
        }
        (ValueType::Enum(source), ValueType::Enum(target)) => {
            remaps.payload.entry(path.to_owned()).or_insert(
                source
                    .cases
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| {
                        target
                            .cases
                            .get(ordinal)
                            .map(|_| {
                                u32::try_from(ordinal).map_err(|_| {
                                    Error::InvalidStoredValue(
                                        "bootstrap copied payload enum tag exhausted",
                                    )
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?,
            );
            remaps.payload_children.entry(path.to_owned()).or_insert(
                source
                    .cases
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| {
                        target
                            .cases
                            .get(ordinal)
                            .map(|_| format!("{path}/case/bootstrap/{ordinal}"))
                    })
                    .collect(),
            );
            for (ordinal, (source_case, target_case)) in
                source.cases.iter().zip(&target.cases).enumerate()
            {
                bootstrap_copy_enum_remaps(
                    &ValueType::Record(Box::new(source_case.payload.clone())),
                    &ValueType::Record(Box::new(target_case.payload.clone())),
                    &format!("{path}/case/bootstrap/{ordinal}"),
                    remaps,
                )?;
            }
        }
        (ValueType::Nullable(source), ValueType::Nullable(target)) => {
            bootstrap_copy_enum_remaps(source, target, &format!("{path}/nullable"), remaps)?;
        }
        (ValueType::Array(source), ValueType::Array(target)) => {
            bootstrap_copy_enum_remaps(source, target, &format!("{path}/array"), remaps)?;
        }
        (ValueType::Tuple(source), ValueType::Tuple(target)) => {
            for (index, (source, target)) in source.iter().zip(target).enumerate() {
                bootstrap_copy_enum_remaps(
                    source,
                    target,
                    &format!("{path}/tuple/{index}"),
                    remaps,
                )?;
            }
        }
        (ValueType::Record(source), ValueType::Record(target)) => {
            for (source, target) in source.fields().iter().zip(target.fields()) {
                let name = source.name.as_deref().ok_or(Error::InvalidStoredValue(
                    "bootstrap copied enum record field unnamed",
                ))?;
                bootstrap_copy_enum_remaps(
                    &source.value_type,
                    &target.value_type,
                    &format!("{path}/record/{name}"),
                    remaps,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub(super) struct GlobalScalarEnumCaseId {
    pub(super) introducing_schema: SchemaVersionId,
    pub(super) introducing_ordinal: u8,
}

/// Explicit authored-to-physical tags for every enum occurrence beneath one
/// user column. Paths are structural (`root`, `nullable`, `array`,
/// `tuple/<n>`, `record/<field>`) and payload children extend the selected
/// parent case identity. Keeping this separate from the compact local tag is
/// what lets recursive values cross the authored/physical boundary safely.
type EnumOccurrenceRemaps = groove::ivm::RecursiveEnumRemaps;

/// A scalar enum discriminant in a physical table is an interned spelling of
/// this identity, never an authored declaration ordinal.  The textual enum
/// names used in the physical descriptor are intentionally opaque: tags are
/// only decoded through this durable catalogue mapping.
fn physical_scalar_enum_case_name(case: &GlobalScalarEnumCaseId) -> String {
    format!(
        "case-{}-{}",
        case.introducing_schema.0.simple(),
        case.introducing_ordinal
    )
}

fn compare_scalar_enum_cases(
    aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    left: &GlobalScalarEnumCaseId,
    right: &GlobalScalarEnumCaseId,
) -> std::cmp::Ordering {
    // A case's *introducing* schema alias is its authoritative, durable
    // introduction position.  Descendants retain the original identity, so
    // ordering by alias preserves the inherited physical prefix and appends a
    // later sibling even when that sibling's authored ordinal is shallower
    // than a case introduced earlier on another branch.  Ordinal only orders
    // multiple cases introduced by the same schema. UUID is solely a
    // deterministic tie-breaker for corrupt/incomplete catalogues.
    aliases
        .get(&left.introducing_schema)
        .cmp(&aliases.get(&right.introducing_schema))
        .then_with(|| left.introducing_ordinal.cmp(&right.introducing_ordinal))
        .then_with(|| left.introducing_schema.cmp(&right.introducing_schema))
}

fn physical_scalar_enum_schema(
    column_id: PhysicalColumnId,
    cases: &[GlobalScalarEnumCaseId],
) -> Result<records::ScalarEnumSchema, Error> {
    records::ScalarEnumSchema::new(
        format!("physical-column-{}", column_id.0),
        cases.iter().map(physical_scalar_enum_case_name),
    )
    .map(|schema| {
        schema.with_registry_id(records::variant_registry_id_for_path(&format!(
            "physical-column/{}/nullable",
            column_id.0
        )))
    })
    .map_err(|_| Error::InvalidStoredValue("invalid physical scalar enum registry"))
}

/// Lower a recursively nested authored value into the durable physical enum
/// registries for one user column.  Scalar and payload enum occurrences have
/// independently interned registries; payload children are addressed beneath
/// their *global* parent identity, never an authored case ordinal.
fn physical_nested_enum_value_type(
    value_type: &records::ValueType,
    path: &str,
    scalar_registries: &BTreeMap<String, Vec<GlobalScalarEnumCaseId>>,
    payload_registries: &BTreeMap<String, Vec<GlobalScalarEnumCaseId>>,
    payload_layouts: &BTreeMap<(String, GlobalScalarEnumCaseId), records::RecordDescriptor>,
    column_id: PhysicalColumnId,
) -> Result<records::ValueType, Error> {
    use records::ValueType;
    Ok(match value_type {
        ValueType::EnumTag(_) => ValueType::EnumTag(physical_scalar_enum_schema(
            column_id,
            scalar_registries
                .get(path)
                .ok_or(Error::InvalidStoredValue(
                    "physical nested scalar enum registry missing",
                ))?,
        )?),
        ValueType::Nullable(inner) => {
            ValueType::Nullable(Box::new(physical_nested_enum_value_type(
                inner,
                &format!("{path}/nullable"),
                scalar_registries,
                payload_registries,
                payload_layouts,
                column_id,
            )?))
        }
        ValueType::Array(inner) => ValueType::Array(Box::new(physical_nested_enum_value_type(
            inner,
            &format!("{path}/array"),
            scalar_registries,
            payload_registries,
            payload_layouts,
            column_id,
        )?)),
        ValueType::Tuple(values) => ValueType::Tuple(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    physical_nested_enum_value_type(
                        value,
                        &format!("{path}/tuple/{index}"),
                        scalar_registries,
                        payload_registries,
                        payload_layouts,
                        column_id,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ValueType::Record(record) => ValueType::Record(Box::new(records::RecordDescriptor::new(
            record
                .fields()
                .iter()
                .map(|field| {
                    let name = field.name.clone().ok_or(Error::InvalidStoredValue(
                        "nested scalar enum record field unnamed",
                    ))?;
                    Ok((
                        name.clone(),
                        physical_nested_enum_value_type(
                            &field.value_type,
                            &format!("{path}/record/{name}"),
                            scalar_registries,
                            payload_registries,
                            payload_layouts,
                            column_id,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?,
        ))),
        ValueType::Enum(_) => {
            let cases = payload_registries
                .get(path)
                .ok_or(Error::InvalidStoredValue(
                    "physical nested payload enum registry missing",
                ))?;
            let cases = cases
                .iter()
                .map(|identity| {
                    let payload = payload_layouts
                        .get(&(path.to_owned(), identity.clone()))
                        .ok_or(Error::InvalidStoredValue(
                            "physical nested payload enum layout missing",
                        ))?;
                    let ValueType::Record(payload) = physical_nested_enum_value_type(
                        &ValueType::Record(Box::new(payload.clone())),
                        &global_case_path(path, identity),
                        scalar_registries,
                        payload_registries,
                        payload_layouts,
                        column_id,
                    )?
                    else {
                        unreachable!("record lowering preserves record shape");
                    };
                    Ok(records::EnumCase::new(
                        physical_scalar_enum_case_name(identity),
                        *payload,
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            ValueType::Enum(Box::new(
                records::EnumSchema::new(format!("physical-column-{}-{path}", column_id.0), cases)
                    .map_err(|_| {
                        Error::InvalidStoredValue("invalid physical nested payload enum registry")
                    })?
                    .with_registry_id(records::variant_registry_id_for_path(&format!(
                        "physical-column/{}/{path}",
                        column_id.0
                    ))),
            ))
        }
        _ => value_type.clone(),
    })
}

/// Collect the authored payload layout for each durable nested payload case.
/// The same `GlobalScalarEnumCaseId` can be observed through many descendant
/// schemas, but it must always describe one layout.  Comparing the complete
/// descriptor here prevents a later sibling declaration from changing the
/// meaning of an already-persisted compact tag.
fn collect_nested_payload_enum_layouts(
    value_type: &records::ValueType,
    path: &str,
    identities: &BTreeMap<String, Vec<GlobalScalarEnumCaseId>>,
    output: &mut BTreeMap<(String, GlobalScalarEnumCaseId), records::RecordDescriptor>,
) -> Result<(), Error> {
    use records::ValueType;
    match value_type {
        ValueType::Nullable(inner) => collect_nested_payload_enum_layouts(
            inner,
            &format!("{path}/nullable"),
            identities,
            output,
        ),
        ValueType::Array(inner) => {
            collect_nested_payload_enum_layouts(inner, &format!("{path}/array"), identities, output)
        }
        ValueType::Tuple(values) => {
            for (index, value) in values.iter().enumerate() {
                collect_nested_payload_enum_layouts(
                    value,
                    &format!("{path}/tuple/{index}"),
                    identities,
                    output,
                )?;
            }
            Ok(())
        }
        ValueType::Record(record) => {
            for field in record.fields() {
                let name = field.name.as_deref().ok_or(Error::InvalidStoredValue(
                    "nested payload enum record field unnamed",
                ))?;
                collect_nested_payload_enum_layouts(
                    &field.value_type,
                    &format!("{path}/record/{name}"),
                    identities,
                    output,
                )?;
            }
            Ok(())
        }
        ValueType::Enum(schema) => {
            let cases = identities.get(path).ok_or(Error::InvalidStoredValue(
                "nested payload enum identity mapping missing",
            ))?;
            if cases.len() != schema.cases.len() {
                return Err(Error::InvalidStoredValue(
                    "nested payload enum identity mapping width mismatch",
                ));
            }
            for (identity, case) in cases.iter().zip(&schema.cases) {
                let key = (path.to_owned(), identity.clone());
                match output.entry(key) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(case.payload.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get() == &case.payload => {}
                    std::collections::btree_map::Entry::Occupied(_) => {
                        return Err(Error::InvalidStoredValue(
                            "same nested payload enum identity has incompatible layout",
                        ));
                    }
                }
                collect_nested_payload_enum_layouts(
                    &ValueType::Record(Box::new(case.payload.clone())),
                    &global_case_path(path, identity),
                    identities,
                    output,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
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

/// A schema-agnostic physical current-row target used only until the
/// Global/Ahead winner has been selected. Its enum tags retain their durable
/// physical meaning; authored decoding belongs strictly after that selection.
fn physical_current_winner_projection_target(
    table_id: PhysicalTableId,
    physical_fields: &[String],
) -> String {
    format!(
        "physical_{}_current_winner_{}",
        table_id.0,
        physical_fields.join("_")
    )
}

/// A query-local current-source target.  The ordinary target projects every
/// authored enum occurrence, which is correct for a whole-row read but makes
/// an older schema fail while decoding an enum cell that the query never
/// consumes.  A narrowed target keeps the fixed logical row shape while
/// replacing unneeded enum cells with typed nulls; the query compiler's
/// requirement closure is therefore the only route by which an enum value is
/// materialized into an authored descriptor.
fn physical_current_projection_target_for_enum_columns(
    schema_alias: SchemaVersionAlias,
    logical_table: &str,
    enum_columns: &BTreeSet<PhysicalColumnId>,
) -> String {
    let base = physical_current_projection_target(schema_alias, logical_table);
    let suffix = enum_columns
        .iter()
        .map(|column| column.0.to_string())
        .collect::<Vec<_>>()
        .join("_");
    format!("{base}_enum_fields_{suffix}")
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
                scalar_enum_cases: BTreeMap::new(),
                payload_enum_cases: BTreeMap::new(),
                nested_scalar_enum_cases: BTreeMap::new(),
                nested_payload_enum_cases: BTreeMap::new(),
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
    pub(super) fn physical_scalar_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
    ) -> Result<Vec<GlobalScalarEnumCaseId>, Error> {
        let mut cases = BTreeSet::new();
        for mapping in self.catalogue.physical_mappings.values() {
            for table in mapping
                .tables
                .values()
                .filter(|table| table.table_id == table_id)
            {
                if let Some(column_cases) = table.scalar_enum_cases.get(&column_id) {
                    cases.extend(column_cases.iter().cloned());
                }
            }
        }
        if cases.is_empty() {
            return Err(Error::InvalidStoredValue(
                "physical scalar enum registry identity mapping missing",
            ));
        }
        let mut cases = cases.into_iter().collect::<Vec<_>>();
        cases.sort_by(|left, right| {
            compare_scalar_enum_cases(&self.catalogue.schema_version_aliases, left, right)
        });
        Ok(cases)
    }

    fn physical_payload_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
    ) -> Result<Vec<GlobalScalarEnumCaseId>, Error> {
        let mut cases = BTreeSet::new();
        for mapping in self.catalogue.physical_mappings.values() {
            for table in mapping
                .tables
                .values()
                .filter(|table| table.table_id == table_id)
            {
                if let Some(column_cases) = table.payload_enum_cases.get(&column_id) {
                    cases.extend(column_cases.iter().cloned());
                }
            }
        }
        if cases.is_empty() {
            return Err(Error::InvalidStoredValue(
                "physical payload enum registry identity mapping missing",
            ));
        }
        let mut cases = cases.into_iter().collect::<Vec<_>>();
        cases.sort_by(|left, right| {
            compare_scalar_enum_cases(&self.catalogue.schema_version_aliases, left, right)
        });
        Ok(cases)
    }

    fn physical_nested_scalar_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
        path: &str,
    ) -> Result<Vec<GlobalScalarEnumCaseId>, Error> {
        let mut cases = BTreeSet::new();
        for mapping in self.catalogue.physical_mappings.values() {
            for table in mapping
                .tables
                .values()
                .filter(|table| table.table_id == table_id)
            {
                if let Some(column_cases) = table
                    .nested_scalar_enum_cases
                    .get(&column_id)
                    .and_then(|paths| paths.get(path))
                {
                    cases.extend(column_cases.iter().cloned());
                }
            }
        }
        if cases.is_empty() {
            return Err(Error::InvalidStoredValue(
                "physical nested scalar enum registry identity mapping missing",
            ));
        }
        let mut cases = cases.into_iter().collect::<Vec<_>>();
        cases.sort_by(|left, right| {
            compare_scalar_enum_cases(&self.catalogue.schema_version_aliases, left, right)
        });
        Ok(cases)
    }

    fn physical_nested_payload_enum_cases(
        &self,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
        path: &str,
    ) -> Result<Vec<GlobalScalarEnumCaseId>, Error> {
        let mut cases = BTreeSet::new();
        for mapping in self.catalogue.physical_mappings.values() {
            for table in mapping
                .tables
                .values()
                .filter(|table| table.table_id == table_id)
            {
                if let Some(column_cases) = table
                    .nested_payload_enum_cases
                    .get(&column_id)
                    .and_then(|paths| paths.get(path))
                {
                    cases.extend(column_cases.iter().cloned());
                }
            }
        }
        if cases.is_empty() {
            return Err(Error::InvalidStoredValue(
                "physical nested payload enum registry identity mapping missing",
            ));
        }
        let mut cases = cases.into_iter().collect::<Vec<_>>();
        cases.sort_by(|left, right| {
            compare_scalar_enum_cases(&self.catalogue.schema_version_aliases, left, right)
        });
        Ok(cases)
    }

    /// Construct the physical-to-authored side of the enum interning boundary
    /// for one user cell.  Every entry is keyed by a structural occurrence;
    /// absent target cases deliberately stay `None` so projection fails rather
    /// than fabricating an older client's value.
    fn physical_to_authored_enum_remaps(
        &self,
        target_mapping: &TablePhysicalMapping,
        column_id: PhysicalColumnId,
    ) -> Result<EnumOccurrenceRemaps, Error> {
        let mut remaps = EnumOccurrenceRemaps::default();
        if let Some(target_cases) = target_mapping.scalar_enum_cases.get(&column_id) {
            // Bootstrap defines the physical table before its freshly
            // hydrated catalogue mapping is durable.  In that one state the
            // target's own registry is necessarily the complete physical
            // registry; later states must use the lineage union below.
            let physical_cases = self
                .physical_scalar_enum_cases(target_mapping.table_id, column_id)
                .unwrap_or_else(|_| target_cases.clone());
            remaps.scalar.insert(
                "root".to_owned(),
                physical_cases
                    .iter()
                    .map(|identity| {
                        target_cases
                            .iter()
                            .position(|candidate| candidate == identity)
                            .map(|tag| {
                                u8::try_from(tag).map_err(|_| {
                                    Error::InvalidStoredValue("target scalar enum tag exhausted")
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?,
            );
        }
        if let Some(target_cases) = target_mapping.payload_enum_cases.get(&column_id) {
            let physical_cases = self
                .physical_payload_enum_cases(target_mapping.table_id, column_id)
                .unwrap_or_else(|_| target_cases.clone());
            remaps.payload.insert(
                "root".to_owned(),
                physical_cases
                    .iter()
                    .map(|identity| {
                        target_cases
                            .iter()
                            .position(|candidate| candidate == identity)
                            .map(|tag| {
                                u32::try_from(tag).map_err(|_| {
                                    Error::InvalidStoredValue("target payload enum tag exhausted")
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?,
            );
            remaps.payload_children.insert(
                "root".to_owned(),
                physical_cases
                    .iter()
                    .map(|identity| Some(global_case_path("root", identity)))
                    .collect(),
            );
        }
        if let Some(paths) = target_mapping.nested_scalar_enum_cases.get(&column_id) {
            for (path, target_cases) in paths {
                let physical_cases = self
                    .physical_nested_scalar_enum_cases(target_mapping.table_id, column_id, path)
                    .unwrap_or_else(|_| target_cases.clone());
                remaps.scalar.insert(
                    path.clone(),
                    physical_cases
                        .iter()
                        .map(|identity| {
                            target_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u8::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "target nested scalar enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
            }
        }
        if let Some(paths) = target_mapping.nested_payload_enum_cases.get(&column_id) {
            for (path, target_cases) in paths {
                let physical_cases = self
                    .physical_nested_payload_enum_cases(target_mapping.table_id, column_id, path)
                    .unwrap_or_else(|_| target_cases.clone());
                remaps.payload.insert(
                    path.clone(),
                    physical_cases
                        .iter()
                        .map(|identity| {
                            target_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u32::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "target nested payload enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
                remaps.payload_children.insert(
                    path.clone(),
                    physical_cases
                        .iter()
                        .map(|identity| Some(global_case_path(path, identity)))
                        .collect(),
                );
            }
        }
        Ok(remaps)
    }

    /// Re-encode a source physical enum occurrence into a distinct copied
    /// column's physical registry. Copying an enum's raw tag is invalid: each
    /// physical column owns an independent durable registry, even when the
    /// authored enum layouts are identical. The lens validates compatible
    /// layouts, so ordinal correspondence is the authored copy relation while
    /// this remap translates both sides' physical tags and nested payload
    /// paths.
    fn physical_copy_enum_remaps(
        &self,
        source_mapping: &TablePhysicalMapping,
        source_column_id: PhysicalColumnId,
        target_mapping: &TablePhysicalMapping,
        target_column_id: PhysicalColumnId,
        source_column_type: &records::ValueType,
        target_column_type: &records::ValueType,
    ) -> Result<EnumOccurrenceRemaps, Error> {
        let mut remaps = EnumOccurrenceRemaps::default();
        if let Some(source_cases) = source_mapping.scalar_enum_cases.get(&source_column_id) {
            let source_cases = self
                .physical_scalar_enum_cases(source_mapping.table_id, source_column_id)
                .unwrap_or_else(|_| source_cases.clone());
            let target_cases = target_mapping
                .scalar_enum_cases
                .get(&target_column_id)
                .map(|fallback| {
                    self.physical_scalar_enum_cases(target_mapping.table_id, target_column_id)
                        .unwrap_or_else(|_| fallback.clone())
                })
                .unwrap_or_default();
            remaps.scalar.insert(
                "root".to_owned(),
                source_cases
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| {
                        target_cases
                            .get(ordinal)
                            .map(|_| {
                                u8::try_from(ordinal).map_err(|_| {
                                    Error::InvalidStoredValue("copied scalar enum tag exhausted")
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?,
            );
        }
        if let Some(source_cases) = source_mapping.payload_enum_cases.get(&source_column_id) {
            let source_cases = self
                .physical_payload_enum_cases(source_mapping.table_id, source_column_id)
                .unwrap_or_else(|_| source_cases.clone());
            let target_cases = target_mapping
                .payload_enum_cases
                .get(&target_column_id)
                .map(|fallback| {
                    self.physical_payload_enum_cases(target_mapping.table_id, target_column_id)
                        .unwrap_or_else(|_| fallback.clone())
                })
                .unwrap_or_default();
            remaps.payload.insert(
                "root".to_owned(),
                source_cases
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| {
                        target_cases
                            .get(ordinal)
                            .map(|_| {
                                u32::try_from(ordinal).map_err(|_| {
                                    Error::InvalidStoredValue("copied payload enum tag exhausted")
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?,
            );
            remaps.payload_children.insert(
                "root".to_owned(),
                source_cases
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| {
                        target_cases
                            .get(ordinal)
                            .map(|identity| global_case_path("root", identity))
                    })
                    .collect(),
            );
        }
        if let Some(source_paths) = source_mapping
            .nested_scalar_enum_cases
            .get(&source_column_id)
        {
            for (path, source_cases) in source_paths {
                if source_cases.is_empty() {
                    continue;
                }
                let source_cases = self
                    .physical_nested_scalar_enum_cases(
                        source_mapping.table_id,
                        source_column_id,
                        path,
                    )
                    .unwrap_or_else(|_| source_cases.clone());
                let target_cases = target_mapping
                    .nested_scalar_enum_cases
                    .get(&target_column_id)
                    .and_then(|paths| paths.get(path))
                    .map(|fallback| {
                        self.physical_nested_scalar_enum_cases(
                            target_mapping.table_id,
                            target_column_id,
                            path,
                        )
                        .unwrap_or_else(|_| fallback.clone())
                    })
                    .unwrap_or_default();
                remaps.scalar.insert(
                    path.clone(),
                    source_cases
                        .iter()
                        .enumerate()
                        .map(|(ordinal, _)| {
                            target_cases
                                .get(ordinal)
                                .map(|_| {
                                    u8::try_from(ordinal).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "copied nested scalar enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
            }
        }
        if let Some(source_paths) = source_mapping
            .nested_payload_enum_cases
            .get(&source_column_id)
        {
            for (path, source_cases) in source_paths {
                if source_cases.is_empty() {
                    continue;
                }
                let source_cases = self
                    .physical_nested_payload_enum_cases(
                        source_mapping.table_id,
                        source_column_id,
                        path,
                    )
                    .unwrap_or_else(|_| source_cases.clone());
                let target_cases = target_mapping
                    .nested_payload_enum_cases
                    .get(&target_column_id)
                    .and_then(|paths| paths.get(path))
                    .map(|fallback| {
                        self.physical_nested_payload_enum_cases(
                            target_mapping.table_id,
                            target_column_id,
                            path,
                        )
                        .unwrap_or_else(|_| fallback.clone())
                    })
                    .unwrap_or_default();
                remaps.payload.insert(
                    path.clone(),
                    source_cases
                        .iter()
                        .enumerate()
                        .map(|(ordinal, _)| {
                            target_cases
                                .get(ordinal)
                                .map(|_| {
                                    u32::try_from(ordinal).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "copied nested payload enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
                remaps.payload_children.insert(
                    path.clone(),
                    source_cases
                        .iter()
                        .enumerate()
                        .map(|(ordinal, _)| {
                            target_cases
                                .get(ordinal)
                                .map(|identity| global_case_path(path, identity))
                        })
                        .collect(),
                );
            }
        }
        bootstrap_copy_enum_remaps(source_column_type, target_column_type, "root", &mut remaps)?;
        Ok(remaps)
    }

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
                "stored row schema version alias missing while resolving physical table",
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

    pub(super) fn physical_current_source_graph_with_projection_target(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        class: PhysicalCurrentClass,
        projection_target: impl Into<String>,
    ) -> Result<GraphBuilder, Error> {
        let binding = physical_current_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
            class,
        )?;
        Ok(GraphBuilder::variant_source(
            binding.storage_table,
            projection_target,
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

    pub(super) fn physical_current_source_scan_graph_with_projection_target(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        class: PhysicalCurrentClass,
        projection_target: impl Into<String>,
        scan: groove::ivm::StaticScanSpec,
    ) -> Result<GraphBuilder, Error> {
        let binding = physical_current_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
            class,
        )?;
        Ok(GraphBuilder::variant_source_scan(
            binding.storage_table,
            projection_target,
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

    /// Register (or refresh) a current-source projection that decodes only
    /// the enum columns required by one query source.  The fixed output shape
    /// deliberately retains the ordinary logical row descriptor: callers can
    /// share the normal current-source lowering, while enum values outside the
    /// requirement closure are represented by typed nulls and never expose a
    /// physical tag.
    pub(super) fn ensure_physical_current_projection_for_enum_columns(
        &mut self,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        required_fields: &BTreeSet<String>,
    ) -> Result<String, Error> {
        let target_alias = self
            .catalogue
            .schema_version_aliases
            .get(&target_schema)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical current projection target schema alias missing",
            ))?;
        let target_table = self.table_in_schema(target_table_name, target_schema)?;
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&target_schema)
            .and_then(|mapping| mapping.tables.get(target_table_name))
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "target enum physical mapping missing",
            ))?;
        let required_enum_columns = target_table
            .columns
            .iter()
            .filter(|column| required_fields.contains(&column.name))
            .filter_map(|column| {
                let column_id = target_mapping.columns.get(&column.name).copied()?;
                let has_enum_boundary = target_mapping.scalar_enum_cases.contains_key(&column_id)
                    || target_mapping.payload_enum_cases.contains_key(&column_id)
                    || target_mapping
                        .nested_scalar_enum_cases
                        .contains_key(&column_id)
                    || target_mapping
                        .nested_payload_enum_cases
                        .contains_key(&column_id)
                    || matches!(
                        column.column_type,
                        records::ValueType::EnumTag(_) | records::ValueType::Enum(_)
                    );
                has_enum_boundary.then_some(column_id)
            })
            .collect::<BTreeSet<_>>();
        let projection_target = physical_current_projection_target_for_enum_columns(
            target_alias,
            target_table_name,
            &required_enum_columns,
        );
        let storage_tables = [
            physical_global_current_table_name(target_mapping.table_id),
            physical_ahead_current_table_name(target_mapping.table_id),
        ];
        for storage_table in &storage_tables {
            let logical_output = target_table.global_current_storage_tables()[0].record_schema();
            // This query-local target is the semantic read boundary. Unlike
            // the durable all-fields storage target, it must expose the
            // authored descriptor itself: enum tags are translated into that
            // descriptor, and an absent target case excludes only this row.
            let output = logical_output;
            self.database
                .define_variant_projection(storage_table, &projection_target, output)?;
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
                let fields = self.physical_current_projection_case_for_enum_columns(
                    source_schema,
                    &source_table_name,
                    &source_mapping,
                    target_schema,
                    target_table_name,
                    present,
                    Some(&required_enum_columns),
                )?;
                for storage_table in &storage_tables {
                    if let Some(fields) = fields.clone() {
                        self.database
                            .register_variant_projection_case_omitting_unrepresentable_enums(
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
        Ok(projection_target)
    }

    /// Register the common physical descriptor used to choose the latest
    /// Global/Ahead version before a query-local old-schema projection can
    /// omit an unrepresentable enum case.
    pub(super) fn ensure_physical_current_winner_projection(
        &mut self,
        target_schema: SchemaVersionId,
        target_table_name: &str,
    ) -> Result<(String, Vec<String>), Error> {
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&target_schema)
            .and_then(|mapping| mapping.tables.get(target_table_name))
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "target current winner physical mapping missing",
            ))?;
        let storage_tables = [
            physical_global_current_table_name(target_mapping.table_id),
            physical_ahead_current_table_name(target_mapping.table_id),
        ];
        let target_table = self.table_in_schema(target_table_name, target_schema)?;
        let authored_output = physical_current_descriptor(&target_table, &target_mapping)?;
        let physical_fields = authored_output
            .fields()
            .iter()
            .map(|field| {
                field.name.clone().ok_or(Error::InvalidStoredValue(
                    "physical current winner field unnamed",
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Winner selection operates on raw physical data. Keep the target
        // layout fixed, but take enum registries from the actual evolved
        // storage descriptor so later tags can reach the logical omission
        // boundary without being decoded against an old registry.
        let output = physical_write_descriptor(
            &authored_output,
            &physical_fields,
            self.database.table_schema(&storage_tables[0])?,
        )?;
        let projection_target =
            physical_current_winner_projection_target(target_mapping.table_id, &physical_fields);
        let mut output_fields = None;
        for storage_table in &storage_tables {
            let fields = output
                .fields()
                .iter()
                .map(|field| {
                    field.name.clone().ok_or(Error::InvalidStoredValue(
                        "physical current winner field unnamed",
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(existing) = &output_fields {
                if existing != &fields {
                    return Err(Error::InvalidStoredValue(
                        "physical current winner descriptors disagree",
                    ));
                }
            } else {
                output_fields = Some(fields);
            }
            self.database
                .define_variant_projection(storage_table, &projection_target, output)?;
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
                    "physical current winner source schema alias missing",
                ))?;
            let source_table = self.table_in_schema(&source_table_name, source_schema)?;
            let target_columns_by_physical_field = target_mapping
                .columns
                .iter()
                .map(|(column, id)| (physical_user_column_field(*id), column.clone()))
                .collect::<BTreeMap<_, _>>();
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
                let available =
                    physical_current_field_names_for_case(&source_table, &source_mapping, present)?
                        .into_iter()
                        .collect::<BTreeSet<_>>();
                for storage_table in &storage_tables {
                    let fields = output
                        .fields()
                        .iter()
                        .map(|field| {
                            let name = field.name.clone().ok_or(Error::InvalidStoredValue(
                                "physical current winner field unnamed",
                            ))?;
                            if available.contains(&name) {
                                Ok(ProjectField::named(name))
                            } else if let Some(column) = target_columns_by_physical_field.get(&name)
                            {
                                // Only mapped user columns can be absent because
                                // of a lens. Witness and system fields must remain
                                // raw physical fields. The resulting field may be
                                // an Add default or a value carried through a
                                // Rename/Copy chain from the source variant.
                                Ok(self
                                    .lens_projection_for_missing_current_field(
                                        source_schema,
                                        &source_table_name,
                                        &source_mapping,
                                        &available,
                                        target_schema,
                                        target_table_name,
                                        &target_mapping,
                                        column,
                                        name.clone(),
                                        field.value_type.clone(),
                                    )?
                                    .unwrap_or_else(|| {
                                        ProjectField::literal_typed(
                                            name,
                                            Value::Nullable(None),
                                            field.value_type.clone(),
                                        )
                                    }))
                            } else if matches!(field.value_type, records::ValueType::Nullable(_)) {
                                Ok(ProjectField::literal_typed(
                                    name,
                                    Value::Nullable(None),
                                    field.value_type.clone(),
                                ))
                            } else {
                                Err(Error::InvalidStoredValue(
                                    "physical current winner source misses required field",
                                ))
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    self.database.refresh_variant_case_for_registry_evolution(
                        storage_table,
                        &projection_target,
                        tag,
                        fields,
                    )?;
                }
            }
        }
        Ok((projection_target, output_fields.unwrap_or_default()))
    }

    /// Resolve a missing target user field through the migration path before
    /// the Global/Ahead arg-max. This deliberately yields a projection field,
    /// not only a literal: `CopyColumn` chains must read their actual source
    /// physical field while the winner still has its source variant layout.
    fn lens_projection_for_missing_current_field(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        available: &BTreeSet<String>,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        target_mapping: &TablePhysicalMapping,
        target_column: &str,
        output_name: String,
        output_type: records::ValueType,
    ) -> Result<Option<ProjectField>, Error> {
        let source_table = self.table_in_schema(source_table_name, source_schema)?;
        let mut cells = source_table
            .columns
            .iter()
            .map(|column| {
                let projection = source_mapping
                    .columns
                    .get(&column.name)
                    .and_then(|column_id| {
                        let name = physical_user_column_field(*column_id);
                        available
                            .contains(&name)
                            .then_some(CurrentWinnerCellProjection::Field {
                                name,
                                column_id: *column_id,
                                column_type: column.column_type.clone(),
                            })
                    })
                    .unwrap_or(CurrentWinnerCellProjection::Null);
                (column.name.clone(), projection)
            })
            .collect::<BTreeMap<_, _>>();
        for direction in [LensPathDirection::Forward, LensPathDirection::Reverse] {
            let Some(path) = self.compiled_lens_path(
                source_schema,
                target_schema,
                direction,
                source_table_name,
            )?
            else {
                continue;
            };
            if path.target_table != target_table_name {
                continue;
            }
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
                            .or_insert(CurrentWinnerCellProjection::Literal(default));
                    }
                    CompiledLensOp::Drop { column } => {
                        cells.remove(&column);
                    }
                }
            }
            return Ok(match cells.remove(target_column) {
                Some(CurrentWinnerCellProjection::Field {
                    name: source,
                    column_id: source_column_id,
                    column_type: source_column_type,
                }) => {
                    let target_column_id =
                        target_mapping.columns.get(target_column).copied().ok_or(
                            Error::InvalidStoredValue(
                                "target current winner column mapping missing",
                            ),
                        )?;
                    let target_column_type = self
                        .table_in_schema(target_table_name, target_schema)?
                        .columns
                        .iter()
                        .find(|column| column.name == target_column)
                        .map(|column| column.column_type.clone())
                        .ok_or(Error::InvalidStoredValue(
                            "target current winner column schema missing",
                        ))?;
                    if source_column_id == target_column_id
                        || !physical_mapping_has_enum_boundary(source_mapping, source_column_id)
                            && !physical_mapping_has_enum_boundary(target_mapping, target_column_id)
                            && !value_type_has_enum_boundary(&source_column_type)
                            && !value_type_has_enum_boundary(&target_column_type)
                    {
                        Some(ProjectField::renamed(source, output_name))
                    } else {
                        Some(ProjectField::recursive_enum_remap(
                            source,
                            output_name,
                            output_type,
                            self.physical_copy_enum_remaps(
                                source_mapping,
                                source_column_id,
                                target_mapping,
                                target_column_id,
                                &source_column_type,
                                &target_column_type,
                            )?,
                        ))
                    }
                }
                Some(CurrentWinnerCellProjection::Literal(default)) => {
                    let default = if matches!(output_type, records::ValueType::Nullable(_))
                        && !matches!(default, Value::Nullable(_))
                    {
                        Value::Nullable(Some(Box::new(default)))
                    } else {
                        default
                    };
                    Some(ProjectField::literal_typed(
                        output_name,
                        default,
                        output_type,
                    ))
                }
                Some(CurrentWinnerCellProjection::Null) | None => None,
            });
        }
        Ok(None)
    }

    /// Build the logical query-local projection placed after physical current
    /// winner selection. Its enum remaps are intentionally non-total row
    /// omissions, never generic query errors.
    pub(super) fn physical_current_post_winner_projection_fields(
        &mut self,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        required_fields: &BTreeSet<String>,
    ) -> Result<Vec<ProjectField>, Error> {
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&target_schema)
            .and_then(|mapping| mapping.tables.get(target_table_name))
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "target post-winner physical mapping missing",
            ))?;
        let target_table = self.table_in_schema(target_table_name, target_schema)?;
        let required_enum_columns = target_table
            .columns
            .iter()
            .filter(|column| required_fields.contains(&column.name))
            .filter_map(|column| target_mapping.columns.get(&column.name).copied())
            .collect::<BTreeSet<_>>();
        self.physical_current_projection_case_for_enum_columns(
            target_schema,
            target_table_name,
            &target_mapping,
            target_schema,
            target_table_name,
            None,
            Some(&required_enum_columns),
        )?
        .ok_or(Error::InvalidStoredValue(
            "post-winner current projection unexpectedly lacks fields",
        ))
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
        self.register_physical_current_variant_projections()?;
        self.register_physical_current_winner_projections()
    }

    /// Keep the raw Global/Ahead winner targets live as the physical enum
    /// registries evolve, so a subsequent query-local lowering pass can read
    /// every newly introduced source variant.
    fn register_physical_current_winner_projections(&mut self) -> Result<(), Error> {
        let targets = self
            .catalogue
            .physical_mappings
            .iter()
            .flat_map(|(schema_version, mapping)| {
                mapping
                    .tables
                    .keys()
                    .map(|table_name| (*schema_version, table_name.clone()))
            })
            .collect::<BTreeSet<_>>();
        for (schema_version, table_name) in targets {
            self.ensure_physical_current_winner_projection(schema_version, &table_name)?;
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
            None,
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
        self.physical_current_projection_case_for_enum_columns(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            present,
            None,
        )
    }

    fn physical_current_projection_case_for_enum_columns(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        present: Option<&BTreeSet<String>>,
        required_enum_columns: Option<&BTreeSet<PhysicalColumnId>>,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        self.physical_content_projection_case(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            ContentProjectionShape::Current,
            present,
            required_enum_columns,
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
        required_enum_columns: Option<&BTreeSet<PhysicalColumnId>>,
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
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&target_schema)
            .and_then(|mapping| mapping.tables.get(target_table_name))
            .ok_or(Error::InvalidStoredValue(
                "target enum physical mapping missing",
            ))?;
        let physical_names = match shape {
            ContentProjectionShape::History => {
                physical_history_field_names(&target_table, target_mapping)?
            }
            ContentProjectionShape::Current => {
                physical_current_field_names(&target_table, target_mapping)?
            }
        };
        let physical_storage = match shape {
            ContentProjectionShape::History => physical_history_table_name(target_mapping.table_id),
            ContentProjectionShape::Current => {
                physical_global_current_table_name(target_mapping.table_id)
            }
        };
        let projection_output = if required_enum_columns.is_some() {
            // Query-local enum targets are authored-descriptor boundaries;
            // their recursive remap must validate/encode against the old
            // schema rather than the physical registry descriptor.
            target_storage.record_schema()
        } else {
            widened_projection_descriptor(
                &target_storage.record_schema(),
                &physical_names,
                self.database.table_schema(&physical_storage)?,
            )?
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
                    let column_id = target_mapping.columns.get(&column.name).copied().ok_or(
                        Error::InvalidStoredValue("target enum physical column mapping missing"),
                    )?;
                    let has_enum_boundary =
                        target_mapping.scalar_enum_cases.contains_key(&column_id)
                            || target_mapping.payload_enum_cases.contains_key(&column_id)
                            || target_mapping
                                .nested_scalar_enum_cases
                                .contains_key(&column_id)
                            || target_mapping
                                .nested_payload_enum_cases
                                .contains_key(&column_id);
                    let direct_enum = matches!(
                        column.column_type,
                        records::ValueType::EnumTag(_) | records::ValueType::Enum(_)
                    );
                    if has_enum_boundary || direct_enum {
                        if required_enum_columns
                            .is_some_and(|required| !required.contains(&column_id))
                        {
                            // This source does not semantically consume the
                            // cell.  Do not decode an unknown physical case
                            // merely to populate an otherwise unused logical
                            // field, and do not let the physical tag cross the
                            // boundary as an authored value.
                            fields.push(ProjectField::literal_typed(
                                output,
                                Value::Nullable(None),
                                records::ValueType::Nullable(Box::new(column.column_type.clone())),
                            ));
                            continue;
                        }
                        let remaps = if has_enum_boundary {
                            self.physical_to_authored_enum_remaps(target_mapping, column_id)?
                        } else {
                            // Initial table construction precedes durable
                            // registry hydration. At that bootstrap boundary
                            // the sole physical descriptor uses exactly this
                            // authored tag order, so an explicit identity map
                            // both disables raw copying and records the same
                            // descriptor-aware operation used after hydration.
                            match &column.column_type {
                                records::ValueType::EnumTag(schema) => EnumOccurrenceRemaps {
                                    scalar: BTreeMap::from([(
                                        "root".to_owned(),
                                        (0..schema.variants.len())
                                            .map(|tag| u8::try_from(tag).ok())
                                            .collect(),
                                    )]),
                                    payload: BTreeMap::new(),
                                    payload_children: BTreeMap::new(),
                                },
                                records::ValueType::Enum(schema) => EnumOccurrenceRemaps {
                                    scalar: BTreeMap::new(),
                                    payload: BTreeMap::from([(
                                        "root".to_owned(),
                                        (0..schema.cases.len())
                                            .map(|tag| u32::try_from(tag).ok())
                                            .collect(),
                                    )]),
                                    payload_children: BTreeMap::from([(
                                        "root".to_owned(),
                                        (0..schema.cases.len())
                                            .map(|tag| Some(format!("root/case/bootstrap/{tag}")))
                                            .collect(),
                                    )]),
                                },
                                _ => unreachable!("direct enum checked above"),
                            }
                        };
                        let target = projection_output
                            .field_index(&user_column_field(&column.name))
                            .and_then(|index| projection_output.fields().get(index))
                            .ok_or(Error::InvalidStoredValue(
                                "target enum projection output field missing",
                            ))?
                            .value_type
                            .clone();
                        fields.push(if required_enum_columns.is_some() {
                            ProjectField::recursive_enum_remap_omitting_unrepresentable(
                                source, output, target, remaps,
                            )
                        } else {
                            ProjectField::recursive_enum_remap(source, output, target, remaps)
                        });
                    } else {
                        fields.push(ProjectField::renamed(source, output));
                    }
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
                "stored row schema version alias missing while resolving storage table",
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

    /// Re-encode every enum occurrence in a logical storage record before it
    /// crosses into a physical table.  History, settled-current and
    /// ahead-current writes share this boundary; allowing one of those paths
    /// to raw-copy an authored tag would make the durable table internally
    /// inconsistent after concurrent schema introductions.
    pub(super) fn remap_authored_enum_cells_for_physical(
        &self,
        values: &mut [Value],
        source_table: &TableSchema,
        source_mapping: &TablePhysicalMapping,
        physical_table: &GrooveTableSchema,
        user_cells: usize,
    ) -> Result<(), Error> {
        for (column_index, column) in source_table.columns.iter().enumerate() {
            let column_id = source_mapping.columns.get(&column.name).copied().ok_or(
                Error::InvalidStoredValue("physical enum write column mapping missing"),
            )?;
            let value_index = user_cells + column_index;
            let value = values
                .get_mut(value_index)
                .ok_or(Error::InvalidStoredValue(
                    "physical enum write field missing",
                ))?;
            let mut remaps = EnumOccurrenceRemaps::default();
            if let Some(authored_cases) = source_mapping.scalar_enum_cases.get(&column_id) {
                let physical_cases =
                    self.physical_scalar_enum_cases(source_mapping.table_id, column_id)?;
                remaps.scalar.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| {
                            physical_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u8::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "physical scalar enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
            }
            if let Some(authored_cases) = source_mapping.payload_enum_cases.get(&column_id) {
                let physical_cases =
                    self.physical_payload_enum_cases(source_mapping.table_id, column_id)?;
                remaps.payload.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| {
                            physical_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u32::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "physical payload enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
                remaps.payload_children.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| Some(global_case_path("root", identity)))
                        .collect(),
                );
            }
            if let Some(authored_paths) = source_mapping.nested_scalar_enum_cases.get(&column_id) {
                for (path, authored_cases) in authored_paths {
                    let physical_cases = self.physical_nested_scalar_enum_cases(
                        source_mapping.table_id,
                        column_id,
                        path,
                    )?;
                    remaps.scalar.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| {
                                physical_cases
                                    .iter()
                                    .position(|candidate| candidate == identity)
                                    .map(|tag| {
                                        u8::try_from(tag).map_err(|_| {
                                            Error::InvalidStoredValue(
                                                "physical nested scalar enum tag exhausted",
                                            )
                                        })
                                    })
                                    .transpose()
                            })
                            .collect::<Result<_, _>>()?,
                    );
                }
            }
            if let Some(authored_paths) = source_mapping.nested_payload_enum_cases.get(&column_id) {
                for (path, authored_cases) in authored_paths {
                    let physical_cases = self.physical_nested_payload_enum_cases(
                        source_mapping.table_id,
                        column_id,
                        path,
                    )?;
                    remaps.payload.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| {
                                physical_cases
                                    .iter()
                                    .position(|candidate| candidate == identity)
                                    .map(|tag| {
                                        u32::try_from(tag).map_err(|_| {
                                            Error::InvalidStoredValue(
                                                "physical nested payload enum tag exhausted",
                                            )
                                        })
                                    })
                                    .transpose()
                            })
                            .collect::<Result<_, _>>()?,
                    );
                    remaps.payload_children.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| Some(global_case_path(path, identity)))
                            .collect(),
                    );
                }
            }
            if remaps.scalar.is_empty() && remaps.payload.is_empty() {
                continue;
            }
            let physical_type = physical_table
                .columns
                .iter()
                .find(|physical| physical.name == physical_user_column_field(column_id))
                .map(|physical| &physical.column_type)
                .ok_or(Error::InvalidStoredValue(
                    "physical enum write column missing",
                ))?;
            let (Value::Nullable(Some(inner)), records::ValueType::Nullable(physical)) =
                (value.clone(), physical_type)
            else {
                continue;
            };
            *value = Value::Nullable(Some(Box::new(remap_nested_enum_value(
                *inner,
                &column.column_type,
                physical,
                &remaps,
                "root",
            )?)));
        }
        Ok(())
    }

    pub(super) fn version_storage_write_binding(
        &mut self,
        version: &VersionRow,
    ) -> Result<(groove::Intern<String>, groove::records::VariantRecord), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored row schema version alias missing while preparing storage write",
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
        let physical_table = self.database.table_schema(&binding.storage_table)?.clone();
        let descriptor = physical_write_descriptor(
            &source_table.history_storage_table().record_schema(),
            &physical_history_field_names(&source_table, source_mapping)?,
            &physical_table,
        )?;
        // The authored row carries declaration-local enum ordinals.  Rewrite
        // those cells through their durable schema-qualified identities before
        // giving the record to the physical table; raw-copying would alias two
        // concurrent siblings which both authored ordinal 2.
        let mut values = version.record.to_values()?;
        for (column_index, column) in source_table.columns.iter().enumerate() {
            let column_id = source_mapping.columns.get(&column.name).copied().ok_or(
                Error::InvalidStoredValue("physical scalar enum write column mapping missing"),
            )?;
            let value_index = HistoryRowRecord::USER_CELLS + column_index;
            let value = values
                .get_mut(value_index)
                .ok_or(Error::InvalidStoredValue(
                    "history scalar enum write field missing",
                ))?;
            let mut remaps = EnumOccurrenceRemaps::default();
            if let Some(authored_cases) = source_mapping.scalar_enum_cases.get(&column_id) {
                let physical_cases =
                    self.physical_scalar_enum_cases(source_mapping.table_id, column_id)?;
                remaps.scalar.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| {
                            physical_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u8::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "physical scalar enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
            }
            if let Some(authored_cases) = source_mapping.payload_enum_cases.get(&column_id) {
                let physical_cases =
                    self.physical_payload_enum_cases(source_mapping.table_id, column_id)?;
                remaps.payload.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| {
                            physical_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u32::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "physical payload enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
                remaps.payload_children.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| Some(global_case_path("root", identity)))
                        .collect(),
                );
            }
            if let Some(authored_paths) = source_mapping.nested_scalar_enum_cases.get(&column_id) {
                for (path, authored_cases) in authored_paths {
                    let physical_cases = self.physical_nested_scalar_enum_cases(
                        source_mapping.table_id,
                        column_id,
                        path,
                    )?;
                    remaps.scalar.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| {
                                physical_cases
                                    .iter()
                                    .position(|candidate| candidate == identity)
                                    .map(|tag| {
                                        u8::try_from(tag).map_err(|_| {
                                            Error::InvalidStoredValue(
                                                "physical nested scalar enum tag exhausted",
                                            )
                                        })
                                    })
                                    .transpose()
                            })
                            .collect::<Result<_, _>>()?,
                    );
                }
            }
            if let Some(authored_paths) = source_mapping.nested_payload_enum_cases.get(&column_id) {
                for (path, authored_cases) in authored_paths {
                    let physical_cases = self.physical_nested_payload_enum_cases(
                        source_mapping.table_id,
                        column_id,
                        path,
                    )?;
                    remaps.payload.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| {
                                physical_cases
                                    .iter()
                                    .position(|candidate| candidate == identity)
                                    .map(|tag| {
                                        u32::try_from(tag).map_err(|_| {
                                            Error::InvalidStoredValue(
                                                "physical nested payload enum tag exhausted",
                                            )
                                        })
                                    })
                                    .transpose()
                            })
                            .collect::<Result<_, _>>()?,
                    );
                    remaps.payload_children.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| Some(global_case_path(path, identity)))
                            .collect(),
                    );
                }
            }
            if remaps.scalar.is_empty() && remaps.payload.is_empty() {
                continue;
            }
            let physical_type = physical_table
                .columns
                .iter()
                .find(|physical| physical.name == physical_user_column_field(column_id))
                .map(|physical| &physical.column_type)
                .ok_or(Error::InvalidStoredValue(
                    "physical enum write column missing",
                ))?;
            let (Value::Nullable(Some(inner)), records::ValueType::Nullable(physical)) =
                (value.clone(), physical_type)
            else {
                continue;
            };
            *value = Value::Nullable(Some(Box::new(remap_nested_enum_value(
                *inner,
                &column.column_type,
                physical,
                &remaps,
                "root",
            )?)));
        }
        let record = OwnedRecord::new(descriptor.create(&values)?, descriptor);
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
pub(super) fn physical_write_descriptor(
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
                // Writes target the physical table itself. Unlike read-side
                // widening, its descriptor must retain the physical enum
                // registry identities so Groove accepts the variant record;
                // values are explicitly re-encoded before this point.
                let _ = logical;
                Ok((name.clone(), physical.column_type.clone()))
            })
            .collect::<Result<Vec<_>, Error>>()?,
    ))
}

#[cfg(test)]
fn remap_authored_scalar_enum_value(
    value: Value,
    authored_cases: &[GlobalScalarEnumCaseId],
    physical_cases: &[GlobalScalarEnumCaseId],
) -> Result<Value, Error> {
    match value {
        Value::EnumTag(authored_tag) => {
            let identity =
                authored_cases
                    .get(usize::from(authored_tag))
                    .ok_or(Error::InvalidStoredValue(
                        "authored scalar enum tag outside identity mapping",
                    ))?;
            let physical_tag = physical_cases
                .iter()
                .position(|candidate| candidate == identity)
                .ok_or(Error::InvalidStoredValue(
                    "authored scalar enum identity absent from physical registry",
                ))?;
            Ok(Value::EnumTag(u8::try_from(physical_tag).map_err(
                |_| Error::InvalidStoredValue("physical scalar enum tag exhausted"),
            )?))
        }
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        Value::Nullable(Some(value)) => Ok(Value::Nullable(Some(Box::new(
            remap_authored_scalar_enum_value(*value, authored_cases, physical_cases)?,
        )))),
        _ => Err(Error::InvalidStoredValue(
            "authored scalar enum value has non-enum representation",
        )),
    }
}

#[cfg(test)]
fn remap_authored_payload_enum_value(
    value: Value,
    authored_schema: &records::EnumSchema,
    authored_cases: &[GlobalScalarEnumCaseId],
    physical_cases: &[GlobalScalarEnumCaseId],
) -> Result<Value, Error> {
    match value {
        Value::Enum(value) => {
            authored_schema.case(value.tag())?;
            let identity = authored_cases
                .get(usize::try_from(value.tag()).map_err(|_| {
                    Error::InvalidStoredValue("authored payload enum tag exhausted")
                })?)
                .ok_or(Error::InvalidStoredValue(
                    "authored payload enum tag outside identity mapping",
                ))?;
            let physical_tag = physical_cases
                .iter()
                .position(|case| case == identity)
                .ok_or(Error::InvalidStoredValue(
                    "authored payload enum identity absent from physical registry",
                ))?;
            // Payload descriptors are checked again by the physical record
            // encoder.  A same-name sibling with a different layout therefore
            // fails rather than being silently reinterpreted.
            Ok(Value::Enum(records::EnumValue::new(
                u32::try_from(physical_tag).map_err(|_| {
                    Error::InvalidStoredValue("physical payload enum tag exhausted")
                })?,
                value.into_record(),
            )))
        }
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        Value::Nullable(Some(value)) => Ok(Value::Nullable(Some(Box::new(
            remap_authored_payload_enum_value(
                *value,
                authored_schema,
                authored_cases,
                physical_cases,
            )?,
        )))),
        _ => Err(Error::InvalidStoredValue(
            "authored payload enum value has non-enum representation",
        )),
    }
}

fn remap_nested_enum_value(
    value: Value,
    authored: &records::ValueType,
    physical: &records::ValueType,
    remaps: &EnumOccurrenceRemaps,
    path: &str,
) -> Result<Value, Error> {
    use records::ValueType;
    match (value, authored, physical) {
        (Value::EnumTag(tag), ValueType::EnumTag(_), ValueType::EnumTag(_)) => remaps
            .scalar
            .get(path)
            .and_then(|tags| tags.get(usize::from(tag)))
            .and_then(|tag| *tag)
            .map(Value::EnumTag)
            .ok_or(Error::InvalidStoredValue(
                "nested scalar enum tag absent from physical mapping",
            )),
        (Value::Nullable(None), ValueType::Nullable(_), ValueType::Nullable(_)) => {
            Ok(Value::Nullable(None))
        }
        (
            Value::Nullable(Some(value)),
            ValueType::Nullable(authored),
            ValueType::Nullable(physical),
        ) => Ok(Value::Nullable(Some(Box::new(remap_nested_enum_value(
            *value,
            authored,
            physical,
            remaps,
            &format!("{path}/nullable"),
        )?)))),
        (Value::Array(values), ValueType::Array(authored), ValueType::Array(physical)) => {
            Ok(Value::Array(
                values
                    .into_iter()
                    .map(|value| {
                        remap_nested_enum_value(
                            value,
                            authored,
                            physical,
                            remaps,
                            &format!("{path}/array"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (Value::Tuple(values), ValueType::Tuple(authored), ValueType::Tuple(physical))
            if authored.len() == physical.len() && values.len() == authored.len() =>
        {
            Ok(Value::Tuple(
                values
                    .into_iter()
                    .zip(authored.iter().zip(physical))
                    .enumerate()
                    .map(|(index, (value, (authored, physical)))| {
                        remap_nested_enum_value(
                            value,
                            authored,
                            physical,
                            remaps,
                            &format!("{path}/tuple/{index}"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (Value::Record(record), ValueType::Record(authored), ValueType::Record(physical))
            if authored.fields().len() == physical.fields().len() =>
        {
            let values = record.to_values()?;
            let values = values
                .into_iter()
                .zip(authored.fields().iter().zip(physical.fields()))
                .map(|(value, (authored, physical))| {
                    let name = authored.name.as_deref().ok_or(Error::InvalidStoredValue(
                        "nested record enum field unnamed",
                    ))?;
                    remap_nested_enum_value(
                        value,
                        &authored.value_type,
                        &physical.value_type,
                        remaps,
                        &format!("{path}/record/{name}"),
                    )
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(Value::Record(OwnedRecord::new(
                physical.create(&values)?,
                (**physical).clone(),
            )))
        }
        (Value::Enum(value), ValueType::Enum(authored), ValueType::Enum(physical)) => {
            let authored_tag = value.tag();
            let physical_tag = remaps
                .payload
                .get(path)
                .and_then(|tags| tags.get(usize::try_from(authored_tag).ok()?))
                .and_then(|tag| *tag)
                .ok_or(Error::InvalidStoredValue(
                    "nested payload enum tag absent from physical mapping",
                ))?;
            let authored_case = authored.case(authored_tag)?;
            let physical_case = physical.case(physical_tag)?;
            if authored_case.payload.fields().len() != physical_case.payload.fields().len() {
                return Err(Error::InvalidStoredValue(
                    "nested payload enum payload width changed",
                ));
            }
            let semantic_child_root = remaps
                .payload_children
                .get(path)
                .and_then(|paths| paths.get(usize::try_from(authored_tag).ok()?))
                .and_then(|path| path.as_deref());
            let child_root = semantic_child_root
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{path}/case/{authored_tag}"));
            let values = value.record().to_values()?;
            let values = values
                .into_iter()
                .zip(
                    authored_case
                        .payload
                        .fields()
                        .iter()
                        .zip(physical_case.payload.fields()),
                )
                .map(|(value, (authored, physical))| {
                    let name = authored.name.as_deref().ok_or(Error::InvalidStoredValue(
                        "nested payload enum field unnamed",
                    ))?;
                    remap_nested_enum_value(
                        value,
                        &authored.value_type,
                        &physical.value_type,
                        remaps,
                        &if semantic_child_root.is_some() {
                            format!("{child_root}/record/{name}")
                        } else {
                            format!("{child_root}/{name}")
                        },
                    )
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(Value::Enum(records::EnumValue::new(
                physical_tag,
                OwnedRecord::new(
                    physical_case.payload.create(&values)?,
                    physical_case.payload.clone(),
                ),
            )))
        }
        (value, authored, physical) if authored == physical => Ok(value),
        _ => Err(Error::InvalidStoredValue(
            "nested enum remap descriptor mismatch",
        )),
    }
}

fn widen_projection_value_type(
    logical: &records::ValueType,
    physical: &records::ValueType,
) -> records::ValueType {
    use records::ValueType;
    match (logical, physical) {
        // Projection crosses the physical interning boundary.  It must expose
        // the target schema's declaration-local enum descriptor after the
        // explicit tag remap above, not leak the physical descriptor/tag space.
        (ValueType::EnumTag(logical), ValueType::EnumTag(_)) => ValueType::EnumTag(logical.clone()),
        (ValueType::Enum(logical), ValueType::Enum(_)) => ValueType::Enum(logical.clone()),
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
        // First form the persistent registry for every scalar enum occurrence.
        // Concurrent schemas may use the same authored ordinal for distinct
        // cases; their schema-qualified identities must therefore be unioned
        // before any descriptor assigns compact local tags.
        let mut scalar_enum_registries =
            BTreeMap::<PhysicalColumnId, BTreeSet<GlobalScalarEnumCaseId>>::new();
        for (schema_version, logical_table, mapping) in &variants {
            for column in &logical_table.columns {
                let records::ValueType::EnumTag(enum_schema) = &column.column_type else {
                    continue;
                };
                let column_id =
                    mapping
                        .columns
                        .get(&column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical scalar enum column mapping missing",
                        ))?;
                let cases = mapping
                    .scalar_enum_cases
                    .get(&column_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        // Provisional bootstrap schemas acquire their durable
                        // mapping immediately after table construction.  Until
                        // then this deterministic spelling is the same mapping
                        // hydration will persist; it is not receipt-order state.
                        enum_schema
                            .variants
                            .iter()
                            .enumerate()
                            .map(|(ordinal, _)| GlobalScalarEnumCaseId {
                                introducing_schema: *schema_version,
                                introducing_ordinal: ordinal as u8,
                            })
                            .collect()
                    });
                scalar_enum_registries
                    .entry(column_id)
                    .or_default()
                    .extend(cases);
            }
        }
        let scalar_enum_registries = scalar_enum_registries
            .into_iter()
            .map(|(column_id, cases)| {
                let mut cases = cases.into_iter().collect::<Vec<_>>();
                cases.sort_by(|left, right| {
                    compare_scalar_enum_cases(schema_version_aliases, left, right)
                });
                Ok((column_id, cases))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        let mut nested_scalar_enum_registries =
            BTreeMap::<(PhysicalColumnId, String), BTreeSet<GlobalScalarEnumCaseId>>::new();
        for (schema_version, logical_table, mapping) in &variants {
            // Bootstrap constructs physical tables before the freshly
            // introduced mapping is hydrated into the catalogue. Seed nested
            // occurrences from the authored descriptor in that one state;
            // otherwise the first table gets a generic registry id and the
            // next synchronization sees an incompatible field definition.
            let mut bootstrap_paths = mapping.nested_scalar_enum_cases.clone();
            if bootstrap_paths.is_empty() {
                for column in &logical_table.columns {
                    if matches!(column.column_type, records::ValueType::EnumTag(_)) {
                        continue;
                    }
                    hydrate_nested_scalar_enum_cases(
                        &column.column_type,
                        *schema_version,
                        "root",
                        bootstrap_paths
                            .entry(mapping.columns.get(&column.name).copied().ok_or(
                                Error::InvalidStoredValue(
                                    "physical nested scalar enum column mapping missing",
                                ),
                            )?)
                            .or_default(),
                    )?;
                }
            }
            for (column_id, paths) in &bootstrap_paths {
                for (path, cases) in paths {
                    nested_scalar_enum_registries
                        .entry((*column_id, path.clone()))
                        .or_default()
                        .extend(cases.iter().cloned());
                }
            }
        }
        let nested_scalar_enum_registries = nested_scalar_enum_registries
            .into_iter()
            .map(|((column_id, path), cases)| {
                let mut cases = cases.into_iter().collect::<Vec<_>>();
                cases.sort_by(|left, right| {
                    compare_scalar_enum_cases(schema_version_aliases, left, right)
                });
                Ok(((column_id, path), cases))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        // Payload enum occurrences, including those inside another payload,
        // need the same lineage-wide union.  The path beneath a payload case
        // is rooted by that case's GlobalCaseId, so concurrent siblings which
        // both authored ordinal `n` never share a descendant registry.
        let mut nested_payload_enum_registries =
            BTreeMap::<(PhysicalColumnId, String), BTreeSet<GlobalScalarEnumCaseId>>::new();
        let mut nested_payload_enum_layouts = BTreeMap::<
            (PhysicalColumnId, String, GlobalScalarEnumCaseId),
            records::RecordDescriptor,
        >::new();
        for (schema_version, logical_table, mapping) in &variants {
            for column in &logical_table.columns {
                let column_id =
                    mapping
                        .columns
                        .get(&column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical nested payload enum column mapping missing",
                        ))?;
                // As for nested scalar cases above, the first physical-table
                // construction precedes catalogue hydration. Seed payload
                // identities from the authored descriptor so reopening does
                // not try to replace a generic nested registry.
                let mut bootstrap_paths = mapping.nested_payload_enum_cases.clone();
                if bootstrap_paths.is_empty() {
                    hydrate_nested_payload_enum_cases(
                        &column.column_type,
                        *schema_version,
                        "root",
                        bootstrap_paths.entry(column_id).or_default(),
                    )?;
                }
                let Some(paths) = bootstrap_paths.get(&column_id) else {
                    continue;
                };
                for (path, cases) in paths {
                    nested_payload_enum_registries
                        .entry((column_id, path.clone()))
                        .or_default()
                        .extend(cases.iter().cloned());
                }
                let mut layouts = BTreeMap::new();
                collect_nested_payload_enum_layouts(
                    &column.column_type,
                    "root",
                    paths,
                    &mut layouts,
                )?;
                for ((path, identity), layout) in layouts {
                    let key = (column_id, path, identity);
                    match nested_payload_enum_layouts.entry(key) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(layout);
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get() == &layout => {}
                        std::collections::btree_map::Entry::Occupied(_) => {
                            return Err(Error::InvalidStoredValue(
                                "same nested payload enum identity has incompatible layout",
                            ));
                        }
                    }
                }
            }
        }
        let nested_payload_enum_registries = nested_payload_enum_registries
            .into_iter()
            .map(|((column_id, path), cases)| {
                let mut cases = cases.into_iter().collect::<Vec<_>>();
                cases.sort_by(|left, right| {
                    compare_scalar_enum_cases(schema_version_aliases, left, right)
                });
                Ok(((column_id, path), cases))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        let mut payload_enum_registries =
            BTreeMap::<PhysicalColumnId, BTreeSet<GlobalScalarEnumCaseId>>::new();
        let mut payload_enum_layouts =
            BTreeMap::<(PhysicalColumnId, GlobalScalarEnumCaseId), records::RecordDescriptor>::new(
            );
        for (schema_version, logical_table, mapping) in &variants {
            for column in &logical_table.columns {
                let records::ValueType::Enum(enum_schema) = &column.column_type else {
                    continue;
                };
                let column_id =
                    mapping
                        .columns
                        .get(&column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical payload enum column mapping missing",
                        ))?;
                let identities = mapping
                    .payload_enum_cases
                    .get(&column_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        enum_schema
                            .cases
                            .iter()
                            .enumerate()
                            .map(|(ordinal, _)| GlobalScalarEnumCaseId {
                                introducing_schema: *schema_version,
                                introducing_ordinal: ordinal as u8,
                            })
                            .collect()
                    });
                if identities.len() != enum_schema.cases.len() {
                    return Err(Error::InvalidStoredValue(
                        "payload enum identity mapping width mismatch",
                    ));
                }
                if let Some(nested_root) = mapping
                    .nested_payload_enum_cases
                    .get(&column_id)
                    .and_then(|paths| paths.get("root"))
                    && nested_root != &identities
                {
                    return Err(Error::InvalidStoredValue(
                        "direct and nested payload enum identity mappings diverged",
                    ));
                }
                for (identity, case) in identities.iter().zip(&enum_schema.cases) {
                    let key = (column_id, identity.clone());
                    match payload_enum_layouts.entry(key) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(case.payload.clone());
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get() == &case.payload => {}
                        std::collections::btree_map::Entry::Occupied(_) => {
                            return Err(Error::InvalidStoredValue(
                                "same payload enum identity has incompatible layout",
                            ));
                        }
                    }
                }
                payload_enum_registries
                    .entry(column_id)
                    .or_default()
                    .extend(identities.iter().cloned());
            }
        }
        let payload_enum_registries = payload_enum_registries
            .into_iter()
            .map(|(column_id, cases)| {
                let mut cases = cases.into_iter().collect::<Vec<_>>();
                cases.sort_by(|left, right| {
                    compare_scalar_enum_cases(schema_version_aliases, left, right)
                });
                Ok((column_id, cases))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

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
                let storage_type = match &column.column_type {
                    records::ValueType::EnumTag(_) => {
                        records::ValueType::EnumTag(physical_scalar_enum_schema(
                            column_id,
                            scalar_enum_registries.get(&column_id).ok_or(
                                Error::InvalidStoredValue("physical scalar enum registry missing"),
                            )?,
                        )?)
                        .nullable()
                    }
                    records::ValueType::Enum(_) => {
                        let cases = payload_enum_registries.get(&column_id).ok_or(
                            Error::InvalidStoredValue("physical payload enum registry missing"),
                        )?;
                        let cases = cases
                            .iter()
                            .map(|identity| {
                                let payload = payload_enum_layouts
                                    .get(&(column_id, identity.clone()))
                                    .ok_or(Error::InvalidStoredValue(
                                        "physical payload enum layout missing",
                                    ))?;
                                let scalar_registries = nested_scalar_enum_registries
                                    .iter()
                                    .filter(|((id, _), _)| *id == column_id)
                                    .map(|((_, path), cases)| (path.clone(), cases.clone()))
                                    .collect::<BTreeMap<_, _>>();
                                let payload_registries = nested_payload_enum_registries
                                    .iter()
                                    .filter(|((id, _), _)| *id == column_id)
                                    .map(|((_, path), cases)| (path.clone(), cases.clone()))
                                    .collect::<BTreeMap<_, _>>();
                                let payload_layouts = nested_payload_enum_layouts
                                    .iter()
                                    .filter(|((id, _, _), _)| *id == column_id)
                                    .map(|((_, path, identity), layout)| {
                                        ((path.clone(), identity.clone()), layout.clone())
                                    })
                                    .collect::<BTreeMap<_, _>>();
                                let records::ValueType::Record(payload) =
                                    physical_nested_enum_value_type(
                                        &records::ValueType::Record(Box::new(payload.clone())),
                                        &global_case_path("root", identity),
                                        &scalar_registries,
                                        &payload_registries,
                                        &payload_layouts,
                                        column_id,
                                    )?
                                else {
                                    unreachable!("record lowering preserves payload shape");
                                };
                                Ok(records::EnumCase::new(
                                    physical_scalar_enum_case_name(identity),
                                    *payload,
                                ))
                            })
                            .collect::<Result<Vec<_>, Error>>()?;
                        records::ValueType::Enum(Box::new(
                            records::EnumSchema::new(
                                format!("physical-column-{}", column_id.0),
                                cases,
                            )
                            .map_err(|_| {
                                Error::InvalidStoredValue("invalid physical payload enum registry")
                            })?
                            .with_registry_id(
                                records::variant_registry_id_for_path(&format!(
                                    "physical-column/{}",
                                    column_id.0
                                )),
                            ),
                        ))
                        .nullable()
                    }
                    _ if nested_scalar_enum_registries
                        .keys()
                        .any(|(id, _)| *id == column_id)
                        || nested_payload_enum_registries
                            .keys()
                            .any(|(id, _)| *id == column_id) =>
                    {
                        let scalar_registries = nested_scalar_enum_registries
                            .iter()
                            .filter(|((id, _), _)| *id == column_id)
                            .map(|((_, path), cases)| (path.clone(), cases.clone()))
                            .collect::<BTreeMap<_, _>>();
                        let payload_registries = nested_payload_enum_registries
                            .iter()
                            .filter(|((id, _), _)| *id == column_id)
                            .map(|((_, path), cases)| (path.clone(), cases.clone()))
                            .collect::<BTreeMap<_, _>>();
                        let payload_layouts = nested_payload_enum_layouts
                            .iter()
                            .filter(|((id, _, _), _)| *id == column_id)
                            .map(|((_, path, identity), layout)| {
                                ((path.clone(), identity.clone()), layout.clone())
                            })
                            .collect::<BTreeMap<_, _>>();
                        physical_nested_enum_value_type(
                            &column.column_type,
                            "root",
                            &scalar_registries,
                            &payload_registries,
                            &payload_layouts,
                            column_id,
                        )?
                        .nullable()
                    }
                    _ => column
                        .column_type
                        .clone()
                        .nullable()
                        .rebind_variant_registries(&format!("physical-column/{}", column_id.0)),
                };
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
            // This helper is also used while combining independently authored
            // snapshots.  A shared authored registry id does not make ordinal
            // `n` globally meaningful: concurrent siblings can legitimately
            // introduce distinct cases at that ordinal.  The physical-table
            // path supplies schema-qualified identities and replaces these
            // names with its durable registry; retain a deterministic union
            // here so descriptor construction never aliases sibling cases.
            // This is a physical registry, so its declaration order is the
            // stored tag order. Preserve the established prefix and append
            // only newly observed opaque case names; a sorted set would
            // silently retag existing values.
            let mut variants = left.variants.clone();
            let appended = right
                .variants
                .iter()
                .filter(|variant| !variants.contains(variant))
                .cloned()
                .collect::<Vec<_>>();
            variants.extend(appended);
            Ok(ValueType::EnumTag(
                records::ScalarEnumSchema::new(left.name.clone(), variants)
                    .map_err(|_| Error::InvalidStoredValue("invalid physical enum registry"))?
                    .with_registry_id(left.registry_id()),
            ))
        }
        (ValueType::Enum(left), ValueType::Enum(right))
            if left.registry_id == right.registry_id =>
        {
            // Preserve the established physical tag prefix. These names are
            // opaque spellings of catalogue identities; sorting them would
            // silently retag stored values merely because two schema IDs sort
            // differently. New identities append in the incoming descriptor's
            // already-canonical catalogue order.
            let mut cases = left.cases.clone();
            for incoming_case in &right.cases {
                if let Some(existing_case) = cases
                    .iter_mut()
                    .find(|existing| existing.name == incoming_case.name)
                {
                    existing_case.payload = merge_physical_record_descriptor(
                        &existing_case.payload,
                        &incoming_case.payload,
                    )?;
                } else {
                    cases.push(incoming_case.clone());
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
    physical_descriptor_with_enum_registries(logical_descriptor, physical_names, mapping)
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
    physical_descriptor_with_enum_registries(logical_descriptor, physical_names, mapping)
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
    physical_descriptor_with_enum_registries(logical_descriptor, physical_names, mapping)
}

fn physical_descriptor_with_enum_registries(
    logical: records::RecordDescriptor,
    physical_names: Vec<String>,
    mapping: &TablePhysicalMapping,
) -> Result<records::RecordDescriptor, Error> {
    Ok(records::RecordDescriptor::new(
        physical_names
            .into_iter()
            .zip(logical.fields())
            .map(|(name, field)| {
                let value_type = if let Some(id) = name
                    .strip_prefix("user_")
                    .and_then(|id| id.parse::<u64>().ok())
                {
                    let id = PhysicalColumnId(id);
                    if let Some(cases) = mapping.scalar_enum_cases.get(&id) {
                        physical_scalar_enum_schema(id, cases)
                            .map(|schema| records::ValueType::EnumTag(schema).nullable())?
                    } else {
                        match &field.value_type {
                            // Physical user cells are nullable for absence, but their
                            // direct enum registry belongs to the column occurrence—not
                            // to the nullable wrapper. Match the storage descriptor's
                            // `physical_scalar_enum_schema(column_id, ...)` identity.
                            records::ValueType::Nullable(inner) => records::ValueType::Nullable(
                                Box::new(inner.as_ref().clone().rebind_variant_registries(
                                    &format!("physical-column/{}", id.0),
                                )),
                            ),
                            value_type => value_type
                                .clone()
                                .rebind_variant_registries(&format!("physical-column/{}", id.0)),
                        }
                    }
                } else {
                    field.value_type.clone()
                };
                Ok((name, value_type))
            })
            .collect::<Result<Vec<_>, Error>>()?,
    ))
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

pub(super) fn physical_current_field_names(
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

    fn case(schema: SchemaVersionId, ordinal: u8) -> GlobalScalarEnumCaseId {
        GlobalScalarEnumCaseId {
            introducing_schema: schema,
            introducing_ordinal: ordinal,
        }
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
                    scalar_enum_cases: BTreeMap::new(),
                    payload_enum_cases: BTreeMap::new(),
                    nested_scalar_enum_cases: BTreeMap::new(),
                    nested_payload_enum_cases: BTreeMap::new(),
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
    fn later_sibling_with_a_shallower_ordinal_appends_after_deeper_introduction() {
        // This is an internal lowering invariant. The same ordering primitive
        // builds scalar, direct-payload, and nested-enum physical registries;
        // their compact tags are not publicly observable on their own.
        //
        // base ──► A (+ ordinal 1) ──► A2 (+ ordinal 2)
        //   └────────────────────────► B (+ ordinal 1)
        //
        // B is published later in the dense catalogue, so it must append
        // after A2 rather than retag A2 merely because B's local ordinal is
        // shallower. The test is sensitive to restoring ordinal-first order.
        let base = schema(1);
        let a = schema(2);
        let a2 = schema(3);
        let b = schema(4);
        let aliases = BTreeMap::from([
            (base, SchemaVersionAlias(1)),
            (a, SchemaVersionAlias(2)),
            (a2, SchemaVersionAlias(3)),
            (b, SchemaVersionAlias(4)),
        ]);
        let base_case = case(base, 0);
        let a_case = case(a, 1);
        let a2_case = case(a2, 2);
        let b_case = case(b, 1);

        for registry_kind in ["scalar", "direct payload", "nested"] {
            let mut registry = vec![
                base_case.clone(),
                a_case.clone(),
                a2_case.clone(),
                b_case.clone(),
            ];
            registry.sort_by(|left, right| compare_scalar_enum_cases(&aliases, left, right));
            assert_eq!(
                registry,
                vec![
                    base_case.clone(),
                    a_case.clone(),
                    a2_case.clone(),
                    b_case.clone()
                ],
                "{registry_kind} registry"
            );
        }
    }

    #[test]
    fn concurrent_scalar_enum_merge_preserves_established_prefix_and_distinct_cases() {
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
            .expect("the opposite established prefix also accepts its sibling");

        // This compatibility helper operates on an already-established local
        // physical descriptor. It is intentionally directional: canonical
        // catalogue ordering has already happened before this point, so
        // sorting or rebuilding this descriptor would retag stored values.
        // The schema-qualified physical lowering path supplies that canonical
        // order; this helper must only append a distinct sibling case.
        let records::ValueType::EnumTag(merged_ab) = merged_ab else {
            panic!("expected scalar enum registry");
        };
        let records::ValueType::EnumTag(merged_ba) = merged_ba else {
            panic!("expected scalar enum registry");
        };
        assert_eq!(
            merged_ab.variants,
            vec!["draft", "published", "archived", "snoozed"],
            "left registry stays an exact physical prefix"
        );
        assert_eq!(
            merged_ba.variants,
            vec!["draft", "published", "snoozed", "archived"],
            "the reverse call preserves its own established prefix"
        );
        assert_eq!(merged_ab.variants.len(), 4);
        assert_eq!(merged_ba.variants.len(), 4);
    }

    #[test]
    fn concurrent_scalar_enum_write_remap_never_aliases_sibling_ordinals() {
        let base = schema(1);
        let archived = schema(2);
        let snoozed = schema(3);
        let base_cases = vec![
            GlobalScalarEnumCaseId {
                introducing_schema: base,
                introducing_ordinal: 0,
            },
            GlobalScalarEnumCaseId {
                introducing_schema: base,
                introducing_ordinal: 1,
            },
        ];
        let archived_cases = base_cases
            .iter()
            .cloned()
            .chain(std::iter::once(GlobalScalarEnumCaseId {
                introducing_schema: archived,
                introducing_ordinal: 2,
            }))
            .collect::<Vec<_>>();
        let snoozed_cases = base_cases
            .iter()
            .cloned()
            .chain(std::iter::once(GlobalScalarEnumCaseId {
                introducing_schema: snoozed,
                introducing_ordinal: 2,
            }))
            .collect::<Vec<_>>();
        let physical_cases = archived_cases
            .iter()
            .cloned()
            .chain(snoozed_cases.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let archived_tag = remap_authored_scalar_enum_value(
            Value::Nullable(Some(Box::new(Value::EnumTag(2)))),
            &archived_cases,
            &physical_cases,
        )
        .unwrap();
        let snoozed_tag = remap_authored_scalar_enum_value(
            Value::Nullable(Some(Box::new(Value::EnumTag(2)))),
            &snoozed_cases,
            &physical_cases,
        )
        .unwrap();
        assert_ne!(archived_tag, snoozed_tag);
    }

    #[test]
    fn concurrent_payload_enum_additions_preserve_distinct_case_layouts() {
        let schema = |cases: Vec<records::EnumCase>| {
            records::ValueType::Enum(Box::new(
                records::EnumSchema::new("status", cases)
                    .unwrap()
                    .with_registry_id(92),
            ))
        };
        let payload = |name| records::RecordDescriptor::new([(name, records::ValueType::String)]);
        let archived = schema(vec![
            records::EnumCase::new("draft", payload("label")),
            records::EnumCase::new("published", payload("label")),
            records::EnumCase::new("archived", payload("reason")),
        ]);
        let snoozed = schema(vec![
            records::EnumCase::new("draft", payload("label")),
            records::EnumCase::new("published", payload("label")),
            records::EnumCase::new("snoozed", payload("until")),
        ]);
        let merged = merge_physical_value_type(&archived, &snoozed)
            .expect("concurrent payload cases must coexist");
        let records::ValueType::Enum(registry) = merged else {
            panic!("expected payload enum registry");
        };
        assert_eq!(registry.cases.len(), 4);
        assert!(registry.cases.iter().any(|case| case.name == "archived"));
        assert!(registry.cases.iter().any(|case| case.name == "snoozed"));
    }

    #[test]
    fn concurrent_same_named_payload_case_must_not_merge_incompatibly() {
        let schema = |payload| {
            records::ValueType::Enum(Box::new(
                records::EnumSchema::new("status", [records::EnumCase::new("draft", payload)])
                    .unwrap()
                    .with_registry_id(93),
            ))
        };
        let left = schema(records::RecordDescriptor::new([(
            "label",
            records::ValueType::String,
        )]));
        let right = schema(records::RecordDescriptor::new([(
            "label",
            records::ValueType::U64,
        )]));
        assert!(merge_physical_value_type(&left, &right).is_err());
    }

    #[test]
    fn concurrent_payload_enum_write_remap_never_aliases_sibling_ordinals() {
        let descriptor = records::RecordDescriptor::new([("value", records::ValueType::String)]);
        let authored = |new_case| {
            records::EnumSchema::new(
                "status",
                [
                    records::EnumCase::new("draft", descriptor.clone()),
                    records::EnumCase::new("published", descriptor.clone()),
                    records::EnumCase::new(new_case, descriptor.clone()),
                ],
            )
            .unwrap()
        };
        let archived = authored("archived");
        let snoozed = authored("snoozed");
        let base = schema(1);
        let archived_schema = schema(2);
        let snoozed_schema = schema(3);
        let archived_cases = vec![
            GlobalScalarEnumCaseId {
                introducing_schema: base,
                introducing_ordinal: 0,
            },
            GlobalScalarEnumCaseId {
                introducing_schema: base,
                introducing_ordinal: 1,
            },
            GlobalScalarEnumCaseId {
                introducing_schema: archived_schema,
                introducing_ordinal: 2,
            },
        ];
        let snoozed_cases = vec![
            archived_cases[0].clone(),
            archived_cases[1].clone(),
            GlobalScalarEnumCaseId {
                introducing_schema: snoozed_schema,
                introducing_ordinal: 2,
            },
        ];
        let physical_cases = archived_cases
            .iter()
            .cloned()
            .chain(snoozed_cases.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let payload =
            records::EnumValue::create(2, descriptor.clone(), &[Value::String("x".to_owned())])
                .unwrap();
        let archived_value = remap_authored_payload_enum_value(
            Value::Enum(payload.clone()),
            &archived,
            &archived_cases,
            &physical_cases,
        )
        .unwrap();
        let snoozed_value = remap_authored_payload_enum_value(
            Value::Enum(payload),
            &snoozed,
            &snoozed_cases,
            &physical_cases,
        )
        .unwrap();
        assert_ne!(archived_value, snoozed_value);
    }

    #[test]
    fn nested_scalar_enum_remap_rewrites_array_and_nullable_tags() {
        let authored_enum = records::ValueType::EnumTag(
            records::ScalarEnumSchema::new("state", ["draft", "archived"]).unwrap(),
        );
        let physical_enum = records::ValueType::EnumTag(
            records::ScalarEnumSchema::new("physical", ["draft", "snoozed", "archived"]).unwrap(),
        );
        let authored = records::ValueType::Tuple(vec![
            records::ValueType::Array(Box::new(authored_enum.clone())),
            records::ValueType::Nullable(Box::new(authored_enum)),
        ]);
        let physical = records::ValueType::Tuple(vec![
            records::ValueType::Array(Box::new(physical_enum.clone())),
            records::ValueType::Nullable(Box::new(physical_enum)),
        ]);
        let remaps = EnumOccurrenceRemaps {
            scalar: BTreeMap::from([
                ("root/tuple/0/array".to_owned(), vec![Some(0), Some(2)]),
                ("root/tuple/1/nullable".to_owned(), vec![Some(0), Some(2)]),
            ]),
            payload: BTreeMap::new(),
            payload_children: BTreeMap::new(),
        };
        let remapped = remap_nested_enum_value(
            Value::Tuple(vec![
                Value::Array(vec![Value::EnumTag(1)]),
                Value::Nullable(Some(Box::new(Value::EnumTag(1)))),
            ]),
            &authored,
            &physical,
            &remaps,
            "root",
        )
        .unwrap();
        assert_eq!(
            remapped,
            Value::Tuple(vec![
                Value::Array(vec![Value::EnumTag(2)]),
                Value::Nullable(Some(Box::new(Value::EnumTag(2)))),
            ])
        );
    }

    #[test]
    fn nested_scalar_registry_reconciliation_preserves_inherited_cases() {
        let base = schema(1);
        let child = schema(2);
        let nested = |variants: &[&str]| {
            records::ValueType::Array(Box::new(records::ValueType::Nullable(Box::new(
                records::ValueType::EnumTag(
                    records::ScalarEnumSchema::new("state", variants.iter().copied()).unwrap(),
                ),
            ))))
        };
        let mut cases = BTreeMap::new();
        hydrate_nested_scalar_enum_cases(
            &nested(&["draft", "published"]),
            base,
            "root",
            &mut cases,
        )
        .unwrap();
        reconcile_nested_scalar_enum_cases(
            &nested(&["draft", "published", "archived"]),
            child,
            "root",
            &mut cases,
        )
        .unwrap();
        assert_eq!(cases["root/array/nullable"].len(), 3);
        assert_eq!(cases["root/array/nullable"][0].introducing_schema, base);
        assert_eq!(cases["root/array/nullable"][2].introducing_schema, child);
    }

    #[test]
    fn nested_payload_descriptor_unions_siblings_by_global_parent_identity() {
        // Two concurrent parent cases both occupy authored ordinal 1. Their
        // nested payload enum layouts must stay under separate global parent
        // paths, and the physical descriptor must retain both after a reopen.
        let base = schema(1);
        let archived = schema(2);
        let snoozed = schema(3);
        let root = "root/record/event";
        let base_case = GlobalScalarEnumCaseId {
            introducing_schema: base,
            introducing_ordinal: 0,
        };
        let archived_case = GlobalScalarEnumCaseId {
            introducing_schema: archived,
            introducing_ordinal: 1,
        };
        let snoozed_case = GlobalScalarEnumCaseId {
            introducing_schema: snoozed,
            introducing_ordinal: 1,
        };
        let inner = |name: &str| {
            records::ValueType::Enum(Box::new(
                records::EnumSchema::new(
                    format!("inner-{name}"),
                    [records::EnumCase::new(
                        name,
                        records::RecordDescriptor::new([("value", records::ValueType::String)]),
                    )],
                )
                .unwrap(),
            ))
        };
        let payload = |name: &str| records::RecordDescriptor::new([("detail", inner(name))]);
        let outer = records::ValueType::Record(Box::new(records::RecordDescriptor::new([(
            "event",
            records::ValueType::Enum(Box::new(
                records::EnumSchema::new(
                    "authored-event",
                    [
                        records::EnumCase::new("base", payload("base")),
                        records::EnumCase::new("archived", payload("archived")),
                    ],
                )
                .unwrap(),
            )),
        )])));

        let mut payload_registries = BTreeMap::from([(
            root.to_owned(),
            vec![
                base_case.clone(),
                archived_case.clone(),
                snoozed_case.clone(),
            ],
        )]);
        for (parent, child) in [
            (&base_case, base),
            (&archived_case, archived),
            (&snoozed_case, snoozed),
        ] {
            payload_registries.insert(
                format!("{}/record/detail", global_case_path(root, parent)),
                vec![GlobalScalarEnumCaseId {
                    introducing_schema: child,
                    introducing_ordinal: 0,
                }],
            );
        }
        let mut layouts = BTreeMap::from([
            ((root.to_owned(), base_case.clone()), payload("base")),
            (
                (root.to_owned(), archived_case.clone()),
                payload("archived"),
            ),
            ((root.to_owned(), snoozed_case.clone()), payload("snoozed")),
        ]);
        for parent in [&base_case, &archived_case, &snoozed_case] {
            layouts.insert(
                (
                    format!("{}/record/detail", global_case_path(root, parent)),
                    GlobalScalarEnumCaseId {
                        introducing_schema: parent.introducing_schema,
                        introducing_ordinal: 0,
                    },
                ),
                records::RecordDescriptor::new([("value", records::ValueType::String)]),
            );
        }
        let physical = physical_nested_enum_value_type(
            &outer,
            "root",
            &BTreeMap::new(),
            &payload_registries,
            &layouts,
            PhysicalColumnId(9),
        )
        .unwrap();
        let records::ValueType::Record(record) = physical else {
            panic!("physical record expected");
        };
        let records::ValueType::Enum(events) = &record.fields()[0].value_type else {
            panic!("physical payload enum expected");
        };
        assert_eq!(events.cases.len(), 3);
        assert_ne!(
            events.cases[1].name, events.cases[2].name,
            "concurrent ordinal-one parents must not collide"
        );
        for case in &events.cases {
            let records::ValueType::Enum(detail) = &case.payload.fields()[0].value_type else {
                panic!("recursively lowered payload enum expected");
            };
            assert_eq!(detail.cases.len(), 1);
        }
    }
}
