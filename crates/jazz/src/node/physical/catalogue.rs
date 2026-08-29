/// Lower Jazz's durable schema alias into Groove's deliberately smaller,
/// table-local union-case space. A future user-declared top-level union will
/// allocate a distinct tag for each `(schema alias, user case)` pair here.
pub(super) fn groove_variant_tag(alias: SchemaVersionAlias) -> Result<u32, Error> {
    u32::try_from(alias.0)
        .map_err(|_| Error::InvalidStoredValue("physical table variant tag exhausted"))
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct SchemaPhysicalMapping {
    /// Globally meaningful immutable identities authored by the catalogue
    /// authority. The remaining u64 ids in this mapping are local aliases.
    pub(super) identities: PhysicalIdentityManifest,
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

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(super) struct GlobalScalarEnumCaseId {
    /// Permanent authority-issued semantic identity. Authored ordinals and
    /// schema versions select this UUID through a publication manifest but
    /// never participate in equality or storage identity.
    pub(super) id: crate::ids::GlobalPhysicalEnumVariantId,
    /// Ordering provenance only. Equality and ordering of semantic identities
    /// deliberately ignore these authored lookup coordinates.
    pub(super) introducing_schema: SchemaVersionId,
    pub(super) introducing_ordinal: u8,
}

impl PartialEq for GlobalScalarEnumCaseId {
    fn eq(&self, other: &Self) -> bool { self.id == other.id }
}
impl Eq for GlobalScalarEnumCaseId {}
impl PartialOrd for GlobalScalarEnumCaseId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for GlobalScalarEnumCaseId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.id.cmp(&other.id) }
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
    format!("case-{}", case.id.0.simple())
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
        .then_with(|| left.id.cmp(&right.id))
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

/// Fixed sparse deletion history shared by every physical content lineage.
///
/// Unlike `physical_register_table_name`, this is not a per-lineage table;
/// callers must pair it with the full `(BranchKey, PhysicalTableId)` key.
pub(super) const SHARED_DELETION_HISTORY_TABLE: &str = "jazz_deletion_history";

pub(super) fn shared_deletion_history_primary_key(
    table_id: PhysicalTableId,
    version: &VersionRow,
) -> PrimaryKeyValue {
    PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::Bytes(version.branch_key().canonical_bytes()),
        PrimaryKeyValue::U64(table_id.0),
        PrimaryKeyValue::Uuid(version.row_uuid().0),
        PrimaryKeyValue::U64(version.tx_time().0),
        PrimaryKeyValue::U64(version.tx_node_alias().0),
    ])
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

pub(super) fn physical_current_index_name(column_id: PhysicalColumnId) -> String {
    format!("by_physical_user_v1_{}", column_id.0)
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
