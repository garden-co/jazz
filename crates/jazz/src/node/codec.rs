//! Storage and wire codec helpers for node-owned records. This module owns
//! groove record layouts, typed row wrappers, storage-key/value construction,
//! alias row encoding, and conversion between Jazz protocol records and groove
//! bytes; schema declarations live in [`crate::schema`], semantic validation in
//! [`super::ingest`] and [`super::policy`], and query execution in
//! [`super::query_eval`]. It is the node layer's boundary to groove storage.

use super::query_engine::{app_column_field, left_field};
use super::*;
use crate::protocol::{
    CompleteTxPayloadCoverageEntry, ContributingMembersEntry, CoveredInputEntry,
    PathCorrelationCoverageEntry, PathHoleState, PointReadEntry, PolicyDecisionEntry,
    PolicyDecisionOutcomeEntry, PolicyWitnessEntry, PredicateOutputSetEntry,
    PredicateOutputSetRoleEntry, PredicateReadEntry, ProgramSourceCoverageEntry, ProgramSourceId,
    ProgramSourceRole, ReadFrontierSettledEntry, RelationEdgeEntry, RelationEdgeKind,
    RelationEdgeRole, ResultMemberPayloadEntry, ResultRowLayer, ResultRowSource,
    RowVersionRefEntry, SnapshotRef, SyntheticReplacementToken, VersionWitnessEntry,
    ViewCompleteExclusiveCoverageEntry,
};
use crate::schema::{ColumnSchema, contribution_merge_storage_type};
use crate::tools::{ObjectId, OutputOccurrenceId, ResultKey};
use crate::tx::{
    BranchViewCopyBase, BranchViewCopyEvidence, BranchWriteIntent, BranchWriteOperation,
};

use groove::schema::TableSchema as GrooveTableSchema;

groove::define_record! {
    pub(super) struct HistoryRowRecord {
        0 => branch_key: Vec<u8>,
        1 => row_uuid: RowUuid,
        2 => tx_time: TxTime,
        3 => tx_node_id: NodeAlias,
        4 => schema_version: SchemaVersionAlias,
        5 => parents: ParentRefs,
        6 => created_by: AuthorSubject,
        7 => created_at: TxTime,
        8 => updated_by: AuthorSubject,
        9 => updated_at: TxTime,
        .. user_cells,
    }
}

groove::define_record! {
    pub(super) struct RegisterRowRecord {
        0 => branch_key: Vec<u8>,
        1 => row_uuid: RowUuid,
        2 => tx_time: TxTime,
        3 => tx_node_id: NodeAlias,
        4 => schema_version: SchemaVersionAlias,
        5 => parents: ParentRefs,
        6 => created_by: AuthorSubject,
        7 => created_at: TxTime,
        8 => updated_by: AuthorSubject,
        9 => updated_at: TxTime,
        10 => _deletion: DeletionEvent,
    }
}

// Fixed physical carrier for the shared deletion-history relation. The logical
// `VersionRow` remains a `RegisterRowRecord`; this wrapper exists only at the
// storage boundary where lineage/table routing is attached.
groove::define_record! {
    pub(super) struct SharedDeletionHistoryRowRecord {
        0 => branch_key: Vec<u8>,
        1 => physical_table_id: u64,
        2 => row_uuid: RowUuid,
        3 => tx_time: TxTime,
        4 => tx_node_id: NodeAlias,
        5 => schema_version: SchemaVersionAlias,
        6 => parents: ParentRefs,
        7 => created_by: AuthorSubject,
        8 => created_at: TxTime,
        9 => updated_by: AuthorSubject,
        10 => updated_at: TxTime,
        11 => _deletion: DeletionEvent,
    }
}

groove::define_record! {
    pub(super) struct GlobalCurrentRowRecord {
        0 => branch_key: Vec<u8>,
        1 => row_uuid: RowUuid,
        2 => tx_time: TxTime,
        3 => tx_node_id: NodeAlias,
        4 => schema_version: SchemaVersionAlias,
        5 => parents: ParentRefs,
        6 => created_by: AuthorSubject,
        7 => created_at: u64,
        8 => updated_by: AuthorSubject,
        9 => updated_at: u64,
        10 => global_time: Option<GlobalTime>,
        .. user_cells,
    }
}

groove::define_record! {
    pub(super) struct RegisterGlobalCurrentRowRecord {
        0 => branch_key: Vec<u8>,
        1 => row_uuid: RowUuid,
        2 => tx_time: TxTime,
        3 => tx_node_id: NodeAlias,
        4 => schema_version: SchemaVersionAlias,
        5 => parents: ParentRefs,
        6 => created_by: AuthorSubject,
        7 => created_at: u64,
        8 => updated_by: AuthorSubject,
        9 => updated_at: u64,
        10 => global_time: Option<GlobalTime>,
        11 => _deletion: DeletionEvent,
    }
}

groove::define_record! {
    pub(super) struct GlobalChangeRowRecord {
        0 => physical_table_id: u64,
        1 => branch_key: Vec<u8>,
        2 => row_uuid: RowUuid,
        3 => layer: Vec<u8>,
        4 => global_time: GlobalTime,
        5 => tx_time: TxTime,
        6 => tx_node_id: NodeAlias,
        7 => _deletion: Option<DeletionEvent>,
    }
}

groove::impl_record_field_u64!(TxTime);
groove::impl_record_field_u64!(GlobalTime);
groove::impl_record_field_u64!(NodeAlias);
groove::impl_record_field_u64!(SchemaVersionAlias);
groove::impl_record_field_uuid!(NodeUuid);
groove::impl_record_field_uuid!(SchemaFamilyId);
groove::impl_record_field_uuid!(RowUuid);
groove::impl_record_field_uuid!(SchemaVersionId);
groove::impl_record_field_enum!(TxKind {
    TxKind::Mergeable = 0,
    TxKind::Exclusive = 1,
});
groove::impl_record_field_enum!(DurabilityTier {
    DurabilityTier::None = 0,
    DurabilityTier::Local = 1,
    DurabilityTier::Edge = 2,
    DurabilityTier::Global = 3,
});
groove::impl_record_field_enum!(DeletionEvent {
    DeletionEvent::Deleted = 0,
    DeletionEvent::Restored = 1,
});
groove::impl_record_field_enum!(MergeAspect {
    MergeAspect::Content = 0,
    MergeAspect::Deletion = 1,
});
groove::impl_record_field_enum!(ResultRowLayer {
    ResultRowLayer::Content = 0,
    ResultRowLayer::Deletion = 1,
    ResultRowLayer::ContentOrDeletion = 2,
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FateTag {
    Pending,
    Accepted,
    Rejected,
}

groove::impl_record_field_enum!(FateTag {
    FateTag::Pending = 0,
    FateTag::Accepted = 1,
    FateTag::Rejected = 2,
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RejectionReasonTag {
    ClientClockTooFarAhead,
    AuthorizationDenied,
    ExclusiveConflict,
    CausalityViolation,
    Cascade,
    MalformedCommit,
}

groove::impl_record_field_enum!(RejectionReasonTag {
    RejectionReasonTag::ClientClockTooFarAhead = 0,
    RejectionReasonTag::AuthorizationDenied = 1,
    RejectionReasonTag::ExclusiveConflict = 2,
    RejectionReasonTag::CausalityViolation = 3,
    RejectionReasonTag::Cascade = 4,
    RejectionReasonTag::MalformedCommit = 5,
});

impl records::RecordField for AuthorSubject {
    fn read(record: &records::BorrowedRecord<'_>, idx: usize) -> Result<Self, records::Error> {
        AuthorSubject::from_canonical(record.get_str(idx)?)
            .map_err(|_| records::Error::NonCanonicalRecord)
    }

    fn to_value(&self) -> Value {
        Value::String(self.canonical().to_owned())
    }

    const COLUMN_KIND: records::FieldKind = records::FieldKind::String;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ParentRefs(Vec<TxId>);

impl records::RecordField for ParentRefs {
    fn read(record: &records::BorrowedRecord<'_>, idx: usize) -> Result<Self, records::Error> {
        tx_ids_from_value(record.get_idx(idx)?)
            .map(Self)
            .map_err(|_| records::Error::TypeMismatch {
                expected: records::ValueType::Array(Box::new(records::ValueType::Tuple(vec![
                    records::ValueType::U64,
                    records::ValueType::Uuid,
                ]))),
            })
    }

    fn to_value(&self) -> Value {
        Value::Array(self.0.iter().map(|parent| tx_id_value(*parent)).collect())
    }

    const COLUMN_KIND: records::FieldKind = records::FieldKind::Array;
}

groove::define_record! {
    pub(super) struct CurrentRowRecord {
        0 => row_uuid: RowUuid,
        .. user_cells,
    }
}

groove::define_record! {
    pub(super) struct WireRowRecord {
        0 => row_uuid: RowUuid,
        1 => parents: ParentRefs,
        2 => created_by: AuthorSubject,
        3 => created_at: u64,
        4 => updated_by: AuthorSubject,
        5 => updated_at: u64,
        6 => _deletion: Option<Value>,
        .. user_cells,
    }
}

groove::define_record! {
    pub(super) struct TransactionRowRecord {
        0 => time: TxTime,
        1 => node_id: NodeAlias,
        2 => kind: TxKind,
        3 => n_total_writes: u32,
        4 => made_by: AuthorSubject,
        5 => base_snapshot: Option<Value>,
        6 => row_read_set: Option<Value>,
        7 => absent_read_set: Option<Value>,
        8 => predicate_read_set: Option<Value>,
        9 => user_metadata: Option<String>,
        10 => contribution_merge: Option<OwnedRecord>,
        11 => permission_subject: Option<AuthorSubject>,
        // Retained physical slot, now used internally to mark redacted
        // view-scoped transaction cardinality without changing row alignment.
        12 => merge_strategy: Option<String>,
        13 => fate: FateTag,
        14 => global_time: Option<GlobalTime>,
        15 => rejection_reason: Option<RejectionReasonTag>,
        16 => cascade_root: Option<Value>,
        17 => reason_detail: Option<String>,
        18 => durability: DurabilityTier,
    }
}

groove::define_record! {
pub(super) struct ContributionMergeStorageRecord {
        0 => source: Vec<u8>,
        1 => target: Vec<u8>,
        2 => substitutions: Vec<Value>,
        3 => branch_view_copy_v1: Vec<Value>,
        4 => branch_write_intent_v1: Vec<Value>,
    }
}

groove::define_record! {
    pub(super) struct BranchWriteIntentStorageRecord {
        0 => version: u8,
        1 => physical_table_id: u64,
        2 => authored_schema: uuid::Uuid,
        3 => row_uuid: uuid::Uuid,
        4 => head: Vec<u8>,
        5 => operation: records::EnumValue,
    }
}

groove::define_record! {
    pub(super) struct BranchViewCopyStorageRecord {
        0 => version: u8,
        1 => head: Vec<u8>,
        2 => base: records::EnumValue,
        3 => table: String,
        4 => row_uuid: uuid::Uuid,
        5 => source_time: u64,
        6 => source_node: uuid::Uuid,
    }
}

groove::define_record! {
    pub(super) struct BranchViewCopyCurrentBaseStorageRecord {
        0 => branch: Vec<u8>,
    }
}

groove::define_record! {
    pub(super) struct BranchViewCopySnapshotBaseStorageRecord {
        0 => branch: Vec<u8>,
        1 => owner: uuid::Uuid,
        2 => global_base: u64,
        3 => local_base: u64,
        4 => dots: Vec<Value>,
    }
}

groove::define_record! {
    pub(super) struct BranchViewCopyDotStorageRecord {
        0 => time: u64,
        1 => node: uuid::Uuid,
    }
}

groove::define_record! {
    pub(super) struct ContributionSubstitutionStorageRecord {
        0 => target: OwnedRecord,
        1 => sources: Vec<Value>,
    }
}

groove::define_record! {
    pub(super) struct ContributionDotStorageRecord {
        0 => tx_time: u64,
        1 => tx_node: uuid::Uuid,
        2 => coordinate: OwnedRecord,
    }
}

groove::define_record! {
    pub(super) struct ContributionCoordinateStorageRecord {
        0 => branch_key: Vec<u8>,
        1 => physical_table_id: u64,
        2 => row_uuid: uuid::Uuid,
        3 => layer: MergeAspect,
        4 => component: records::EnumValue,
    }
}

groove::define_record! {
    pub(super) struct ContributionColumnStorageRecord {
        0 => physical_column_id: u64,
    }
}

groove::define_record! {
    pub(super) struct ContributionOperationStorageRecord {
        0 => physical_column_id: u64,
        1 => identity: Vec<u8>,
    }
}

groove::define_record! {
    struct ResultMemberUnionArmStorageRecord {
        0 => position: u32,
        1 => label: String,
    }
}

groove::define_record! {
    struct ResultMemberOccurrenceStorageRecord {
        0 => root: uuid::Uuid,
        1 => joined: Vec<Value>,
        2 => union_arms: Vec<Value>,
    }
}

groove::define_record! {
    struct ResultMemberSnapshotSourceStorageRecord {
        0 => owner: uuid::Uuid,
        1 => global_base: u64,
        2 => local_base: u64,
        3 => dots: Vec<Value>,
    }
}

groove::define_record! {
    struct ResultMemberHistoryCutSourceStorageRecord {
        0 => global_time: u64,
    }
}

groove::define_record! {
    struct ResultMemberMergeSourceStorageRecord {
        0 => inputs: Vec<Value>,
    }
}

groove::define_record! {
    struct ResultMemberLensSourceStorageRecord {
        0 => schema_version: uuid::Uuid,
        1 => base: Vec<u8>,
    }
}

groove::define_record! {
    struct ResultMemberOverlaySourceStorageRecord {
        0 => tx: Value,
        1 => base: Vec<u8>,
    }
}

groove::define_record! {
    struct ResultMemberRealRowStorageRecord {
        0 => table: String,
        1 => row_uuid: uuid::Uuid,
        2 => occurrence_id: Option<OwnedRecord>,
        3 => content_tx: Option<Value>,
        4 => layer: ResultRowLayer,
        5 => deletion_tx: Option<Value>,
        6 => source: records::EnumValue,
        7 => read_view: uuid::Uuid,
        8 => schema_version: Option<uuid::Uuid>,
        9 => branch_or_prefix: Option<Vec<u8>>,
        10 => row_digest: Option<Vec<u8>>,
        11 => batch: Option<Value>,
        12 => settle_position: Option<u64>,
    }
}

groove::define_record! {
    struct ResultMemberSyntheticStorageRecord {
        0 => table: String,
        1 => row: Vec<u8>,
        2 => replacement: Vec<u8>,
    }
}

groove::define_record! {
    struct ResultMemberPathTupleStorageRecord {
        0 => path: String,
        1 => source_table: String,
        2 => source_row: uuid::Uuid,
        3 => target_table: String,
        4 => target_row: uuid::Uuid,
        5 => edge_id: Option<Vec<u8>>,
        6 => revision: Vec<u8>,
    }
}

groove::define_record! {
    struct ResultMemberTypedRowStorageRecord {
        0 => row: OwnedRecord,
        1 => occurrence_key: OwnedRecord,
    }
}

groove::define_record! {
    pub(super) struct NodeAliasRowRecord {
        0 => id: NodeAlias,
        1 => uuid: NodeUuid,
    }
}

groove::define_record! {
    pub(super) struct SchemaVersionAliasRowRecord {
        0 => id: SchemaVersionAlias,
        1 => uuid: SchemaVersionId,
        2 => physical_mapping: Vec<u8>,
    }
}

groove::define_record! {
    pub(super) struct CatalogueRowRecord {
        // This is the epoch-pinned catalogue kernel.  Its numeric cases are
        // permanent: higher-level Jazz records are described by catalogue
        // entries rather than by adding more hard-coded storage layouts.
        0 => kind: CatalogueRecordKind,
        1 => id: uuid::Uuid,
        2 => payload: Vec<u8>,
    }
}

/// Permanent discriminators for the tiny hard-coded catalogue kernel.
///
/// These values identify only the bootstrap records required to discover and
/// activate immutable descriptors.  Application and ordinary Jazz system
/// tables must remain catalogue-described rather than extending this enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CatalogueRecordKind {
    Genesis,
    Schema,
    Lens,
    SchemaLineageStaged,
    SchemaLineagePending,
    SchemaLineageActive,
    WritePointerPending,
    BootstrapReady,
}

impl CatalogueRecordKind {
    pub(super) const fn key(self) -> u64 {
        match self {
            Self::Genesis => 0,
            Self::Schema => 1,
            Self::Lens => 2,
            Self::SchemaLineageStaged => 3,
            Self::SchemaLineagePending => 4,
            Self::SchemaLineageActive => 5,
            Self::WritePointerPending => 6,
            Self::BootstrapReady => 7,
        }
    }

    pub(super) fn from_key(key: u64) -> Result<Self, records::Error> {
        match key {
            0 => Ok(Self::Genesis),
            1 => Ok(Self::Schema),
            2 => Ok(Self::Lens),
            3 => Ok(Self::SchemaLineageStaged),
            4 => Ok(Self::SchemaLineagePending),
            5 => Ok(Self::SchemaLineageActive),
            6 => Ok(Self::WritePointerPending),
            7 => Ok(Self::BootstrapReady),
            _ => Err(records::Error::NonCanonicalRecord),
        }
    }
}

impl records::RecordField for CatalogueRecordKind {
    fn read(record: &records::BorrowedRecord<'_>, idx: usize) -> Result<Self, records::Error> {
        Self::from_key(record.get_u64(idx)?)
    }

    fn to_value(&self) -> Value {
        Value::U64(self.key())
    }

    const COLUMN_KIND: records::FieldKind = records::FieldKind::U64;
}

// The catalogue table is the fixed, bootstrap-only Groove kernel.  Its
// payloads deliberately do not use serde/postcard: those formats make the
// durable bytes depend on Rust field layout and accept trailing data in some
// configurations.  Every payload below starts with its own permanent format
// version and consumes exactly its input.
const CATALOGUE_SCHEMA_VERSION: u8 = 1;
const CATALOGUE_BOOTSTRAP_READY_VERSION: u8 = 1;
const CATALOGUE_WRITE_POINTER_VERSION: u8 = 1;
const CATALOGUE_LINEAGE_ACTIVATION_VERSION: u8 = 1;
const CATALOGUE_PROTOCOL_PUBLICATION_VERSION: u8 = 1;
const CATALOGUE_STAGED_LINEAGE_VERSION: u8 = 1;
const CATALOGUE_PENDING_LINEAGE_VERSION: u8 = 1;
// This is deliberately independent of the catalogue-record payload versions:
// physical mappings live in the fixed `jazz_schema_versions` carrier rather
// than in `jazz_catalogue`.
const PHYSICAL_MAPPING_VERSION: u8 = 1;

/// Encode node-local physical storage metadata as one typed, canonical value.
///
/// The mapping is not a wire identity: table and column ids are opaque local
/// handles.  The schema-qualified enum case identities inside it are semantic,
/// and their vector order is the already-authoritative physical tag order.
pub(super) fn encode_physical_mapping(mapping: &SchemaPhysicalMapping) -> Result<Vec<u8>, Error> {
    let mut payload = vec![PHYSICAL_MAPPING_VERSION];
    encode_physical_identity_manifest(&mut payload, &mapping.identities)?;
    put_len(
        &mut payload,
        mapping.tables.len(),
        "physical mapping table count",
    )?;
    for (table_name, table) in &mapping.tables {
        put_string(&mut payload, table_name, "physical mapping table name")?;
        payload.extend_from_slice(&table.table_id.0.to_le_bytes());
        put_len(
            &mut payload,
            table.columns.len(),
            "physical mapping column count",
        )?;
        for (column_name, column_id) in &table.columns {
            put_string(&mut payload, column_name, "physical mapping column name")?;
            payload.extend_from_slice(&column_id.0.to_le_bytes());
        }
        put_len(
            &mut payload,
            table.variant_cases.len(),
            "physical mapping variant case count",
        )?;
        for case in &table.variant_cases {
            payload.extend_from_slice(&case.tag.to_le_bytes());
            put_len(
                &mut payload,
                case.fields.len(),
                "physical mapping variant field count",
            )?;
            for field in &case.fields {
                put_string(&mut payload, field, "physical mapping variant field")?;
            }
        }
        encode_direct_scalar_enum_registries(&mut payload, &table.scalar_enum_cases)?;
        encode_direct_payload_enum_registries(&mut payload, &table.payload_enum_cases)?;
        encode_nested_scalar_enum_registries(&mut payload, &table.nested_scalar_enum_cases)?;
        encode_nested_payload_enum_registries(&mut payload, &table.nested_payload_enum_cases)?;
    }
    Ok(payload)
}

pub(super) fn decode_physical_mapping(payload: &[u8]) -> Result<SchemaPhysicalMapping, Error> {
    const CONTEXT: &str = "invalid physical mapping payload";
    let version = payload
        .first()
        .copied()
        .ok_or(Error::InvalidStoredValue(CONTEXT))?;
    if version != PHYSICAL_MAPPING_VERSION {
        return Err(Error::InvalidStoredValue(CONTEXT));
    }
    let mut cursor = CataloguePayloadCursor::new(payload, version, CONTEXT)?;
    let identities = decode_physical_identity_manifest(&mut cursor)?;
    let table_count = cursor.u32()?;
    let mut tables = BTreeMap::new();
    let mut previous_table = None;
    for _ in 0..table_count {
        let table_name = cursor.string()?;
        require_strictly_increasing(previous_table.as_deref(), &table_name, CONTEXT)?;
        previous_table = Some(table_name.clone());
        let table_id = PhysicalTableId(cursor.u64()?);
        if table_id.0 == 0 {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
        let column_count = cursor.u32()?;
        let mut columns = BTreeMap::new();
        let mut previous_column = None;
        for _ in 0..column_count {
            let name = cursor.string()?;
            require_strictly_increasing(previous_column.as_deref(), &name, CONTEXT)?;
            previous_column = Some(name.clone());
            let id = PhysicalColumnId(cursor.u64()?);
            if id.0 == 0 || columns.insert(name, id).is_some() {
                return Err(Error::InvalidStoredValue(CONTEXT));
            }
        }
        let variant_count = cursor.u32()?;
        // Do not reserve an attacker-controlled count before checking that
        // the corresponding entries exist in the exact-consumed payload.
        let mut variant_cases = Vec::new();
        for _ in 0..variant_count {
            let tag = cursor.u32()?;
            let field_count = cursor.u32()?;
            let mut fields = BTreeSet::new();
            let mut previous_field = None;
            for _ in 0..field_count {
                let field = cursor.string()?;
                require_strictly_increasing(previous_field.as_deref(), &field, CONTEXT)?;
                previous_field = Some(field.clone());
                if !fields.insert(field) {
                    return Err(Error::InvalidStoredValue(CONTEXT));
                }
            }
            variant_cases.push(PhysicalVariantCase { tag, fields });
        }
        let scalar_enum_cases = decode_direct_scalar_enum_registries(&mut cursor)?;
        let payload_enum_cases = decode_direct_payload_enum_registries(&mut cursor)?;
        let nested_scalar_enum_cases = decode_nested_scalar_enum_registries(&mut cursor)?;
        let nested_payload_enum_cases = decode_nested_payload_enum_registries(&mut cursor)?;
        if tables
            .insert(
                table_name,
                TablePhysicalMapping {
                    table_id,
                    columns,
                    variant_cases,
                    scalar_enum_cases,
                    payload_enum_cases,
                    nested_scalar_enum_cases,
                    nested_payload_enum_cases,
                },
            )
            .is_some()
        {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
    }
    cursor.finish()?;
    Ok(SchemaPhysicalMapping { identities, tables })
}

fn encode_physical_identity_manifest(
    payload: &mut Vec<u8>,
    manifest: &PhysicalIdentityManifest,
) -> Result<(), Error> {
    put_len(
        payload,
        manifest.tables.len(),
        "physical identity table count",
    )?;
    for (table_name, table) in &manifest.tables {
        put_string(payload, table_name, "physical identity table name")?;
        payload.extend_from_slice(table.id.0.as_bytes());
        put_len(
            payload,
            table.columns.len(),
            "physical identity column count",
        )?;
        for (column_name, column) in &table.columns {
            put_string(payload, column_name, "physical identity column name")?;
            payload.extend_from_slice(column.id.0.as_bytes());
            put_len(
                payload,
                column.enum_variants.len(),
                "physical identity enum occurrence count",
            )?;
            for (path, variants) in &column.enum_variants {
                put_string(payload, path, "physical identity enum path")?;
                put_len(payload, variants.len(), "physical identity enum case count")?;
                for variant in variants {
                    payload.extend_from_slice(variant.0.as_bytes());
                }
            }
        }
    }
    Ok(())
}

fn decode_physical_identity_manifest(
    cursor: &mut CataloguePayloadCursor<'_>,
) -> Result<PhysicalIdentityManifest, Error> {
    const CONTEXT: &str = "invalid physical mapping payload";
    let table_count = cursor.u32()?;
    let mut tables = BTreeMap::new();
    let mut previous_table = None;
    for _ in 0..table_count {
        let table_name = cursor.string()?;
        require_strictly_increasing(previous_table.as_deref(), &table_name, CONTEXT)?;
        previous_table = Some(table_name.clone());
        let id = crate::ids::GlobalPhysicalTableId(cursor.uuid()?);
        let column_count = cursor.u32()?;
        let mut columns = BTreeMap::new();
        let mut previous_column = None;
        for _ in 0..column_count {
            let column_name = cursor.string()?;
            require_strictly_increasing(previous_column.as_deref(), &column_name, CONTEXT)?;
            previous_column = Some(column_name.clone());
            let id = crate::ids::GlobalPhysicalColumnId(cursor.uuid()?);
            let occurrence_count = cursor.u32()?;
            let mut enum_variants = BTreeMap::new();
            let mut previous_path = None;
            for _ in 0..occurrence_count {
                let path = cursor.string()?;
                require_strictly_increasing(previous_path.as_deref(), &path, CONTEXT)?;
                previous_path = Some(path.clone());
                let case_count = cursor.u32()?;
                let mut variants = Vec::new();
                for _ in 0..case_count {
                    variants.push(crate::ids::GlobalPhysicalEnumVariantId(cursor.uuid()?));
                }
                enum_variants.insert(path, variants);
            }
            columns.insert(column_name, PhysicalColumnIdentity { id, enum_variants });
        }
        tables.insert(table_name, PhysicalTableIdentity { id, columns });
    }
    Ok(PhysicalIdentityManifest { tables })
}

fn put_len(payload: &mut Vec<u8>, length: usize, context: &'static str) -> Result<(), Error> {
    let length = u32::try_from(length).map_err(|_| Error::InvalidStoredValue(context))?;
    payload.extend_from_slice(&length.to_le_bytes());
    Ok(())
}

fn put_string(payload: &mut Vec<u8>, value: &str, context: &'static str) -> Result<(), Error> {
    put_len(payload, value.len(), context)?;
    payload.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_scalar_case_id(payload: &mut Vec<u8>, case: &GlobalScalarEnumCaseId) {
    payload.extend_from_slice(case.id.0.as_bytes());
    payload.extend_from_slice(case.introducing_schema.0.as_bytes());
    payload.push(case.introducing_ordinal);
}

fn encode_payload_case_id(payload: &mut Vec<u8>, case: &GlobalEnumCaseId) {
    payload.extend_from_slice(case.id.0.as_bytes());
    payload.extend_from_slice(case.introducing_schema.0.as_bytes());
    payload.extend_from_slice(&case.introducing_ordinal.to_le_bytes());
}

fn encode_scalar_cases(
    payload: &mut Vec<u8>,
    cases: &[GlobalScalarEnumCaseId],
) -> Result<(), Error> {
    put_len(payload, cases.len(), "physical mapping enum case count")?;
    for case in cases {
        encode_scalar_case_id(payload, case);
    }
    Ok(())
}

fn encode_payload_cases(payload: &mut Vec<u8>, cases: &[GlobalEnumCaseId]) -> Result<(), Error> {
    put_len(payload, cases.len(), "physical mapping enum case count")?;
    for case in cases {
        encode_payload_case_id(payload, case);
    }
    Ok(())
}

fn encode_direct_scalar_enum_registries(
    payload: &mut Vec<u8>,
    registries: &BTreeMap<PhysicalColumnId, Vec<GlobalScalarEnumCaseId>>,
) -> Result<(), Error> {
    put_len(
        payload,
        registries.len(),
        "physical mapping direct registry count",
    )?;
    for (column_id, cases) in registries {
        payload.extend_from_slice(&column_id.0.to_le_bytes());
        encode_scalar_cases(payload, cases)?;
    }
    Ok(())
}

fn encode_direct_payload_enum_registries(
    payload: &mut Vec<u8>,
    registries: &BTreeMap<PhysicalColumnId, Vec<GlobalEnumCaseId>>,
) -> Result<(), Error> {
    put_len(
        payload,
        registries.len(),
        "physical mapping direct registry count",
    )?;
    for (column_id, cases) in registries {
        payload.extend_from_slice(&column_id.0.to_le_bytes());
        encode_payload_cases(payload, cases)?;
    }
    Ok(())
}

fn encode_nested_scalar_enum_registries(
    payload: &mut Vec<u8>,
    registries: &BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<GlobalScalarEnumCaseId>>>,
) -> Result<(), Error> {
    put_len(
        payload,
        registries.len(),
        "physical mapping nested registry count",
    )?;
    for (column_id, paths) in registries {
        payload.extend_from_slice(&column_id.0.to_le_bytes());
        put_len(
            payload,
            paths.len(),
            "physical mapping nested registry path count",
        )?;
        for (path, cases) in paths {
            put_string(payload, path, "physical mapping nested registry path")?;
            encode_scalar_cases(payload, cases)?;
        }
    }
    Ok(())
}

fn encode_nested_payload_enum_registries(
    payload: &mut Vec<u8>,
    registries: &BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<GlobalEnumCaseId>>>,
) -> Result<(), Error> {
    put_len(
        payload,
        registries.len(),
        "physical mapping nested registry count",
    )?;
    for (column_id, paths) in registries {
        payload.extend_from_slice(&column_id.0.to_le_bytes());
        put_len(
            payload,
            paths.len(),
            "physical mapping nested registry path count",
        )?;
        for (path, cases) in paths {
            put_string(payload, path, "physical mapping nested registry path")?;
            encode_payload_cases(payload, cases)?;
        }
    }
    Ok(())
}

fn decode_scalar_case_id(
    cursor: &mut CataloguePayloadCursor<'_>,
) -> Result<GlobalScalarEnumCaseId, Error> {
    Ok(GlobalScalarEnumCaseId {
        id: crate::ids::GlobalPhysicalEnumVariantId(cursor.uuid()?),
        introducing_schema: SchemaVersionId(cursor.uuid()?),
        introducing_ordinal: cursor.bytes(1)?[0],
    })
}

fn decode_payload_case_id(
    cursor: &mut CataloguePayloadCursor<'_>,
) -> Result<GlobalEnumCaseId, Error> {
    Ok(GlobalEnumCaseId {
        id: crate::ids::GlobalPhysicalEnumVariantId(cursor.uuid()?),
        introducing_schema: SchemaVersionId(cursor.uuid()?),
        introducing_ordinal: cursor.u32()?,
    })
}

fn decode_scalar_cases(
    cursor: &mut CataloguePayloadCursor<'_>,
) -> Result<Vec<GlobalScalarEnumCaseId>, Error> {
    const CONTEXT: &str = "invalid physical mapping payload";
    let count = cursor.u32()?;
    let mut cases = Vec::new();
    let mut unique = BTreeSet::new();
    for _ in 0..count {
        let case = decode_scalar_case_id(cursor)?;
        if !unique.insert(case.clone()) {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
        cases.push(case);
    }
    Ok(cases)
}

fn decode_payload_cases(
    cursor: &mut CataloguePayloadCursor<'_>,
) -> Result<Vec<GlobalEnumCaseId>, Error> {
    const CONTEXT: &str = "invalid physical mapping payload";
    let count = cursor.u32()?;
    let mut cases = Vec::new();
    let mut unique = BTreeSet::new();
    for _ in 0..count {
        let case = decode_payload_case_id(cursor)?;
        if !unique.insert(case.clone()) {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
        cases.push(case);
    }
    Ok(cases)
}

fn decode_direct_scalar_enum_registries(
    cursor: &mut CataloguePayloadCursor<'_>,
) -> Result<BTreeMap<PhysicalColumnId, Vec<GlobalScalarEnumCaseId>>, Error> {
    const CONTEXT: &str = "invalid physical mapping payload";
    let count = cursor.u32()?;
    let mut registries = BTreeMap::new();
    let mut previous = None;
    for _ in 0..count {
        let id = PhysicalColumnId(cursor.u64()?);
        if id.0 == 0 || previous.is_some_and(|previous| id <= previous) {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
        previous = Some(id);
        if registries
            .insert(id, decode_scalar_cases(cursor)?)
            .is_some()
        {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
    }
    Ok(registries)
}

fn decode_direct_payload_enum_registries(
    cursor: &mut CataloguePayloadCursor<'_>,
) -> Result<BTreeMap<PhysicalColumnId, Vec<GlobalEnumCaseId>>, Error> {
    const CONTEXT: &str = "invalid physical mapping payload";
    let count = cursor.u32()?;
    let mut registries = BTreeMap::new();
    let mut previous = None;
    for _ in 0..count {
        let id = PhysicalColumnId(cursor.u64()?);
        if id.0 == 0 || previous.is_some_and(|previous| id <= previous) {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
        previous = Some(id);
        if registries
            .insert(id, decode_payload_cases(cursor)?)
            .is_some()
        {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
    }
    Ok(registries)
}

fn decode_nested_scalar_enum_registries(
    cursor: &mut CataloguePayloadCursor<'_>,
) -> Result<BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<GlobalScalarEnumCaseId>>>, Error> {
    const CONTEXT: &str = "invalid physical mapping payload";
    let count = cursor.u32()?;
    let mut registries = BTreeMap::new();
    let mut previous_column = None;
    for _ in 0..count {
        let id = PhysicalColumnId(cursor.u64()?);
        if id.0 == 0 || previous_column.is_some_and(|previous| id <= previous) {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
        previous_column = Some(id);
        let path_count = cursor.u32()?;
        let mut paths = BTreeMap::new();
        let mut previous_path = None;
        for _ in 0..path_count {
            let path = cursor.string()?;
            require_strictly_increasing(previous_path.as_deref(), &path, CONTEXT)?;
            previous_path = Some(path.clone());
            if paths.insert(path, decode_scalar_cases(cursor)?).is_some() {
                return Err(Error::InvalidStoredValue(CONTEXT));
            }
        }
        if registries.insert(id, paths).is_some() {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
    }
    Ok(registries)
}

fn decode_nested_payload_enum_registries(
    cursor: &mut CataloguePayloadCursor<'_>,
) -> Result<BTreeMap<PhysicalColumnId, BTreeMap<String, Vec<GlobalEnumCaseId>>>, Error> {
    const CONTEXT: &str = "invalid physical mapping payload";
    let count = cursor.u32()?;
    let mut registries = BTreeMap::new();
    let mut previous_column = None;
    for _ in 0..count {
        let id = PhysicalColumnId(cursor.u64()?);
        if id.0 == 0 || previous_column.is_some_and(|previous| id <= previous) {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
        previous_column = Some(id);
        let path_count = cursor.u32()?;
        let mut paths = BTreeMap::new();
        let mut previous_path = None;
        for _ in 0..path_count {
            let path = cursor.string()?;
            require_strictly_increasing(previous_path.as_deref(), &path, CONTEXT)?;
            previous_path = Some(path.clone());
            if paths.insert(path, decode_payload_cases(cursor)?).is_some() {
                return Err(Error::InvalidStoredValue(CONTEXT));
            }
        }
        if registries.insert(id, paths).is_some() {
            return Err(Error::InvalidStoredValue(CONTEXT));
        }
    }
    Ok(registries)
}

fn require_strictly_increasing(
    previous: Option<&str>,
    value: &str,
    context: &'static str,
) -> Result<(), Error> {
    if previous.is_some_and(|previous| previous >= value) {
        Err(Error::InvalidStoredValue(context))
    } else {
        Ok(())
    }
}

pub(super) fn encode_catalogue_schema(schema: &SchemaVersion) -> Result<Vec<u8>, Error> {
    crate::protocol::canonical_catalogue_schema_v1_bytes(schema)
        .map_err(|_| Error::InvalidStoredValue("encode catalogue public schema"))
}

pub(super) fn decode_catalogue_schema(payload: &[u8]) -> Result<SchemaVersion, Error> {
    let mut cursor = CataloguePayloadCursor::new(
        payload,
        CATALOGUE_SCHEMA_VERSION,
        "invalid catalogue schema payload",
    )?;
    let id = SchemaVersionId(cursor.uuid()?);
    let public_schema = cursor.sized_bytes()?;
    cursor.finish()?;
    let schema = crate::tools::public_schema_convert::decode_public_schema_json(public_schema)
        .map_err(|_| Error::InvalidStoredValue("invalid catalogue schema public schema"))?;
    let canonical_public_schema = serde_json::to_vec(schema.public_schema())
        .map_err(|_| Error::InvalidStoredValue("encode catalogue public schema"))?;
    if canonical_public_schema != public_schema {
        return Err(Error::InvalidStoredValue(
            "non-canonical catalogue schema public schema",
        ));
    }
    if schema.version_id() != id {
        return Err(Error::InvalidStoredValue(
            "catalogue schema content id mismatch",
        ));
    }
    Ok(SchemaVersion { id, schema })
}

pub(super) fn encode_catalogue_bootstrap_ready(ready: &CatalogueBootstrapReady) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 16 + 8 + 16 + 8);
    payload.push(CATALOGUE_BOOTSTRAP_READY_VERSION);
    payload.extend_from_slice(ready.genesis.0.as_bytes());
    payload.extend_from_slice(&ready.current_write_schema.revision.to_le_bytes());
    payload.extend_from_slice(ready.current_write_schema.schema.0.as_bytes());
    payload.extend_from_slice(&ready.active_catalogue_seq.to_le_bytes());
    payload
}

pub(super) fn decode_catalogue_bootstrap_ready(
    payload: &[u8],
) -> Result<CatalogueBootstrapReady, Error> {
    let mut cursor = CataloguePayloadCursor::new(
        payload,
        CATALOGUE_BOOTSTRAP_READY_VERSION,
        "invalid catalogue bootstrap receipt payload",
    )?;
    let genesis = SchemaVersionId(cursor.uuid()?);
    let revision = cursor.u64()?;
    let schema = SchemaVersionId(cursor.uuid()?);
    let active_catalogue_seq = cursor.u64()?;
    cursor.finish()?;
    Ok(CatalogueBootstrapReady {
        genesis,
        current_write_schema: CurrentWriteSchema { revision, schema },
        active_catalogue_seq,
    })
}

pub(super) fn encode_catalogue_write_pointer(pointer: CurrentWriteSchema) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 8 + 16);
    payload.push(CATALOGUE_WRITE_POINTER_VERSION);
    payload.extend_from_slice(&pointer.revision.to_le_bytes());
    payload.extend_from_slice(pointer.schema.0.as_bytes());
    payload
}

pub(super) fn catalogue_write_pointer_id(pointer: CurrentWriteSchema) -> uuid::Uuid {
    uuid::Uuid::new_v5(&pointer.schema.0, &pointer.revision.to_le_bytes())
}

pub(super) fn decode_catalogue_write_pointer(payload: &[u8]) -> Result<CurrentWriteSchema, Error> {
    let mut cursor = CataloguePayloadCursor::new(
        payload,
        CATALOGUE_WRITE_POINTER_VERSION,
        "invalid catalogue write-pointer payload",
    )?;
    let revision = cursor.u64()?;
    let schema = SchemaVersionId(cursor.uuid()?);
    cursor.finish()?;
    Ok(CurrentWriteSchema { revision, schema })
}

pub(super) fn encode_catalogue_lineage_activation(activation: SchemaLineageActivation) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 16 + 8);
    payload.push(CATALOGUE_LINEAGE_ACTIVATION_VERSION);
    payload.extend_from_slice(activation.id.0.as_bytes());
    payload.extend_from_slice(&activation.catalogue_seq.to_le_bytes());
    payload
}

pub(super) fn decode_catalogue_lineage_activation(
    payload: &[u8],
) -> Result<SchemaLineageActivation, Error> {
    let mut cursor = CataloguePayloadCursor::new(
        payload,
        CATALOGUE_LINEAGE_ACTIVATION_VERSION,
        "invalid catalogue lineage activation payload",
    )?;
    let id = SchemaLineagePublicationId(cursor.uuid()?);
    let catalogue_seq = cursor.u64()?;
    cursor.finish()?;
    Ok(SchemaLineageActivation { id, catalogue_seq })
}

/// The protocol lens has a dedicated canonical grammar because it is a durable
/// sync/publication fact.  It is deliberately not the jazz-server schema
/// editor's `LensTransform`: those types have different operations.
pub(super) fn encode_catalogue_lens(lens: &MigrationLens) -> Vec<u8> {
    let mut payload = vec![1];
    payload.extend_from_slice(&crate::protocol::canonical_lens_bytes(lens));
    payload
}

pub(super) fn decode_catalogue_lens(payload: &[u8]) -> Result<MigrationLens, Error> {
    let bytes = payload
        .strip_prefix(&[1])
        .ok_or(Error::InvalidStoredValue("invalid catalogue lens payload"))?;
    crate::protocol::decode_canonical_lens_bytes(bytes)
        .map_err(|_| Error::InvalidStoredValue("invalid catalogue lens payload"))
}

fn encode_catalogue_publication(publication: &SchemaLineagePublication) -> Result<Vec<u8>, Error> {
    let schema = encode_catalogue_schema(&publication.schema)?;
    let lens = encode_catalogue_lens(&publication.lens);
    let mut payload = vec![CATALOGUE_PROTOCOL_PUBLICATION_VERSION];
    payload.extend_from_slice(publication.id.0.as_bytes());
    put_len(
        &mut payload,
        schema.len(),
        "catalogue publication schema length",
    )?;
    payload.extend_from_slice(&schema);
    put_len(
        &mut payload,
        lens.len(),
        "catalogue publication lens length",
    )?;
    payload.extend_from_slice(&lens);
    let mut new_tables = publication.new_tables.clone();
    new_tables.sort();
    if new_tables.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Error::InvalidStoredValue(
            "catalogue publication duplicate new table",
        ));
    }
    put_len(
        &mut payload,
        new_tables.len(),
        "catalogue publication new table count",
    )?;
    for table in new_tables {
        put_string(&mut payload, &table, "catalogue publication table")?;
    }
    let mut dropped_tables = publication.dropped_tables.clone();
    dropped_tables.sort();
    if dropped_tables.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Error::InvalidStoredValue(
            "catalogue publication duplicate dropped table",
        ));
    }
    put_len(
        &mut payload,
        dropped_tables.len(),
        "catalogue publication dropped table count",
    )?;
    for table in dropped_tables {
        put_string(&mut payload, &table, "catalogue publication table")?;
    }
    encode_physical_identity_manifest(&mut payload, &publication.physical_identities)?;
    Ok(payload)
}

fn decode_catalogue_publication(payload: &[u8]) -> Result<SchemaLineagePublication, Error> {
    const CONTEXT: &str = "invalid catalogue protocol publication";
    let mut cursor =
        CataloguePayloadCursor::new(payload, CATALOGUE_PROTOCOL_PUBLICATION_VERSION, CONTEXT)?;
    let id = SchemaLineagePublicationId(cursor.uuid()?);
    let schema = decode_catalogue_schema(cursor.sized_bytes()?)?;
    let lens = decode_catalogue_lens(cursor.sized_bytes()?)?;
    let new_tables = decode_sorted_strings(&mut cursor, CONTEXT)?;
    let dropped_tables = decode_sorted_strings(&mut cursor, CONTEXT)?;
    let physical_identities = decode_physical_identity_manifest(&mut cursor)?;
    cursor.finish()?;
    let publication = SchemaLineagePublication {
        id,
        schema,
        lens,
        new_tables,
        dropped_tables,
        physical_identities,
    };
    if publication.id != publication.content_id()
        || encode_catalogue_publication(&publication)? != payload
    {
        return Err(Error::InvalidStoredValue(CONTEXT));
    }
    Ok(publication)
}

fn decode_sorted_strings(
    cursor: &mut CataloguePayloadCursor<'_>,
    context: &'static str,
) -> Result<Vec<String>, Error> {
    let mut values = Vec::new();
    let mut previous = None;
    for _ in 0..cursor.u32()? {
        let value = cursor.string()?;
        require_strictly_increasing(previous.as_deref(), &value, context)?;
        previous = Some(value.clone());
        values.push(value);
    }
    Ok(values)
}

pub(super) fn encode_catalogue_staged_lineage(
    staged: &StagedSchemaLineage,
) -> Result<Vec<u8>, Error> {
    let mapping = encode_physical_mapping(&staged.mapping)?;
    encode_catalogue_staged_lineage_with_mapping(staged, &mapping)
}

fn encode_catalogue_staged_lineage_with_mapping(
    staged: &StagedSchemaLineage,
    mapping: &[u8],
) -> Result<Vec<u8>, Error> {
    let publication = encode_catalogue_publication(&staged.publication)?;
    let mut payload = vec![CATALOGUE_STAGED_LINEAGE_VERSION];
    payload.extend_from_slice(&staged.catalogue_seq.to_le_bytes());
    put_len(
        &mut payload,
        publication.len(),
        "staged catalogue publication length",
    )?;
    payload.extend_from_slice(&publication);
    payload.extend_from_slice(&staged.alias.0.to_le_bytes());
    put_len(
        &mut payload,
        mapping.len(),
        "staged catalogue mapping length",
    )?;
    payload.extend_from_slice(mapping);
    Ok(payload)
}

pub(super) fn decode_catalogue_staged_lineage(
    payload: &[u8],
) -> Result<StagedSchemaLineage, Error> {
    const CONTEXT: &str = "invalid staged catalogue lineage";
    let mut cursor =
        CataloguePayloadCursor::new(payload, CATALOGUE_STAGED_LINEAGE_VERSION, CONTEXT)?;
    let catalogue_seq = cursor.u64()?;
    let publication = decode_catalogue_publication(cursor.sized_bytes()?)?;
    let alias = SchemaVersionAlias(cursor.u64()?);
    let mapping_payload = cursor.sized_bytes()?;
    let mapping = decode_physical_mapping(mapping_payload)?;
    cursor.finish()?;
    let staged = StagedSchemaLineage {
        catalogue_seq,
        publication,
        alias,
        mapping,
    };
    if encode_physical_mapping(&staged.mapping)? != mapping_payload
        || encode_catalogue_staged_lineage_with_mapping(&staged, mapping_payload)? != payload
    {
        return Err(Error::InvalidStoredValue(CONTEXT));
    }
    Ok(staged)
}

pub(super) fn encode_catalogue_pending_lineage(
    pending: &PendingSchemaLineage,
) -> Result<Vec<u8>, Error> {
    let publication = encode_catalogue_publication(&pending.publication)?;
    let mut payload = vec![CATALOGUE_PENDING_LINEAGE_VERSION];
    payload.extend_from_slice(&pending.catalogue_seq.to_le_bytes());
    put_len(
        &mut payload,
        publication.len(),
        "pending catalogue publication length",
    )?;
    payload.extend_from_slice(&publication);
    Ok(payload)
}

pub(super) fn decode_catalogue_pending_lineage(
    payload: &[u8],
) -> Result<PendingSchemaLineage, Error> {
    const CONTEXT: &str = "invalid pending catalogue lineage";
    let mut cursor =
        CataloguePayloadCursor::new(payload, CATALOGUE_PENDING_LINEAGE_VERSION, CONTEXT)?;
    let catalogue_seq = cursor.u64()?;
    let publication = decode_catalogue_publication(cursor.sized_bytes()?)?;
    cursor.finish()?;
    let pending = PendingSchemaLineage {
        catalogue_seq,
        publication,
    };
    if encode_catalogue_pending_lineage(&pending)? != payload {
        return Err(Error::InvalidStoredValue(CONTEXT));
    }
    Ok(pending)
}

struct CataloguePayloadCursor<'a> {
    payload: &'a [u8],
    offset: usize,
    context: &'static str,
}

impl<'a> CataloguePayloadCursor<'a> {
    fn new(payload: &'a [u8], version: u8, context: &'static str) -> Result<Self, Error> {
        if payload.first().copied() != Some(version) {
            return Err(Error::InvalidStoredValue(context));
        }
        Ok(Self {
            payload,
            offset: 1,
            context,
        })
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(Error::InvalidStoredValue(self.context))?;
        let bytes = self
            .payload
            .get(self.offset..end)
            .ok_or(Error::InvalidStoredValue(self.context))?;
        self.offset = end;
        Ok(bytes)
    }

    fn uuid(&mut self) -> Result<uuid::Uuid, Error> {
        let bytes: [u8; 16] = self.bytes(16)?.try_into().expect("exact UUID width");
        Ok(uuid::Uuid::from_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, Error> {
        let bytes: [u8; 8] = self.bytes(8)?.try_into().expect("exact u64 width");
        Ok(u64::from_le_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32, Error> {
        let bytes: [u8; 4] = self.bytes(4)?.try_into().expect("exact u32 width");
        Ok(u32::from_le_bytes(bytes))
    }

    fn string(&mut self) -> Result<String, Error> {
        let bytes = self.sized_bytes()?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| Error::InvalidStoredValue(self.context))
    }

    fn sized_bytes(&mut self) -> Result<&'a [u8], Error> {
        let length = self.u32()? as usize;
        self.bytes(length)
    }

    fn finish(self) -> Result<(), Error> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(Error::InvalidStoredValue(self.context))
        }
    }
}

groove::define_record! {
    pub(super) struct CataloguePointerRowRecord {
        0 => revision: u64,
        1 => schema: SchemaVersionId,
    }
}

#[cfg(test)]
mod catalogue_payload_tests {
    use super::*;

    fn schema_id(value: u128) -> SchemaVersionId {
        SchemaVersionId(uuid::Uuid::from_u128(value))
    }

    fn mapping_fixture() -> SchemaPhysicalMapping {
        let case = |byte, ordinal| GlobalScalarEnumCaseId {
            id: crate::ids::GlobalPhysicalEnumVariantId(uuid::Uuid::from_bytes([byte; 16])),
            introducing_schema: SchemaVersionId(uuid::Uuid::from_bytes([byte; 16])),
            introducing_ordinal: ordinal,
        };
        let payload_case = |identity_byte, schema_byte, ordinal| GlobalEnumCaseId {
            id: crate::ids::GlobalPhysicalEnumVariantId(uuid::Uuid::from_bytes(
                [identity_byte; 16],
            )),
            introducing_schema: SchemaVersionId(uuid::Uuid::from_bytes([schema_byte; 16])),
            introducing_ordinal: ordinal,
        };
        SchemaPhysicalMapping {
            identities: PhysicalIdentityManifest {
                tables: BTreeMap::from([(
                    "items".to_owned(),
                    PhysicalTableIdentity {
                        id: crate::ids::GlobalPhysicalTableId(uuid::Uuid::from_bytes([0x71; 16])),
                        columns: BTreeMap::from([(
                            "state".to_owned(),
                            PhysicalColumnIdentity {
                                id: crate::ids::GlobalPhysicalColumnId(uuid::Uuid::from_bytes(
                                    [0x72; 16],
                                )),
                                enum_variants: BTreeMap::new(),
                            },
                        )]),
                    },
                )]),
            },
            tables: BTreeMap::from([(
                "items".to_owned(),
                TablePhysicalMapping {
                    table_id: PhysicalTableId(7),
                    columns: BTreeMap::from([("state".to_owned(), PhysicalColumnId(11))]),
                    variant_cases: vec![PhysicalVariantCase {
                        tag: 3,
                        fields: BTreeSet::from(["state".to_owned()]),
                    }],
                    scalar_enum_cases: BTreeMap::from([(
                        PhysicalColumnId(11),
                        vec![case(0x11, 2)],
                    )]),
                    payload_enum_cases: BTreeMap::from([(
                        PhysicalColumnId(11),
                        vec![payload_case(0x22, 0xa2, 1)],
                    )]),
                    nested_scalar_enum_cases: BTreeMap::from([(
                        PhysicalColumnId(11),
                        BTreeMap::from([("root/array".to_owned(), vec![case(0x33, 0)])]),
                    )]),
                    nested_payload_enum_cases: BTreeMap::from([(
                        PhysicalColumnId(11),
                        BTreeMap::from([(
                            "root/case/canonical".to_owned(),
                            vec![payload_case(0x44, 0xc4, 3)],
                        )]),
                    )]),
                },
            )]),
        }
    }

    fn assert_global_enum_case_metadata(
        actual: &[GlobalEnumCaseId],
        expected: &[GlobalEnumCaseId],
    ) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert_eq!(actual.id, expected.id);
            assert_eq!(actual.introducing_schema, expected.introducing_schema);
            assert_eq!(actual.introducing_ordinal, expected.introducing_ordinal);
        }
    }

    fn assert_payload_case_metadata(
        actual: &SchemaPhysicalMapping,
        expected: &SchemaPhysicalMapping,
    ) {
        for (table_name, expected_table) in &expected.tables {
            let actual_table = &actual.tables[table_name];
            assert_eq!(
                actual_table.payload_enum_cases.len(),
                expected_table.payload_enum_cases.len()
            );
            for (column, expected_cases) in &expected_table.payload_enum_cases {
                assert_global_enum_case_metadata(
                    &actual_table.payload_enum_cases[column],
                    expected_cases,
                );
            }
            assert_eq!(
                actual_table.nested_payload_enum_cases.len(),
                expected_table.nested_payload_enum_cases.len()
            );
            for (column, expected_paths) in &expected_table.nested_payload_enum_cases {
                let actual_paths = &actual_table.nested_payload_enum_cases[column];
                assert_eq!(actual_paths.len(), expected_paths.len());
                for (path, expected_cases) in expected_paths {
                    assert_global_enum_case_metadata(&actual_paths[path], expected_cases);
                }
            }
        }
    }

    fn staged_fixture() -> StagedSchemaLineage {
        let schema = SchemaVersion::new(JazzSchema::empty());
        let lens =
            MigrationLens::new(schema.id, schema.id, Vec::new()).expect("valid migration lens");
        let publication = SchemaLineagePublication::new_genesis_fixture(
            schema,
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        );
        StagedSchemaLineage {
            catalogue_seq: 7,
            publication,
            alias: SchemaVersionAlias(3),
            mapping: SchemaPhysicalMapping {
                identities: PhysicalIdentityManifest {
                    tables: BTreeMap::new(),
                },
                tables: BTreeMap::new(),
            },
        }
    }

    #[test]
    fn staged_and_pending_lineage_payloads_are_exact_versioned_and_fail_closed() {
        let staged = staged_fixture();
        let exact = encode_catalogue_staged_lineage(&staged).unwrap();
        assert_eq!(&exact[..9], &[1, 7, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            decode_catalogue_staged_lineage(&exact)
                .unwrap()
                .catalogue_seq,
            7
        );
        let mut mapped_staged = staged.clone();
        mapped_staged.mapping = mapping_fixture();
        let mapped_exact = encode_catalogue_staged_lineage(&mapped_staged).unwrap();
        let mut mapped_cursor = CataloguePayloadCursor::new(
            &mapped_exact,
            CATALOGUE_STAGED_LINEAGE_VERSION,
            "mapped staged fixture",
        )
        .unwrap();
        assert_eq!(mapped_cursor.u64().unwrap(), 7);
        mapped_cursor.sized_bytes().unwrap();
        assert_eq!(mapped_cursor.u64().unwrap(), 3);
        assert_eq!(
            mapped_cursor.sized_bytes().unwrap()[0],
            PHYSICAL_MAPPING_VERSION
        );
        mapped_cursor.finish().unwrap();
        let decoded_mapped = decode_catalogue_staged_lineage(&mapped_exact).unwrap();
        assert_eq!(decoded_mapped.catalogue_seq, mapped_staged.catalogue_seq);
        assert_eq!(decoded_mapped.publication, mapped_staged.publication);
        assert_eq!(decoded_mapped.alias, mapped_staged.alias);
        assert_eq!(decoded_mapped.mapping, mapped_staged.mapping);
        let pending = PendingSchemaLineage {
            catalogue_seq: 8,
            publication: staged.publication.clone(),
        };
        let pending_exact = encode_catalogue_pending_lineage(&pending).unwrap();
        assert_eq!(&pending_exact[..9], &[1, 8, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            decode_catalogue_pending_lineage(&pending_exact).unwrap(),
            pending
        );
        for malformed in [
            vec![],
            vec![2],
            exact[..exact.len() - 1].to_vec(),
            [exact.clone(), vec![0]].concat(),
            {
                let mut future = exact.clone();
                future[0] = 2;
                future
            },
            {
                let mut huge = exact[..13].to_vec();
                huge[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
                huge
            },
            // Planted: changing the stored publication ID must fail before
            // a lineage can become resident.
            {
                let mut wrong = exact.clone();
                // staged v1 + sequence + publication length precede the
                // publication's own version byte; byte 14 is its first ID
                // byte, not byte 13.
                wrong[14] ^= 1;
                wrong
            },
        ] {
            assert!(decode_catalogue_staged_lineage(&malformed).is_err());
        }
        for malformed in [
            vec![],
            pending_exact[..pending_exact.len() - 1].to_vec(),
            [pending_exact.clone(), vec![0]].concat(),
            {
                let mut x = pending_exact.clone();
                x[0] = 2;
                x
            },
        ] {
            assert!(decode_catalogue_pending_lineage(&malformed).is_err());
        }
    }

    #[test]
    fn physical_mapping_payload_has_exact_v1_wide_payload_ordinal_fixture() {
        let mapping = mapping_fixture();
        let mut expected = [
            &[1, 1, 0, 0, 0, 5, 0, 0, 0][..],
            b"items",
            &[7, 0, 0, 0, 0, 0, 0, 0],
            &[1, 0, 0, 0, 5, 0, 0, 0][..],
            b"state",
            &[11, 0, 0, 0, 0, 0, 0, 0],
            &[1, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0][..],
            b"state",
            &[1, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0][..],
            &[0x11; 16],
            &[0x11; 16],
            &[2],
            &[1, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0][..],
            &[0x22; 16],
            &[0xa2; 16],
            &[1, 0, 0, 0],
            &[1, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 10, 0, 0, 0][..],
            b"root/array",
            &[1, 0, 0, 0][..],
            &[0x33; 16],
            &[0x33; 16],
            &[0],
            &[1, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 19, 0, 0, 0][..],
            b"root/case/canonical",
            &[1, 0, 0, 0][..],
            &[0x44; 16],
            &[0xc4; 16],
            &[3, 0, 0, 0],
        ]
        .concat();
        let identity_prefix = [
            &[1, 0, 0, 0, 5, 0, 0, 0][..],
            b"items",
            &[0x71; 16],
            &[1, 0, 0, 0, 5, 0, 0, 0][..],
            b"state",
            &[0x72; 16],
            &[0, 0, 0, 0],
        ]
        .concat();
        expected.splice(1..1, identity_prefix);
        let encoded = encode_physical_mapping(&mapping).unwrap();
        assert_eq!(encoded, expected);
        let decoded = decode_physical_mapping(&encoded).unwrap();
        assert_eq!(decoded, mapping);
        assert_payload_case_metadata(&decoded, &mapping);

        let mut wide_mapping = mapping;
        wide_mapping
            .tables
            .get_mut("items")
            .unwrap()
            .payload_enum_cases
            .get_mut(&PhysicalColumnId(11))
            .unwrap()[0]
            .introducing_ordinal = 256;
        let mut expected_wide = expected;
        for (identity_byte, schema_byte, ordinal, replacement) in [(0x22, 0xa2, 1_u32, 256_u32)] {
            let mut marker = vec![identity_byte; 16];
            marker.extend_from_slice(&[schema_byte; 16]);
            marker.extend_from_slice(&ordinal.to_le_bytes());
            let start = expected_wide
                .windows(marker.len())
                .position(|window| window == marker)
                .expect("payload case marker")
                + marker.len()
                - std::mem::size_of::<u32>();
            expected_wide.splice(
                start..start + std::mem::size_of::<u32>(),
                replacement.to_le_bytes(),
            );
        }
        let encoded_wide = encode_physical_mapping(&wide_mapping).unwrap();
        assert_eq!(encoded_wide, expected_wide);
        let decoded_wide = decode_physical_mapping(&encoded_wide).unwrap();
        assert_eq!(decoded_wide, wide_mapping);
        assert_payload_case_metadata(&decoded_wide, &wide_mapping);
    }

    #[test]
    fn physical_mapping_payload_rejects_unknown_malformed_trailing_and_noncanonical_forms() {
        let valid = encode_physical_mapping(&mapping_fixture()).unwrap();
        let mut unsupported_version = valid.clone();
        unsupported_version[0] = 2;
        for malformed in [
            vec![],
            vec![2],
            unsupported_version,
            valid[..valid.len() - 1].to_vec(),
            [valid.clone(), vec![0]].concat(),
        ] {
            assert!(
                decode_physical_mapping(&malformed).is_err(),
                "{malformed:?}"
            );
        }

        // Two otherwise empty tables deliberately appear in reverse byte order.
        // Decoder rejection is sensitive to replacing its strict-order check
        // with BTreeMap insertion (which would silently normalize the bytes).
        let noncanonical = [
            &[1, 2, 0, 0, 0, 1, 0, 0, 0][..],
            b"z",
            &[1, 0, 0, 0, 0, 0, 0, 0],
            &[0; 20],
            &[1, 0, 0, 0],
            b"a",
            &[2, 0, 0, 0, 0, 0, 0, 0],
            &[0; 20],
        ]
        .concat();
        assert!(decode_physical_mapping(&noncanonical).is_err());
    }

    #[test]
    fn catalogue_bootstrap_and_receipt_payloads_have_exact_v1_golden_bytes() {
        let genesis = schema_id(0x00112233445566778899aabbccddeeff);
        let current = schema_id(0xffeeddccbbaa99887766554433221100);
        let ready = CatalogueBootstrapReady {
            genesis,
            current_write_schema: CurrentWriteSchema {
                revision: 0x0102_0304_0506_0708,
                schema: current,
            },
            active_catalogue_seq: 0x1112_1314_1516_1718,
        };
        assert_eq!(
            encode_catalogue_bootstrap_ready(&ready),
            vec![
                1, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc,
                0xdd, 0xee, 0xff, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0xff, 0xee, 0xdd,
                0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00, 0x18,
                0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11,
            ]
        );
        assert_eq!(
            decode_catalogue_bootstrap_ready(&encode_catalogue_bootstrap_ready(&ready)).unwrap(),
            ready
        );

        let pointer = ready.current_write_schema;
        assert_eq!(
            encode_catalogue_write_pointer(pointer),
            vec![
                1, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0xff, 0xee, 0xdd, 0xcc, 0xbb,
                0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00,
            ]
        );
        assert_eq!(
            decode_catalogue_write_pointer(&encode_catalogue_write_pointer(pointer)).unwrap(),
            pointer
        );

        let activation = SchemaLineageActivation {
            id: SchemaLineagePublicationId(uuid::Uuid::from_u128(
                0x102030405060708090a0b0c0d0e0f000,
            )),
            catalogue_seq: 9,
        };
        assert_eq!(
            encode_catalogue_lineage_activation(activation),
            vec![
                1, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0,
                0xe0, 0xf0, 0x00, 9, 0, 0, 0, 0, 0, 0, 0,
            ]
        );
        assert_eq!(
            decode_catalogue_lineage_activation(&encode_catalogue_lineage_activation(activation))
                .unwrap(),
            activation
        );
    }

    #[test]
    fn catalogue_schema_payload_is_versioned_and_round_trips_public_schema() {
        let schema = SchemaVersion::new(JazzSchema::empty());
        let encoded = encode_catalogue_schema(&schema).unwrap();
        assert_eq!(encoded[0], CATALOGUE_SCHEMA_VERSION);
        assert_eq!(&encoded[1..17], schema.id.0.as_bytes());
        // Internal format receipt: publication content addressing consumes this
        // exact CATS V1 byte payload rather than a serde SchemaVersion layout.
        assert_eq!(
            hex::encode(&encoded),
            "010c17f7ec32c55d97acc37a8aeda133760d0000007b227461626c6573223a7b7d7d"
        );
        assert_eq!(decode_catalogue_schema(&encoded).unwrap(), schema);
    }

    #[test]
    fn catalogue_schema_payload_rejects_noncanonical_public_schema_json() {
        let schema = SchemaVersion::new(JazzSchema::empty());
        let mut encoded = encode_catalogue_schema(&schema).unwrap();
        let length = u32::from_le_bytes(encoded[17..21].try_into().unwrap());
        encoded[17..21].copy_from_slice(&(length + 1).to_le_bytes());
        encoded.insert(21, b' ');
        assert!(matches!(
            decode_catalogue_schema(&encoded),
            Err(Error::InvalidStoredValue(
                "non-canonical catalogue schema public schema"
            ))
        ));
    }

    #[test]
    fn catalogue_kernel_payloads_reject_unknown_truncated_and_trailing_bytes() {
        let pointer = CurrentWriteSchema {
            revision: 7,
            schema: schema_id(8),
        };
        let valid = encode_catalogue_write_pointer(pointer);
        for malformed in [
            vec![],
            vec![2],
            valid[..valid.len() - 1].to_vec(),
            [valid.clone(), vec![0]].concat(),
        ] {
            assert!(
                decode_catalogue_write_pointer(&malformed).is_err(),
                "{malformed:?}"
            );
        }

        let schema = SchemaVersion::new(JazzSchema::empty());
        let valid = encode_catalogue_schema(&schema).unwrap();
        for malformed in [
            vec![2],
            valid[..valid.len() - 1].to_vec(),
            [valid.clone(), vec![0]].concat(),
        ] {
            assert!(
                decode_catalogue_schema(&malformed).is_err(),
                "{malformed:?}"
            );
        }
    }
}

groove::define_record! {
    pub(super) struct RejectedTransactionRowRecord {
        0 => time: TxTime,
        1 => node_id: NodeAlias,
        2 => kind: TxKind,
        3 => made_by: AuthorSubject,
        4 => rejection_reason: RejectionReasonTag,
        5 => cascade_root: Option<Value>,
        6 => reason_detail: Option<String>,
        7 => user_metadata: Option<String>,
    }
}

groove::define_record! {
    pub(super) struct PendingEdgeRowRecord {
        0 => child_time: TxTime,
        1 => child_node_id: NodeAlias,
        2 => parent_time: TxTime,
        3 => parent_node_id: NodeAlias,
        4 => physical_table_id: u64,
        5 => branch_key: Vec<u8>,
        6 => row_uuid: RowUuid,
        7 => layer: Vec<u8>,
    }
}

groove::define_record! {
    pub(super) struct RejectedVersionRowRecord {
        0 => tx_time: TxTime,
        1 => tx_node_id: NodeAlias,
        2 => row_uuid: RowUuid,
        3 => layer: Vec<u8>,
        4 => parents: ParentRefs,
        5 => _deletion: Option<Value>,
        .. user_cells,
    }
}

impl VersionRecord {
    pub(super) fn from_commit(
        commit: &MergeableCommit,
        table: &TableSchema,
        schema_version: SchemaVersionId,
    ) -> Result<Self, Error> {
        TxTime::from_physical_ms(commit.now_ms).map_err(|_| {
            Error::InvalidMergeableCommit(
                "commit now_ms exceeds packed HLC physical-millisecond range",
            )
        })?;
        let positional = positional_cells_from_map(table, &commit.cells)?;
        VersionRecord::encode(
            table,
            schema_version,
            commit.row_uuid,
            commit.parents.clone(),
            commit.made_by,
            commit.now_ms,
            commit.made_by,
            commit.now_ms,
            &positional,
            commit.deletion,
        )
        .map(|record| record.with_authored_columns(commit.authored_columns.clone()))
        .map_err(Error::from)
    }

    pub(super) fn from_stored(
        stored: &VersionRow,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        authored_columns: Option<BTreeSet<String>>,
    ) -> Result<Self, Error> {
        // Wire records remain the replicated immutable projection. Content and
        // register rows now live in different storage tables, so projection at
        // this API boundary is assembled from typed row accessors.
        let cells = table
            .columns
            .iter()
            .map(|column| stored.cell(table, &column.name))
            .collect::<Result<Vec<_>, _>>()?;
        VersionRecord::encode(
            table,
            schema_version,
            stored.row_uuid(),
            stored.parents(),
            stored.created_by(),
            stored.created_at().physical_ms(),
            stored.updated_by(),
            stored.updated_at().physical_ms(),
            &cells,
            stored.deletion(),
        )
        .map(|record| record.with_branch_key(stored.branch_key().clone()))
        .map(|record| record.with_authored_columns(authored_columns))
        .map_err(Error::from)
    }
}

pub(super) fn debug_assert_lowered_layouts(schema: &JazzSchema) {
    #[cfg(not(debug_assertions))]
    let _ = schema;

    #[cfg(debug_assertions)]
    {
        fn assert_user_field(descriptor: &records::RecordDescriptor, idx: usize, name: &str) {
            debug_assert_eq!(
                descriptor
                    .fields()
                    .get(idx)
                    .and_then(|field| field.name.as_deref()),
                Some(name),
                "lowered field index drifted for {name}"
            );
        }

        let groove_schema = schema.lower_to_groove();
        let node_descriptor = groove_schema
            .table("jazz_nodes")
            .expect("nodes table")
            .record_schema();
        NodeAliasRowRecord::assert_layout(&node_descriptor);

        let tx_descriptor = groove_schema
            .table("jazz_transactions")
            .expect("transactions table")
            .record_schema();
        TransactionRowRecord::assert_layout(&tx_descriptor);

        let schema_version_descriptor = groove_schema
            .table("jazz_schema_versions")
            .expect("schema versions table")
            .record_schema();
        SchemaVersionAliasRowRecord::assert_layout(&schema_version_descriptor);

        let rejected_tx_descriptor = groove_schema
            .table("jazz_rejected_transactions")
            .expect("rejected transactions table")
            .record_schema();
        RejectedTransactionRowRecord::assert_layout(&rejected_tx_descriptor);

        let pending_edge_descriptor = groove_schema
            .table("jazz_pending_edges")
            .expect("pending edges table")
            .record_schema();
        PendingEdgeRowRecord::assert_layout(&pending_edge_descriptor);

        let global_change_descriptor = groove_schema
            .table("jazz_global_changes")
            .expect("global changes table")
            .record_schema();
        GlobalChangeRowRecord::assert_layout(&global_change_descriptor);

        for table in &schema.tables {
            let rejected_version_descriptor =
                table.rejected_versions_storage_table().record_schema();
            RejectedVersionRowRecord::assert_layout(&rejected_version_descriptor);
        }

        for table in &schema.tables {
            let descriptor = table.history_storage_table().record_schema();
            HistoryRowRecord::assert_layout(&descriptor);
            for (idx, column) in table.columns.iter().enumerate() {
                assert_user_field(
                    &descriptor,
                    HistoryRowRecord::USER_CELLS + idx,
                    &app_column_field(&column.name),
                );
            }

            let register_descriptor = table.register_storage_table().record_schema();
            RegisterRowRecord::assert_layout(&register_descriptor);

            for global_table in table.global_current_storage_tables() {
                let descriptor = global_table.record_schema();
                if global_table.name.ends_with("_register_global_current") {
                    RegisterGlobalCurrentRowRecord::assert_layout(&descriptor);
                } else {
                    GlobalCurrentRowRecord::assert_layout(&descriptor);
                    for (idx, column) in table.columns.iter().enumerate() {
                        assert_user_field(
                            &descriptor,
                            GlobalCurrentRowRecord::USER_CELLS + idx,
                            &app_column_field(&column.name),
                        );
                    }
                }
            }

            let wire_descriptor = table.wire_record_descriptor();
            WireRowRecord::assert_layout(&wire_descriptor);
            for (idx, column) in table.columns.iter().enumerate() {
                assert_user_field(
                    &wire_descriptor,
                    WireRowRecord::USER_CELLS + idx,
                    &app_column_field(&column.name),
                );
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct StoredTransaction {
    pub(super) tx: Transaction,
    pub(super) node_alias: NodeAlias,
    pub(super) fate: Fate,
    pub(super) global_time: Option<GlobalTime>,
    pub(super) durability: DurabilityTier,
    /// True when `n_total_writes` is only the locally known view cardinality.
    pub(super) view_scoped_cardinality: bool,
}

impl StoredTransaction {
    pub(super) fn to_record(&self) -> TransactionRecord {
        TransactionRecord {
            tx_id: self.tx.tx_id,
            made_by: self.tx.made_by,
            kind: self.tx.kind,
            n_total_writes: self.tx.n_total_writes,
            fate: self.fate.clone(),
            global_time: self.global_time,
            durability: self.durability,
            user_metadata_json: self.tx.user_metadata_json.clone(),
            contribution_merge: self.tx.contribution_merge.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionRow {
    pub(super) table: groove::Intern<String>,
    pub(super) branch_key: BranchKey,
    pub(super) record: OwnedRecord,
}

pub(super) struct VersionRowParts {
    pub(super) table: String,
    pub(super) branch_key: BranchKey,
    pub(super) row_uuid: RowUuid,
    pub(super) tx_node_alias: NodeAlias,
    pub(super) schema_version_alias: SchemaVersionAlias,
    pub(super) tx_time: TxTime,
    pub(super) parents: Vec<TxId>,
    pub(super) created_by: AuthorSubject,
    pub(super) created_at: TxTime,
    pub(super) updated_by: AuthorSubject,
    pub(super) updated_at: TxTime,
    pub(super) cells: BTreeMap<String, Value>,
    pub(super) authored_columns: Option<BTreeSet<PhysicalColumnId>>,
    pub(super) deletion: Option<DeletionEvent>,
}

impl VersionRow {
    pub(super) fn from_parts_with_schema_version(
        table: &TableSchema,
        parts: VersionRowParts,
        _storage_schema_version: Option<SchemaVersionId>,
        history_descriptor: Option<records::RecordDescriptor>,
    ) -> Result<Self, Error> {
        let is_deletion = parts.deletion.is_some();
        let values = if is_deletion {
            register_values_from_parts(&parts)?
        } else {
            history_values_from_parts(table, &parts)?
        };
        let record = if is_deletion {
            owned_record_from_storage_values(&table.register_storage_table(), values)?
        } else {
            match history_descriptor {
                Some(descriptor) => {
                    owned_record_from_storage_values_with_descriptor(descriptor, values)?
                }
                None => owned_record_from_storage_values(
                    &table.authored_history_storage_table(),
                    values,
                )?,
            }
        };
        Ok(Self {
            table: groove::Intern::new(parts.table),
            branch_key: parts.branch_key,
            record,
        })
    }

    pub(super) fn from_wire_with_schema_version(
        table: &TableSchema,
        version: &VersionRecord,
        authored_columns: Option<BTreeSet<PhysicalColumnId>>,
        tx_node_alias: NodeAlias,
        schema_version_alias: SchemaVersionAlias,
        tx_time: TxTime,
        _storage_schema_version: Option<SchemaVersionId>,
    ) -> Result<Self, Error> {
        if !version.branch_key().is_canonical() {
            return Err(Error::InvalidMergeableCommit(
                "row version branch key is not canonical",
            ));
        }
        if version.parents().windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::InvalidMergeableCommit(
                "row version parents must be sorted and unique",
            ));
        }
        let (storage_table, values) = if let Some(deletion) = version.deletion() {
            (
                table.register_storage_table(),
                register_values_from_wire(
                    version,
                    tx_node_alias,
                    schema_version_alias,
                    tx_time,
                    deletion,
                )?,
            )
        } else {
            (
                table.authored_history_storage_table(),
                history_values_from_wire(
                    table,
                    version,
                    authored_columns,
                    tx_node_alias,
                    schema_version_alias,
                    tx_time,
                )?,
            )
        };
        Ok(Self {
            table: groove::Intern::new(version.table().to_owned()),
            branch_key: version.branch_key().clone(),
            record: owned_record_from_storage_values(&storage_table, values)?,
        })
    }

    pub(super) fn table(&self) -> &str {
        self.table.as_str()
    }

    pub(super) fn branch_key(&self) -> &BranchKey {
        &self.branch_key
    }

    pub(super) fn row_uuid(&self) -> RowUuid {
        let idx = if self.is_register_record() {
            RegisterRowRecord::FIELD_ROW_UUID_IDX
        } else {
            HistoryRowRecord::FIELD_ROW_UUID_IDX
        };
        RowUuid(
            self.record
                .borrowed()
                .get_uuid(idx)
                .expect("valid row_uuid"),
        )
    }

    pub(super) fn tx_node_alias(&self) -> NodeAlias {
        let idx = if self.is_register_record() {
            RegisterRowRecord::FIELD_TX_NODE_ID_IDX
        } else {
            HistoryRowRecord::FIELD_TX_NODE_ID_IDX
        };
        NodeAlias(
            self.record
                .borrowed()
                .get_u64(idx)
                .expect("valid tx_node_id"),
        )
    }

    pub(super) fn tx_time(&self) -> TxTime {
        let idx = if self.is_register_record() {
            RegisterRowRecord::FIELD_TX_TIME_IDX
        } else {
            HistoryRowRecord::FIELD_TX_TIME_IDX
        };
        TxTime(self.record.borrowed().get_u64(idx).expect("valid tx_time"))
    }

    pub(super) fn parents(&self) -> Vec<TxId> {
        self.checked_parents()
            .expect("valid canonical parent tx ids")
    }

    pub(super) fn validate_canonical(&self) -> Result<(), Error> {
        validate_canonical_version_parts(&self.branch_key, &self.checked_parents()?)
    }

    fn checked_parents(&self) -> Result<Vec<TxId>, Error> {
        let idx = if self.is_register_record() {
            RegisterRowRecord::FIELD_PARENTS_IDX
        } else {
            HistoryRowRecord::FIELD_PARENTS_IDX
        };
        tx_ids_from_value(self.record.borrowed().get_idx(idx)?)
    }

    pub(super) fn created_by(&self) -> AuthorSubject {
        let idx = if self.is_register_record() {
            RegisterRowRecord::FIELD_CREATED_BY_IDX
        } else {
            HistoryRowRecord::FIELD_CREATED_BY_IDX
        };
        AuthorSubject::from_canonical(
            self.record
                .borrowed()
                .get_str(idx)
                .expect("valid created_by"),
        )
        .expect("canonical created_by")
    }

    pub(super) fn created_at(&self) -> TxTime {
        let idx = if self.is_register_record() {
            RegisterRowRecord::FIELD_CREATED_AT_IDX
        } else {
            HistoryRowRecord::FIELD_CREATED_AT_IDX
        };
        TxTime(
            self.record
                .borrowed()
                .get_u64(idx)
                .expect("valid created_at"),
        )
    }

    pub(super) fn updated_by(&self) -> AuthorSubject {
        let idx = if self.is_register_record() {
            RegisterRowRecord::FIELD_UPDATED_BY_IDX
        } else {
            HistoryRowRecord::FIELD_UPDATED_BY_IDX
        };
        AuthorSubject::from_canonical(
            self.record
                .borrowed()
                .get_str(idx)
                .expect("valid updated_by"),
        )
        .expect("canonical updated_by")
    }

    pub(super) fn updated_at(&self) -> TxTime {
        let idx = if self.is_register_record() {
            RegisterRowRecord::FIELD_UPDATED_AT_IDX
        } else {
            HistoryRowRecord::FIELD_UPDATED_AT_IDX
        };
        TxTime(
            self.record
                .borrowed()
                .get_u64(idx)
                .expect("valid updated_at"),
        )
    }

    pub(super) fn schema_version_alias(&self) -> SchemaVersionAlias {
        let idx = if self.is_register_record() {
            RegisterRowRecord::FIELD_SCHEMA_VERSION_IDX
        } else {
            HistoryRowRecord::FIELD_SCHEMA_VERSION_IDX
        };
        SchemaVersionAlias(
            self.record
                .borrowed()
                .get_u64(idx)
                .expect("valid schema_version"),
        )
    }

    /// Bind a derived storage row to the same schema version as this version.
    pub(super) fn bind_groove_record(&self, record: OwnedRecord) -> groove::records::VariantRecord {
        groove::records::VariantRecord::new(
            u32::try_from(self.schema_version_alias().0)
                .expect("schema aliases are allocated in Groove's variant-tag space"),
            record,
        )
    }

    pub(super) fn deletion(&self) -> Option<DeletionEvent> {
        if !self.is_register_record() {
            return None;
        }
        deletion_event_from_value(
            self.record
                .borrowed()
                .get_idx(RegisterRowRecord::FIELD__DELETION_IDX)
                .expect("valid deletion"),
        )
        .map(Some)
        .expect("valid deletion")
    }

    pub(super) fn layer(&self) -> VersionLayer {
        version_layer_from_deletion(self.deletion())
    }

    pub(super) fn cells(&self, table: &TableSchema) -> Result<BTreeMap<String, Value>, Error> {
        let mut cells = BTreeMap::new();
        if self.is_register_record() {
            return Ok(cells);
        }
        let borrowed = self.record.borrowed();
        for (idx, column) in table.columns.iter().enumerate() {
            if let Some(value) =
                nullable_value(borrowed.get_idx(HistoryRowRecord::USER_CELLS + idx)?)?
            {
                cells.insert(column.name.clone(), value);
            }
        }
        Ok(cells)
    }

    pub(super) fn cell(&self, table: &TableSchema, column: &str) -> Result<Option<Value>, Error> {
        if self.is_register_record() {
            return Ok(None);
        }
        let field = HistoryRowRecord::USER_CELLS
            + table
                .columns
                .iter()
                .position(|candidate| candidate.name == column)
                .ok_or(Error::InvalidStoredValue("missing user column field"))?;
        nullable_value(self.record.borrowed().get_idx(field)?)
    }

    /// `None` is the deliberate legacy/lens fallback: every present cell is
    /// treated as authored by merge code. Exact sets use strictly increasing
    /// node-local physical column ids; alternate set spellings are invalid.
    pub(super) fn authored_column_ids(&self) -> Result<Option<BTreeSet<PhysicalColumnId>>, Error> {
        if self.is_register_record() {
            return Ok(None);
        }
        let Some(field) = self.record.descriptor().field_index("authored_columns") else {
            return Ok(None);
        };
        let value = nullable_value(self.record.borrowed().get_idx(field)?)?;
        value.map(authored_column_ids_from_value).transpose()
    }

    pub(super) fn is_register_record(&self) -> bool {
        self.record.descriptor().field_index("_deletion").is_some()
    }

    pub(super) fn to_history_entry(
        &self,
        tx: &StoredTransaction,
        is_locally_current: bool,
        is_globally_current: bool,
    ) -> HistoryEntry {
        HistoryEntry::new(
            self.table().to_owned(),
            self.record.clone(),
            TransactionRecord {
                tx_id: tx.tx.tx_id,
                made_by: tx.tx.made_by,
                kind: tx.tx.kind,
                n_total_writes: tx.tx.n_total_writes,
                fate: tx.fate.clone(),
                global_time: tx.global_time,
                durability: tx.durability,
                user_metadata_json: tx.tx.user_metadata_json.clone(),
                contribution_merge: tx.tx.contribution_merge.clone(),
            },
            is_locally_current,
            is_globally_current,
        )
    }
}

pub(super) fn owned_record_from_storage_values(
    storage_table: &GrooveTableSchema,
    values: Vec<Value>,
) -> Result<OwnedRecord, Error> {
    let descriptor = storage_table.record_schema();
    owned_record_from_storage_values_with_descriptor(descriptor, values)
}

pub(super) fn owned_record_from_storage_values_with_descriptor(
    descriptor: groove::records::RecordDescriptor,
    values: Vec<Value>,
) -> Result<OwnedRecord, Error> {
    let raw = descriptor.create(&values)?;
    Ok(OwnedRecord::new(raw, descriptor))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ParkedIngressRole {
    Relay,
    EdgeAuthority,
    Authority,
    EdgeAccepted,
}

impl ParkedIngressRole {
    pub(super) fn strongest(self, other: Self) -> Self {
        use ParkedIngressRole::{Authority, EdgeAccepted, EdgeAuthority, Relay};
        match (self, other) {
            (EdgeAccepted, _) | (_, EdgeAccepted) => EdgeAccepted,
            (Authority, _) | (_, Authority) => Authority,
            (EdgeAuthority, _) | (_, EdgeAuthority) => EdgeAuthority,
            (Relay, Relay) => Relay,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ParkedCommitUnit {
    pub(super) tx: Transaction,
    pub(super) versions: Vec<VersionRecord>,
    pub(super) now_ms: u64,
    pub(super) ingest_context: Option<CommitUnitIngestContext>,
    pub(super) ingress_role: ParkedIngressRole,
}

pub(super) fn current_version_index(
    versions: &[VersionRow],
    candidate_indices: &[usize],
    layer: VersionLayer,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
) -> Option<usize> {
    match layer {
        VersionLayer::Content => {
            let heads = content_head_indices(versions, candidate_indices, node_aliases);
            heads.into_iter().max_by_key(|idx| {
                let tx_id = version_tx_id_from_aliases(&versions[*idx], node_aliases)
                    .expect("valid version tx id");
                versions[*idx].tx_time().sort_key(tx_id.node)
            })
        }
        VersionLayer::Deletion => candidate_indices.iter().copied().max_by_key(|idx| {
            let tx_id = version_tx_id_from_aliases(&versions[*idx], node_aliases)
                .expect("valid version tx id");
            versions[*idx].tx_time().sort_key(tx_id.node)
        }),
    }
}

pub(super) fn version_wins_over_open_winner(
    incoming: &VersionRow,
    incoming_tx_id: TxId,
    incoming_made_at: TxTime,
    open_winner: Option<(&VersionRow, TxId, TxTime)>,
) -> bool {
    match open_winner {
        None => true,
        Some((_, winner_tx_id, _)) if incoming.parents().contains(&winner_tx_id) => true,
        Some((_, winner_tx_id, winner_made_at)) => {
            incoming_made_at.sort_key(incoming_tx_id.node)
                > winner_made_at.sort_key(winner_tx_id.node)
        }
    }
}

pub(super) fn content_head_indices(
    versions: &[VersionRow],
    candidate_indices: &[usize],
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
) -> Vec<usize> {
    let txs = candidate_indices
        .iter()
        .map(|idx| {
            version_tx_id_from_aliases(&versions[*idx], node_aliases).expect("valid version tx id")
        })
        .collect::<std::collections::BTreeSet<_>>();
    let parents_by_tx = candidate_indices
        .iter()
        .map(|idx| {
            let tx_id = version_tx_id_from_aliases(&versions[*idx], node_aliases)
                .expect("valid version tx id");
            (tx_id, versions[*idx].parents())
        })
        .collect::<BTreeMap<_, _>>();
    let dominated = candidate_indices
        .iter()
        .flat_map(|idx| {
            let mut dominated = Vec::new();
            let mut stack = versions[*idx].parents();
            let mut seen = std::collections::BTreeSet::new();
            while let Some(parent) = stack.pop() {
                if !seen.insert(parent) {
                    continue;
                }
                if txs.contains(&parent) {
                    dominated.push(parent);
                }
                if let Some(parents) = parents_by_tx.get(&parent) {
                    stack.extend(parents.iter().copied());
                }
            }
            dominated
        })
        .collect::<std::collections::BTreeSet<_>>();
    candidate_indices
        .iter()
        .copied()
        .filter(|idx| {
            let tx_id = version_tx_id_from_aliases(&versions[*idx], node_aliases)
                .expect("valid version tx id");
            !dominated.contains(&tx_id)
        })
        .collect()
}

pub(super) fn version_tx_id_from_aliases(
    version: &VersionRow,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
) -> Option<TxId> {
    node_aliases
        .iter()
        .find_map(|(node, alias)| (*alias == version.tx_node_alias()).then_some(*node))
        .map(|node| TxId::new(version.tx_time(), node))
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum VersionLayer {
    Content,
    Deletion,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ParentCoordinate {
    pub(super) physical_table_id: PhysicalTableId,
    pub(super) branch_key: BranchKey,
    pub(super) row_uuid: RowUuid,
    pub(super) layer: VersionLayer,
}

impl VersionLayer {
    pub(super) fn for_commit(commit: &MergeableCommit) -> Self {
        if commit.deletion.is_some() {
            Self::Deletion
        } else {
            Self::Content
        }
    }

    pub(super) fn for_record(record: &VersionRecord) -> Self {
        if record.deletion().is_some() {
            Self::Deletion
        } else {
            Self::Content
        }
    }
}

pub(super) fn version_layer_from_deletion(deletion: Option<DeletionEvent>) -> VersionLayer {
    if deletion.is_some() {
        VersionLayer::Deletion
    } else {
        VersionLayer::Content
    }
}

fn contribution_merge_descriptor() -> &'static records::RecordDescriptor {
    static DESCRIPTOR: std::sync::LazyLock<records::RecordDescriptor> =
        std::sync::LazyLock::new(|| {
            let records::ValueType::Nullable(inner) = contribution_merge_storage_type() else {
                unreachable!("contribution merge storage column is nullable")
            };
            let records::ValueType::Record(descriptor) = *inner else {
                unreachable!("contribution merge storage column contains a record")
            };
            *descriptor
        });
    &DESCRIPTOR
}

fn branch_view_copy_descriptor() -> &'static records::RecordDescriptor {
    let descriptor = contribution_merge_descriptor();
    let records::ValueType::Array(inner) = record_field_type(descriptor, 3) else {
        unreachable!("branch-view copy is an array")
    };
    let records::ValueType::Record(descriptor) = &**inner else {
        unreachable!("branch-view copy is a record")
    };
    descriptor
}

fn branch_write_intent_descriptor() -> &'static records::RecordDescriptor {
    let descriptor = contribution_merge_descriptor();
    let records::ValueType::Array(inner) = record_field_type(descriptor, 4) else {
        unreachable!("branch write intent is an array")
    };
    let records::ValueType::Record(descriptor) = &**inner else {
        unreachable!("branch write intent is a record")
    };
    descriptor
}

fn branch_write_operation_schema() -> &'static records::EnumSchema {
    let records::ValueType::Enum(schema) = record_field_type(branch_write_intent_descriptor(), 5)
    else {
        unreachable!("branch write operation is an enum")
    };
    schema
}

fn branch_write_intent_storage_record(
    intent: &BranchWriteIntent,
    evidence_index: Option<u32>,
) -> Result<OwnedRecord, Error> {
    let schema = branch_write_operation_schema();
    let (name, values): (&str, Vec<Value>) = match (&intent.operation, evidence_index) {
        (BranchWriteOperation::ExactHeadInsert, None) => ("exact_head_insert", Vec::new()),
        (BranchWriteOperation::ExactHeadUpdate, None) => ("exact_head_update", Vec::new()),
        (BranchWriteOperation::ViewUpdateCopy(_), Some(index)) => {
            ("view_update_copy", vec![Value::U32(index)])
        }
        _ => {
            return Err(Error::InvalidStoredValue(
                "branch write intent evidence binding is invalid",
            ));
        }
    };
    let tag = schema.tag(name).expect("declared branch write operation");
    let payload = &schema
        .case(tag)
        .expect("declared branch write operation")
        .payload;
    let payload = OwnedRecord::new(payload.create(&values)?, payload.clone());
    Ok(BranchWriteIntentStorageRecord::encode(
        branch_write_intent_descriptor(),
        intent.version,
        intent.physical_table_id.0,
        intent.authored_schema.0,
        intent.row_uuid.0,
        intent
            .head
            .try_canonical_bytes()
            .map_err(|_| Error::InvalidStoredValue("branch write intent head is invalid"))?,
        records::EnumValue::new(tag, payload),
    )
    .map(|record| record.record().clone())?)
}

fn branch_write_intent_from_storage_record(
    record: OwnedRecord,
    copies: &[BranchViewCopyEvidence],
) -> Result<BranchWriteIntent, Error> {
    if record.descriptor() != branch_write_intent_descriptor() {
        return Err(Error::InvalidStoredValue(
            "branch write intent must be a v1 record",
        ));
    }
    let record = BranchWriteIntentStorageRecord::new(record);
    let operation_value = record.operation()?;
    let schema = branch_write_operation_schema();
    let case = schema
        .case(operation_value.tag())
        .map_err(|_| Error::InvalidStoredValue("branch write operation tag is invalid"))?;
    let operation = match case.name.as_str() {
        "exact_head_insert" => BranchWriteOperation::ExactHeadInsert,
        "exact_head_update" => BranchWriteOperation::ExactHeadUpdate,
        "view_update_copy" => {
            let payload = operation_value.into_record();
            let index = payload
                .get_idx(0)
                .map_err(|_| Error::InvalidStoredValue("branch write copy index is invalid"))?;
            let Value::U32(index) = index else {
                return Err(Error::InvalidStoredValue(
                    "branch write copy index is invalid",
                ));
            };
            let evidence = copies
                .get(index as usize)
                .cloned()
                .ok_or(Error::InvalidStoredValue(
                    "branch write copy evidence is missing",
                ))?;
            BranchWriteOperation::ViewUpdateCopy(evidence)
        }
        _ => {
            return Err(Error::InvalidStoredValue(
                "branch write operation tag is invalid",
            ));
        }
    };
    Ok(BranchWriteIntent {
        version: record.version()?,
        physical_table_id: PhysicalTableId(record.physical_table_id()?),
        authored_schema: SchemaVersionId(record.authored_schema()?),
        row_uuid: RowUuid(record.row_uuid()?),
        head: BranchKey::from_canonical_bytes(&record.head()?)
            .map_err(|_| Error::InvalidStoredValue("branch write intent head is invalid"))?,
        operation,
    })
}

fn branch_view_copy_base_schema() -> &'static records::EnumSchema {
    let records::ValueType::Enum(schema) = record_field_type(branch_view_copy_descriptor(), 2)
    else {
        unreachable!("branch-view copy base is an enum")
    };
    schema
}

fn branch_view_copy_base_storage_value(
    base: &BranchViewCopyBase,
) -> Result<records::EnumValue, Error> {
    let schema = branch_view_copy_base_schema();
    match base {
        BranchViewCopyBase::Current(branch) => {
            let tag = schema.tag("current").expect("declared current base");
            let payload = &schema.case(tag).expect("declared current base").payload;
            let record = BranchViewCopyCurrentBaseStorageRecord::encode(
                payload,
                branch
                    .try_canonical_bytes()
                    .map_err(|_| Error::InvalidStoredValue("branch-view copy base is invalid"))?,
            )?
            .record()
            .clone();
            Ok(records::EnumValue::new(tag, record))
        }
        BranchViewCopyBase::Snapshot { branch, snapshot } => {
            let tag = schema.tag("snapshot").expect("declared snapshot base");
            let payload = &schema.case(tag).expect("declared snapshot base").payload;
            let dots_type = record_field_type(payload, 4);
            let dot_descriptor = nested_record_descriptor(array_element_type(dots_type));
            let dots = snapshot
                .dots
                .iter()
                .map(|dot| {
                    BranchViewCopyDotStorageRecord::encode(dot_descriptor, dot.time.0, dot.node.0)
                        .map(|record| Value::Record(record.record().clone()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let record = BranchViewCopySnapshotBaseStorageRecord::encode(
                payload,
                branch
                    .try_canonical_bytes()
                    .map_err(|_| Error::InvalidStoredValue("branch-view copy base is invalid"))?,
                snapshot.owner.0,
                snapshot.global_base.0,
                snapshot.local_base.0,
                dots,
            )?
            .record()
            .clone();
            Ok(records::EnumValue::new(tag, record))
        }
    }
}

fn branch_view_copy_storage_record(
    evidence: &BranchViewCopyEvidence,
) -> Result<OwnedRecord, Error> {
    let descriptor = branch_view_copy_descriptor();
    Ok(BranchViewCopyStorageRecord::encode(
        descriptor,
        evidence.version,
        evidence
            .head
            .try_canonical_bytes()
            .map_err(|_| Error::InvalidStoredValue("branch-view copy head is invalid"))?,
        branch_view_copy_base_storage_value(&evidence.base)?,
        evidence.table.clone(),
        evidence.row_uuid.0,
        evidence.source_version.time.0,
        evidence.source_version.node.0,
    )?
    .record()
    .clone())
}

fn branch_view_copy_from_storage_record(
    record: OwnedRecord,
) -> Result<BranchViewCopyEvidence, Error> {
    let descriptor = branch_view_copy_descriptor();
    if record.descriptor() != descriptor {
        return Err(Error::InvalidStoredValue(
            "branch-view copy evidence must be a v1 record",
        ));
    }
    let record = BranchViewCopyStorageRecord::new(record);
    let base_schema = branch_view_copy_base_schema();
    let base_value = record.base()?;
    let base_case = base_schema
        .case(base_value.tag())
        .map_err(|_| Error::InvalidStoredValue("branch-view copy base tag is invalid"))?;
    let base = match base_case.name.as_str() {
        "current" => {
            let payload = BranchViewCopyCurrentBaseStorageRecord::new(base_value.into_record());
            BranchViewCopyBase::Current(
                BranchKey::from_canonical_bytes(&payload.branch()?).map_err(|_| {
                    Error::InvalidStoredValue("branch-view copy current branch is invalid")
                })?,
            )
        }
        "snapshot" => {
            let payload = BranchViewCopySnapshotBaseStorageRecord::new(base_value.into_record());
            let dots = payload
                .dots()?
                .into_iter()
                .map(|value| match value {
                    Value::Record(record) => {
                        let dot = BranchViewCopyDotStorageRecord::new(record);
                        Ok(TxId::new(TxTime(dot.time()?), NodeUuid(dot.node()?)))
                    }
                    _ => Err(Error::InvalidStoredValue(
                        "branch-view copy snapshot dot must be a record",
                    )),
                })
                .collect::<Result<Vec<_>, Error>>()?;
            BranchViewCopyBase::Snapshot {
                branch: BranchKey::from_canonical_bytes(&payload.branch()?).map_err(|_| {
                    Error::InvalidStoredValue("branch-view copy snapshot branch is invalid")
                })?,
                snapshot: SnapshotRef {
                    owner: NodeUuid(payload.owner()?),
                    global_base: GlobalTime(payload.global_base()?),
                    local_base: TxTime(payload.local_base()?),
                    dots,
                },
            }
        }
        _ => {
            return Err(Error::InvalidStoredValue(
                "branch-view copy base tag is invalid",
            ));
        }
    };
    Ok(BranchViewCopyEvidence {
        version: record.version()?,
        head: BranchKey::from_canonical_bytes(&record.head()?)
            .map_err(|_| Error::InvalidStoredValue("branch-view copy head is invalid"))?,
        base,
        table: record.table()?,
        row_uuid: RowUuid(record.row_uuid()?),
        source_version: TxId::new(
            TxTime(record.source_time()?),
            NodeUuid(record.source_node()?),
        ),
    })
}

pub(super) fn record_field_type(
    descriptor: &records::RecordDescriptor,
    index: usize,
) -> &records::ValueType {
    &descriptor
        .fields()
        .get(index)
        .expect("contribution storage descriptor field")
        .value_type
}

fn nested_record_descriptor(value_type: &records::ValueType) -> &records::RecordDescriptor {
    let records::ValueType::Record(descriptor) = value_type else {
        unreachable!("contribution storage field is a record")
    };
    descriptor
}

fn array_element_type(value_type: &records::ValueType) -> &records::ValueType {
    let records::ValueType::Array(element) = value_type else {
        unreachable!("contribution storage field is an array")
    };
    element
}

fn contribution_component_storage_value(
    coordinate: &ContributionCoordinate,
    component: &ContributionComponent,
    value_type: &records::ValueType,
    resolve_column_id: &mut impl FnMut(&str, &str) -> Result<PhysicalColumnId, Error>,
) -> Result<records::EnumValue, Error> {
    let records::ValueType::Enum(schema) = value_type else {
        unreachable!("contribution component uses a Groove payload enum")
    };
    let case_name = match component {
        ContributionComponent::Column(_) => "column",
        ContributionComponent::Operation { .. } => "operation",
        ContributionComponent::Register => "register",
    };
    let tag = schema
        .tag(case_name)
        .expect("declared contribution component case");
    let payload = &schema
        .case(tag)
        .expect("declared contribution component case")
        .payload;
    let record = match component {
        ContributionComponent::Column(name) => {
            let id = resolve_column_id(&coordinate.table, name)?;
            if id.0 == 0 {
                return Err(Error::InvalidStoredValue(
                    "contribution physical column id must be nonzero",
                ));
            }
            ContributionColumnStorageRecord::encode(payload, id.0)?
                .record()
                .clone()
        }
        ContributionComponent::Operation { column, identity } => {
            let id = resolve_column_id(&coordinate.table, column)?;
            if id.0 == 0 {
                return Err(Error::InvalidStoredValue(
                    "contribution physical column id must be nonzero",
                ));
            }
            ContributionOperationStorageRecord::encode(payload, id.0, identity.clone())?
                .record()
                .clone()
        }
        ContributionComponent::Register => OwnedRecord::new(payload.create(&[])?, *payload),
    };
    Ok(records::EnumValue::new(tag, record))
}

fn contribution_coordinate_storage_record(
    coordinate: &ContributionCoordinate,
    descriptor: &records::RecordDescriptor,
    resolve_table_id: &mut impl FnMut(&str) -> Result<PhysicalTableId, Error>,
    resolve_column_id: &mut impl FnMut(&str, &str) -> Result<PhysicalColumnId, Error>,
) -> Result<OwnedRecord, Error> {
    let table_id = resolve_table_id(&coordinate.table)?;
    if table_id.0 == 0 {
        return Err(Error::InvalidStoredValue(
            "contribution physical table id must be nonzero",
        ));
    }
    let record = ContributionCoordinateStorageRecord::encode(
        descriptor,
        coordinate.branch_key.canonical_bytes(),
        table_id.0,
        coordinate.row_uuid.0,
        coordinate.layer,
        contribution_component_storage_value(
            coordinate,
            &coordinate.component,
            record_field_type(descriptor, 4),
            resolve_column_id,
        )?,
    )?
    .record()
    .clone();
    Ok(record)
}

fn contribution_dot_storage_record(
    dot: &ContributionDot,
    descriptor: &records::RecordDescriptor,
    resolve_table_id: &mut impl FnMut(&str) -> Result<PhysicalTableId, Error>,
    resolve_column_id: &mut impl FnMut(&str, &str) -> Result<PhysicalColumnId, Error>,
) -> Result<OwnedRecord, Error> {
    let coordinate = nested_record_descriptor(record_field_type(descriptor, 2));
    let record = ContributionDotStorageRecord::encode(
        descriptor,
        dot.tx_id.time.0,
        dot.tx_id.node.0,
        contribution_coordinate_storage_record(
            &dot.coordinate,
            coordinate,
            resolve_table_id,
            resolve_column_id,
        )?,
    )?
    .record()
    .clone();
    Ok(record)
}

fn contribution_substitution_storage_record(
    substitution: &ContributionSubstitution,
    descriptor: &records::RecordDescriptor,
    resolve_table_id: &mut impl FnMut(&str) -> Result<PhysicalTableId, Error>,
    resolve_column_id: &mut impl FnMut(&str, &str) -> Result<PhysicalColumnId, Error>,
) -> Result<OwnedRecord, Error> {
    let target = nested_record_descriptor(record_field_type(descriptor, 0));
    let source = nested_record_descriptor(array_element_type(record_field_type(descriptor, 1)));
    let record = ContributionSubstitutionStorageRecord::encode(
        descriptor,
        contribution_coordinate_storage_record(
            &substitution.target,
            target,
            resolve_table_id,
            resolve_column_id,
        )?,
        substitution
            .sources
            .iter()
            .map(|dot| {
                contribution_dot_storage_record(dot, source, resolve_table_id, resolve_column_id)
                    .map(Value::Record)
            })
            .collect::<Result<Vec<_>, _>>()?,
    )?
    .record()
    .clone();
    Ok(record)
}

pub(super) fn contribution_merge_storage_value(
    provenance: Option<&ContributionMergeProvenance>,
    mut resolve_table_id: impl FnMut(&str) -> Result<PhysicalTableId, Error>,
    mut resolve_column_id: impl FnMut(&str, &str) -> Result<PhysicalColumnId, Error>,
) -> Result<Value, Error> {
    let descriptor = contribution_merge_descriptor();
    let record = provenance
        .map(|provenance| -> Result<OwnedRecord, Error> {
            let substitution =
                nested_record_descriptor(array_element_type(record_field_type(descriptor, 2)));
            Ok(ContributionMergeStorageRecord::encode(
                descriptor,
                provenance.source.canonical_bytes(),
                provenance.target.canonical_bytes(),
                provenance
                    .substitutions
                    .iter()
                    .map(|item| {
                        contribution_substitution_storage_record(
                            item,
                            substitution,
                            &mut resolve_table_id,
                            &mut resolve_column_id,
                        )
                        .map(Value::Record)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                provenance
                    .branch_view_copies
                    .iter()
                    .map(|evidence| branch_view_copy_storage_record(evidence).map(Value::Record))
                    .collect::<Result<Vec<_>, _>>()?,
                provenance
                    .branch_write_intents
                    .iter()
                    .map(|intent| {
                        let index = match &intent.operation {
                            BranchWriteOperation::ViewUpdateCopy(evidence) => provenance
                                .branch_view_copies
                                .iter()
                                .position(|candidate| candidate == evidence)
                                .map(|index| index as u32),
                            _ => None,
                        };
                        branch_write_intent_storage_record(intent, index).map(Value::Record)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )?
            .record()
            .clone())
        })
        .transpose()?;
    Ok(records::RecordField::to_value(&record))
}

pub(super) fn contribution_component_from_storage(
    value: records::EnumValue,
    schema: &records::EnumSchema,
    table: &str,
    resolve_column_name: &mut impl FnMut(&str, PhysicalColumnId) -> Result<String, Error>,
) -> Result<ContributionComponent, Error> {
    let case = schema.case(value.tag()).map_err(|_| {
        Error::InvalidStoredValue("transaction contribution component tag is invalid")
    })?;
    let payload = value.into_record();
    match case.name.as_str() {
        "column" => {
            let id = PhysicalColumnId(
                ContributionColumnStorageRecord::new(payload).physical_column_id()?,
            );
            if id.0 == 0 {
                return Err(Error::InvalidStoredValue(
                    "stored contribution physical column id must be nonzero",
                ));
            }
            Ok(ContributionComponent::Column(resolve_column_name(
                table, id,
            )?))
        }
        "operation" => {
            let payload = ContributionOperationStorageRecord::new(payload);
            let id = PhysicalColumnId(payload.physical_column_id()?);
            if id.0 == 0 {
                return Err(Error::InvalidStoredValue(
                    "stored contribution physical column id must be nonzero",
                ));
            }
            Ok(ContributionComponent::Operation {
                column: resolve_column_name(table, id)?,
                identity: payload.identity()?,
            })
        }
        "register" if payload.descriptor().fields().is_empty() => {
            payload.to_values()?;
            Ok(ContributionComponent::Register)
        }
        _ => Err(Error::InvalidStoredValue(
            "transaction contribution component payload is invalid",
        )),
    }
}

fn contribution_coordinate_from_storage(
    record: OwnedRecord,
    resolve_table_name: &mut impl FnMut(PhysicalTableId) -> Result<String, Error>,
    resolve_column_name: &mut impl FnMut(&str, PhysicalColumnId) -> Result<String, Error>,
) -> Result<ContributionCoordinate, Error> {
    let records::ValueType::Enum(component_schema) = record_field_type(record.descriptor(), 4)
    else {
        return Err(Error::InvalidStoredValue(
            "transaction contribution component descriptor is invalid",
        ));
    };
    let component_schema = component_schema.clone();
    let record = ContributionCoordinateStorageRecord::new(record);
    let table_id = PhysicalTableId(record.physical_table_id()?);
    if table_id.0 == 0 {
        return Err(Error::InvalidStoredValue(
            "stored contribution physical table id must be nonzero",
        ));
    }
    let table = resolve_table_name(table_id)?;
    let branch_key = BranchKey::from_canonical_bytes(&record.branch_key()?)
        .map_err(|_| Error::InvalidStoredValue("transaction contribution branch key is invalid"))?;
    Ok(ContributionCoordinate {
        branch_key,
        table: table.clone(),
        row_uuid: RowUuid(record.row_uuid()?),
        layer: record.layer()?,
        component: contribution_component_from_storage(
            record.component()?,
            &component_schema,
            &table,
            resolve_column_name,
        )?,
    })
}

fn contribution_dot_from_storage(
    record: OwnedRecord,
    resolve_table_name: &mut impl FnMut(PhysicalTableId) -> Result<String, Error>,
    resolve_column_name: &mut impl FnMut(&str, PhysicalColumnId) -> Result<String, Error>,
) -> Result<ContributionDot, Error> {
    let record = ContributionDotStorageRecord::new(record);
    Ok(ContributionDot {
        tx_id: TxId::new(TxTime(record.tx_time()?), NodeUuid(record.tx_node()?)),
        coordinate: contribution_coordinate_from_storage(
            record.coordinate()?,
            resolve_table_name,
            resolve_column_name,
        )?,
    })
}

fn contribution_substitution_from_storage(
    record: OwnedRecord,
    resolve_table_name: &mut impl FnMut(PhysicalTableId) -> Result<String, Error>,
    resolve_column_name: &mut impl FnMut(&str, PhysicalColumnId) -> Result<String, Error>,
) -> Result<ContributionSubstitution, Error> {
    let record = ContributionSubstitutionStorageRecord::new(record);
    Ok(ContributionSubstitution {
        target: contribution_coordinate_from_storage(
            record.target()?,
            resolve_table_name,
            resolve_column_name,
        )?,
        sources: record
            .sources()?
            .into_iter()
            .map(|source| match source {
                Value::Record(record) => {
                    contribution_dot_from_storage(record, resolve_table_name, resolve_column_name)
                }
                _ => Err(Error::InvalidStoredValue(
                    "transaction contribution dot must be a record",
                )),
            })
            .collect::<Result<_, _>>()?,
    })
}

pub(super) fn contribution_merge_from_storage_record(
    record: OwnedRecord,
    mut resolve_table_name: impl FnMut(PhysicalTableId) -> Result<String, Error>,
    mut resolve_column_name: impl FnMut(&str, PhysicalColumnId) -> Result<String, Error>,
) -> Result<ContributionMergeProvenance, Error> {
    let descriptor = contribution_merge_descriptor();
    if record.descriptor() != descriptor {
        return Err(Error::InvalidStoredValue(
            "transaction contribution provenance must be a record",
        ));
    }
    let record = ContributionMergeStorageRecord::new(record);
    let provenance = ContributionMergeProvenance {
        source: BranchKey::from_canonical_bytes(&record.source()?).map_err(|_| {
            Error::InvalidStoredValue("transaction contribution source branch key is invalid")
        })?,
        target: BranchKey::from_canonical_bytes(&record.target()?).map_err(|_| {
            Error::InvalidStoredValue("transaction contribution target branch key is invalid")
        })?,
        substitutions: record
            .substitutions()?
            .into_iter()
            .map(|substitution| match substitution {
                Value::Record(record) => contribution_substitution_from_storage(
                    record,
                    &mut resolve_table_name,
                    &mut resolve_column_name,
                ),
                _ => Err(Error::InvalidStoredValue(
                    "transaction contribution substitution must be a record",
                )),
            })
            .collect::<Result<_, _>>()?,
        branch_view_copies: record
            .branch_view_copy_v1()?
            .into_iter()
            .map(|value| match value {
                Value::Record(record) => branch_view_copy_from_storage_record(record),
                _ => Err(Error::InvalidStoredValue(
                    "branch-view copy evidence must be a record",
                )),
            })
            .collect::<Result<_, _>>()?,
        branch_write_intents: Vec::new(),
    };
    let branch_write_intents = record
        .branch_write_intent_v1()?
        .into_iter()
        .map(|value| match value {
            Value::Record(record) => {
                branch_write_intent_from_storage_record(record, &provenance.branch_view_copies)
            }
            _ => Err(Error::InvalidStoredValue(
                "branch write intent must be a record",
            )),
        })
        .collect::<Result<_, _>>()?;
    let provenance = ContributionMergeProvenance {
        branch_write_intents,
        ..provenance
    };
    provenance.validate().map_err(|_| {
        Error::InvalidStoredValue("transaction contribution provenance must be canonical")
    })?;
    Ok(provenance)
}

const RESULT_MEMBER_STORAGE_MAGIC: &[u8; 4] = b"JRME";
const RESULT_MEMBER_STORAGE_VERSION: u8 = 1;
// This is a durable key codec, not a wire codec.  Tags below are permanent:
// never renumber or reuse them; introduce a new storage version instead.
const PROGRAM_FACT_STORAGE_MAGIC: &[u8; 4] = b"JPFK";
const PROGRAM_FACT_STORAGE_VERSION: u8 = 1;
const MAX_PROGRAM_FACT_STORAGE_BYTES: usize = 1024 * 1024;
const MAX_PROGRAM_FACT_NESTING: usize = 32;
const RESULT_ROW_SOURCE_STORAGE_MAGIC: &[u8; 4] = b"JRSE";
const RESULT_ROW_SOURCE_STORAGE_VERSION: u8 = 1;
const RESULT_MEMBER_STORAGE_ENVELOPE_HEADER_LEN: usize = 4 + 1 + 4;
const RESULT_MEMBER_ROW_TAG: u32 = 0;
const RESULT_MEMBER_SYNTHETIC_TAG: u32 = 1;
const RESULT_MEMBER_PATH_TUPLE_TAG: u32 = 2;
const RESULT_MEMBER_TYPED_ROW_TAG: u32 = 3;
const RESULT_ROW_SOURCE_CURRENT_TAG: u32 = 0;
const RESULT_ROW_SOURCE_SNAPSHOT_TAG: u32 = 1;
const RESULT_ROW_SOURCE_HISTORY_CUT_TAG: u32 = 2;
const RESULT_ROW_SOURCE_MERGE_TAG: u32 = 3;
const RESULT_ROW_SOURCE_LENS_PROJECTION_TAG: u32 = 4;
const RESULT_ROW_SOURCE_OVERLAY_TAG: u32 = 5;
const MAX_RESULT_MEMBER_STORAGE_BYTES: usize = 1024 * 1024;
const MAX_RESULT_ROW_SOURCE_DEPTH: usize = 32;
const MAX_RESULT_MEMBER_JOINED_SOURCES: usize = 256;
const MAX_RESULT_MEMBER_UNION_ARM_LABEL_BYTES: usize = 4 * 1024;

struct ResultMemberStorageLayout {
    member_envelope: records::RecordDescriptor,
    member_schema: records::EnumSchema,
    source_envelope: records::RecordDescriptor,
    source_schema: records::EnumSchema,
    occurrence: records::RecordDescriptor,
    union_arm: records::RecordDescriptor,
    real_row: records::RecordDescriptor,
    settled_value: records::RecordDescriptor,
}

fn result_member_storage_layout() -> &'static ResultMemberStorageLayout {
    static LAYOUT: std::sync::LazyLock<ResultMemberStorageLayout> =
        std::sync::LazyLock::new(|| {
            let tx_id =
                records::ValueType::Tuple(vec![records::ValueType::U64, records::ValueType::Uuid]);
            let union_arm = records::RecordDescriptor::new([
                ("position", records::ValueType::U32),
                ("label", records::ValueType::String),
            ]);
            let occurrence = records::RecordDescriptor::new([
                ("root", records::ValueType::Uuid),
                (
                    "joined",
                    records::ValueType::Array(Box::new(records::ValueType::Uuid)),
                ),
                (
                    "union_arms",
                    records::ValueType::Array(Box::new(records::ValueType::Record(Box::new(
                        union_arm,
                    )))),
                ),
            ]);
            let snapshot_source = records::RecordDescriptor::new([
                ("owner", records::ValueType::Uuid),
                ("global_base", records::ValueType::U64),
                ("local_base", records::ValueType::U64),
                ("dots", records::ValueType::Array(Box::new(tx_id.clone()))),
            ]);
            let history_cut_source =
                records::RecordDescriptor::new([("global_time", records::ValueType::U64)]);
            let merge_source = records::RecordDescriptor::new([(
                "inputs",
                records::ValueType::Array(Box::new(records::ValueType::Bytes)),
            )]);
            let lens_source = records::RecordDescriptor::new([
                ("schema_version", records::ValueType::Uuid),
                ("base", records::ValueType::Bytes),
            ]);
            let overlay_source = records::RecordDescriptor::new([
                ("tx", tx_id.clone()),
                ("base", records::ValueType::Bytes),
            ]);
            let source_cases = [
                (
                    RESULT_ROW_SOURCE_CURRENT_TAG,
                    records::EnumCase::new(
                        "Current",
                        records::RecordDescriptor::new(std::iter::empty::<(
                            &'static str,
                            records::ValueType,
                        )>()),
                    ),
                ),
                (
                    RESULT_ROW_SOURCE_SNAPSHOT_TAG,
                    records::EnumCase::new("Snapshot", snapshot_source),
                ),
                (
                    RESULT_ROW_SOURCE_HISTORY_CUT_TAG,
                    records::EnumCase::new("HistoryCut", history_cut_source),
                ),
                (
                    RESULT_ROW_SOURCE_MERGE_TAG,
                    records::EnumCase::new("Merge", merge_source),
                ),
                (
                    RESULT_ROW_SOURCE_LENS_PROJECTION_TAG,
                    records::EnumCase::new("LensProjection", lens_source),
                ),
                (
                    RESULT_ROW_SOURCE_OVERLAY_TAG,
                    records::EnumCase::new("Overlay", overlay_source),
                ),
            ];
            debug_assert!(
                source_cases
                    .iter()
                    .enumerate()
                    .all(|(index, (tag, _))| usize::try_from(*tag) == Ok(index))
            );
            let source_schema = records::EnumSchema::new(
                "jazz.internal.result_row_source.v1",
                source_cases.map(|(_, case)| case),
            )
            .expect("fixed result-row-source storage schema is valid");
            let source_envelope = records::RecordDescriptor::new([(
                "source",
                records::ValueType::Enum(Box::new(source_schema.clone())),
            )]);

            let layer = records::ScalarEnumSchema::new(
                "jazz.internal.result_row_layer.v1",
                ["Content", "Deletion", "ContentOrDeletion"],
            )
            .expect("fixed result-row-layer storage schema is valid");
            debug_assert_eq!(
                [
                    ResultRowLayer::Content.discriminant(),
                    ResultRowLayer::Deletion.discriminant(),
                    ResultRowLayer::ContentOrDeletion.discriminant(),
                ],
                [0, 1, 2]
            );
            let real_row = records::RecordDescriptor::new([
                ("table", records::ValueType::String),
                ("row_uuid", records::ValueType::Uuid),
                (
                    "occurrence_id",
                    records::ValueType::Nullable(Box::new(records::ValueType::Record(Box::new(
                        occurrence,
                    )))),
                ),
                (
                    "content_tx",
                    records::ValueType::Nullable(Box::new(tx_id.clone())),
                ),
                ("layer", records::ValueType::EnumTag(layer)),
                (
                    "deletion_tx",
                    records::ValueType::Nullable(Box::new(tx_id.clone())),
                ),
                (
                    "source",
                    records::ValueType::Enum(Box::new(source_schema.clone())),
                ),
                ("read_view", records::ValueType::Uuid),
                (
                    "schema_version",
                    records::ValueType::Nullable(Box::new(records::ValueType::Uuid)),
                ),
                (
                    "branch_or_prefix",
                    records::ValueType::Nullable(Box::new(records::ValueType::Bytes)),
                ),
                (
                    "row_digest",
                    records::ValueType::Nullable(Box::new(records::ValueType::Bytes)),
                ),
                ("batch", records::ValueType::Nullable(Box::new(tx_id))),
                (
                    "settle_position",
                    records::ValueType::Nullable(Box::new(records::ValueType::U64)),
                ),
            ]);
            let synthetic = records::RecordDescriptor::new([
                ("table", records::ValueType::String),
                ("row", records::ValueType::Bytes),
                ("replacement", records::ValueType::Bytes),
            ]);
            let path_tuple = records::RecordDescriptor::new([
                ("path", records::ValueType::String),
                ("source_table", records::ValueType::String),
                ("source_row", records::ValueType::Uuid),
                ("target_table", records::ValueType::String),
                ("target_row", records::ValueType::Uuid),
                (
                    "edge_id",
                    records::ValueType::Nullable(Box::new(records::ValueType::Bytes)),
                ),
                ("revision", records::ValueType::Bytes),
            ]);
            let typed_row = records::RecordDescriptor::new([
                ("row", records::ValueType::Record(Box::new(real_row))),
                (
                    "occurrence_key",
                    records::ValueType::Record(Box::new(occurrence)),
                ),
            ]);
            let member_cases = [
                (
                    RESULT_MEMBER_ROW_TAG,
                    records::EnumCase::new("Row", real_row),
                ),
                (
                    RESULT_MEMBER_SYNTHETIC_TAG,
                    records::EnumCase::new("Synthetic", synthetic),
                ),
                (
                    RESULT_MEMBER_PATH_TUPLE_TAG,
                    records::EnumCase::new("PathTuple", path_tuple),
                ),
                (
                    RESULT_MEMBER_TYPED_ROW_TAG,
                    records::EnumCase::new("TypedRow", typed_row),
                ),
            ];
            debug_assert!(
                member_cases
                    .iter()
                    .enumerate()
                    .all(|(index, (tag, _))| usize::try_from(*tag) == Ok(index))
            );
            let member_schema = records::EnumSchema::new(
                "jazz.internal.result_member.v1",
                member_cases.map(|(_, case)| case),
            )
            .expect("fixed result-member storage schema is valid");
            let member_envelope = records::RecordDescriptor::new([(
                "member",
                records::ValueType::Enum(Box::new(member_schema.clone())),
            )]);
            // The descriptor and value payloads are each normal canonical
            // Groove encodings. Keeping them as fields of a fixed record
            // avoids smuggling a serde/postcard representation into a durable
            // result-member key while retaining their dynamic type boundary.
            let settled_value = records::RecordDescriptor::new([
                ("descriptor", records::ValueType::Bytes),
                ("value", records::ValueType::Bytes),
            ]);
            ResultMemberStorageLayout {
                member_envelope,
                member_schema,
                source_envelope,
                source_schema,
                occurrence,
                union_arm,
                real_row,
                settled_value,
            }
        });
    &LAYOUT
}

fn encode_result_member_envelope(
    magic: &[u8; 4],
    version: u8,
    descriptor: records::RecordDescriptor,
    value: records::EnumValue,
) -> Result<Vec<u8>, Error> {
    let payload = descriptor.create(&[Value::Enum(value)])?;
    let total_len = magic
        .len()
        .checked_add(1)
        .and_then(|len| len.checked_add(4))
        .and_then(|len| len.checked_add(payload.len()))
        .ok_or(Error::InvalidStoredValue(
            "settled result member encoding is too large",
        ))?;
    if total_len > MAX_RESULT_MEMBER_STORAGE_BYTES {
        return Err(Error::InvalidStoredValue(
            "settled result member encoding is too large",
        ));
    }
    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(magic);
    encoded.push(version);
    encoded.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| Error::InvalidStoredValue("settled result member encoding is too large"))?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_result_member_envelope(
    encoded: &[u8],
    magic: &[u8; 4],
    version: u8,
    descriptor: records::RecordDescriptor,
    context: &'static str,
) -> Result<records::EnumValue, Error> {
    if encoded.len() > MAX_RESULT_MEMBER_STORAGE_BYTES {
        return Err(Error::InvalidStoredValue(context));
    }
    let Some((header, payload)) =
        encoded.split_at_checked(RESULT_MEMBER_STORAGE_ENVELOPE_HEADER_LEN)
    else {
        return Err(Error::InvalidStoredValue(context));
    };
    if &header[..magic.len()] != magic || header[magic.len()] != version {
        return Err(Error::InvalidStoredValue(context));
    }
    let payload_len = u32::from_le_bytes(
        header[magic.len() + 1..RESULT_MEMBER_STORAGE_ENVELOPE_HEADER_LEN]
            .try_into()
            .expect("result-member payload length has fixed width"),
    );
    if usize::try_from(payload_len) != Ok(payload.len()) {
        return Err(Error::InvalidStoredValue(context));
    }
    let values = descriptor.bind(payload).to_values()?;
    let [Value::Enum(value)] = values.as_slice() else {
        return Err(Error::InvalidStoredValue(context));
    };
    if descriptor.create(&values)? != payload {
        return Err(Error::InvalidStoredValue(context));
    }
    Ok(value.clone())
}

/// Encode one dynamically typed synthetic identity value as a fixed outer
/// Groove record. The nested descriptor is itself a canonical Groove record
/// descriptor, and the nested bytes are a record under that exact descriptor.
/// There is deliberately no serde/postcard payload in settled keys.
pub(super) fn settled_result_value_storage_bytes(
    value: &Value,
    value_type: &records::ValueType,
) -> Result<Vec<u8>, Error> {
    let value_descriptor = records::RecordDescriptor::new([("value", value_type.clone())]);
    let descriptor = records::encode_record_descriptor(&value_descriptor)?;
    let value = value_descriptor.create(std::slice::from_ref(value))?;
    let layout = result_member_storage_layout();
    let encoded = layout
        .settled_value
        .create(&[Value::Bytes(descriptor), Value::Bytes(value)])?;
    if encoded.len() > MAX_RESULT_MEMBER_STORAGE_BYTES {
        return Err(Error::InvalidStoredValue(
            "settled result value encoding is too large",
        ));
    }
    Ok(encoded)
}

fn validate_settled_result_value_storage_bytes(encoded: &[u8]) -> Result<(), Error> {
    if encoded.len() > MAX_RESULT_MEMBER_STORAGE_BYTES {
        return Err(Error::InvalidStoredValue(
            "settled result value encoding is too large",
        ));
    }
    let layout = result_member_storage_layout();
    let values = layout.settled_value.bind(encoded).to_values()?;
    if layout.settled_value.create(&values)? != encoded {
        return Err(Error::InvalidStoredValue(
            "settled result value encoding is not canonical",
        ));
    }
    let [Value::Bytes(descriptor), Value::Bytes(value)] = values.as_slice() else {
        return Err(Error::InvalidStoredValue(
            "settled result value encoding is invalid",
        ));
    };
    let descriptor = records::decode_record_descriptor(descriptor)?;
    let [field] = descriptor.fields() else {
        return Err(Error::InvalidStoredValue(
            "settled result value descriptor is invalid",
        ));
    };
    if field.name.as_deref() != Some("value") {
        return Err(Error::InvalidStoredValue(
            "settled result value descriptor is invalid",
        ));
    }
    let decoded = descriptor.bind(value).to_values()?;
    if descriptor.create(&decoded)? != *value {
        return Err(Error::InvalidStoredValue(
            "settled result value is not canonical",
        ));
    }
    if settled_result_value_storage_bytes(&decoded[0], &field.value_type)? != encoded {
        return Err(Error::InvalidStoredValue(
            "settled result value encoding is not canonical",
        ));
    }
    Ok(())
}

fn result_member_case_descriptor(
    schema: &records::EnumSchema,
    tag: u32,
) -> Result<records::RecordDescriptor, Error> {
    schema
        .case(tag)
        .map(|case| case.payload)
        .map_err(|_| Error::InvalidStoredValue("settled result member tag is invalid"))
}

fn result_member_occurrence_storage_record(
    occurrence: &OutputOccurrenceId,
) -> Result<OwnedRecord, Error> {
    let layout = result_member_storage_layout();
    if occurrence.joined_sources().len() > MAX_RESULT_MEMBER_JOINED_SOURCES {
        return Err(Error::InvalidStoredValue(
            "settled result member has too many joined sources",
        ));
    }
    let mut previous_position = None;
    let mut union_arms = Vec::with_capacity(occurrence.union_arms().len());
    for (position, label) in occurrence.union_arms() {
        if *position >= occurrence.joined_sources().len()
            || label.is_empty()
            || label.len() > MAX_RESULT_MEMBER_UNION_ARM_LABEL_BYTES
            || previous_position.is_some_and(|previous| previous >= *position)
        {
            return Err(Error::InvalidStoredValue(
                "settled result member occurrence is not canonical",
            ));
        }
        previous_position = Some(*position);
        let position = u32::try_from(*position).map_err(|_| {
            Error::InvalidStoredValue("settled result member occurrence position is too large")
        })?;
        union_arms.push(Value::Record(
            ResultMemberUnionArmStorageRecord::encode(&layout.union_arm, position, label.clone())?
                .record()
                .clone(),
        ));
    }
    Ok(ResultMemberOccurrenceStorageRecord::encode(
        &layout.occurrence,
        *occurrence.root_source().uuid(),
        occurrence
            .joined_sources()
            .iter()
            .map(|source| Value::Uuid(*source.uuid()))
            .collect(),
        union_arms,
    )?
    .record()
    .clone())
}

fn result_member_occurrence_from_storage_record(
    record: OwnedRecord,
) -> Result<OutputOccurrenceId, Error> {
    let layout = result_member_storage_layout();
    if record.descriptor() != &layout.occurrence {
        return Err(Error::InvalidStoredValue(
            "settled result member occurrence descriptor is invalid",
        ));
    }
    let record = ResultMemberOccurrenceStorageRecord::new(record);
    let joined = record.joined()?;
    if joined.len() > MAX_RESULT_MEMBER_JOINED_SOURCES {
        return Err(Error::InvalidStoredValue(
            "settled result member has too many joined sources",
        ));
    }
    let joined = joined
        .into_iter()
        .map(|value| match value {
            Value::Uuid(value) => Ok(ObjectId::from_uuid(value)),
            _ => Err(Error::InvalidStoredValue(
                "settled result member joined source must be a UUID",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut previous_position = None;
    let mut union_arms = Vec::new();
    for value in record.union_arms()? {
        let Value::Record(arm) = value else {
            return Err(Error::InvalidStoredValue(
                "settled result member union arm must be a record",
            ));
        };
        if arm.descriptor() != &layout.union_arm {
            return Err(Error::InvalidStoredValue(
                "settled result member union arm descriptor is invalid",
            ));
        }
        let arm = ResultMemberUnionArmStorageRecord::new(arm);
        let position = usize::try_from(arm.position()?).map_err(|_| {
            Error::InvalidStoredValue("settled result member occurrence position is too large")
        })?;
        let label = arm.label()?;
        if position >= joined.len()
            || label.is_empty()
            || label.len() > MAX_RESULT_MEMBER_UNION_ARM_LABEL_BYTES
            || previous_position.is_some_and(|previous| previous >= position)
        {
            return Err(Error::InvalidStoredValue(
                "settled result member occurrence is not canonical",
            ));
        }
        previous_position = Some(position);
        union_arms.push((position, label));
    }
    let root = ObjectId::from_uuid(record.root()?);
    if union_arms.is_empty() {
        Ok(OutputOccurrenceId::new(root, joined))
    } else {
        OutputOccurrenceId::with_union_arms(root, joined, union_arms).ok_or(
            Error::InvalidStoredValue("settled result member occurrence is invalid"),
        )
    }
}

fn result_row_source_storage_value(
    source: &ResultRowSource,
    depth: usize,
) -> Result<records::EnumValue, Error> {
    if depth > MAX_RESULT_ROW_SOURCE_DEPTH {
        return Err(Error::InvalidStoredValue(
            "settled result member source is too deeply nested",
        ));
    }
    let layout = result_member_storage_layout();
    let (tag, record) = match source {
        ResultRowSource::Current => {
            let payload = result_member_case_descriptor(
                &layout.source_schema,
                RESULT_ROW_SOURCE_CURRENT_TAG,
            )?;
            (
                RESULT_ROW_SOURCE_CURRENT_TAG,
                OwnedRecord::new(payload.create(&[])?, payload),
            )
        }
        ResultRowSource::Snapshot { snapshot } => {
            let tag = RESULT_ROW_SOURCE_SNAPSHOT_TAG;
            let payload = result_member_case_descriptor(&layout.source_schema, tag)?;
            let record = ResultMemberSnapshotSourceStorageRecord::encode(
                &payload,
                snapshot.owner.0,
                snapshot.global_base.0,
                snapshot.local_base.0,
                snapshot.dots.iter().copied().map(tx_id_value).collect(),
            )?
            .record()
            .clone();
            (tag, record)
        }
        ResultRowSource::HistoryCut { global_time } => {
            let tag = RESULT_ROW_SOURCE_HISTORY_CUT_TAG;
            let payload = result_member_case_descriptor(&layout.source_schema, tag)?;
            let record =
                ResultMemberHistoryCutSourceStorageRecord::encode(&payload, global_time.0)?
                    .record()
                    .clone();
            (tag, record)
        }
        ResultRowSource::Merge { inputs } => {
            let tag = RESULT_ROW_SOURCE_MERGE_TAG;
            let payload = result_member_case_descriptor(&layout.source_schema, tag)?;
            let inputs = inputs
                .iter()
                .map(|input| result_row_source_storage_bytes(input, depth + 1).map(Value::Bytes))
                .collect::<Result<Vec<_>, _>>()?;
            let record = ResultMemberMergeSourceStorageRecord::encode(&payload, inputs)?
                .record()
                .clone();
            (tag, record)
        }
        ResultRowSource::LensProjection {
            schema_version,
            base,
        } => {
            let tag = RESULT_ROW_SOURCE_LENS_PROJECTION_TAG;
            let payload = result_member_case_descriptor(&layout.source_schema, tag)?;
            let record = ResultMemberLensSourceStorageRecord::encode(
                &payload,
                schema_version.0,
                result_row_source_storage_bytes(base, depth + 1)?,
            )?
            .record()
            .clone();
            (tag, record)
        }
        ResultRowSource::Overlay { tx, base } => {
            let tag = RESULT_ROW_SOURCE_OVERLAY_TAG;
            let payload = result_member_case_descriptor(&layout.source_schema, tag)?;
            let record = ResultMemberOverlaySourceStorageRecord::encode(
                &payload,
                tx_id_value(*tx),
                result_row_source_storage_bytes(base, depth + 1)?,
            )?
            .record()
            .clone();
            (tag, record)
        }
    };
    Ok(records::EnumValue::new(tag, record))
}

fn result_row_source_storage_bytes(
    source: &ResultRowSource,
    depth: usize,
) -> Result<Vec<u8>, Error> {
    let layout = result_member_storage_layout();
    encode_result_member_envelope(
        RESULT_ROW_SOURCE_STORAGE_MAGIC,
        RESULT_ROW_SOURCE_STORAGE_VERSION,
        layout.source_envelope,
        result_row_source_storage_value(source, depth)?,
    )
}

fn result_row_source_from_storage_value(
    value: records::EnumValue,
    depth: usize,
) -> Result<ResultRowSource, Error> {
    if depth > MAX_RESULT_ROW_SOURCE_DEPTH {
        return Err(Error::InvalidStoredValue(
            "settled result member source is too deeply nested",
        ));
    }
    let tag = value.tag();
    let record = value.into_record();
    match tag {
        RESULT_ROW_SOURCE_CURRENT_TAG => {
            record.to_values()?;
            if !record.descriptor().fields().is_empty() {
                return Err(Error::InvalidStoredValue(
                    "settled result member current source is invalid",
                ));
            }
            Ok(ResultRowSource::Current)
        }
        RESULT_ROW_SOURCE_SNAPSHOT_TAG => {
            let record = ResultMemberSnapshotSourceStorageRecord::new(record);
            let dots = record.dots()?;
            Ok(ResultRowSource::Snapshot {
                snapshot: SnapshotRef {
                    owner: NodeUuid(record.owner()?),
                    global_base: GlobalTime(record.global_base()?),
                    local_base: TxTime(record.local_base()?),
                    dots: dots
                        .into_iter()
                        .map(tx_id_from_value)
                        .collect::<Result<Vec<_>, _>>()?,
                },
            })
        }
        RESULT_ROW_SOURCE_HISTORY_CUT_TAG => {
            let record = ResultMemberHistoryCutSourceStorageRecord::new(record);
            Ok(ResultRowSource::HistoryCut {
                global_time: GlobalTime(record.global_time()?),
            })
        }
        RESULT_ROW_SOURCE_MERGE_TAG => {
            let record = ResultMemberMergeSourceStorageRecord::new(record);
            let inputs = record.inputs()?;
            Ok(ResultRowSource::Merge {
                inputs: inputs
                    .into_iter()
                    .map(|input| match input {
                        Value::Bytes(bytes) => {
                            result_row_source_from_storage_bytes(&bytes, depth + 1)
                        }
                        _ => Err(Error::InvalidStoredValue(
                            "settled result member merge input must be bytes",
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        RESULT_ROW_SOURCE_LENS_PROJECTION_TAG => {
            let record = ResultMemberLensSourceStorageRecord::new(record);
            Ok(ResultRowSource::LensProjection {
                schema_version: SchemaVersionId(record.schema_version()?),
                base: Box::new(result_row_source_from_storage_bytes(
                    &record.base()?,
                    depth + 1,
                )?),
            })
        }
        RESULT_ROW_SOURCE_OVERLAY_TAG => {
            let record = ResultMemberOverlaySourceStorageRecord::new(record);
            Ok(ResultRowSource::Overlay {
                tx: tx_id_from_value(record.tx()?)?,
                base: Box::new(result_row_source_from_storage_bytes(
                    &record.base()?,
                    depth + 1,
                )?),
            })
        }
        _ => Err(Error::InvalidStoredValue(
            "settled result member source tag is invalid",
        )),
    }
}

fn result_row_source_from_storage_bytes(
    encoded: &[u8],
    depth: usize,
) -> Result<ResultRowSource, Error> {
    let layout = result_member_storage_layout();
    let value = decode_result_member_envelope(
        encoded,
        RESULT_ROW_SOURCE_STORAGE_MAGIC,
        RESULT_ROW_SOURCE_STORAGE_VERSION,
        layout.source_envelope,
        "settled result member source encoding is invalid",
    )?;
    let source = result_row_source_from_storage_value(value, depth)?;
    if result_row_source_storage_bytes(&source, depth)? != encoded {
        return Err(Error::InvalidStoredValue(
            "settled result member source encoding is not canonical",
        ));
    }
    Ok(source)
}

fn result_member_real_row_storage_record(row: &RealRowMemberEntry) -> Result<OwnedRecord, Error> {
    let layout = result_member_storage_layout();
    Ok(ResultMemberRealRowStorageRecord::encode(
        &layout.real_row,
        row.table.as_str().to_owned(),
        row.row_uuid.0,
        row.occurrence_id
            .as_ref()
            .map(result_member_occurrence_storage_record)
            .transpose()?,
        row.content_tx.map(tx_id_value),
        row.layer,
        row.deletion_tx.map(tx_id_value),
        result_row_source_storage_value(&row.source, 0)?,
        row.read_view.id,
        row.schema_version.map(|value| value.0),
        row.branch_or_prefix.clone(),
        row.row_digest.clone(),
        row.batch.map(tx_id_value),
        row.settle_position.map(|value| value.0),
    )?
    .record()
    .clone())
}

fn result_member_real_row_from_storage_record(
    record: OwnedRecord,
) -> Result<RealRowMemberEntry, Error> {
    let layout = result_member_storage_layout();
    if record.descriptor() != &layout.real_row {
        return Err(Error::InvalidStoredValue(
            "settled real-row member descriptor is invalid",
        ));
    }
    let record = ResultMemberRealRowStorageRecord::new(record);
    Ok(RealRowMemberEntry {
        table: record.table()?.into(),
        row_uuid: RowUuid(record.row_uuid()?),
        occurrence_id: record
            .occurrence_id()?
            .map(result_member_occurrence_from_storage_record)
            .transpose()?,
        content_tx: record.content_tx()?.map(tx_id_from_value).transpose()?,
        layer: record.layer()?,
        deletion_tx: record.deletion_tx()?.map(tx_id_from_value).transpose()?,
        source: result_row_source_from_storage_value(record.source()?, 0)?,
        read_view: ReadViewKey {
            id: record.read_view()?,
        },
        schema_version: record.schema_version()?.map(SchemaVersionId),
        branch_or_prefix: record.branch_or_prefix()?,
        row_digest: record.row_digest()?,
        batch: record.batch()?.map(tx_id_from_value).transpose()?,
        settle_position: record.settle_position()?.map(GlobalTime),
    })
}

fn result_member_storage_value(member: &ResultMemberEntry) -> Result<records::EnumValue, Error> {
    let layout = result_member_storage_layout();
    let (tag, record) = match member {
        ResultMemberEntry::Row(row) => (
            RESULT_MEMBER_ROW_TAG,
            result_member_real_row_storage_record(row)?,
        ),
        ResultMemberEntry::Synthetic {
            table,
            row,
            replacement,
        } => {
            validate_settled_result_value_storage_bytes(row)?;
            validate_settled_result_value_storage_bytes(replacement.encoded_record())?;
            let tag = RESULT_MEMBER_SYNTHETIC_TAG;
            let payload = result_member_case_descriptor(&layout.member_schema, tag)?;
            let record = ResultMemberSyntheticStorageRecord::encode(
                &payload,
                table.clone(),
                row.clone(),
                replacement.encoded_record().to_vec(),
            )?
            .record()
            .clone();
            (tag, record)
        }
        ResultMemberEntry::PathTuple {
            path,
            source_table,
            source_row,
            target_table,
            target_row,
            edge_id,
            revision,
        } => {
            let tag = RESULT_MEMBER_PATH_TUPLE_TAG;
            let payload = result_member_case_descriptor(&layout.member_schema, tag)?;
            let record = ResultMemberPathTupleStorageRecord::encode(
                &payload,
                path.clone(),
                source_table.as_str().to_owned(),
                source_row.0,
                target_table.as_str().to_owned(),
                target_row.0,
                edge_id.clone(),
                revision.clone(),
            )?
            .record()
            .clone();
            (tag, record)
        }
        ResultMemberEntry::TypedRow {
            row,
            occurrence_key,
        } => {
            let tag = RESULT_MEMBER_TYPED_ROW_TAG;
            let payload = result_member_case_descriptor(&layout.member_schema, tag)?;
            let record = ResultMemberTypedRowStorageRecord::encode(
                &payload,
                result_member_real_row_storage_record(row)?,
                result_member_occurrence_storage_record(occurrence_key.as_occurrence())?,
            )?
            .record()
            .clone();
            (tag, record)
        }
    };
    Ok(records::EnumValue::new(tag, record))
}

pub(super) fn result_member_storage_bytes(member: &ResultMemberEntry) -> Result<Vec<u8>, Error> {
    let layout = result_member_storage_layout();
    encode_result_member_envelope(
        RESULT_MEMBER_STORAGE_MAGIC,
        RESULT_MEMBER_STORAGE_VERSION,
        layout.member_envelope,
        result_member_storage_value(member)?,
    )
}

pub(super) fn result_member_from_storage_bytes(encoded: &[u8]) -> Result<ResultMemberEntry, Error> {
    let layout = result_member_storage_layout();
    let value = decode_result_member_envelope(
        encoded,
        RESULT_MEMBER_STORAGE_MAGIC,
        RESULT_MEMBER_STORAGE_VERSION,
        layout.member_envelope,
        "settled result member encoding is invalid",
    )?;
    let tag = value.tag();
    let record = value.into_record();
    let member = match tag {
        RESULT_MEMBER_ROW_TAG => {
            ResultMemberEntry::Row(result_member_real_row_from_storage_record(record)?)
        }
        RESULT_MEMBER_SYNTHETIC_TAG => {
            let record = ResultMemberSyntheticStorageRecord::new(record);
            ResultMemberEntry::Synthetic {
                table: record.table()?,
                row: record.row()?,
                replacement: SyntheticReplacementToken::from_encoded_record(record.replacement()?),
            }
        }
        RESULT_MEMBER_PATH_TUPLE_TAG => {
            let record = ResultMemberPathTupleStorageRecord::new(record);
            ResultMemberEntry::PathTuple {
                path: record.path()?,
                source_table: record.source_table()?.into(),
                source_row: RowUuid(record.source_row()?),
                target_table: record.target_table()?.into(),
                target_row: RowUuid(record.target_row()?),
                edge_id: record.edge_id()?,
                revision: record.revision()?,
            }
        }
        RESULT_MEMBER_TYPED_ROW_TAG => {
            let record = ResultMemberTypedRowStorageRecord::new(record);
            ResultMemberEntry::TypedRow {
                row: result_member_real_row_from_storage_record(record.row()?)?,
                occurrence_key: ResultKey::from_occurrence(
                    result_member_occurrence_from_storage_record(record.occurrence_key()?)?,
                ),
            }
        }
        _ => {
            return Err(Error::InvalidStoredValue(
                "settled result member tag is invalid",
            ));
        }
    };
    if result_member_storage_bytes(&member)? != encoded {
        return Err(Error::InvalidStoredValue(
            "settled result member encoding is not canonical",
        ));
    }
    Ok(member)
}

struct ProgramFactStorageReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ProgramFactStorageReader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(Error::InvalidStoredValue(
                "settled program fact encoding is truncated",
            ))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("length checked"),
        ))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("length checked"),
        ))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, Error> {
        let len = usize::try_from(self.u32()?)
            .map_err(|_| Error::InvalidStoredValue("settled program fact length is too large"))?;
        if len > MAX_PROGRAM_FACT_STORAGE_BYTES {
            return Err(Error::InvalidStoredValue(
                "settled program fact field is too large",
            ));
        }
        Ok(self.take(len)?.to_vec())
    }
    fn string(&mut self) -> Result<String, Error> {
        String::from_utf8(self.bytes()?)
            .map_err(|_| Error::InvalidStoredValue("settled program fact string is invalid UTF-8"))
    }
    fn uuid(&mut self) -> Result<uuid::Uuid, Error> {
        Ok(uuid::Uuid::from_bytes(
            self.take(16)?.try_into().expect("length checked"),
        ))
    }
    fn finish(&self) -> Result<(), Error> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::InvalidStoredValue(
                "settled program fact encoding has trailing bytes",
            ))
        }
    }
}

fn program_fact_put_u32(bytes: &mut Vec<u8>, value: usize) -> Result<(), Error> {
    bytes.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| Error::InvalidStoredValue("settled program fact length is too large"))?
            .to_le_bytes(),
    );
    Ok(())
}
fn program_fact_put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), Error> {
    if value.len() > MAX_PROGRAM_FACT_STORAGE_BYTES {
        return Err(Error::InvalidStoredValue(
            "settled program fact field is too large",
        ));
    }
    program_fact_put_u32(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}
fn program_fact_put_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    program_fact_put_bytes(bytes, value.as_bytes())
}
fn program_fact_put_uuid(bytes: &mut Vec<u8>, value: uuid::Uuid) {
    bytes.extend_from_slice(value.as_bytes());
}
fn program_fact_put_tx(bytes: &mut Vec<u8>, value: TxId) {
    bytes.extend_from_slice(&value.time.0.to_le_bytes());
    program_fact_put_uuid(bytes, value.node.0);
}
fn program_fact_tx(reader: &mut ProgramFactStorageReader<'_>) -> Result<TxId, Error> {
    Ok(TxId::new(TxTime(reader.u64()?), NodeUuid(reader.uuid()?)))
}
fn program_fact_put_option<T>(
    bytes: &mut Vec<u8>,
    value: &Option<T>,
    put: impl FnOnce(&mut Vec<u8>, &T) -> Result<(), Error>,
) -> Result<(), Error> {
    match value {
        None => bytes.push(0),
        Some(value) => {
            bytes.push(1);
            put(bytes, value)?;
        }
    };
    Ok(())
}
fn program_fact_option<T>(
    reader: &mut ProgramFactStorageReader<'_>,
    get: impl FnOnce(&mut ProgramFactStorageReader<'_>) -> Result<T, Error>,
) -> Result<Option<T>, Error> {
    match reader.u8()? {
        0 => Ok(None),
        1 => get(reader).map(Some),
        _ => Err(Error::InvalidStoredValue(
            "settled program fact option tag is invalid",
        )),
    }
}

fn program_fact_put_member(
    bytes: &mut Vec<u8>,
    member: &ResultMemberEntry,
    depth: usize,
) -> Result<(), Error> {
    if depth > MAX_PROGRAM_FACT_NESTING {
        return Err(Error::InvalidStoredValue(
            "settled program fact is too deeply nested",
        ));
    }
    program_fact_put_bytes(bytes, &result_member_storage_bytes(member)?)
}
fn program_fact_member(
    reader: &mut ProgramFactStorageReader<'_>,
    depth: usize,
) -> Result<ResultMemberEntry, Error> {
    if depth > MAX_PROGRAM_FACT_NESTING {
        return Err(Error::InvalidStoredValue(
            "settled program fact is too deeply nested",
        ));
    }
    result_member_from_storage_bytes(&reader.bytes()?)
}
fn program_fact_put_source(bytes: &mut Vec<u8>, value: &ProgramSourceId) -> Result<(), Error> {
    if !value.is_wire_valid() || value.path.len() > MAX_PROGRAM_FACT_NESTING {
        return Err(Error::InvalidStoredValue(
            "settled program fact source identity is invalid",
        ));
    }
    program_fact_put_string(bytes, value.table.as_str())?;
    program_fact_put_u32(bytes, value.path.len())?;
    for role in &value.path {
        match role {
            ProgramSourceRole::Root => bytes.push(0),
            ProgramSourceRole::Alias(name) => {
                bytes.push(1);
                program_fact_put_string(bytes, name)?;
            }
            ProgramSourceRole::RecursiveSeed(name) => {
                bytes.push(2);
                program_fact_put_string(bytes, name)?;
            }
            ProgramSourceRole::RecursiveStep(name) => {
                bytes.push(3);
                program_fact_put_string(bytes, name)?;
            }
            ProgramSourceRole::CorrelatedChild(name) => {
                bytes.push(4);
                program_fact_put_string(bytes, name)?;
            }
            ProgramSourceRole::Policy(name) => {
                bytes.push(5);
                program_fact_put_string(bytes, name)?;
            }
        }
    }
    Ok(())
}
fn program_fact_source(
    reader: &mut ProgramFactStorageReader<'_>,
) -> Result<ProgramSourceId, Error> {
    let table: groove::Intern<String> = reader.string()?.into();
    let len = usize::try_from(reader.u32()?)
        .map_err(|_| Error::InvalidStoredValue("settled program fact source path is too large"))?;
    if len == 0 || len > MAX_PROGRAM_FACT_NESTING {
        return Err(Error::InvalidStoredValue(
            "settled program fact source identity is invalid",
        ));
    }
    let mut path = Vec::with_capacity(len);
    for _ in 0..len {
        path.push(match reader.u8()? {
            0 => ProgramSourceRole::Root,
            1 => ProgramSourceRole::Alias(reader.string()?),
            2 => ProgramSourceRole::RecursiveSeed(reader.string()?),
            3 => ProgramSourceRole::RecursiveStep(reader.string()?),
            4 => ProgramSourceRole::CorrelatedChild(reader.string()?),
            5 => ProgramSourceRole::Policy(reader.string()?),
            _ => {
                return Err(Error::InvalidStoredValue(
                    "settled program fact source role tag is invalid",
                ));
            }
        });
    }
    let source = ProgramSourceId { table, path };
    if !source.is_wire_valid() || source.path.len() > MAX_PROGRAM_FACT_NESTING {
        return Err(Error::InvalidStoredValue(
            "settled program fact source identity is invalid",
        ));
    }
    Ok(source)
}
fn program_fact_put_version(bytes: &mut Vec<u8>, value: &RowVersionRefEntry) -> Result<(), Error> {
    program_fact_put_tx(bytes, value.tx);
    program_fact_put_option(bytes, &value.schema_version, |b, v| {
        program_fact_put_uuid(b, v.0);
        Ok(())
    })?;
    bytes.push(match value.layer {
        ResultRowLayer::Content => 0,
        ResultRowLayer::Deletion => 1,
        ResultRowLayer::ContentOrDeletion => 2,
    });
    program_fact_put_option(bytes, &value.batch, |b, v| {
        program_fact_put_tx(b, *v);
        Ok(())
    })?;
    program_fact_put_option(bytes, &value.branch_or_prefix, |b, v: &Vec<u8>| {
        program_fact_put_bytes(b, v)
    })?;
    program_fact_put_option(bytes, &value.row_digest, |b, v: &Vec<u8>| {
        program_fact_put_bytes(b, v)
    })
}
fn program_fact_version(
    reader: &mut ProgramFactStorageReader<'_>,
) -> Result<RowVersionRefEntry, Error> {
    let tx = program_fact_tx(reader)?;
    let schema_version = program_fact_option(reader, |r| Ok(SchemaVersionId(r.uuid()?)))?;
    let layer = match reader.u8()? {
        0 => ResultRowLayer::Content,
        1 => ResultRowLayer::Deletion,
        2 => ResultRowLayer::ContentOrDeletion,
        _ => {
            return Err(Error::InvalidStoredValue(
                "settled program fact row layer tag is invalid",
            ));
        }
    };
    let batch = program_fact_option(reader, program_fact_tx)?;
    let branch_or_prefix = program_fact_option(reader, |r| r.bytes())?;
    let row_digest = program_fact_option(reader, |r| r.bytes())?;
    Ok(RowVersionRefEntry {
        tx,
        schema_version,
        layer,
        batch,
        branch_or_prefix,
        row_digest,
    })
}
fn program_fact_tier(reader: &mut ProgramFactStorageReader<'_>) -> Result<DurabilityTier, Error> {
    match reader.u8()? {
        0 => Ok(DurabilityTier::None),
        1 => Ok(DurabilityTier::Local),
        2 => Ok(DurabilityTier::Edge),
        3 => Ok(DurabilityTier::Global),
        _ => Err(Error::InvalidStoredValue(
            "settled program fact durability tag is invalid",
        )),
    }
}
fn program_fact_put_tier(bytes: &mut Vec<u8>, value: DurabilityTier) {
    bytes.push(match value {
        DurabilityTier::None => 0,
        DurabilityTier::Local => 1,
        DurabilityTier::Edge => 2,
        DurabilityTier::Global => 3,
    });
}

fn validate_result_member_payload_storage(payload: &ResultMemberPayloadEntry) -> Result<(), Error> {
    let descriptor = records::decode_record_descriptor(&payload.descriptor)
        .map_err(|_| Error::InvalidStoredValue("settled result payload descriptor is invalid"))?;
    if descriptor.fields().iter().any(|field| field.name.is_none()) {
        return Err(Error::InvalidStoredValue(
            "settled result payload descriptor field must be named",
        ));
    }
    let values = descriptor
        .bind(&payload.record)
        .to_values()
        .map_err(|_| Error::InvalidStoredValue("settled result payload record is invalid"))?;
    if descriptor.create(&values)? != payload.record
        || records::encode_record_descriptor(&descriptor)? != payload.descriptor
    {
        return Err(Error::InvalidStoredValue(
            "settled result payload encoding is not canonical",
        ));
    }
    Ok(())
}

/// Explicit versioned canonical codec for settled-program-fact durable keys.
/// Nested result-member values use their own Groove-record codec; no serde
/// representation of `ProgramFactEntry` is durable.
pub(super) fn program_fact_storage_bytes(fact: &ProgramFactEntry) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(PROGRAM_FACT_STORAGE_MAGIC);
    bytes.push(PROGRAM_FACT_STORAGE_VERSION);
    match fact {
        ProgramFactEntry::ResultPayload(v) => {
            validate_result_member_payload_storage(v)?;
            bytes.push(0);
            program_fact_put_member(&mut bytes, &v.member, 1)?;
            program_fact_put_bytes(&mut bytes, &v.descriptor)?;
            program_fact_put_bytes(&mut bytes, &v.record)?;
        }
        ProgramFactEntry::RelationEdge(v) => {
            bytes.push(1);
            program_fact_put_string(&mut bytes, &v.path)?;
            program_fact_put_string(&mut bytes, v.source_table.as_str())?;
            program_fact_put_uuid(&mut bytes, v.source_row.0);
            program_fact_put_string(&mut bytes, v.target_table.as_str())?;
            program_fact_put_uuid(&mut bytes, v.target_row.0);
            program_fact_put_option(&mut bytes, &v.kind, |b, v| {
                b.push(match v {
                    RelationEdgeKind::Include => 0,
                    RelationEdgeKind::Join => 1,
                    RelationEdgeKind::Relation => 2,
                    RelationEdgeKind::Recursive => 3,
                    RelationEdgeKind::Policy => 4,
                });
                Ok(())
            })?;
            program_fact_put_option(&mut bytes, &v.source_version, program_fact_put_version)?;
            program_fact_put_option(&mut bytes, &v.target_version, program_fact_put_version)?;
            program_fact_put_option(&mut bytes, &v.depth, |b, v| {
                b.extend_from_slice(&v.to_le_bytes());
                Ok(())
            })?;
            program_fact_put_option(&mut bytes, &v.edge_id, |b, v| program_fact_put_bytes(b, v))?;
            program_fact_put_option(&mut bytes, &v.branch, |b, v| program_fact_put_bytes(b, v))?;
            program_fact_put_option(&mut bytes, &v.role, |b, v| {
                b.push(match v {
                    RelationEdgeRole::Intermediate => 0,
                    RelationEdgeRole::Frontier => 1,
                    RelationEdgeRole::Terminal => 2,
                });
                Ok(())
            })?;
            program_fact_put_option(&mut bytes, &v.order, |b, v| program_fact_put_bytes(b, v))?;
            program_fact_put_option(&mut bytes, &v.hole_state, |b, v| {
                b.push(match v {
                    PathHoleState::Matched => 0,
                    PathHoleState::Hole => 1,
                });
                Ok(())
            })?;
        }
        ProgramFactEntry::PathCorrelationCoverage(v) => {
            bytes.push(2);
            program_fact_put_string(&mut bytes, &v.path)?;
            program_fact_put_string(&mut bytes, v.source_table.as_str())?;
            program_fact_put_uuid(&mut bytes, v.source_row.0);
            program_fact_put_bytes(&mut bytes, &v.correlation_key)?;
            bytes.push(u8::from(v.complete));
        }
        ProgramFactEntry::ProgramSourceCoverage(v) => {
            bytes.push(3);
            program_fact_put_source(&mut bytes, &v.source)?;
            bytes.push(u8::from(v.complete));
        }
        ProgramFactEntry::ReadFrontierSettled(v) => {
            bytes.push(4);
            program_fact_put_string(&mut bytes, &v.scope)?;
            program_fact_put_tier(&mut bytes, v.tier);
            program_fact_put_option(&mut bytes, &v.stream, |b, v| program_fact_put_string(b, v))?;
            program_fact_put_bytes(&mut bytes, &v.frontier)?;
        }
        ProgramFactEntry::CompleteTxPayloadCoverage(v) => {
            bytes.push(5);
            program_fact_put_tx(&mut bytes, v.tx);
            program_fact_put_tier(&mut bytes, v.tier);
            program_fact_put_bytes(&mut bytes, &v.payload_digest)?;
        }
        ProgramFactEntry::ViewCompleteExclusiveCoverage(v) => {
            bytes.push(6);
            program_fact_put_tx(&mut bytes, v.tx);
            program_fact_put_string(&mut bytes, &v.scope)?;
            program_fact_put_option(&mut bytes, &v.result, |b, v| {
                program_fact_put_member(b, v, 1)
            })?;
            program_fact_put_tier(&mut bytes, v.tier);
            program_fact_put_bytes(&mut bytes, &v.covered_members_digest)?;
        }
        ProgramFactEntry::PolicyDecision(v) => {
            bytes.push(7);
            program_fact_put_bytes(&mut bytes, &v.decision)?;
            match &v.outcome {
                PolicyDecisionOutcomeEntry::Allowed => bytes.push(0),
                PolicyDecisionOutcomeEntry::Denied => bytes.push(1),
                PolicyDecisionOutcomeEntry::IndeterminateRequiresInput { input } => {
                    bytes.push(2);
                    program_fact_put_string(&mut bytes, input)?
                }
                PolicyDecisionOutcomeEntry::RequiresCoverage { scope, frontier } => {
                    bytes.push(3);
                    program_fact_put_string(&mut bytes, scope)?;
                    program_fact_put_bytes(&mut bytes, frontier)?
                }
            };
            program_fact_put_option(&mut bytes, &v.reason, |b, v| program_fact_put_string(b, v))?;
        }
        ProgramFactEntry::VersionWitness(v) => {
            bytes.push(8);
            program_fact_put_string(&mut bytes, &v.role)?;
            program_fact_put_version(&mut bytes, &v.version)?;
            program_fact_put_option(&mut bytes, &v.member, |b, v| {
                program_fact_put_member(b, v, 1)
            })?;
        }
        ProgramFactEntry::CoveredInput(v) => {
            if !v.is_wire_valid() {
                return Err(Error::InvalidStoredValue(
                    "settled covered input identity is invalid",
                ));
            }
            bytes.push(14);
            program_fact_put_source(&mut bytes, &v.source)?;
            program_fact_put_string(&mut bytes, v.version_table.as_str())?;
            program_fact_put_uuid(&mut bytes, v.source_row.0);
            program_fact_put_version(&mut bytes, &v.version)?;
        }
        ProgramFactEntry::PolicyWitness(v) => {
            bytes.push(9);
            program_fact_put_member(&mut bytes, &v.protected, 1)?;
            program_fact_put_string(&mut bytes, &v.policy_path)?;
            program_fact_put_version(&mut bytes, &v.witness)?;
            program_fact_put_option(&mut bytes, &v.edge_kind, |b, v| {
                b.push(match v {
                    RelationEdgeKind::Include => 0,
                    RelationEdgeKind::Join => 1,
                    RelationEdgeKind::Relation => 2,
                    RelationEdgeKind::Recursive => 3,
                    RelationEdgeKind::Policy => 4,
                });
                Ok(())
            })?;
        }
        ProgramFactEntry::ContributingMembers(v) => {
            bytes.push(10);
            program_fact_put_member(&mut bytes, &v.result, 1)?;
            program_fact_put_member(&mut bytes, &v.contributor, 1)?;
            program_fact_put_option(&mut bytes, &v.batch, |b, v| {
                program_fact_put_tx(b, *v);
                Ok(())
            })?;
            program_fact_put_option(&mut bytes, &v.role, |b, v| program_fact_put_string(b, v))?;
        }
        ProgramFactEntry::PredicateRead(v) => {
            bytes.push(11);
            bytes.push(match v.role {
                PredicateOutputSetRoleEntry::Base => 0,
                PredicateOutputSetRoleEntry::Now => 1,
            });
            program_fact_put_uuid(&mut bytes, v.shape_id.0);
            program_fact_put_uuid(&mut bytes, v.binding_id.0);
            program_fact_put_bytes(&mut bytes, &v.predicate)?;
            program_fact_put_bytes(&mut bytes, &v.frontier)?;
        }
        ProgramFactEntry::PredicateOutputSet(v) => {
            bytes.push(12);
            bytes.push(match v.role {
                PredicateOutputSetRoleEntry::Base => 0,
                PredicateOutputSetRoleEntry::Now => 1,
            });
            program_fact_put_string(&mut bytes, v.table.as_str())?;
            program_fact_put_uuid(&mut bytes, v.row.0);
            program_fact_put_version(&mut bytes, &v.version)?;
            program_fact_put_uuid(&mut bytes, v.shape_id.0);
            program_fact_put_uuid(&mut bytes, v.binding_id.0);
        }
        ProgramFactEntry::PointRead(v) => {
            bytes.push(13);
            bytes.push(u8::from(v.present));
            program_fact_put_string(&mut bytes, v.table.as_str())?;
            program_fact_put_uuid(&mut bytes, v.row.0);
            program_fact_put_option(&mut bytes, &v.version, program_fact_put_version)?;
            program_fact_put_uuid(&mut bytes, v.shape_id.0);
            program_fact_put_uuid(&mut bytes, v.binding_id.0);
        }
    }
    if bytes.len() > MAX_PROGRAM_FACT_STORAGE_BYTES {
        return Err(Error::InvalidStoredValue(
            "settled program fact is too large",
        ));
    }
    Ok(bytes)
}

pub(super) fn program_fact_from_storage_bytes(encoded: &[u8]) -> Result<ProgramFactEntry, Error> {
    if encoded.len() > MAX_PROGRAM_FACT_STORAGE_BYTES
        || encoded.len() < 6
        || &encoded[..4] != PROGRAM_FACT_STORAGE_MAGIC
        || encoded[4] != PROGRAM_FACT_STORAGE_VERSION
    {
        return Err(Error::InvalidStoredValue(
            "settled program fact encoding is invalid or unsupported",
        ));
    }
    let mut r = ProgramFactStorageReader {
        bytes: encoded,
        offset: 5,
    };
    let tag = r.u8()?;
    let role =
        |r: &mut ProgramFactStorageReader<'_>| -> Result<PredicateOutputSetRoleEntry, Error> {
            match r.u8()? {
                0 => Ok(PredicateOutputSetRoleEntry::Base),
                1 => Ok(PredicateOutputSetRoleEntry::Now),
                _ => Err(Error::InvalidStoredValue(
                    "settled program fact role tag is invalid",
                )),
            }
        };
    let kind = |r: &mut ProgramFactStorageReader<'_>| -> Result<RelationEdgeKind, Error> {
        match r.u8()? {
            0 => Ok(RelationEdgeKind::Include),
            1 => Ok(RelationEdgeKind::Join),
            2 => Ok(RelationEdgeKind::Relation),
            3 => Ok(RelationEdgeKind::Recursive),
            4 => Ok(RelationEdgeKind::Policy),
            _ => Err(Error::InvalidStoredValue(
                "settled program fact edge kind tag is invalid",
            )),
        }
    };
    let edge_role = |r: &mut ProgramFactStorageReader<'_>| -> Result<RelationEdgeRole, Error> {
        match r.u8()? {
            0 => Ok(RelationEdgeRole::Intermediate),
            1 => Ok(RelationEdgeRole::Frontier),
            2 => Ok(RelationEdgeRole::Terminal),
            _ => Err(Error::InvalidStoredValue(
                "settled program fact edge role tag is invalid",
            )),
        }
    };
    let fact = match tag {
        0 => ProgramFactEntry::ResultPayload(ResultMemberPayloadEntry {
            member: program_fact_member(&mut r, 1)?,
            descriptor: r.bytes()?,
            record: r.bytes()?,
        }),
        1 => ProgramFactEntry::RelationEdge(RelationEdgeEntry {
            path: r.string()?,
            source_table: r.string()?.into(),
            source_row: RowUuid(r.uuid()?),
            target_table: r.string()?.into(),
            target_row: RowUuid(r.uuid()?),
            kind: program_fact_option(&mut r, kind)?,
            source_version: program_fact_option(&mut r, program_fact_version)?,
            target_version: program_fact_option(&mut r, program_fact_version)?,
            depth: program_fact_option(&mut r, |r| r.u32())?,
            edge_id: program_fact_option(&mut r, |r| r.bytes())?,
            branch: program_fact_option(&mut r, |r| r.bytes())?,
            role: program_fact_option(&mut r, edge_role)?,
            order: program_fact_option(&mut r, |r| r.bytes())?,
            hole_state: program_fact_option(&mut r, |r| match r.u8()? {
                0 => Ok(PathHoleState::Matched),
                1 => Ok(PathHoleState::Hole),
                _ => Err(Error::InvalidStoredValue(
                    "settled program fact hole tag is invalid",
                )),
            })?,
        }),
        2 => ProgramFactEntry::PathCorrelationCoverage(PathCorrelationCoverageEntry {
            path: r.string()?,
            source_table: r.string()?.into(),
            source_row: RowUuid(r.uuid()?),
            correlation_key: r.bytes()?,
            complete: match r.u8()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled program fact boolean is invalid",
                    ));
                }
            },
        }),
        3 => ProgramFactEntry::ProgramSourceCoverage(ProgramSourceCoverageEntry {
            source: program_fact_source(&mut r)?,
            complete: match r.u8()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled program fact boolean is invalid",
                    ));
                }
            },
        }),
        4 => ProgramFactEntry::ReadFrontierSettled(ReadFrontierSettledEntry {
            scope: r.string()?,
            tier: program_fact_tier(&mut r)?,
            stream: program_fact_option(&mut r, |r| r.string())?,
            frontier: r.bytes()?,
        }),
        5 => ProgramFactEntry::CompleteTxPayloadCoverage(CompleteTxPayloadCoverageEntry {
            tx: program_fact_tx(&mut r)?,
            tier: program_fact_tier(&mut r)?,
            payload_digest: r.bytes()?,
        }),
        6 => ProgramFactEntry::ViewCompleteExclusiveCoverage(ViewCompleteExclusiveCoverageEntry {
            tx: program_fact_tx(&mut r)?,
            scope: r.string()?,
            result: program_fact_option(&mut r, |r| program_fact_member(r, 1))?,
            tier: program_fact_tier(&mut r)?,
            covered_members_digest: r.bytes()?,
        }),
        7 => {
            let decision = r.bytes()?;
            let outcome = match r.u8()? {
                0 => PolicyDecisionOutcomeEntry::Allowed,
                1 => PolicyDecisionOutcomeEntry::Denied,
                2 => PolicyDecisionOutcomeEntry::IndeterminateRequiresInput { input: r.string()? },
                3 => PolicyDecisionOutcomeEntry::RequiresCoverage {
                    scope: r.string()?,
                    frontier: r.bytes()?,
                },
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled program fact policy outcome tag is invalid",
                    ));
                }
            };
            ProgramFactEntry::PolicyDecision(PolicyDecisionEntry {
                decision,
                outcome,
                reason: program_fact_option(&mut r, |r| r.string())?,
            })
        }
        8 => ProgramFactEntry::VersionWitness(VersionWitnessEntry {
            role: r.string()?,
            version: program_fact_version(&mut r)?,
            member: program_fact_option(&mut r, |r| program_fact_member(r, 1))?,
        }),
        14 => {
            let input = CoveredInputEntry {
                source: program_fact_source(&mut r)?,
                version_table: r.string()?.into(),
                source_row: RowUuid(r.uuid()?),
                version: program_fact_version(&mut r)?,
            };
            if !input.is_wire_valid() {
                return Err(Error::InvalidStoredValue(
                    "settled covered input identity is invalid",
                ));
            }
            ProgramFactEntry::CoveredInput(input)
        }
        9 => ProgramFactEntry::PolicyWitness(PolicyWitnessEntry {
            protected: program_fact_member(&mut r, 1)?,
            policy_path: r.string()?,
            witness: program_fact_version(&mut r)?,
            edge_kind: program_fact_option(&mut r, kind)?,
        }),
        10 => ProgramFactEntry::ContributingMembers(ContributingMembersEntry {
            result: program_fact_member(&mut r, 1)?,
            contributor: program_fact_member(&mut r, 1)?,
            batch: program_fact_option(&mut r, program_fact_tx)?,
            role: program_fact_option(&mut r, |r| r.string())?,
        }),
        11 => ProgramFactEntry::PredicateRead(PredicateReadEntry {
            role: role(&mut r)?,
            shape_id: ShapeId(r.uuid()?),
            binding_id: BindingId(r.uuid()?),
            predicate: r.bytes()?,
            frontier: r.bytes()?,
        }),
        12 => ProgramFactEntry::PredicateOutputSet(PredicateOutputSetEntry {
            role: role(&mut r)?,
            table: r.string()?.into(),
            row: RowUuid(r.uuid()?),
            version: program_fact_version(&mut r)?,
            shape_id: ShapeId(r.uuid()?),
            binding_id: BindingId(r.uuid()?),
        }),
        13 => ProgramFactEntry::PointRead(PointReadEntry {
            present: match r.u8()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled program fact boolean is invalid",
                    ));
                }
            },
            table: r.string()?.into(),
            row: RowUuid(r.uuid()?),
            version: program_fact_option(&mut r, program_fact_version)?,
            shape_id: ShapeId(r.uuid()?),
            binding_id: BindingId(r.uuid()?),
        }),
        _ => {
            return Err(Error::InvalidStoredValue(
                "settled program fact tag is invalid",
            ));
        }
    };
    r.finish()?;
    if program_fact_storage_bytes(&fact)? != encoded {
        return Err(Error::InvalidStoredValue(
            "settled program fact encoding is not canonical",
        ));
    }
    Ok(fact)
}

pub(super) fn transaction_values(
    node_alias: NodeAlias,
    tx: &Transaction,
    fate: Fate,
    global_time: Option<GlobalTime>,
    durability: DurabilityTier,
    contribution_merge: Value,
) -> Vec<Value> {
    transaction_values_with_cardinality_scope(
        node_alias,
        tx,
        fate,
        global_time,
        durability,
        false,
        contribution_merge,
    )
}

pub(super) fn transaction_values_with_cardinality_scope(
    node_alias: NodeAlias,
    tx: &Transaction,
    fate: Fate,
    global_time: Option<GlobalTime>,
    durability: DurabilityTier,
    view_scoped_cardinality: bool,
    contribution_merge: Value,
) -> Vec<Value> {
    vec![
        Value::U64(tx.tx_id.time.0),
        Value::U64(node_alias.0),
        Value::String(match tx.kind {
            TxKind::Mergeable => "mergeable".to_owned(),
            TxKind::Exclusive => "exclusive".to_owned(),
        }),
        Value::U32(tx.n_total_writes),
        Value::String(tx.made_by.canonical().to_owned()),
        Value::Nullable(None),
        Value::Nullable(None),
        Value::Nullable(None),
        Value::Nullable(None),
        Value::Nullable(
            tx.user_metadata_json
                .clone()
                .map(|value| Box::new(Value::String(value))),
        ),
        contribution_merge,
        Value::Nullable(
            tx.permission_subject
                .map(|id| Box::new(Value::String(id.canonical().to_owned()))),
        ),
        Value::Nullable(
            view_scoped_cardinality
                .then(|| Box::new(Value::String("view-scoped-cardinality".to_owned()))),
        ),
        Value::String(fate_string(&fate)),
        Value::Nullable(global_time.map(|time| Box::new(Value::U64(time.0)))),
        Value::Nullable(rejection_reason_tag(&fate).map(|reason| Box::new(Value::String(reason)))),
        Value::Nullable(
            rejection_reason_cascade_root(&fate).map(|root| Box::new(tx_id_value(root))),
        ),
        Value::Nullable(
            rejection_reason_detail(&fate).map(|detail| Box::new(Value::String(detail))),
        ),
        Value::String(durability_string(durability).to_owned()),
    ]
}

pub(super) fn rejected_transaction_values(
    node_alias: NodeAlias,
    tx: &Transaction,
    reason: RejectionReason,
) -> Vec<Value> {
    vec![
        Value::U64(tx.tx_id.time.0),
        Value::U64(node_alias.0),
        Value::String(match tx.kind {
            TxKind::Mergeable => "mergeable".to_owned(),
            TxKind::Exclusive => "exclusive".to_owned(),
        }),
        Value::String(tx.made_by.canonical().to_owned()),
        Value::String(rejection_reason_tag_for_reason(&reason)),
        Value::Nullable(
            rejection_reason_cascade_root_for_reason(&reason)
                .map(|root| Box::new(tx_id_value(root))),
        ),
        Value::Nullable(
            rejection_reason_detail_for_reason(&reason)
                .map(|detail| Box::new(Value::String(detail))),
        ),
        Value::Nullable(
            tx.user_metadata_json
                .clone()
                .map(|value| Box::new(Value::String(value))),
        ),
    ]
}

pub(super) fn pending_edge_values(
    child_alias: NodeAlias,
    child: TxId,
    parent_alias: NodeAlias,
    parent: TxId,
    coordinate: &ParentCoordinate,
) -> Result<Vec<Value>, Error> {
    Ok(vec![
        Value::U64(child.time.0),
        Value::U64(child_alias.0),
        Value::U64(parent.time.0),
        Value::U64(parent_alias.0),
        Value::U64(coordinate.physical_table_id.0),
        Value::Bytes(coordinate.branch_key.try_canonical_bytes().map_err(|_| {
            Error::InvalidMergeableCommit("pending parent coordinate branch key is not canonical")
        })?),
        Value::Uuid(coordinate.row_uuid.0),
        Value::Bytes(version_layer_string(coordinate.layer).into_bytes()),
    ])
}

pub(super) fn pending_edge_primary_key(
    child_alias: NodeAlias,
    child: TxId,
    parent_alias: NodeAlias,
    parent: TxId,
    coordinate: &ParentCoordinate,
) -> Result<PrimaryKeyValue, Error> {
    Ok(PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::U64(child.time.0),
        PrimaryKeyValue::U64(child_alias.0),
        PrimaryKeyValue::U64(parent.time.0),
        PrimaryKeyValue::U64(parent_alias.0),
        PrimaryKeyValue::U64(coordinate.physical_table_id.0),
        PrimaryKeyValue::Bytes(coordinate.branch_key.try_canonical_bytes().map_err(|_| {
            Error::InvalidStoredValue("pending parent coordinate branch key is invalid")
        })?),
        PrimaryKeyValue::Uuid(coordinate.row_uuid.0),
        PrimaryKeyValue::Bytes(version_layer_string(coordinate.layer).into_bytes()),
    ]))
}

pub(super) fn pending_edge_coordinate_from_record(
    record: BorrowedRecord<'_>,
) -> Result<ParentCoordinate, Error> {
    let layer = match record.get_bytes(PendingEdgeRowRecord::FIELD_LAYER_IDX)? {
        b"content" => VersionLayer::Content,
        b"deletion" => VersionLayer::Deletion,
        _ => {
            return Err(Error::InvalidStoredValue(
                "pending parent coordinate layer is invalid",
            ));
        }
    };
    Ok(ParentCoordinate {
        physical_table_id: PhysicalTableId(
            record.get_u64(PendingEdgeRowRecord::FIELD_PHYSICAL_TABLE_ID_IDX)?,
        ),
        branch_key: BranchKey::from_canonical_bytes(
            record.get_bytes(PendingEdgeRowRecord::FIELD_BRANCH_KEY_IDX)?,
        )
        .map_err(|_| {
            Error::InvalidStoredValue("pending parent coordinate branch key is invalid")
        })?,
        row_uuid: RowUuid(record.get_uuid(PendingEdgeRowRecord::FIELD_ROW_UUID_IDX)?),
        layer,
    })
}

pub(super) fn rejected_version_values(
    table_schema: &TableSchema,
    version: &VersionRow,
) -> Result<Vec<Value>, Error> {
    let cells = version.cells(table_schema)?;
    let mut values = vec![
        Value::U64(version.tx_time().0),
        Value::U64(version.tx_node_alias().0),
        Value::Uuid(version.row_uuid().0),
        Value::Bytes(version_layer_string(version.layer()).into_bytes()),
        Value::Array(
            version
                .parents()
                .iter()
                .map(|parent| tx_id_value(*parent))
                .collect(),
        ),
        Value::Nullable(version.deletion().map(|deletion| {
            Box::new(Value::EnumTag(match deletion {
                DeletionEvent::Deleted => 0,
                DeletionEvent::Restored => 1,
            }))
        })),
    ];
    for column in &table_schema.columns {
        values.push(Value::Nullable(
            cells.get(&column.name).cloned().map(Box::new),
        ));
    }
    Ok(values)
}

pub(super) fn fate_string(fate: &Fate) -> String {
    match fate {
        Fate::Pending => "pending".to_owned(),
        Fate::Accepted => "accepted".to_owned(),
        Fate::Rejected(_) => "rejected".to_owned(),
    }
}

pub(super) fn durability_string(durability: DurabilityTier) -> &'static str {
    match durability {
        DurabilityTier::None => "none",
        DurabilityTier::Local => "local",
        DurabilityTier::Edge => "edge",
        DurabilityTier::Global => "global",
    }
}

pub(super) fn next_fate(current: &Fate, incoming: Fate) -> Result<Fate, Error> {
    match (current, incoming) {
        (Fate::Pending, next) => Ok(next),
        (Fate::Accepted, Fate::Pending | Fate::Accepted) => Ok(Fate::Accepted),
        (Fate::Rejected(reason), Fate::Pending) => Ok(Fate::Rejected(reason.clone())),
        (Fate::Rejected(reason), Fate::Rejected(next)) if *reason == next => {
            Ok(Fate::Rejected(reason.clone()))
        }
        (Fate::Rejected(reason), Fate::Rejected(_)) => Ok(Fate::Rejected(reason.clone())),
        (Fate::Accepted, Fate::Rejected(_)) | (Fate::Rejected(_), Fate::Accepted) => {
            Err(Error::ConflictingFate)
        }
    }
}

pub(super) fn rejected_root_for(fate: &Fate, tx_id: TxId) -> Option<TxId> {
    match fate {
        Fate::Rejected(RejectionReason::Cascade { root }) => Some(*root),
        Fate::Rejected(_) => Some(tx_id),
        Fate::Pending | Fate::Accepted => None,
    }
}

pub(super) fn known_transaction_payload_matches(
    existing: &Transaction,
    incoming: &Transaction,
) -> bool {
    let mut redacted_existing = existing.clone();
    redacted_existing.base_snapshot = None;
    redacted_existing.row_read_set = None;
    redacted_existing.absent_read_set = None;
    redacted_existing.predicate_read_set = None;
    let mut redacted_incoming = incoming.clone();
    redacted_incoming.base_snapshot = None;
    redacted_incoming.row_read_set = None;
    redacted_incoming.absent_read_set = None;
    redacted_incoming.predicate_read_set = None;
    existing == incoming
        || &redacted_existing == incoming
        || existing == &redacted_incoming
        || redacted_existing == redacted_incoming
}

pub(super) fn known_transaction_payload_matches_redacted_cardinality(
    existing: &Transaction,
    incoming: &Transaction,
) -> bool {
    let mut existing = existing.clone();
    existing.n_total_writes = 0;
    let mut incoming = incoming.clone();
    incoming.n_total_writes = 0;
    known_transaction_payload_matches(&existing, &incoming)
}

pub(super) fn rejection_reason_tag(fate: &Fate) -> Option<String> {
    match fate {
        Fate::Rejected(reason) => Some(rejection_reason_tag_for_reason(reason)),
        Fate::Pending | Fate::Accepted => None,
    }
}

pub(super) fn rejection_reason_tag_for_reason(reason: &RejectionReason) -> String {
    match reason {
        RejectionReason::ClientClockTooFarAhead => "client_clock_too_far_ahead".to_owned(),
        RejectionReason::AuthorizationDenied => "authorization_denied".to_owned(),
        RejectionReason::ExclusiveConflict => "exclusive_conflict".to_owned(),
        RejectionReason::CausalityViolation => "causality_violation".to_owned(),
        RejectionReason::Cascade { .. } => "cascade".to_owned(),
        RejectionReason::MalformedCommit(_) => "malformed_commit".to_owned(),
    }
}

pub(super) fn rejection_reason_cascade_root(fate: &Fate) -> Option<TxId> {
    match fate {
        Fate::Rejected(reason) => rejection_reason_cascade_root_for_reason(reason),
        Fate::Pending | Fate::Accepted => None,
    }
}

pub(super) fn rejection_reason_cascade_root_for_reason(reason: &RejectionReason) -> Option<TxId> {
    match reason {
        RejectionReason::Cascade { root } => Some(*root),
        _ => None,
    }
}

pub(super) fn rejection_reason_detail(fate: &Fate) -> Option<String> {
    match fate {
        Fate::Rejected(reason) => rejection_reason_detail_for_reason(reason),
        Fate::Pending | Fate::Accepted => None,
    }
}

pub(super) fn rejection_reason_detail_for_reason(reason: &RejectionReason) -> Option<String> {
    match reason {
        RejectionReason::MalformedCommit(reason) => Some(reason.clone()),
        _ => None,
    }
}

pub(super) fn canonical_versions(mut versions: Vec<VersionRecord>) -> Vec<VersionRecord> {
    versions.sort();
    versions
}

pub(super) fn authored_column_ids_from_value(
    value: Value,
) -> Result<BTreeSet<PhysicalColumnId>, Error> {
    // This is an intentional pre-v1 storage cut. Do not accept the former
    // JSON-in-Bytes representation: durable rows have one schema-declared,
    // canonical native representation.
    let Value::Array(values) = value else {
        return Err(Error::InvalidStoredValue(
            "authored columns must be an array of physical column ids",
        ));
    };
    let mut ids = BTreeSet::new();
    let mut previous = None;
    for value in values {
        let Value::U64(id) = value else {
            return Err(Error::InvalidStoredValue(
                "authored columns must contain physical column ids",
            ));
        };
        if id == 0 {
            return Err(Error::InvalidStoredValue(
                "authored physical column ids must be nonzero",
            ));
        }
        if previous.is_some_and(|previous| previous >= id) {
            return Err(Error::InvalidStoredValue(
                "authored physical column ids must be strictly increasing",
            ));
        }
        previous = Some(id);
        ids.insert(PhysicalColumnId(id));
    }
    Ok(ids)
}

fn authored_column_ids_value(columns: Option<&BTreeSet<PhysicalColumnId>>) -> Value {
    Value::Nullable(columns.map(|columns| {
        Box::new(Value::Array(
            columns.iter().map(|column| Value::U64(column.0)).collect(),
        ))
    }))
}

pub(super) fn history_values_from_parts(
    table: &TableSchema,
    version: &VersionRowParts,
) -> Result<Vec<Value>, Error> {
    let mut values = vec![
        Value::Bytes(version.branch_key.canonical_bytes()),
        Value::Uuid(version.row_uuid.0),
        Value::U64(version.tx_time.0),
        Value::U64(version.tx_node_alias.0),
        Value::U64(version.schema_version_alias.0),
        Value::Array(
            version
                .parents
                .iter()
                .map(|parent| tx_id_value(*parent))
                .collect(),
        ),
        Value::String(version.created_by.canonical().to_owned()),
        Value::U64(version.created_at.0),
        Value::String(version.updated_by.canonical().to_owned()),
        Value::U64(version.updated_at.0),
    ];
    for column in &table.columns {
        values.push(Value::Nullable(
            version.cells.get(&column.name).cloned().map(Box::new),
        ));
    }
    values.push(authored_column_ids_value(version.authored_columns.as_ref()));
    Ok(values)
}

fn history_values_from_wire(
    table: &TableSchema,
    version: &VersionRecord,
    authored_columns: Option<BTreeSet<PhysicalColumnId>>,
    tx_node_alias: NodeAlias,
    schema_version_alias: SchemaVersionAlias,
    tx_time: TxTime,
) -> Result<Vec<Value>, Error> {
    let mut values = Vec::with_capacity(HistoryRowRecord::USER_CELLS + table.columns.len());
    values.push(Value::Bytes(version.branch_key().canonical_bytes()));
    values.push(Value::Uuid(version.row_uuid().0));
    values.push(Value::U64(tx_time.0));
    values.push(Value::U64(tx_node_alias.0));
    values.push(Value::U64(schema_version_alias.0));
    values.push(Value::Array(
        version
            .parents()
            .iter()
            .map(|parent| tx_id_value(*parent))
            .collect(),
    ));
    values.push(Value::String(version.created_by().canonical().to_owned()));
    // Wire provenance carries public Unix milliseconds. Reconstruct the
    // internal HLC with logical counter zero at this ingestion boundary.
    values.push(Value::U64(
        TxTime::from_physical_ms(version.created_at_ms())
            .map_err(|_| Error::InvalidStoredValue("wire created_at_ms exceeds packed HLC range"))?
            .0,
    ));
    values.push(Value::String(version.updated_by().canonical().to_owned()));
    values.push(Value::U64(
        TxTime::from_physical_ms(version.updated_at_ms())
            .map_err(|_| Error::InvalidStoredValue("wire updated_at_ms exceeds packed HLC range"))?
            .0,
    ));
    for (idx, column) in table.columns.iter().enumerate() {
        let value = version.optional_cell_at(idx);
        if let Some(value) = value.as_ref() {
            validate_cell_value(column, value)?;
        }
        values.push(Value::Nullable(value.map(Box::new)));
    }
    values.push(authored_column_ids_value(authored_columns.as_ref()));
    Ok(values)
}

pub(super) fn register_values_from_parts(version: &VersionRowParts) -> Result<Vec<Value>, Error> {
    let deletion = version
        .deletion
        .ok_or(Error::InvalidStoredValue("register row requires deletion"))?;
    Ok(vec![
        Value::Bytes(version.branch_key.canonical_bytes()),
        Value::Uuid(version.row_uuid.0),
        Value::U64(version.tx_time.0),
        Value::U64(version.tx_node_alias.0),
        Value::U64(version.schema_version_alias.0),
        Value::Array(
            version
                .parents
                .iter()
                .map(|parent| tx_id_value(*parent))
                .collect(),
        ),
        Value::String(version.created_by.canonical().to_owned()),
        Value::U64(version.created_at.0),
        Value::String(version.updated_by.canonical().to_owned()),
        Value::U64(version.updated_at.0),
        deletion_event_value(deletion),
    ])
}

fn register_values_from_wire(
    version: &VersionRecord,
    tx_node_alias: NodeAlias,
    schema_version_alias: SchemaVersionAlias,
    tx_time: TxTime,
    deletion: DeletionEvent,
) -> Result<Vec<Value>, Error> {
    Ok(vec![
        Value::Bytes(version.branch_key().canonical_bytes()),
        Value::Uuid(version.row_uuid().0),
        Value::U64(tx_time.0),
        Value::U64(tx_node_alias.0),
        Value::U64(schema_version_alias.0),
        Value::Array(
            version
                .parents()
                .iter()
                .map(|parent| tx_id_value(*parent))
                .collect(),
        ),
        Value::String(version.created_by().canonical().to_owned()),
        Value::U64(
            TxTime::from_physical_ms(version.created_at_ms())
                .map_err(|_| {
                    Error::InvalidStoredValue("wire created_at_ms exceeds packed HLC range")
                })?
                .0,
        ),
        Value::String(version.updated_by().canonical().to_owned()),
        Value::U64(
            TxTime::from_physical_ms(version.updated_at_ms())
                .map_err(|_| {
                    Error::InvalidStoredValue("wire updated_at_ms exceeds packed HLC range")
                })?
                .0,
        ),
        deletion_event_value(deletion),
    ])
}

pub(super) fn deletion_event_value(deletion: DeletionEvent) -> Value {
    Value::EnumTag(match deletion {
        DeletionEvent::Deleted => 0,
        DeletionEvent::Restored => 1,
    })
}

pub(super) fn history_primary_key(version: &VersionRow) -> PrimaryKeyValue {
    PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::Bytes(version.branch_key().canonical_bytes()),
        PrimaryKeyValue::Uuid(version.row_uuid().0),
        PrimaryKeyValue::U64(version.tx_time().0),
        PrimaryKeyValue::U64(version.tx_node_alias().0),
    ])
}

pub(super) fn global_current_primary_key(
    branch_key: &BranchKey,
    row_uuid: RowUuid,
) -> PrimaryKeyValue {
    PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::Bytes(branch_key.canonical_bytes()),
        PrimaryKeyValue::Uuid(row_uuid.0),
    ])
}

fn stored_version_prefix_values(version: &VersionRow) -> Vec<Value> {
    vec![
        Value::Bytes(version.branch_key().canonical_bytes()),
        Value::Uuid(version.row_uuid().0),
        Value::U64(version.tx_time().0),
        Value::U64(version.tx_node_alias().0),
        Value::U64(version.schema_version_alias().0),
        Value::Array(
            version
                .parents()
                .iter()
                .map(|parent| tx_id_value(*parent))
                .collect(),
        ),
        Value::String(version.created_by().canonical().to_owned()),
        Value::U64(version.created_at().0),
        Value::String(version.updated_by().canonical().to_owned()),
        Value::U64(version.updated_at().0),
    ]
}

pub(super) fn global_current_values(
    table: &TableSchema,
    version: &VersionRow,
    global_time: Option<GlobalTime>,
) -> Result<Vec<Value>, Error> {
    let mut values = stored_version_prefix_values(version);
    // Current rows are the public read carrier. HLC ordering remains on the
    // version/transaction fields; provenance exposes Unix milliseconds.
    values[GlobalCurrentRowRecord::FIELD_CREATED_AT_IDX] =
        Value::U64(version.created_at().physical_ms());
    values[GlobalCurrentRowRecord::FIELD_UPDATED_AT_IDX] =
        Value::U64(version.updated_at().physical_ms());
    values.push(Value::Nullable(
        global_time.map(|seq| Box::new(Value::U64(seq.0))),
    ));
    for (idx, _column) in table.columns.iter().enumerate() {
        let field = HistoryRowRecord::USER_CELLS + idx;
        values.push(Value::Nullable(
            nullable_value(version.record.borrowed().get_idx(field)?)?.map(Box::new),
        ));
    }
    values.push(authored_column_ids_value(
        version.authored_column_ids()?.as_ref(),
    ));
    Ok(values)
}

pub(super) fn register_global_current_values(
    version: &VersionRow,
    global_time: Option<GlobalTime>,
) -> Vec<Value> {
    let mut values = stored_version_prefix_values(version);
    values[RegisterGlobalCurrentRowRecord::FIELD_CREATED_AT_IDX] =
        Value::U64(version.created_at().physical_ms());
    values[RegisterGlobalCurrentRowRecord::FIELD_UPDATED_AT_IDX] =
        Value::U64(version.updated_at().physical_ms());
    values.push(Value::Nullable(
        global_time.map(|seq| Box::new(Value::U64(seq.0))),
    ));
    values.push(deletion_event_value(
        version
            .deletion()
            .expect("register global-current row requires deletion"),
    ));
    values
}

pub(super) fn global_change_values(
    table_id: PhysicalTableId,
    version: &VersionRow,
    global_time: GlobalTime,
) -> Vec<Value> {
    vec![
        Value::U64(table_id.0),
        Value::Bytes(version.branch_key().canonical_bytes()),
        Value::Uuid(version.row_uuid().0),
        Value::Bytes(version_layer_string(version.layer()).into_bytes()),
        Value::U64(global_time.0),
        Value::U64(version.tx_time().0),
        Value::U64(version.tx_node_alias().0),
        Value::Nullable(
            version
                .deletion()
                .map(|deletion| Box::new(deletion_event_value(deletion))),
        ),
    ]
}

#[allow(dead_code)]
pub(super) fn global_change_primary_key_from_record(
    record: &BorrowedRecord<'_>,
) -> Result<PrimaryKeyValue, Error> {
    Ok(PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::U64(record.get_u64(GlobalChangeRowRecord::FIELD_PHYSICAL_TABLE_ID_IDX)?),
        PrimaryKeyValue::Bytes(
            record
                .get_bytes(GlobalChangeRowRecord::FIELD_BRANCH_KEY_IDX)?
                .to_vec(),
        ),
        PrimaryKeyValue::Uuid(record.get_uuid(GlobalChangeRowRecord::FIELD_ROW_UUID_IDX)?),
        PrimaryKeyValue::Bytes(
            record
                .get_bytes(GlobalChangeRowRecord::FIELD_LAYER_IDX)?
                .to_vec(),
        ),
        PrimaryKeyValue::U64(record.get_u64(GlobalChangeRowRecord::FIELD_GLOBAL_TIME_IDX)?),
    ]))
}

pub(super) fn rejected_transaction_primary_key(alias: NodeAlias, tx_id: TxId) -> PrimaryKeyValue {
    PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::U64(tx_id.time.0),
        PrimaryKeyValue::U64(alias.0),
    ])
}

pub(super) fn rejected_version_primary_key_from_record(
    record: &BorrowedRecord<'_>,
) -> Result<PrimaryKeyValue, Error> {
    Ok(PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::U64(expect_u64(
            record.get_idx(RejectedVersionRowRecord::FIELD_TX_TIME_IDX)?,
            "tx_time",
        )?),
        PrimaryKeyValue::U64(expect_u64(
            record.get_idx(RejectedVersionRowRecord::FIELD_TX_NODE_ID_IDX)?,
            "tx_node_id",
        )?),
        PrimaryKeyValue::Uuid(expect_uuid(
            record.get_idx(RejectedVersionRowRecord::FIELD_ROW_UUID_IDX)?,
            "row_uuid",
        )?),
        PrimaryKeyValue::Bytes(expect_bytes(
            record.get_idx(RejectedVersionRowRecord::FIELD_LAYER_IDX)?,
            "layer",
        )?),
    ]))
}

pub(super) fn visible_current_graph(table: &TableSchema, settled: DurabilityTier) -> GraphBuilder {
    let user_fields = table
        .columns
        .iter()
        .map(|column| app_column_field(&column.name))
        .collect::<Vec<_>>();
    let mut content_fields = vec!["row_uuid".to_owned()];
    content_fields.extend(user_fields.iter().cloned());
    content_fields.extend([
        "created_by".to_owned(),
        "created_at".to_owned(),
        "updated_by".to_owned(),
        "updated_at".to_owned(),
    ]);
    content_fields.push("tx_time".to_owned());
    content_fields.push("tx_node_id".to_owned());
    let edge_visible_ahead = |table_name: String, fields: Vec<String>| {
        GraphBuilder::join(
            GraphBuilder::table(table_name).project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::And(vec![
                        PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
                        PredicateExpr::Or(vec![
                            PredicateExpr::eq("durability", Value::EnumTag(2)),
                            PredicateExpr::eq("durability", Value::EnumTag(3)),
                        ])
                        .canonicalize(),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            fields
                .into_iter()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    };
    let (content_current, deleted_winners) = if settled == DurabilityTier::Global {
        // The global-current table now carries every user cell, so current rows
        // resolve directly from it in O(current rows) — no join against the full
        // history table (which made cold subscription hydration O(history depth)).
        let content = GraphBuilder::table(global_current_table_name(&table.name))
            .project(content_fields.clone());
        let deleted = GraphBuilder::table(register_global_current_table_name(&table.name))
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]);
        (content, deleted)
    } else {
        let ahead_content = if settled == DurabilityTier::Edge {
            edge_visible_ahead(
                ahead_current_table_name(&table.name),
                content_fields.clone(),
            )
        } else {
            GraphBuilder::table(ahead_current_table_name(&table.name))
                .project(content_fields.clone())
        };
        let deletion_fields = vec![
            "row_uuid".to_owned(),
            "tx_time".to_owned(),
            "tx_node_id".to_owned(),
            "created_by".to_owned(),
            "created_at".to_owned(),
            "updated_by".to_owned(),
            "updated_at".to_owned(),
            "_deletion".to_owned(),
        ];
        let ahead_deleted = if settled == DurabilityTier::Edge {
            edge_visible_ahead(
                register_ahead_current_table_name(&table.name),
                deletion_fields.clone(),
            )
        } else {
            GraphBuilder::table(register_ahead_current_table_name(&table.name))
                .project(deletion_fields.clone())
        };
        let content = GraphBuilder::arg_max_by(
            GraphBuilder::union([
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(content_fields.clone()),
                ahead_content,
            ]),
            ["row_uuid"],
            ["tx_time", "tx_node_id"],
        )
        .project(content_fields);
        let deleted = GraphBuilder::arg_max_by(
            GraphBuilder::union([
                GraphBuilder::table(register_global_current_table_name(&table.name))
                    .project(deletion_fields),
                ahead_deleted,
            ]),
            ["row_uuid"],
            ["tx_time", "tx_node_id"],
        )
        .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
        .project(["row_uuid"]);
        (content, deleted)
    };
    GraphBuilder::anti_join(content_current, deleted_winners, ["row_uuid"], ["row_uuid"])
        .project_fields(
            std::iter::once(ProjectField::named("row_uuid"))
                .chain(user_fields.into_iter().map(ProjectField::named))
                .chain([
                    ProjectField::renamed("created_by", "$createdBy"),
                    ProjectField::renamed("created_at", "$createdAt"),
                    ProjectField::renamed("updated_by", "$updatedBy"),
                    ProjectField::renamed("updated_at", "$updatedAt"),
                    ProjectField::named("tx_time"),
                    ProjectField::named("tx_node_id"),
                ]),
        )
}

pub(super) fn decode_current_row(
    table: &TableSchema,
    record: BorrowedRecord<'_>,
) -> Result<CurrentRow, Error> {
    Ok(CurrentRow::new(
        table.name.clone(),
        OwnedRecord::new(record.raw().to_vec(), record.descriptor()),
    ))
}

pub(super) fn sort_current_rows(rows: &mut [CurrentRow]) {
    rows.sort_by(|left, right| {
        left.row_uuid()
            .to_bytes()
            .cmp(&right.row_uuid().to_bytes())
            .then_with(|| left.record.raw().cmp(right.record.raw()))
    });
}

/// Build a current row from cells that are already app-facing values.
///
/// Build a row from ordinary app-facing cells.
pub(super) fn current_row_from_cells(
    table: &TableSchema,
    row_uuid: RowUuid,
    cells: &BTreeMap<String, Value>,
) -> Result<CurrentRow, Error> {
    let positional = positional_cells_from_map(table, cells)?;
    current_row_from_positional_cells(table, row_uuid, &positional)
}

pub(super) fn current_row_from_version_projection(
    table: &TableSchema,
    version: &VersionRow,
) -> Result<CurrentRow, Error> {
    let descriptor = current_row_descriptor(table);
    let mut values = current_row_prefix_and_cells_from_version(table, version)?;
    append_current_row_provenance(&mut values, version);
    let raw = descriptor.create(&values)?;
    Ok(CurrentRow::new(
        table.name.clone(),
        OwnedRecord::new(raw, descriptor),
    ))
}

pub(super) fn current_row_from_materialized_cells(
    table: &TableSchema,
    version: &VersionRow,
    cells: &BTreeMap<String, Value>,
) -> Result<CurrentRow, Error> {
    current_row_from_materialized_cells_with_provenance(table, version, version, cells)
}

pub(super) fn current_row_from_materialized_cells_with_provenance(
    table: &TableSchema,
    content: &VersionRow,
    provenance: &VersionRow,
    cells: &BTreeMap<String, Value>,
) -> Result<CurrentRow, Error> {
    current_row_from_materialized_cells_with_layer_provenance(
        table, content, provenance, provenance, cells,
    )
}

/// Build a current row whose application cells and creation provenance come
/// from the content winner while its update provenance comes from the winner
/// of the layer that most recently changed the logical row. Deletion and
/// restoration records carry no user cells, but they still update the row's
/// public `$updatedBy`/`$updatedAt` identity.
pub(super) fn current_row_from_materialized_cells_with_layer_provenance(
    table: &TableSchema,
    content: &VersionRow,
    created: &VersionRow,
    updated: &VersionRow,
    cells: &BTreeMap<String, Value>,
) -> Result<CurrentRow, Error> {
    let descriptor = current_row_descriptor(table);
    let mut values = Vec::with_capacity(table.columns.len() + 7);
    values.push(Value::Uuid(content.row_uuid().0));
    for column in &table.columns {
        values.push(Value::Nullable(
            cells.get(&column.name).cloned().map(Box::new),
        ));
    }
    values.push(Value::String(created.created_by().canonical().to_owned()));
    values.push(Value::U64(created.created_at().physical_ms()));
    values.push(Value::String(updated.updated_by().canonical().to_owned()));
    values.push(Value::U64(updated.updated_at().physical_ms()));
    values.push(Value::U64(updated.tx_time().0));
    values.push(Value::U64(updated.tx_node_alias().0));
    let raw = descriptor.create(&values)?;
    Ok(CurrentRow::new(
        table.name.clone(),
        OwnedRecord::new(raw, descriptor),
    ))
}

pub(super) fn current_row_from_cells_with_explicit_provenance(
    table: &TableSchema,
    row_uuid: RowUuid,
    cells: &BTreeMap<String, Value>,
    provenance: RowProvenance,
    projected_tx: Option<(TxTime, NodeAlias)>,
) -> Result<CurrentRow, Error> {
    let descriptor = current_row_descriptor(table);
    let mut values = Vec::with_capacity(table.columns.len() + 7);
    values.push(Value::Uuid(row_uuid.0));
    for column in &table.columns {
        values.push(Value::Nullable(
            cells.get(&column.name).cloned().map(Box::new),
        ));
    }
    values.push(Value::String(provenance.created_by.canonical().to_owned()));
    values.push(Value::U64(provenance.created_at));
    values.push(Value::String(provenance.updated_by.canonical().to_owned()));
    values.push(Value::U64(provenance.updated_at));
    let (tx_time, tx_node_alias) = projected_tx.unwrap_or((TxTime(0), NodeAlias(0)));
    values.push(Value::U64(tx_time.0));
    values.push(Value::U64(tx_node_alias.0));
    let raw = descriptor.create(&values)?;
    Ok(CurrentRow::new(
        table.name.clone(),
        OwnedRecord::new(raw, descriptor),
    ))
}

fn current_row_prefix_and_cells_from_version(
    table: &TableSchema,
    version: &VersionRow,
) -> Result<Vec<Value>, Error> {
    let mut values = Vec::with_capacity(table.columns.len() + 7);
    values.push(Value::Uuid(version.row_uuid().0));
    if version.is_register_record() {
        values.extend(table.columns.iter().map(|_| Value::Nullable(None)));
        return Ok(values);
    }
    let borrowed = version.record.borrowed();
    for (idx, _) in table.columns.iter().enumerate() {
        values.push(Value::Nullable(
            nullable_value(borrowed.get_idx(HistoryRowRecord::USER_CELLS + idx)?)?.map(Box::new),
        ));
    }
    Ok(values)
}

fn append_current_row_provenance(values: &mut Vec<Value>, provenance: &VersionRow) {
    values.push(Value::String(
        provenance.created_by().canonical().to_owned(),
    ));
    values.push(Value::U64(provenance.created_at().physical_ms()));
    values.push(Value::String(
        provenance.updated_by().canonical().to_owned(),
    ));
    values.push(Value::U64(provenance.updated_at().physical_ms()));
    values.push(Value::U64(provenance.tx_time().0));
    values.push(Value::U64(provenance.tx_node_alias().0));
}

fn current_row_descriptor(table: &TableSchema) -> records::RecordDescriptor {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<Vec<CurrentRowDescriptorCacheEntry>>> =
        std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    let mut cache = cache.lock().expect("current row descriptor cache poisoned");
    if let Some(descriptor) = cache
        .iter()
        .find(|entry| entry.matches(table))
        .map(|entry| entry.descriptor)
    {
        return descriptor;
    }
    let descriptor = build_current_row_descriptor(table);
    cache.push(CurrentRowDescriptorCacheEntry::new(table, descriptor));
    descriptor
}

struct CurrentRowDescriptorCacheEntry {
    table_name: String,
    columns: Vec<(String, groove::schema::ColumnType)>,
    descriptor: records::RecordDescriptor,
}

impl CurrentRowDescriptorCacheEntry {
    fn new(table: &TableSchema, descriptor: records::RecordDescriptor) -> Self {
        Self {
            table_name: table.name.clone(),
            columns: table
                .columns
                .iter()
                .map(|column| (column.name.clone(), column.column_type.clone()))
                .collect(),
            descriptor,
        }
    }

    fn matches(&self, table: &TableSchema) -> bool {
        self.table_name == table.name
            && self.columns.len() == table.columns.len()
            && self
                .columns
                .iter()
                .zip(&table.columns)
                .all(|((name, column_type), column)| {
                    name == &column.name && column_type == &column.column_type
                })
    }
}

fn build_current_row_descriptor(table: &TableSchema) -> records::RecordDescriptor {
    records::RecordDescriptor::new(
        std::iter::once(("row_uuid".to_owned(), records::ValueType::Uuid))
            .chain(table.columns.iter().map(|column| {
                (
                    app_column_field(&column.name),
                    records::ValueType::Nullable(Box::new(column.column_type.clone())),
                )
            }))
            .chain([
                ("$createdBy".to_owned(), records::ValueType::String),
                ("$createdAt".to_owned(), records::ValueType::U64),
                ("$updatedBy".to_owned(), records::ValueType::String),
                ("$updatedAt".to_owned(), records::ValueType::U64),
                ("tx_time".to_owned(), records::ValueType::U64),
                ("tx_node_id".to_owned(), records::ValueType::U64),
            ]),
    )
}

pub(super) fn current_row_from_positional_cells(
    table: &TableSchema,
    row_uuid: RowUuid,
    cells: &[Option<Value>],
) -> Result<CurrentRow, Error> {
    let descriptor = records::RecordDescriptor::new(
        std::iter::once(("row_uuid".to_owned(), records::ValueType::Uuid)).chain(
            table.columns.iter().map(|column| {
                (
                    column.name.clone(),
                    records::ValueType::Nullable(Box::new(column.column_type.clone())),
                )
            }),
        ),
    );
    let mut values = vec![Value::Uuid(row_uuid.0)];
    for (idx, _column) in table.columns.iter().enumerate() {
        values.push(Value::Nullable(
            cells.get(idx).and_then(Clone::clone).map(Box::new),
        ));
    }
    let raw = descriptor.create(&values)?;
    // This app-facing positional projection uses logical schema names, not
    // private `_app_{column}` storage carriers. Preserve that distinction for
    // any later source re-encoding.
    let bindings = std::iter::once(CurrentRowBindingField::HiddenMetadata)
        .chain(std::iter::repeat_n(
            CurrentRowBindingField::ResultField,
            table.columns.len(),
        ))
        .collect();
    Ok(CurrentRow::new_with_explicit_binding_fields(
        table.name.clone(),
        OwnedRecord::new(raw, descriptor),
        bindings,
    ))
}

pub(super) fn positional_cells_from_map(
    table: &TableSchema,
    cells: &BTreeMap<String, Value>,
) -> Result<Vec<Option<Value>>, Error> {
    for column in cells.keys() {
        if !table
            .columns
            .iter()
            .any(|candidate| &candidate.name == column)
        {
            return Err(Error::InvalidMergeableCommit("unknown user cell column"));
        }
    }
    table
        .columns
        .iter()
        .map(|column| {
            cells
                .get(&column.name)
                .cloned()
                .map(|value| {
                    validate_cell_value(column, &value)?;
                    Ok(value)
                })
                .transpose()
        })
        .collect()
}

pub(super) fn cells_from_positional(
    table: &TableSchema,
    cells: &[Option<Value>],
) -> BTreeMap<String, Value> {
    table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| {
            cells
                .get(idx)
                .and_then(Clone::clone)
                .map(|value| (column.name.clone(), value))
        })
        .collect()
}

pub(super) fn nullable_value(value: Value) -> Result<Option<Value>, Error> {
    match value {
        Value::Nullable(None) => Ok(None),
        Value::Nullable(Some(value)) => Ok(Some(*value)),
        _ => Err(Error::InvalidStoredValue("nullable value expected")),
    }
}

pub(super) fn validate_cell_value(column: &ColumnSchema, value: &Value) -> Result<(), Error> {
    records::RecordDescriptor::new([("cell", column.column_type.clone())])
        .create(std::slice::from_ref(value))?;
    Ok(())
}

// Diagnostic helper used only by debug_assert duplicate-version checks and a
// test helper; not compiled into release production builds (its production
// callers are gated to debug builds, but a #[cfg(test)] helper references it,
// so it must exist in any test build including `cargo test --release`).
#[cfg(any(debug_assertions, test))]
pub(super) fn duplicate_output_occurrence_result_set(
    result_set: &BTreeSet<ResultMemberEntry>,
) -> Option<(crate::tools::OutputOccurrenceId, TxId, TxId)> {
    let mut rows = BTreeMap::new();
    for member in result_set {
        let Some(occurrence_id) = member.output_occurrence_id() else {
            continue;
        };
        let Some((_, _, tx_id)) = member.as_row() else {
            continue;
        };
        if let Some(first) = rows.insert(occurrence_id.clone(), tx_id) {
            return Some((occurrence_id, first, tx_id));
        }
    }
    None
}

pub(super) fn expect_u64(value: Value, field: &'static str) -> Result<u64, Error> {
    match value {
        Value::U64(value) => Ok(value),
        _ => Err(Error::InvalidStoredValue(field)),
    }
}

pub(super) fn expect_bytes(value: Value, field: &'static str) -> Result<Vec<u8>, Error> {
    match value {
        Value::Bytes(value) => Ok(value),
        _ => Err(Error::InvalidStoredValue(field)),
    }
}

pub(super) fn expect_uuid(value: Value, field: &'static str) -> Result<uuid::Uuid, Error> {
    match value {
        Value::Uuid(value) => Ok(value),
        _ => Err(Error::InvalidStoredValue(field)),
    }
}

pub(super) fn tx_ids_from_value(value: Value) -> Result<Vec<TxId>, Error> {
    match value {
        Value::Array(values) => {
            let parents = values
                .into_iter()
                .map(tx_id_from_value)
                .collect::<Result<Vec<_>, _>>()?;
            validate_parent_tx_ids(&parents)?;
            Ok(parents)
        }
        _ => Err(Error::InvalidStoredValue("parents must be array")),
    }
}

pub(super) fn validate_parent_tx_ids(parents: &[TxId]) -> Result<(), Error> {
    if parents.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(Error::InvalidMergeableCommit(
            "row version parents must be sorted and unique",
        ));
    }
    Ok(())
}

pub(super) fn validate_canonical_version_parts(
    branch_key: &BranchKey,
    parents: &[TxId],
) -> Result<(), Error> {
    branch_key
        .try_canonical_bytes()
        .map_err(|_| Error::InvalidMergeableCommit("row version branch key is not canonical"))?;
    validate_parent_tx_ids(parents)
}

pub(super) fn merge_heads_value(heads: &BTreeSet<TxId>) -> Value {
    Value::Array(heads.iter().copied().map(tx_id_value).collect())
}

pub(super) fn merge_heads_from_value(value: Value) -> Result<BTreeSet<TxId>, Error> {
    // This is an intentional pre-v1 storage cut. Do not accept the former
    // postcard-in-Bytes representation: this derived table has one
    // schema-declared representation and can be rebuilt from history.
    let Value::Array(values) = value else {
        return Err(Error::InvalidStoredValue(
            "merge heads must be an array of transaction ids",
        ));
    };
    let mut heads = BTreeSet::new();
    let mut previous = None;
    for value in values {
        let head = tx_id_from_value(value)?;
        if previous.is_some_and(|previous| previous >= head) {
            return Err(Error::InvalidStoredValue(
                "merge heads must be strictly increasing",
            ));
        }
        previous = Some(head);
        heads.insert(head);
    }
    Ok(heads)
}

pub(super) fn tx_id_from_value(value: Value) -> Result<TxId, Error> {
    match value {
        Value::Tuple(values) if values.len() == 2 => {
            let mut values = values.into_iter();
            let Value::U64(time) = values.next().expect("len checked") else {
                return Err(Error::InvalidStoredValue("tx id time must be u64"));
            };
            let Value::Uuid(node) = values.next().expect("len checked") else {
                return Err(Error::InvalidStoredValue("tx id node must be uuid"));
            };
            Ok(TxId::new(TxTime(time), NodeUuid(node)))
        }
        _ => Err(Error::InvalidStoredValue("tx id must be tuple(u64, uuid)")),
    }
}

pub(super) fn tx_kind_from_discriminant(value: u8) -> Result<TxKind, Error> {
    match value {
        0 => Ok(TxKind::Mergeable),
        1 => Ok(TxKind::Exclusive),
        _ => Err(Error::InvalidStoredValue("unknown tx kind")),
    }
}

pub(super) fn fate_from_encoded_fields(record: BorrowedRecord<'_>) -> Result<Fate, Error> {
    match record.get_enum(TransactionRowRecord::FIELD_FATE_IDX)? {
        0 => Ok(Fate::Pending),
        1 => Ok(Fate::Accepted),
        2 => Ok(Fate::Rejected(rejection_reason_from_encoded_fields(
            record,
        )?)),
        _ => Err(Error::InvalidStoredValue("unknown fate")),
    }
}

pub(super) fn rejection_reason_from_encoded_fields(
    record: BorrowedRecord<'_>,
) -> Result<RejectionReason, Error> {
    let tag = record
        .get_nullable_enum(TransactionRowRecord::FIELD_REJECTION_REASON_IDX)?
        .ok_or(Error::InvalidStoredValue(
            "rejected transaction missing reason",
        ))?;
    match tag {
        0 => Ok(RejectionReason::ClientClockTooFarAhead),
        1 => Ok(RejectionReason::AuthorizationDenied),
        2 => Ok(RejectionReason::ExclusiveConflict),
        3 => Ok(RejectionReason::CausalityViolation),
        4 => Ok(RejectionReason::Cascade {
            root: nullable_tx_id_value(
                record.get_idx(TransactionRowRecord::FIELD_CASCADE_ROOT_IDX)?,
            )?
            .ok_or(Error::InvalidStoredValue("cascade rejection missing root"))?,
        }),
        5 => Ok(RejectionReason::MalformedCommit(
            record
                .get_nullable_string(TransactionRowRecord::FIELD_REASON_DETAIL_IDX)?
                .unwrap_or_default()
                .to_owned(),
        )),
        _ => Err(Error::InvalidStoredValue("unknown rejection reason")),
    }
}

pub(super) fn nullable_tx_id_value(value: Value) -> Result<Option<TxId>, Error> {
    match value {
        Value::Nullable(None) => Ok(None),
        Value::Nullable(Some(value)) => tx_id_from_value(*value).map(Some),
        _ => Err(Error::InvalidStoredValue("tx id must be nullable tuple")),
    }
}

pub(super) fn durability_from_discriminant(value: u8) -> Result<DurabilityTier, Error> {
    match value {
        0 => Ok(DurabilityTier::None),
        1 => Ok(DurabilityTier::Local),
        2 => Ok(DurabilityTier::Edge),
        3 => Ok(DurabilityTier::Global),
        _ => Err(Error::InvalidStoredValue("unknown durability")),
    }
}

pub(super) fn deletion_event_from_value(value: Value) -> Result<DeletionEvent, Error> {
    match value {
        Value::EnumTag(0) => Ok(DeletionEvent::Deleted),
        Value::EnumTag(1) => Ok(DeletionEvent::Restored),
        _ => Err(Error::InvalidStoredValue("unknown deletion event")),
    }
}

pub(super) fn tx_id_value(tx_id: TxId) -> Value {
    Value::Tuple(vec![Value::U64(tx_id.time.0), Value::Uuid(tx_id.node.0)])
}

pub(super) fn global_current_table_name(table: &str) -> String {
    format!("jazz_{table}_global_current")
}

pub(super) fn register_global_current_table_name(table: &str) -> String {
    format!("jazz_{table}_register_global_current")
}

pub(super) fn ahead_current_table_name(table: &str) -> String {
    format!("jazz_{table}_ahead_current")
}

pub(super) fn register_ahead_current_table_name(table: &str) -> String {
    format!("jazz_{table}_register_ahead_current")
}

pub(super) fn version_layer_string(layer: VersionLayer) -> String {
    match layer {
        VersionLayer::Content => "content".to_owned(),
        VersionLayer::Deletion => "deletion".to_owned(),
    }
}

#[cfg(test)]
mod authority_storage_codec_tests {
    use super::*;

    fn tx(time: u64, node: u8) -> TxId {
        TxId::new(TxTime(time), NodeUuid::from_bytes([node; 16]))
    }

    fn ordinary_row() -> RealRowMemberEntry {
        let mut row = RealRowMemberEntry::current_content((
            "todos".to_owned().into(),
            RowUuid::from_bytes([0x11; 16]),
            tx(7, 0x22),
        ))
        .with_settle_position(Some(GlobalTime(9)));
        row.read_view = ReadViewKey {
            id: uuid::Uuid::from_bytes([0x19; 16]),
        };
        row
    }

    fn settled_value(value: Value, value_type: records::ValueType) -> Vec<u8> {
        settled_result_value_storage_bytes(&value, &value_type).unwrap()
    }

    fn fixture_member() -> ResultMemberEntry {
        ResultMemberEntry::Synthetic {
            table: "facts".to_owned(),
            row: settled_value(Value::U8(0x11), records::ValueType::U8),
            replacement: SyntheticReplacementToken::from_encoded_record(settled_value(
                Value::U8(0x12),
                records::ValueType::U8,
            )),
        }
    }

    fn fixture_version() -> RowVersionRefEntry {
        RowVersionRefEntry {
            tx: tx(31, 0x33),
            schema_version: Some(SchemaVersionId::from_bytes([0x34; 16])),
            layer: ResultRowLayer::ContentOrDeletion,
            batch: Some(tx(32, 0x35)),
            branch_or_prefix: Some(vec![0x36]),
            row_digest: Some(vec![0x37]),
        }
    }

    fn fixture_source_with_all_roles() -> ProgramSourceId {
        ProgramSourceId {
            table: "tasks".to_owned().into(),
            path: vec![
                ProgramSourceRole::Root,
                ProgramSourceRole::Alias("self".to_owned()),
                ProgramSourceRole::RecursiveSeed("seed".to_owned()),
                ProgramSourceRole::RecursiveStep("step".to_owned()),
                ProgramSourceRole::CorrelatedChild("items".to_owned()),
                ProgramSourceRole::Policy("read".to_owned()),
            ],
        }
    }

    // This is necessarily an internal test: raw durable keys are an engine
    // boundary. Peer authority receipts persist only exact source-closure
    // facts; lock those surviving tags and bytes without preserving retired
    // authority-output fixtures as a compatibility contract.
    #[test]
    fn peer_source_closure_storage_codec_has_permanent_tags_and_exact_fixtures() {
        let version = fixture_version();
        let facts = vec![
            ProgramFactEntry::ProgramSourceCoverage(ProgramSourceCoverageEntry {
                source: fixture_source_with_all_roles(),
                complete: true,
            }),
            ProgramFactEntry::CoveredInput(CoveredInputEntry {
                source: fixture_source_with_all_roles(),
                version_table: "a".to_owned().into(),
                source_row: RowUuid::from_bytes([38; 16]),
                version: version.clone(),
            }),
        ];
        let encoded = facts
            .iter()
            .map(|fact| program_fact_storage_bytes(fact).unwrap())
            .collect::<Vec<_>>();
        for (expected_tag, (fact, bytes)) in [3, 14].into_iter().zip(facts.iter().zip(&encoded)) {
            assert_eq!(&bytes[..4], PROGRAM_FACT_STORAGE_MAGIC);
            assert_eq!(bytes[4], PROGRAM_FACT_STORAGE_VERSION);
            assert_eq!(usize::from(bytes[5]), expected_tag);
            assert_eq!(program_fact_from_storage_bytes(bytes).unwrap(), *fact);
        }
        assert_eq!(
            encoded
                .iter()
                .map(|bytes| blake3::hash(bytes).to_hex().to_string())
                .collect::<Vec<_>>(),
            [
                "cf2f01f026edaec1097ee2d1463f1719561eeda83e671a3ff07af13c0f861d34",
                "6f3af744b862a1da729a60506be303af633493fa0ba9987b759cbd33b46fe812",
            ]
        );
    }

    #[test]
    fn covered_input_source_codec_pins_every_role_and_rejects_malformed_paths() {
        let fact = ProgramFactEntry::CoveredInput(CoveredInputEntry {
            source: fixture_source_with_all_roles(),
            version_table: "tasks".to_owned().into(),
            source_row: RowUuid::from_bytes([0x38; 16]),
            version: fixture_version(),
        });
        let encoded = program_fact_storage_bytes(&fact).unwrap();
        assert_eq!(
            hex::encode(&encoded),
            "4a50464b010e050000007461736b730600000000010400000073656c6602040000007365656403040000007374657004050000006974656d73050400000072656164050000007461736b73383838383838383838383838383838381f000000000000003333333333333333333333333333333301343434343434343434343434343434340201200000000000000035353535353535353535353535353535010100000036010100000037"
        );
        assert_eq!(program_fact_from_storage_bytes(&encoded).unwrap(), fact);

        let source_offset = 6 + 4 + "tasks".len() + 4;
        let mut unknown_role = encoded.clone();
        unknown_role[source_offset] = 0xff;
        assert!(program_fact_from_storage_bytes(&unknown_role).is_err());

        let mut empty_path = encoded.clone();
        let path_len_offset = 6 + 4 + "tasks".len();
        empty_path[path_len_offset..path_len_offset + 4].copy_from_slice(&0_u32.to_le_bytes());
        empty_path.remove(source_offset);
        assert!(program_fact_from_storage_bytes(&empty_path).is_err());

        let empty_alias = ProgramFactEntry::CoveredInput(CoveredInputEntry {
            source: ProgramSourceId {
                table: "tasks".to_owned().into(),
                path: vec![ProgramSourceRole::Alias(String::new())],
            },
            version_table: "tasks".to_owned().into(),
            source_row: RowUuid::from_bytes([0x39; 16]),
            version: fixture_version(),
        });
        assert!(program_fact_storage_bytes(&empty_alias).is_err());

        let mut trailing = encoded;
        trailing.push(0);
        assert!(program_fact_from_storage_bytes(&trailing).is_err());
    }

    #[test]
    fn program_fact_storage_codec_rejects_legacy_unknown_trailing_and_noncanonical_bytes() {
        let encoded =
            program_fact_storage_bytes(&ProgramFactEntry::PolicyDecision(PolicyDecisionEntry {
                decision: vec![],
                outcome: PolicyDecisionOutcomeEntry::Allowed,
                reason: None,
            }))
            .unwrap();
        assert!(
            program_fact_from_storage_bytes(
                &postcard::to_allocvec(&ProgramFactEntry::PolicyDecision(PolicyDecisionEntry {
                    decision: vec![],
                    outcome: PolicyDecisionOutcomeEntry::Allowed,
                    reason: None
                }))
                .unwrap()
            )
            .is_err()
        );
        let mut wrong_version = encoded.clone();
        wrong_version[4] += 1;
        assert!(program_fact_from_storage_bytes(&wrong_version).is_err());
        let mut unknown = encoded.clone();
        unknown[5] = 255;
        assert!(program_fact_from_storage_bytes(&unknown).is_err());
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(program_fact_from_storage_bytes(&trailing).is_err());
        let mut noncanonical =
            program_fact_storage_bytes(&ProgramFactEntry::PointRead(PointReadEntry {
                present: true,
                table: "facts".to_owned().into(),
                row: RowUuid::from_bytes([0; 16]),
                version: None,
                shape_id: ShapeId(uuid::Uuid::nil()),
                binding_id: BindingId(uuid::Uuid::nil()),
            }))
            .unwrap();
        noncanonical[6] = 2;
        assert!(program_fact_from_storage_bytes(&noncanonical).is_err());
    }

    #[test]
    fn nested_settled_result_values_are_canonical_groove_records_and_reject_corruption() {
        // This is intentionally an internal durable-boundary receipt: the
        // descriptor and synthetic identity bytes are not application values,
        // but they become part of persistent JPFK/JRME keys.  Pinning the
        // complete enclosing bytes below makes an accidental return to a
        // Rust-private serializer observable; these targeted corruptions prove
        // the production readers fail before accepting a different spelling.
        let descriptor = records::RecordDescriptor::new([
            ("name", records::ValueType::String),
            (
                "labels",
                records::ValueType::Array(Box::new(records::ValueType::String)),
            ),
            (
                "optional_count",
                records::ValueType::Nullable(Box::new(records::ValueType::U64)),
            ),
        ]);
        let descriptor_bytes = records::encode_record_descriptor(&descriptor).unwrap();
        assert_eq!(
            blake3::hash(&descriptor_bytes).to_hex().as_str(),
            "e7fcf66bb23dd514678c3b3960b69f020935d01a366c83d7b6fda963d2346e0a"
        );
        let decoded_descriptor = records::decode_record_descriptor(&descriptor_bytes).unwrap();
        assert_eq!(
            records::encode_record_descriptor(&decoded_descriptor).unwrap(),
            descriptor_bytes
        );
        let mut descriptor_trailing = descriptor_bytes.clone();
        descriptor_trailing.push(0);
        assert!(records::decode_record_descriptor(&descriptor_trailing).is_err());

        let payload = ResultMemberPayloadEntry {
            member: fixture_member(),
            descriptor: descriptor_bytes.clone(),
            record: descriptor
                .create(&[
                    Value::String("synth".to_owned()),
                    Value::Array(vec![Value::String("a".to_owned())]),
                    Value::Nullable(Some(Box::new(Value::U64(7)))),
                ])
                .unwrap(),
        };
        let encoded_fact =
            program_fact_storage_bytes(&ProgramFactEntry::ResultPayload(payload.clone())).unwrap();
        assert_eq!(
            blake3::hash(&encoded_fact).to_hex().as_str(),
            "266952cded111efe3209e4a478835693924f09c52d18b01293057d5307b2e3c8"
        );
        let descriptor_offset = encoded_fact
            .windows(payload.descriptor.len())
            .position(|window| window == payload.descriptor)
            .expect("payload descriptor appears once in its durable fact");
        let mut corrupt_descriptor = encoded_fact.clone();
        corrupt_descriptor[descriptor_offset] ^= 1;
        assert!(program_fact_from_storage_bytes(&corrupt_descriptor).is_err());

        let replacement = settled_value(Value::U8(2), records::ValueType::U8);
        let layout = result_member_storage_layout();
        let synthetic = ResultMemberSyntheticStorageRecord::encode(
            &result_member_case_descriptor(&layout.member_schema, RESULT_MEMBER_SYNTHETIC_TAG)
                .unwrap(),
            "facts".to_owned(),
            vec![0],
            replacement,
        )
        .unwrap()
        .record()
        .clone();
        let encoded_member = encode_result_member_envelope(
            RESULT_MEMBER_STORAGE_MAGIC,
            RESULT_MEMBER_STORAGE_VERSION,
            layout.member_envelope,
            records::EnumValue::new(RESULT_MEMBER_SYNTHETIC_TAG, synthetic),
        )
        .unwrap();
        assert!(result_member_from_storage_bytes(&encoded_member).is_err());
    }

    // These stay internal because exact physical key bytes and malformed
    // engine-owned payloads cannot be observed through Jazz's public API.
    // The ordinary persisted/reopen behavior is covered by the known-state
    // restart tests.
    #[test]
    fn result_member_storage_codec_has_permanent_tags_and_golden_bytes() {
        let root = ObjectId::from_uuid(uuid::Uuid::from_bytes([0x31; 16]));
        let joined = ObjectId::from_uuid(uuid::Uuid::from_bytes([0x32; 16]));
        let typed_key =
            ResultKey::from_union_occurrence(root, [joined], [(0, "direct".to_owned())]).unwrap();
        let members = [
            ResultMemberEntry::Row(ordinary_row()),
            ResultMemberEntry::Synthetic {
                table: "totals".to_owned(),
                row: settled_value(Value::U16(0x0201), records::ValueType::U16),
                replacement: SyntheticReplacementToken::from_encoded_record(settled_value(
                    Value::U16(0x0403),
                    records::ValueType::U16,
                )),
            },
            ResultMemberEntry::PathTuple {
                path: "owner".to_owned(),
                source_table: "todos".to_owned().into(),
                source_row: RowUuid::from_bytes([0x41; 16]),
                target_table: "users".to_owned().into(),
                target_row: RowUuid::from_bytes([0x42; 16]),
                edge_id: Some(vec![5, 6]),
                revision: vec![7, 8],
            },
            ResultMemberEntry::TypedRow {
                row: ordinary_row(),
                occurrence_key: typed_key,
            },
        ];
        let encoded = members
            .iter()
            .map(|member| result_member_storage_bytes(member).unwrap())
            .collect::<Vec<_>>();
        for (expected_tag, (member, encoded)) in members.iter().zip(&encoded).enumerate() {
            assert_eq!(&encoded[..4], RESULT_MEMBER_STORAGE_MAGIC);
            assert_eq!(encoded[4], RESULT_MEMBER_STORAGE_VERSION);
            assert_eq!(
                decode_result_member_envelope(
                    encoded,
                    RESULT_MEMBER_STORAGE_MAGIC,
                    RESULT_MEMBER_STORAGE_VERSION,
                    result_member_storage_layout().member_envelope,
                    "test member",
                )
                .unwrap()
                .tag(),
                u32::try_from(expected_tag).unwrap()
            );
            assert_eq!(result_member_from_storage_bytes(encoded).unwrap(), *member);
        }
        assert_eq!(
            encoded
                .iter()
                .map(|bytes| blake3::hash(bytes).to_hex().to_string())
                .collect::<Vec<_>>(),
            [
                "0e1d541c58211d93e04f7f53eff924c639ce7640989384430236be315222072b",
                "ef1b7386b6f019471e6b96f2c233d1862b933301c107bc5347db2786a720660a",
                "67b1049e3ab2654cac0076a2894e16f97667cee5bd9defa51f86dd4dd890fb49",
                "64d9489980bb8678717621a02b7518748e019a2a55104cbb9a0614498d7ed01f",
            ]
        );
    }

    #[test]
    fn result_member_storage_codec_round_trips_all_source_variants_and_rejects_v0() {
        let source = ResultRowSource::Merge {
            inputs: vec![
                ResultRowSource::Current,
                ResultRowSource::Snapshot {
                    snapshot: SnapshotRef {
                        owner: NodeUuid::from_bytes([0x51; 16]),
                        global_base: GlobalTime(11),
                        local_base: TxTime(12),
                        dots: vec![tx(13, 0x52), tx(14, 0x53)],
                    },
                },
                ResultRowSource::HistoryCut {
                    global_time: GlobalTime(15),
                },
                ResultRowSource::LensProjection {
                    schema_version: SchemaVersionId::from_bytes([0x54; 16]),
                    base: Box::new(ResultRowSource::Current),
                },
                ResultRowSource::Overlay {
                    tx: tx(16, 0x55),
                    base: Box::new(ResultRowSource::Current),
                },
            ],
        };
        let mut row = ordinary_row();
        row.source = source;
        row.layer = ResultRowLayer::ContentOrDeletion;
        row.deletion_tx = Some(tx(17, 0x56));
        row.schema_version = Some(SchemaVersionId::from_bytes([0x57; 16]));
        row.branch_or_prefix = Some(vec![1, 2, 3]);
        row.row_digest = Some(vec![4, 5, 6]);
        row.batch = Some(tx(18, 0x58));
        let member = ResultMemberEntry::Row(row);
        let encoded = result_member_storage_bytes(&member).unwrap();
        assert_eq!(result_member_from_storage_bytes(&encoded).unwrap(), member);

        let mut wrong_version = encoded.clone();
        wrong_version[4] = RESULT_MEMBER_STORAGE_VERSION + 1;
        assert!(result_member_from_storage_bytes(&wrong_version).is_err());
        assert!(result_member_from_storage_bytes(&[0, 1, 2, 3]).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(result_member_from_storage_bytes(&trailing).is_err());
    }

    #[test]
    fn result_row_source_storage_codec_has_permanent_tags_and_golden_bytes() {
        let sources = [
            (0_u32, ResultRowSource::Current),
            (
                1_u32,
                ResultRowSource::Snapshot {
                    snapshot: SnapshotRef {
                        owner: NodeUuid::from_bytes([0x61; 16]),
                        global_base: GlobalTime(21),
                        local_base: TxTime(22),
                        dots: vec![tx(23, 0x62)],
                    },
                },
            ),
            (
                2_u32,
                ResultRowSource::HistoryCut {
                    global_time: GlobalTime(24),
                },
            ),
            (
                3_u32,
                ResultRowSource::Merge {
                    inputs: vec![ResultRowSource::Current],
                },
            ),
            (
                4_u32,
                ResultRowSource::LensProjection {
                    schema_version: SchemaVersionId::from_bytes([0x63; 16]),
                    base: Box::new(ResultRowSource::Current),
                },
            ),
            (
                5_u32,
                ResultRowSource::Overlay {
                    tx: tx(25, 0x64),
                    base: Box::new(ResultRowSource::Current),
                },
            ),
        ];
        let encoded = sources
            .iter()
            .map(|(_, source)| result_row_source_storage_bytes(source, 0).unwrap())
            .collect::<Vec<_>>();
        for ((expected_tag, source), encoded) in sources.iter().zip(&encoded) {
            assert_eq!(&encoded[..4], RESULT_ROW_SOURCE_STORAGE_MAGIC);
            assert_eq!(encoded[4], RESULT_ROW_SOURCE_STORAGE_VERSION);
            assert_eq!(
                decode_result_member_envelope(
                    encoded,
                    RESULT_ROW_SOURCE_STORAGE_MAGIC,
                    RESULT_ROW_SOURCE_STORAGE_VERSION,
                    result_member_storage_layout().source_envelope,
                    "test source",
                )
                .unwrap()
                .tag(),
                *expected_tag
            );
            assert_eq!(
                result_row_source_from_storage_bytes(encoded, 0).unwrap(),
                *source
            );
        }
        assert_eq!(
            encoded
                .iter()
                .map(|bytes| blake3::hash(bytes).to_hex().to_string())
                .collect::<Vec<_>>(),
            [
                "c3615f39f699ab18ffe7c4290ae9b4f8e030c68c51b79745fa3b53960d4f74d7",
                "b2cd10a88cae72fb030756f53633cd03ecd1a5d624275d4c4aeb5dc6dcc5c425",
                "7d10fac91f6c2a8a9a340ed5e79cec1fc09552790ab7d60dad91a5e01dc369c7",
                "26c05aa7fabf7867acfbdfc37e3c27dc9c4fc64b2757a11b42800fe32fe1e54f",
                "fa9739e0513bdf9accde5901ddcf1a9a7beabd2f61a491cc2c627dfd30ad739d",
                "d07b06377c81e1dadee1cc2a148bf988f2fbdbcab79931016683e91a60eb252d",
            ],
        );
    }
}
