//! Durable physical identity metadata and Groove history-table lowering.

use super::*;
use crate::ids::{PhysicalColumnId, PhysicalTableId};
use crate::protocol::PhysicalIdentityManifest;
use crate::schema::ColumnSchema;
use groove::schema::{
    ColumnSchema as GrooveColumnSchema, IndexSchema as GrooveIndexSchema,
    TableSchema as GrooveTableSchema, TableVariantField as GrooveTableVariantField,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum PhysicalWriteTarget {
    History,
    GlobalCurrent,
    AheadCurrent,
}

#[derive(Debug)]
pub(super) struct PreparedPhysicalWritePlan {
    pub(super) storage_table: String,
    pub(super) source_table: Arc<TableSchema>,
    #[cfg(test)]
    pub(super) source_mapping: Arc<TablePhysicalMapping>,
    #[cfg(test)]
    pub(super) physical_table: Arc<GrooveTableSchema>,
    pub(super) logical_descriptor: records::RecordDescriptor,
    pub(super) physical_descriptor: records::RecordDescriptor,
    history_descriptor: records::RecordDescriptor,
    write_fields: Vec<PhysicalWriteField>,
    enum_remaps: Vec<EnumOccurrenceRemaps>,
}

#[derive(Debug)]
enum PhysicalWriteField {
    Copy(usize),
    Decode(usize),
    Enum { source: usize, column: usize },
    CreatedAtMillis,
    UpdatedAtMillis,
    GlobalTime,
}

include!("physical/catalogue.rs");
include!("physical/names.rs");
include!("physical/bindings.rs");
include!("physical/enum_lookup.rs");
include!("physical/projections.rs");
include!("physical/storage.rs");
include!("physical/descriptors.rs");
include!("physical/tests.rs");
