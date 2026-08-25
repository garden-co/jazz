//! Durable physical identity metadata and Groove history-table lowering.

use super::*;
use crate::ids::{PhysicalColumnId, PhysicalTableId};
use crate::schema::ColumnSchema;
use groove::schema::{
    ColumnSchema as GrooveColumnSchema, IndexSchema as GrooveIndexSchema,
    TableSchema as GrooveTableSchema, TableVariantField as GrooveTableVariantField,
};

include!("physical/catalogue.rs");
include!("physical/names.rs");
include!("physical/bindings.rs");
include!("physical/enum_lookup.rs");
include!("physical/projections.rs");
include!("physical/storage.rs");
include!("physical/descriptors.rs");
include!("physical/tests.rs");
