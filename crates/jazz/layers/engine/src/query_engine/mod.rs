#![allow(dead_code)]

//! Destination vocabulary for the unified Jazz query engine.
//!
//! The compiler boundary is deliberately smaller than the set of public
//! facades:
//!
//! 1. public query and relation builders normalize into one row-set shape,
//! 2. callers choose one read view and one policy context,
//! 3. callers request app rows, internal facts, or a policy decision,
//! 4. one lowering pass resolves sources, policy, relation semantics, sync
//!    coverage, payload witnesses, and transaction read tracking into Groove
//!    IVM graphs.
//!
//! Snapshot reads, live subscriptions, and sync scopes are lifecycles around
//! the same lowered row-set program. They do not participate in lowering keys.
//! Cached subscription results are likewise runtime state, not a read source.

use std::collections::{BTreeMap, BTreeSet};

use groove::db::GraphBuilder;
use groove::records::{RecordDescriptor, Value};
use groove::schema::ColumnType;

#[cfg(test)]
use crate::ids::RowUuid;
use crate::ids::{AuthorSubject, SchemaFamilyId, SchemaVersionId};
use crate::model::transaction::OpenTransactionId;
use crate::protocol::{BindingViewKey, BranchKey, RegisterShapeOptions, SnapshotRef};
use crate::query::{BindingId, Query, RecursionBound, RelationQuery, ShapeId};
use crate::schema::TableSchema;
use crate::time::GlobalTime;
use crate::tx::{DurabilityTier, Snapshot, TxId};

mod binding_values;
mod fields;
mod input;
mod lowering;
mod output;
mod policy;
mod publication;
mod read;
pub use binding_values::coerce_prepared_binding_value;
pub use publication::{
    CurrentRowBindingRole, CurrentRowPublicationField, CurrentRowResultVisibility,
};
mod resolver;
mod schemas;

pub use fields::*;
pub use input::*;
#[allow(unused_imports)]
pub use lowering::*;
pub use output::*;
pub use policy::*;
pub use read::*;
pub use resolver::*;
pub use schemas::*;

#[cfg(test)]
mod tests;
