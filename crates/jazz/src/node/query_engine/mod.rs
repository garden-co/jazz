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

use super::OpenTxId;
use crate::ids::{AuthorId, BranchId, RowUuid, SchemaVersionId};
use crate::protocol::{BindingViewKey, RegisterShapeOptions};
use crate::query::{BindingId, Query, RecursionBound, RelationQuery, ShapeId};
use crate::schema::TableSchema;
use crate::time::GlobalSeq;
use crate::tx::{DurabilityTier, Snapshot, TxId, TxKind};

mod input;
mod lowering;
mod output;
mod policy;
mod read;
mod resolver;
mod schemas;

pub(crate) use input::*;
#[allow(unused_imports)]
pub(crate) use lowering::*;
pub(crate) use output::*;
pub(crate) use policy::*;
pub(crate) use read::*;
pub(crate) use resolver::*;
pub(crate) use schemas::*;

#[cfg(test)]
mod tests;
