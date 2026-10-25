//! Jazz's database: the thread-affine `Db` facade over SQLite.
//!
//! Schema, query and policy types are Jazz's own (`jazz-model`); storage,
//! query execution, policies and subscription invalidation are SQLite.
//!
//! Model (alpha.60 linear history): one current table per Jazz table plus a
//! `__h` table of superseded images, a global commit `seq`, soft deletes.
//! Queries and policies compile to SQL; `Exists` policies are correlated
//! `EXISTS`, `Inherits` unrolls to its depth bound, relation `Gather` is
//! `WITH RECURSIVE`. Subscriptions re-run on commits that touch one of their
//! dependency tables and diff the result. The `jazz` crate re-exports this
//! crate's modules (`jazz::db`, `jazz::result_tree`).

// Sync and fan-out bookkeeping keys maps by (table, row, branch) tuples;
// naming each nesting would not make them clearer.
#![allow(clippy::type_complexity)]

// The layers below, under the paths this crate's code uses.
use jazz_model::{query, schema, tx};
use jazz_protocol::{protocol, wire};
use jazz_types::{ids, local_executor, object, records};

/// The public schema and policy vocabulary.
mod tools {
    pub use jazz_model::model::public_schema::{
        CmpOp, Operation, PolicyExpr, PolicyValue, TablePolicies, Value,
    };
    pub use jazz_model::model::transaction::OpenTransactionId;
}

mod array_index;
pub mod binding_args;
pub mod binding_codec;
pub mod binding_host;
pub mod compile;
pub mod db;
#[cfg(feature = "durable-object-limits")]
mod durable_object_limits;
pub mod foreground_node_lease;
pub(crate) mod large;
pub(crate) mod merge;
pub mod names;
pub mod options;
#[cfg(feature = "sql-trace")]
mod sql_trace;
pub mod sync_codec;
pub mod values;

#[cfg(feature = "testing")]
pub use large::full_materializations_for_test;

/// Structured read results.
pub mod result_tree {
    use std::collections::BTreeMap;

    use crate::values::CurrentRow;

    /// A structured read result.
    #[derive(Clone, Debug, PartialEq)]
    pub struct ResultTree {
        /// Root rows in query order.
        pub roots: Vec<ResultNode>,
    }

    /// One row and its named relation arrays.
    #[derive(Clone, Debug, PartialEq)]
    pub struct ResultNode {
        /// Projected row.
        pub row: CurrentRow,
        /// Named relation outputs.
        pub relations: BTreeMap<String, ResultRelation>,
    }

    /// One named relation output.
    #[derive(Clone, Debug, PartialEq)]
    pub enum ResultRelation {
        /// Ordered child rows.
        Array(Vec<ResultNode>),
        /// Explicit null.
        Null,
        /// Unresolvable target.
        Hole,
    }
}
