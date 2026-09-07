//! Pure Jazz query AST, validation, canonical form, bindings, and
//! content-addressed shape ids for the `jazz/SPEC/6_queries.md` contract. This module
//! owns syntax and schema-level validation only; execution, read-set recording,
//! and groove plan preparation live in [`crate::node::query_eval`], with emitted
//! view payloads assembled by [`crate::node::views`]. It sits above groove query
//! planning as Jazz's stable query vocabulary.

use std::collections::{BTreeMap, BTreeSet};

use groove::records::Value;
use groove::schema::ColumnType;
use thiserror::Error;

use crate::ids::SchemaVersionId;
use crate::schema::{ColumnSchema as JazzColumnSchema, JazzSchema, RuntimeSchema, TableSchema};

/// Whether the legacy `id` spelling resolves to a table's physical row UUID.
///
/// An authored `id` column always wins. The physical UUID is therefore only
/// available through the implicit spelling on older tables that have not
/// declared such a column.
pub(crate) fn is_implicit_row_id_alias(table: &TableSchema, column: &str) -> bool {
    column == "id" && !table.columns.iter().any(|candidate| candidate.name == "id")
}

// Stable public syntax and relation-facade vocabulary.
include!("query/ast.rs");
include!("query/relation_codec.rs");

// Relation-facade validation and name resolution into the stable query vocabulary.
// Executable lowering and normalization remain in `node::query_eval`.
include!("query/relation_resolution.rs");

// Fluent query builders.
include!("query/builders.rs");

// Policy expressions and stable public request contracts.
include!("query/policy_and_request_contracts.rs");

// Predicate and operand combinators.
include!("query/combinators.rs");

// Schema validation, parameter inference, and binding validation.
include!("query/validation.rs");

// Canonical request ordering, identity bytes, and normalized-facing contracts.
include!("query/canonical_request.rs");

include!("query/doctest_support.rs");
include!("query/tests.rs");
