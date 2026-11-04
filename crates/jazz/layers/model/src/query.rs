//! Pure Jazz query AST, validation, canonical form, bindings, and
//! content-addressed shape ids for the `jazz/SPEC/6_queries.md` contract. This module
//! owns syntax and schema-level validation only; `jazz-db` compiles queries to
//! SQL and runs them (`compile.rs`).

use std::collections::{BTreeMap, BTreeSet};

use jazz_types::column_schema::ColumnType;
use jazz_types::records::Value;
use thiserror::Error;

use crate::ids::SchemaVersionId;
use crate::schema::{ColumnSchema as JazzColumnSchema, JazzSchema, RuntimeSchema, TableSchema};

/// Semantic limits on recursively encoded policy expressions.
pub mod policy_limits;

// Stable public syntax and relation-facade vocabulary.
include!("query/ast.rs");
include!("query/relation_codec.rs");

// Relation-facade validation and name resolution into the stable query vocabulary.
include!("query/relation_resolution.rs");
include!("query/relation_gather.rs");
include!("query/relation_values.rs");

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
