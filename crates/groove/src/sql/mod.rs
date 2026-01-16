//! SQL layer for Jazz.
//!
//! See: docs/content/docs/internals/sql-layer.mdx

mod binary;
mod catalog;
mod database;
mod index;
mod lens;
mod parser;
mod policy;
pub mod query_graph;
mod row;
pub mod row_buffer;
mod schema;
mod table_rows;
mod types;

pub use binary::{
    DELTA_ADDED, DELTA_REMOVED, DELTA_UPDATED, encode_delta, encode_delta_batch, encode_owned_rows,
    encode_rows, encode_single_owned_row, encode_single_row,
};
pub use catalog::{
    Catalog, CatalogError, DescriptorId, SchemaConflictError, TableDescriptor, find_target_schema,
    get_lens_path,
};
pub use database::{
    ExecuteResult, IncrementalQuery, MigrationError, MigrationResult, QueryManager,
    QueryManagerError, QueryManagerState,
};
pub use index::RefIndex;
pub use lens::{
    ColumnTransform, DefaultValue, Lens, LensContext, LensError, LensGenerationOptions,
    LensGenerationResult, LensWarning, LensWarningKind, PotentialRename, QueryLensContext,
    RenameConfidence, SchemaDiff, SqlExpr, TypeChange, diff_schemas, generate_lens,
};
pub use parser::{
    Condition, ConditionValue, CreateTable, FromClause, Insert, Join, JoinCondition, ParseError,
    Projection, QualifiedColumn, Select, SelectExpr, Statement, Update, parse,
};
// Policy types are also re-exported from parser for convenience (via Statement::CreatePolicy)
#[cfg(feature = "sync-server")]
pub use policy::ViewerContext;
pub use policy::{
    EvalContext, Policy, PolicyAction, PolicyColumnRef, PolicyConfig, PolicyError, PolicyEvaluator,
    PolicyExpr, PolicyLookup, PolicyResult, PolicyValue, RowLookup, TablePolicies,
    clear_policy_warnings,
};
pub use query_graph::PredicateValue;
pub use row::RowError;
pub use row_buffer::{
    ColDescriptor, IdentifiedRow, OwnedRow, RowBuilder, RowDescriptor, RowRef, RowValue, join_rows,
    project_row,
};
pub use schema::{ColumnDef, ColumnType, SchemaError, TableSchema};
pub use table_rows::TableRows;
pub use types::{IndexKey, ObjectIdParseError, QueryState, SchemaId};
