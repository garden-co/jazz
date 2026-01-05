mod binary;
mod database;
mod index;
mod parser;
mod policy;
pub mod query_graph;
mod row;
mod schema;
mod table_rows;
mod types;

pub use binary::{
    encode_delta, encode_delta_batch, encode_delta_batch_with_nullability,
    encode_delta_with_nullability, encode_rows, encode_rows_with_nullability,
    encode_single_row, DELTA_ADDED, DELTA_REMOVED, DELTA_UPDATED,
};
pub use database::{Database, DatabaseError, DatabaseState, ExecuteResult, IncrementalQuery};
pub use index::RefIndex;
pub use parser::{
    Condition, ConditionValue, CreateTable, FromClause, Insert, Join, JoinCondition, ParseError,
    Projection, QualifiedColumn, Select, SelectExpr, Statement, Update, parse,
};
// Policy types are also re-exported from parser for convenience (via Statement::CreatePolicy)
pub use policy::{
    EvalContext, Policy, PolicyAction, PolicyColumnRef, PolicyConfig, PolicyError, PolicyEvaluator,
    PolicyExpr, PolicyLookup, PolicyResult, PolicyValue, RowLookup, TablePolicies,
    clear_policy_warnings,
};
pub use row::{Row, RowError, Value, decode_row, encode_row};
pub use schema::{ColumnDef, ColumnType, SchemaError, TableSchema};
pub use table_rows::TableRows;
pub use types::{IndexKey, ObjectIdParseError, QueryState, SchemaId};
