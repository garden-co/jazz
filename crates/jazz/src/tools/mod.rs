// The data-model and identity modules below moved out of `tools` so the
// lower layers no longer reach up into the public API module; they stay
// reachable at their old `tools::` paths.
#[doc(hidden)]
pub use crate::model::admin_catalogue_row_format;
pub(crate) use crate::model::public_api;
#[doc(hidden)]
pub use crate::model::public_schema_convert;
pub use crate::model::{metadata, policy_claims, public_schema, transaction};
pub use crate::{app_id, identity};
#[doc(hidden)]
pub mod deployment;
/// Target-shell factory boundary for native peer transports.
pub mod schema_lens;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub mod transport_error;
pub mod websocket_prelude_auth;

pub use app_id::AppId;
pub use public_api::session::unverified_jwt_scope_subject;
pub use public_schema::{
    AuthMode, CmpOp, ColumnDescriptor, ColumnMergeStrategy, ColumnType, Operation, OrderedRowDelta,
    PolicyExpr, PolicyValue, QueryResult, QueryResultField, Row, RowDelta, RowDescriptor, Schema,
    SchemaBuilder, SchemaHash, Session, TableName, TablePolicies, TableSchema, TableSchemaBuilder,
    TransactionId, Value, WriteContext, permissions, policy_expr,
};
pub use schema_lens::{Direction, Lens, LensOp, LensTransform};
pub use transaction::OpenTransactionId;

pub use crate::object::{BranchName, ObjectId, ResultKey};
