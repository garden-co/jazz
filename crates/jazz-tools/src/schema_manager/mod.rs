//! Thin schema/catalogue API glue.
//!
//! The old schema-lens runtime, auto-lens generation, catalogue rehydrate, and
//! schema-manager storage engine have been removed. What remains here is the
//! small public surface still used by server routes and client setup.

pub mod context;
pub mod encoding;
pub mod lens;
pub mod manager;
pub mod types;

pub use context::{QuerySchemaContext, SchemaContext, SchemaError};
pub use encoding::{
    CatalogueEncodingError, decode_lens_transform, decode_permissions, decode_schema,
    encode_lens_transform, encode_permissions, encode_schema,
};
pub use lens::{Direction, Lens, LensOp, LensTransform};
pub use manager::{CurrentPermissionsSummary, PermissionsHeadSummary, SchemaManager};
pub use types::AppId;
