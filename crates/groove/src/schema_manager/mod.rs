//! Schema Manager - Content-addressed schema evolution with bidirectional lenses.
//!
//! This module provides schema versioning through content-addressed hashing,
//! environment-based branch naming, and bidirectional migration lenses.
//!
//! # Overview
//!
//! - **SchemaHash**: Content-addressed schema identification (BLAKE3)
//! - **ComposedBranchName**: `{env}-{hash8}-{userBranch}` format
//! - **Lens**: Bidirectional row transformation between schema versions
//! - **SchemaContext**: Tracks current schema and live versions
//! - **SchemaManager**: Top-level coordination API
//! - **AppId**: Application identifier for catalogue queries
//!
//! # Example
//!
//! ```ignore
//! use groove::schema_manager::{AppId, SchemaManager};
//! use groove::query_manager::types::{SchemaBuilder, TableSchema, ColumnType};
//!
//! let app_id = AppId::from_name("my-app");
//! let schema = SchemaBuilder::new()
//!     .table(TableSchema::builder("users")
//!         .column("id", ColumnType::Uuid)
//!         .column("name", ColumnType::Text))
//!     .build();
//!
//! let mut manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main")?;
//!
//! // Add old schema version as live (auto-generates lens)
//! manager.add_live_schema(old_schema)?;
//!
//! // Persist schema and lens to catalogue
//! manager.persist_schema();
//! manager.persist_lens(&lens);
//!
//! // Get all branches for querying
//! let branches = manager.all_branches();
//! ```

pub mod auto_lens;
pub mod context;
pub mod encoding;
pub mod lens;
pub mod manager;
pub mod transformer;
pub mod types;
pub mod writer;

#[cfg(test)]
mod integration_tests;

// Re-exports
pub use auto_lens::generate_lens;
pub use context::{QuerySchemaContext, SchemaContext, SchemaError};
pub use encoding::{
    CatalogueEncodingError, decode_lens_transform, decode_schema, encode_lens_transform,
    encode_schema,
};
pub use lens::{Direction, Lens, LensOp, LensTransform};
pub use manager::SchemaManager;
pub use transformer::{
    LensTransformer, TransformError, TransformResult, translate_column_for_index,
};
pub use types::AppId;
pub use writer::{CopyOnWriteWriter, RowWriteInfo, WriteError, WriteResult};
