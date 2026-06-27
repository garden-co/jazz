//! Minimal schema context types retained for public API compatibility.

use serde::{Deserialize, Serialize};

use crate::object::ObjectId;
use crate::query_manager::types::{ComposedBranchName, Schema, SchemaHash};

/// Schema context for a query operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySchemaContext {
    pub env: String,
    pub schema_hash: SchemaHash,
    pub user_branch: String,
}

impl QuerySchemaContext {
    pub fn new(
        env: impl Into<String>,
        schema_hash: SchemaHash,
        user_branch: impl Into<String>,
    ) -> Self {
        Self {
            env: env.into(),
            schema_hash,
            user_branch: user_branch.into(),
        }
    }

    pub fn branch_name(&self) -> ComposedBranchName {
        ComposedBranchName::new(&self.env, self.schema_hash, &self.user_branch)
    }
}

/// Error type kept for catalogue/schema route glue.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaError {
    DraftLensInPath {
        source: SchemaHash,
        target: SchemaHash,
    },
    NoLensPath {
        source: SchemaHash,
        target: SchemaHash,
    },
    SchemaNotFound(SchemaHash),
    LensNotFound {
        source: SchemaHash,
        target: SchemaHash,
    },
    StalePermissionsParent {
        expected: Option<ObjectId>,
        current: Option<ObjectId>,
    },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::DraftLensInPath { source, target } => {
                write!(
                    f,
                    "Draft lens in path from {} to {}",
                    source.short(),
                    target.short()
                )
            }
            SchemaError::NoLensPath { source, target } => {
                write!(
                    f,
                    "No lens path from {} to {}",
                    source.short(),
                    target.short()
                )
            }
            SchemaError::SchemaNotFound(hash) => write!(f, "Schema not found: {}", hash.short()),
            SchemaError::LensNotFound { source, target } => {
                write!(
                    f,
                    "Lens not found: {} -> {}",
                    source.short(),
                    target.short()
                )
            }
            SchemaError::StalePermissionsParent { expected, current } => {
                write!(
                    f,
                    "stale permissions parent: expected {:?}, current {:?}",
                    expected, current
                )
            }
        }
    }
}

impl std::error::Error for SchemaError {}

/// Compatibility shell for callers that still construct a schema context.
#[derive(Debug, Clone)]
pub struct SchemaContext {
    pub current_schema: Schema,
    pub current_hash: SchemaHash,
    pub env: String,
    pub user_branch: String,
    is_initialized: bool,
}

impl SchemaContext {
    pub fn empty() -> Self {
        Self {
            current_schema: Schema::new(),
            current_hash: SchemaHash::from_bytes([0; 32]),
            env: String::new(),
            user_branch: String::new(),
            is_initialized: false,
        }
    }

    pub fn new(schema: Schema, env: &str, user_branch: &str) -> Self {
        let current_hash = SchemaHash::compute(&schema);
        Self {
            current_schema: schema,
            current_hash,
            env: env.to_string(),
            user_branch: user_branch.to_string(),
            is_initialized: true,
        }
    }

    pub fn with_defaults(schema: Schema, user_branch: &str) -> Self {
        Self::new(schema, "dev", user_branch)
    }

    pub fn is_initialized(&self) -> bool {
        self.is_initialized
    }
}
