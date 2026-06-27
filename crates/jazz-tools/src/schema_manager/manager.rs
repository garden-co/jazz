//! Minimal SchemaManager compatibility shell.
//!
//! Server catalogue state now lives in `server::catalogue`; the old local
//! schema-lens runtime and catalogue rehydration engine have been removed.

use std::collections::HashMap;

use crate::object::ObjectId;
use crate::query_manager::types::{RowPolicyMode, Schema, SchemaHash, TableName, TablePolicies};

use super::context::{SchemaContext, SchemaError};
use super::types::AppId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PermissionsHeadSummary {
    pub schema_hash: SchemaHash,
    pub version: u64,
    pub parent_bundle_object_id: Option<ObjectId>,
    pub bundle_object_id: ObjectId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CurrentPermissionsSummary {
    pub head: PermissionsHeadSummary,
    pub permissions: HashMap<TableName, TablePolicies>,
}

pub struct SchemaManager {
    context: SchemaContext,
    app_id: AppId,
}

impl SchemaManager {
    pub fn new(
        schema: Schema,
        app_id: AppId,
        env: &str,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        Self::new_with_policy_mode(
            schema,
            app_id,
            env,
            user_branch,
            RowPolicyMode::PermissiveLocal,
        )
    }

    pub fn new_with_policy_mode(
        schema: Schema,
        app_id: AppId,
        env: &str,
        user_branch: &str,
        _row_policy_mode: RowPolicyMode,
    ) -> Result<Self, SchemaError> {
        Ok(Self {
            context: SchemaContext::new(strip_schema_policies(&schema), env, user_branch),
            app_id,
        })
    }

    pub fn with_defaults(
        schema: Schema,
        app_id: AppId,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        Self::new(schema, app_id, "dev", user_branch)
    }

    pub fn new_server(app_id: AppId, _env: &str) -> Self {
        Self {
            context: SchemaContext::empty(),
            app_id,
        }
    }

    pub fn has_current_schema(&self) -> bool {
        self.context.is_initialized()
    }

    pub fn app_id(&self) -> AppId {
        self.app_id
    }

    pub fn current_schema(&self) -> &Schema {
        &self.context.current_schema
    }

    pub fn current_hash(&self) -> SchemaHash {
        self.context.current_hash
    }

    pub fn env(&self) -> &str {
        &self.context.env
    }

    pub fn user_branch(&self) -> &str {
        &self.context.user_branch
    }

    pub fn context(&self) -> &SchemaContext {
        &self.context
    }
}

fn strip_schema_policies(schema: &Schema) -> Schema {
    schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut structural = table_schema.clone();
            structural.policies = TablePolicies::default();
            (*table_name, structural)
        })
        .collect()
}
