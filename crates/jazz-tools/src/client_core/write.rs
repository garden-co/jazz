use std::collections::HashMap;

use crate::batch_fate::BatchMode;
use crate::object::ObjectId;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{ComposedBranchName, SchemaHash, Value};
use crate::row_histories::BatchId;
use crate::runtime_core::DirectInsertResult;

use super::{ClientError, ClientRuntimeHost, JazzClientCore};

#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    pub object_id: Option<ObjectId>,
    pub write_context: Option<WriteContext>,
    pub session: Option<Session>,
    pub attribution: Option<String>,
    pub updated_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct WriteBatchContextCore {
    mode: BatchMode,
    batch_id: BatchId,
    target_branch_name: String,
}

impl WriteBatchContextCore {
    fn new(mode: BatchMode, env: &str, schema_hash: SchemaHash, user_branch: &str) -> Self {
        Self {
            mode,
            batch_id: BatchId::new(),
            target_branch_name: ComposedBranchName::new(env, schema_hash, user_branch)
                .to_branch_name()
                .to_string(),
        }
    }

    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    pub fn mode(&self) -> BatchMode {
        self.mode
    }

    pub fn target_branch_name(&self) -> &str {
        &self.target_branch_name
    }
}

pub(crate) fn write_context(
    options: &WriteOptions,
    batch_context: Option<&WriteBatchContextCore>,
) -> Option<WriteContext> {
    if options.session.is_none()
        && options.write_context.is_none()
        && options.attribution.is_none()
        && options.updated_at.is_none()
        && batch_context.is_none()
    {
        return None;
    }

    let mut context = options
        .write_context
        .clone()
        .or_else(|| options.session.clone().map(WriteContext::from_session))
        .unwrap_or_default();

    if let Some(session) = options.session.clone() {
        context.session = Some(session);
    }
    if let Some(attribution) = options.attribution.clone() {
        context.attribution = Some(attribution);
    }
    if let Some(updated_at) = options.updated_at {
        context.updated_at = Some(updated_at);
    }

    if options.write_context.is_none() {
        context.attribution = options.attribution.clone();
        context.updated_at = options.updated_at;
    }

    if let Some(batch) = batch_context {
        context = context
            .with_batch_mode(batch.mode)
            .with_batch_id(batch.batch_id)
            .with_target_branch_name(batch.target_branch_name.clone());
    }

    Some(context)
}

fn runtime_error(error: impl ToString) -> ClientError {
    ClientError::new(error.to_string())
}

impl<H: ClientRuntimeHost> JazzClientCore<H> {
    fn insert_with_batch_context(
        &mut self,
        batch_context: Option<&WriteBatchContextCore>,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<DirectInsertResult, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, batch_context);
        self.with_runtime_mut(|runtime| {
            runtime.insert_with_id(table, values, options.object_id, context.as_ref())
        })
        .map_err(runtime_error)
    }

    fn update_with_batch_context(
        &mut self,
        batch_context: Option<&WriteBatchContextCore>,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        options: Option<WriteOptions>,
    ) -> Result<BatchId, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, batch_context);
        self.with_runtime_mut(|runtime| runtime.update(object_id, values, context.as_ref()))
            .map_err(runtime_error)
    }

    fn delete_with_batch_context(
        &mut self,
        batch_context: Option<&WriteBatchContextCore>,
        object_id: ObjectId,
        options: Option<WriteOptions>,
    ) -> Result<BatchId, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, batch_context);
        self.with_runtime_mut(|runtime| runtime.delete(object_id, context.as_ref()))
            .map_err(runtime_error)
    }

    pub(crate) fn insert_unsealed(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<DirectInsertResult, ClientError> {
        self.insert_with_batch_context(None, table, values, options)
    }

    pub(crate) fn update_unsealed(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        options: Option<WriteOptions>,
    ) -> Result<BatchId, ClientError> {
        self.update_with_batch_context(None, object_id, values, options)
    }

    pub(crate) fn delete_unsealed(
        &mut self,
        object_id: ObjectId,
        options: Option<WriteOptions>,
    ) -> Result<BatchId, ClientError> {
        self.delete_with_batch_context(None, object_id, options)
    }

    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<DirectInsertResult, ClientError> {
        let result = self.insert_unsealed(table, values, options)?;
        self.seal_batch(result.1)?;
        Ok(result)
    }

    pub fn update(
        &mut self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        options: Option<WriteOptions>,
    ) -> Result<BatchId, ClientError> {
        let batch_id = self.update_unsealed(object_id, values, options)?;
        self.seal_batch(batch_id)?;
        Ok(batch_id)
    }

    pub fn delete(
        &mut self,
        object_id: ObjectId,
        options: Option<WriteOptions>,
    ) -> Result<BatchId, ClientError> {
        let batch_id = self.delete_unsealed(object_id, options)?;
        self.seal_batch(batch_id)?;
        Ok(batch_id)
    }

    pub(crate) fn seal_batch(&mut self, batch_id: BatchId) -> Result<(), ClientError> {
        self.with_runtime_mut(|runtime| runtime.seal_batch(batch_id))
            .map_err(runtime_error)
    }

    pub(crate) fn begin_write_batch_context(&self, mode: BatchMode) -> WriteBatchContextCore {
        let schema = self.current_schema();
        let schema_hash = SchemaHash::compute(&schema);
        WriteBatchContextCore::new(
            mode,
            &self.config.env,
            schema_hash,
            &self.config.user_branch,
        )
    }
}
