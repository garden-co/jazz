use std::collections::HashMap;

use crate::batch_fate::{BatchMode, LocalBatchRecord};
use crate::object::ObjectId;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{ComposedBranchName, SchemaHash, Value};
use crate::row_histories::BatchId;
use crate::runtime_core::Scheduler;
use crate::storage::Storage;

use super::{ClientError, ClientErrorCode, JazzClientCore};

#[derive(Debug, Clone, PartialEq)]
pub struct ClientRow {
    pub id: ObjectId,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteHandleCore {
    pub batch_id: BatchId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriteResultCore {
    pub row: ClientRow,
    pub handle: WriteHandleCore,
}

#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    pub object_id: Option<ObjectId>,
    pub session: Option<Session>,
    pub attribution: Option<String>,
    pub updated_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct BatchContext {
    mode: BatchMode,
    batch_id: BatchId,
    target_branch_name: String,
}

impl BatchContext {
    fn new(mode: BatchMode, env: &str, schema_hash: SchemaHash, user_branch: &str) -> Self {
        Self {
            mode,
            batch_id: BatchId::new(),
            target_branch_name: ComposedBranchName::new(env, schema_hash, user_branch)
                .to_branch_name()
                .to_string(),
        }
    }
}

pub(crate) fn write_context(
    options: &WriteOptions,
    batch_context: Option<&BatchContext>,
) -> Option<WriteContext> {
    if options.session.is_none()
        && options.attribution.is_none()
        && options.updated_at.is_none()
        && batch_context.is_none()
    {
        return None;
    }

    let mut context = options
        .session
        .clone()
        .map(WriteContext::from_session)
        .unwrap_or_default();
    context.attribution = options.attribution.clone();
    context.updated_at = options.updated_at;

    if let Some(batch) = batch_context {
        context = context
            .with_batch_mode(batch.mode)
            .with_batch_id(batch.batch_id)
            .with_target_branch_name(batch.target_branch_name.clone());
    }

    Some(context)
}

impl<S: Storage, Sch: Scheduler> JazzClientCore<S, Sch> {
    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<WriteResultCore, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, None);
        let ((id, values), batch_id) = self
            .runtime_mut()
            .insert_with_id(table, values, options.object_id, context.as_ref())
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        self.runtime_mut()
            .seal_batch(batch_id)
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        Ok(WriteResultCore {
            row: ClientRow { id, values },
            handle: WriteHandleCore { batch_id },
        })
    }

    pub fn local_batch_record(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<LocalBatchRecord>, ClientError> {
        self.runtime()
            .local_batch_record(batch_id)
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))
    }

    pub fn begin_direct_batch(&mut self) -> DirectBatchCore<'_, S, Sch> {
        let schema_hash = SchemaHash::compute(self.current_schema());
        let context = BatchContext::new(
            BatchMode::Direct,
            &self.config().env,
            schema_hash,
            &self.config().user_branch,
        );

        DirectBatchCore {
            client: self,
            context,
        }
    }
}

pub struct DirectBatchCore<'a, S: Storage, Sch: Scheduler> {
    client: &'a mut JazzClientCore<S, Sch>,
    context: BatchContext,
}

impl<'a, S: Storage, Sch: Scheduler> DirectBatchCore<'a, S, Sch> {
    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<WriteResultCore, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, Some(&self.context));
        let ((id, values), batch_id) = self
            .client
            .runtime_mut()
            .insert_with_id(table, values, options.object_id, context.as_ref())
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        Ok(WriteResultCore {
            row: ClientRow { id, values },
            handle: WriteHandleCore { batch_id },
        })
    }

    pub fn commit(self) -> Result<WriteHandleCore, ClientError> {
        self.client
            .runtime_mut()
            .seal_batch(self.context.batch_id)
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        Ok(WriteHandleCore {
            batch_id: self.context.batch_id,
        })
    }
}
