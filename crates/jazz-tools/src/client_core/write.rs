use std::collections::HashMap;

use crate::batch_fate::{BatchMode, BatchSettlement, LocalBatchRecord};
use crate::object::ObjectId;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{ComposedBranchName, SchemaHash, Value};
use crate::row_histories::BatchId;
use crate::sync_manager::DurabilityTier;

use super::{ClientError, ClientErrorCode, ClientRuntimeHost, JazzClientCore};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchWaitOutcome {
    Pending,
    Satisfied,
    Rejected { code: String, reason: String },
    Missing,
}

#[derive(Debug, Clone)]
pub struct WriteBatchContextCore {
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
}

pub(crate) fn write_context(
    options: &WriteOptions,
    batch_context: Option<&WriteBatchContextCore>,
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

fn tier_rank(tier: DurabilityTier) -> u8 {
    match tier {
        DurabilityTier::Local => 0,
        DurabilityTier::EdgeServer => 1,
        DurabilityTier::GlobalServer => 2,
    }
}

fn settlement_satisfies_tier(
    settlement: Option<&BatchSettlement>,
    tier: DurabilityTier,
) -> BatchWaitOutcome {
    match settlement {
        Some(BatchSettlement::Rejected { code, reason, .. }) => BatchWaitOutcome::Rejected {
            code: code.clone(),
            reason: reason.clone(),
        },
        Some(BatchSettlement::DurableDirect { confirmed_tier, .. })
        | Some(BatchSettlement::AcceptedTransaction { confirmed_tier, .. })
            if tier_rank(*confirmed_tier) >= tier_rank(tier) =>
        {
            BatchWaitOutcome::Satisfied
        }
        Some(_) | None => BatchWaitOutcome::Pending,
    }
}

impl<H: ClientRuntimeHost> JazzClientCore<H> {
    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<WriteResultCore, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, None);
        let ((id, values), batch_id) = self
            .with_runtime_mut(|runtime| {
                runtime.insert_with_id(table, values, options.object_id, context.as_ref())
            })
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        self.with_runtime_mut(|runtime| runtime.seal_batch(batch_id))
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
        self.with_runtime(|runtime| runtime.local_batch_record(batch_id))
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))
    }

    pub fn begin_direct_batch_context(&self) -> WriteBatchContextCore {
        let schema = self.current_schema();
        let schema_hash = SchemaHash::compute(&schema);
        WriteBatchContextCore::new(
            BatchMode::Direct,
            &self.config().env,
            schema_hash,
            &self.config().user_branch,
        )
    }

    pub fn begin_direct_batch(&mut self) -> DirectBatchCore<'_, H> {
        let context = self.begin_direct_batch_context();

        DirectBatchCore {
            client: self,
            context,
        }
    }

    pub fn begin_transaction_context(&self) -> WriteBatchContextCore {
        let schema = self.current_schema();
        let schema_hash = SchemaHash::compute(&schema);
        WriteBatchContextCore::new(
            BatchMode::Transactional,
            &self.config().env,
            schema_hash,
            &self.config().user_branch,
        )
    }

    pub fn begin_transaction(&mut self) -> TransactionCore<'_, H> {
        let context = self.begin_transaction_context();

        TransactionCore {
            client: self,
            context,
        }
    }

    pub fn insert_in_batch(
        &mut self,
        batch_context: &WriteBatchContextCore,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<WriteResultCore, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, Some(batch_context));
        let ((id, values), batch_id) = self
            .with_runtime_mut(|runtime| {
                runtime.insert_with_id(table, values, options.object_id, context.as_ref())
            })
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        Ok(WriteResultCore {
            row: ClientRow { id, values },
            handle: WriteHandleCore { batch_id },
        })
    }

    pub fn update_in_batch(
        &mut self,
        batch_context: &WriteBatchContextCore,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        options: Option<WriteOptions>,
    ) -> Result<WriteHandleCore, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, Some(batch_context));
        let batch_id = self
            .with_runtime_mut(|runtime| runtime.update(object_id, values, context.as_ref()))
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        Ok(WriteHandleCore { batch_id })
    }

    pub fn delete_in_batch(
        &mut self,
        batch_context: &WriteBatchContextCore,
        object_id: ObjectId,
        options: Option<WriteOptions>,
    ) -> Result<WriteHandleCore, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, Some(batch_context));
        let batch_id = self
            .with_runtime_mut(|runtime| runtime.delete(object_id, context.as_ref()))
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        Ok(WriteHandleCore { batch_id })
    }

    pub fn commit_batch_context(
        &mut self,
        batch_context: WriteBatchContextCore,
    ) -> Result<WriteHandleCore, ClientError> {
        self.with_runtime_mut(|runtime| runtime.seal_batch(batch_context.batch_id))
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        Ok(WriteHandleCore {
            batch_id: batch_context.batch_id,
        })
    }

    pub fn check_batch_wait(&self, batch_id: BatchId, tier: DurabilityTier) -> BatchWaitOutcome {
        let record = match self.with_runtime(|runtime| runtime.local_batch_record(batch_id)) {
            Ok(Some(record)) => record,
            Ok(None) => return BatchWaitOutcome::Missing,
            Err(error) => {
                return BatchWaitOutcome::Rejected {
                    code: "storage_error".to_string(),
                    reason: error.to_string(),
                };
            }
        };

        if tier == DurabilityTier::Local && record.sealed {
            return BatchWaitOutcome::Satisfied;
        }

        settlement_satisfies_tier(record.latest_settlement.as_ref(), tier)
    }
}

pub struct DirectBatchCore<'a, H: ClientRuntimeHost> {
    client: &'a mut JazzClientCore<H>,
    context: WriteBatchContextCore,
}

impl<'a, H: ClientRuntimeHost> DirectBatchCore<'a, H> {
    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<WriteResultCore, ClientError> {
        self.client
            .insert_in_batch(&self.context, table, values, options)
    }

    pub fn commit(self) -> Result<WriteHandleCore, ClientError> {
        self.client.commit_batch_context(self.context)
    }
}

pub struct TransactionCore<'a, H: ClientRuntimeHost> {
    client: &'a mut JazzClientCore<H>,
    context: WriteBatchContextCore,
}

impl<'a, H: ClientRuntimeHost> TransactionCore<'a, H> {
    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<WriteResultCore, ClientError> {
        self.client
            .insert_in_batch(&self.context, table, values, options)
    }

    pub fn commit(self) -> Result<WriteHandleCore, ClientError> {
        self.client.commit_batch_context(self.context)
    }
}
