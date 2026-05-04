pub mod config;
pub mod error;
pub mod write;

#[cfg(test)]
mod tests;

pub use config::{ClientConfig, ClientRuntimeFlavor, ClientStorageMode};
pub use error::{ClientError, ClientErrorCode};
pub use write::{
    BatchWaitOutcome, DirectBatchCore, TransactionCore, WriteHandleCore, WriteOptions,
    WriteResultCore,
};

use crate::query_manager::types::Schema;
use crate::runtime_core::{RuntimeCore, Scheduler};
use crate::storage::Storage;

pub struct JazzClientCore<S: Storage, Sch: Scheduler> {
    config: ClientConfig,
    runtime: RuntimeCore<S, Sch>,
}

impl<S: Storage, Sch: Scheduler> JazzClientCore<S, Sch> {
    pub fn from_runtime_parts(
        config: ClientConfig,
        runtime: RuntimeCore<S, Sch>,
    ) -> Result<Self, ClientError> {
        Ok(Self { config, runtime })
    }

    pub fn current_schema(&self) -> &Schema {
        self.runtime.current_schema()
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn runtime(&self) -> &RuntimeCore<S, Sch> {
        &self.runtime
    }

    pub fn runtime_mut(&mut self) -> &mut RuntimeCore<S, Sch> {
        &mut self.runtime
    }
}
