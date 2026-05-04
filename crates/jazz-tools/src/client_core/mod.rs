pub mod config;
pub mod error;
pub mod query;
pub mod subscription;
pub mod write;

#[cfg(test)]
mod tests;

pub use config::{ClientConfig, ClientRuntimeFlavor, ClientStorageMode};
pub use error::{ClientError, ClientErrorCode};
pub use query::{ClientQueryOptions, QueryRowCore};
pub use subscription::SubscriptionCoreHandle;
pub use write::{
    BatchWaitOutcome, DirectBatchCore, TransactionCore, WriteHandleCore, WriteOptions,
    WriteResultCore,
};

use crate::query_manager::types::Schema;
use crate::runtime_core::{RuntimeCore, Scheduler};
use crate::storage::Storage;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

pub trait ClientRuntimeHost {
    type Storage: Storage;
    type Scheduler: Scheduler;

    fn with_runtime<T>(
        &self,
        f: impl FnOnce(&RuntimeCore<Self::Storage, Self::Scheduler>) -> T,
    ) -> T;

    fn with_runtime_mut<T>(
        &mut self,
        f: impl FnOnce(&mut RuntimeCore<Self::Storage, Self::Scheduler>) -> T,
    ) -> T;
}

pub struct OwnedRuntimeHost<S: Storage, Sch: Scheduler> {
    runtime: RuntimeCore<S, Sch>,
}

impl<S: Storage, Sch: Scheduler> OwnedRuntimeHost<S, Sch> {
    pub fn new(runtime: RuntimeCore<S, Sch>) -> Self {
        Self { runtime }
    }
}

impl<S: Storage, Sch: Scheduler> ClientRuntimeHost for OwnedRuntimeHost<S, Sch> {
    type Storage = S;
    type Scheduler = Sch;

    fn with_runtime<T>(&self, f: impl FnOnce(&RuntimeCore<S, Sch>) -> T) -> T {
        f(&self.runtime)
    }

    fn with_runtime_mut<T>(&mut self, f: impl FnOnce(&mut RuntimeCore<S, Sch>) -> T) -> T {
        f(&mut self.runtime)
    }
}

pub struct SharedRuntimeHost<S: Storage, Sch: Scheduler> {
    runtime: Arc<Mutex<RuntimeCore<S, Sch>>>,
}

impl<S: Storage, Sch: Scheduler> SharedRuntimeHost<S, Sch> {
    pub fn new(runtime: Arc<Mutex<RuntimeCore<S, Sch>>>) -> Self {
        Self { runtime }
    }
}

impl<S: Storage, Sch: Scheduler> ClientRuntimeHost for SharedRuntimeHost<S, Sch> {
    type Storage = S;
    type Scheduler = Sch;

    fn with_runtime<T>(&self, f: impl FnOnce(&RuntimeCore<S, Sch>) -> T) -> T {
        let guard = self.runtime.lock().expect("runtime lock poisoned");
        f(&guard)
    }

    fn with_runtime_mut<T>(&mut self, f: impl FnOnce(&mut RuntimeCore<S, Sch>) -> T) -> T {
        let mut guard = self.runtime.lock().expect("runtime lock poisoned");
        f(&mut guard)
    }
}

pub struct LocalRuntimeHost<S: Storage, Sch: Scheduler> {
    runtime: Rc<RefCell<RuntimeCore<S, Sch>>>,
}

impl<S: Storage, Sch: Scheduler> LocalRuntimeHost<S, Sch> {
    pub fn new(runtime: Rc<RefCell<RuntimeCore<S, Sch>>>) -> Self {
        Self { runtime }
    }
}

impl<S: Storage, Sch: Scheduler> ClientRuntimeHost for LocalRuntimeHost<S, Sch> {
    type Storage = S;
    type Scheduler = Sch;

    fn with_runtime<T>(&self, f: impl FnOnce(&RuntimeCore<S, Sch>) -> T) -> T {
        let guard = self.runtime.borrow();
        f(&guard)
    }

    fn with_runtime_mut<T>(&mut self, f: impl FnOnce(&mut RuntimeCore<S, Sch>) -> T) -> T {
        let mut guard = self.runtime.borrow_mut();
        f(&mut guard)
    }
}

pub struct JazzClientCore<H: ClientRuntimeHost> {
    config: ClientConfig,
    host: H,
}

impl<S: Storage, Sch: Scheduler> JazzClientCore<OwnedRuntimeHost<S, Sch>> {
    pub fn from_runtime_parts(
        config: ClientConfig,
        runtime: RuntimeCore<S, Sch>,
    ) -> Result<Self, ClientError> {
        Self::from_runtime_host(config, OwnedRuntimeHost::new(runtime))
    }
}

impl<H: ClientRuntimeHost> JazzClientCore<H> {
    pub fn from_runtime_host(config: ClientConfig, host: H) -> Result<Self, ClientError> {
        Ok(Self { config, host })
    }

    pub fn current_schema(&self) -> Schema {
        self.with_runtime(|runtime| runtime.current_schema().clone())
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn with_runtime<T>(
        &self,
        f: impl FnOnce(&RuntimeCore<H::Storage, H::Scheduler>) -> T,
    ) -> T {
        self.host.with_runtime(f)
    }

    pub fn with_runtime_mut<T>(
        &mut self,
        f: impl FnOnce(&mut RuntimeCore<H::Storage, H::Scheduler>) -> T,
    ) -> T {
        self.host.with_runtime_mut(f)
    }
}
