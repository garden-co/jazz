pub mod config;
pub mod error;
pub mod write;

#[cfg(test)]
mod tests;

pub use config::ClientConfig;
pub use error::ClientError;
pub use write::WriteOptions;

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

pub struct SharedRuntimeHost<S: Storage, Sch: Scheduler> {
    runtime: Arc<Mutex<RuntimeCore<S, Sch>>>,
}

impl<S: Storage, Sch: Scheduler> Clone for SharedRuntimeHost<S, Sch> {
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
        }
    }
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

impl<S: Storage, Sch: Scheduler> Clone for LocalRuntimeHost<S, Sch> {
    fn clone(&self) -> Self {
        Self {
            runtime: Rc::clone(&self.runtime),
        }
    }
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

impl<H: ClientRuntimeHost + Clone> Clone for JazzClientCore<H> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            host: self.host.clone(),
        }
    }
}

impl<H: ClientRuntimeHost> JazzClientCore<H> {
    pub fn from_runtime_host(config: ClientConfig, host: H) -> Self {
        Self { config, host }
    }

    pub(crate) fn current_schema(&self) -> Schema {
        self.with_runtime(|runtime| runtime.current_schema().clone())
    }

    pub(crate) fn with_runtime<T>(
        &self,
        f: impl FnOnce(&RuntimeCore<H::Storage, H::Scheduler>) -> T,
    ) -> T {
        self.host.with_runtime(f)
    }

    pub(crate) fn with_runtime_mut<T>(
        &mut self,
        f: impl FnOnce(&mut RuntimeCore<H::Storage, H::Scheduler>) -> T,
    ) -> T {
        self.host.with_runtime_mut(f)
    }
}
