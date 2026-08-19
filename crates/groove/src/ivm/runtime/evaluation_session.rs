//! Owned work and storage-request state for interruptible evaluation.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::storage::{
    Error as StorageError, KeyValue, OrderedKvStorage, OwnedStorage, StorageFuture,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum StorageRequestKey {
    #[allow(dead_code)] // Point-source conversion follows the scan-source slice.
    Get {
        family: String,
        key: Vec<u8>,
    },
    ScanRange {
        family: String,
        start: Vec<u8>,
        end: Vec<u8>,
    },
    ScanPrefix {
        family: String,
        prefix: Vec<u8>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum StorageRequestOutput {
    Value(Option<Vec<u8>>),
    Rows(Vec<KeyValue>),
}

#[derive(Default)]
pub(super) struct EvaluationInputs {
    loaded: BTreeMap<StorageRequestKey, StorageRequestOutput>,
}

impl EvaluationInputs {
    pub(super) fn rows(
        &mut self,
        key: StorageRequestKey,
    ) -> Result<&[KeyValue], super::IvmRuntimeError> {
        if !self.loaded.contains_key(&key) {
            return Err(super::IvmRuntimeError::EvaluationBlocked);
        }
        match self.loaded.get(&key).expect("loaded key checked") {
            StorageRequestOutput::Rows(rows) => Ok(rows),
            StorageRequestOutput::Value(_) => Err(super::IvmRuntimeError::UnsupportedOperator),
        }
    }

    pub(super) fn install(&mut self, ready: BTreeMap<StorageRequestKey, StorageRequestOutput>) {
        self.loaded.extend(ready);
    }
}

type PendingRequest<'a> = StorageFuture<'a, Result<StorageRequestOutput, StorageError>>;

/// One request registry is shared by every work entry in an evaluation
/// session. Equal semantic requests therefore have one future and one result.
pub(super) struct StorageRequests<'a> {
    pending: BTreeMap<StorageRequestKey, PendingRequest<'a>>,
    ready: BTreeMap<StorageRequestKey, Result<StorageRequestOutput, StorageError>>,
}

impl<'a> StorageRequests<'a> {
    pub(super) fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
            ready: BTreeMap::new(),
        }
    }

    /// Register a request if neither its future nor result already exists.
    /// Returns `true` only for the caller that created the in-flight work.
    pub(super) fn request<S>(&mut self, key: StorageRequestKey, storage: &OwnedStorage<S>) -> bool
    where
        S: OrderedKvStorage + 'a,
    {
        if self.pending.contains_key(&key) || self.ready.contains_key(&key) {
            return false;
        }
        let future = match &key {
            StorageRequestKey::Get { family, key } => {
                let future = storage.get(family.clone(), key.clone());
                Box::pin(async move { future.await.map(StorageRequestOutput::Value) })
                    as PendingRequest<'a>
            }
            StorageRequestKey::ScanRange { family, start, end } => {
                let future = storage.scan_range(family.clone(), start.clone(), end.clone());
                Box::pin(async move { future.await.map(StorageRequestOutput::Rows) })
                    as PendingRequest<'a>
            }
            StorageRequestKey::ScanPrefix { family, prefix } => {
                let future = storage.scan_prefix(family.clone(), prefix.clone());
                Box::pin(async move { future.await.map(StorageRequestOutput::Rows) })
                    as PendingRequest<'a>
            }
        };
        self.pending.insert(key, future);
        true
    }

    /// Advance every in-flight request once so one blocked source cannot hide
    /// a different request that is already resident.
    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> usize {
        let mut completed = Vec::new();
        for (key, request) in &mut self.pending {
            if let Poll::Ready(result) = Pin::new(request).poll(cx) {
                completed.push((key.clone(), result));
            }
        }
        let count = completed.len();
        for (key, result) in completed {
            self.pending.remove(&key);
            self.ready.insert(key, result);
        }
        count
    }

    #[cfg(test)]
    pub(super) fn take(
        &mut self,
        key: &StorageRequestKey,
    ) -> Option<Result<StorageRequestOutput, StorageError>> {
        self.ready.remove(key)
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(super) fn drain_ready(
        &mut self,
    ) -> Result<BTreeMap<StorageRequestKey, StorageRequestOutput>, StorageError> {
        let ready = std::mem::take(&mut self.ready);
        ready
            .into_iter()
            .map(|(key, value)| Ok((key, value?)))
            .collect()
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::task::Context;

    use futures::task::noop_waker;

    use super::*;
    use crate::storage::{TestStorage, TestStorageOperation};

    #[test]
    fn equal_requests_share_one_retained_future() {
        let (storage, control) = TestStorage::controlled(&["rows"]);
        control.pause_on(TestStorageOperation::Get);
        let storage = OwnedStorage::new(Rc::new(storage));
        let mut requests = StorageRequests::new();
        let key = StorageRequestKey::Get {
            family: "rows".to_owned(),
            key: b"one".to_vec(),
        };

        assert!(requests.request(key.clone(), &storage));
        assert!(!requests.request(key.clone(), &storage));
        assert_eq!(requests.pending_len(), 1);

        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert_eq!(requests.poll(&mut context), 0);
        assert_eq!(control.observed(), vec![TestStorageOperation::Get]);

        control.resume_operation(TestStorageOperation::Get);
        assert_eq!(requests.poll(&mut context), 1);
        assert_eq!(
            requests.take(&key).unwrap().unwrap(),
            StorageRequestOutput::Value(None)
        );
        assert_eq!(control.observed(), vec![TestStorageOperation::Get]);
    }
}
