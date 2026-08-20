//! Owned work and storage-request state for interruptible evaluation.

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::schema::DatabaseSchema;
use crate::storage::{KeyValue, OwnedStorage, StorageFuture};

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
    IndexedRowsPrefix {
        table: String,
        index: String,
        prefix: Vec<u8>,
    },
    IndexedRowsRange {
        table: String,
        index: String,
        start: Vec<u8>,
        end: Vec<u8>,
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

type PendingRequest<'a> = StorageFuture<'a, Result<StorageRequestOutput, super::IvmRuntimeError>>;

/// One request registry is shared by every work entry in an evaluation
/// session. Equal semantic requests therefore have one future and one result.
pub(super) struct StorageRequests<'a> {
    pending: BTreeMap<StorageRequestKey, PendingRequest<'a>>,
    ready: BTreeMap<StorageRequestKey, Result<StorageRequestOutput, super::IvmRuntimeError>>,
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
    pub(super) fn request(
        &mut self,
        key: StorageRequestKey,
        storage: &OwnedStorage<'a>,
        schema: &DatabaseSchema,
    ) -> bool {
        if self.pending.contains_key(&key) || self.ready.contains_key(&key) {
            return false;
        }
        let future = match &key {
            StorageRequestKey::Get { family, key } => {
                let future = storage.get(family.clone(), key.clone());
                Box::pin(async move {
                    future
                        .await
                        .map(StorageRequestOutput::Value)
                        .map_err(Into::into)
                }) as PendingRequest<'a>
            }
            StorageRequestKey::ScanRange { family, start, end } => {
                let future = storage.scan_range(family.clone(), start.clone(), end.clone());
                Box::pin(async move {
                    future
                        .await
                        .map(StorageRequestOutput::Rows)
                        .map_err(Into::into)
                }) as PendingRequest<'a>
            }
            StorageRequestKey::ScanPrefix { family, prefix } => {
                let future = storage.scan_prefix(family.clone(), prefix.clone());
                Box::pin(async move {
                    future
                        .await
                        .map(StorageRequestOutput::Rows)
                        .map_err(Into::into)
                }) as PendingRequest<'a>
            }
            StorageRequestKey::IndexedRowsPrefix {
                table,
                index,
                prefix,
            } => {
                let table_schema = schema
                    .table(table)
                    .expect("compiled indexed-row source table exists")
                    .clone();
                let index_schema = table_schema
                    .indices
                    .iter()
                    .find(|candidate| candidate.name == *index)
                    .expect("compiled indexed-row source index exists")
                    .clone();
                let index = index.clone();
                let scan = storage.scan_prefix("indices".to_owned(), prefix.clone());
                let storage = storage.clone();
                Box::pin(async move {
                    let entries = scan.await?;
                    load_indexed_rows(storage, table_schema, index_schema, index, entries).await
                })
            }
            StorageRequestKey::IndexedRowsRange {
                table,
                index,
                start,
                end,
            } => {
                let table_schema = schema
                    .table(table)
                    .expect("compiled indexed-row source table exists")
                    .clone();
                let index_schema = table_schema
                    .indices
                    .iter()
                    .find(|candidate| candidate.name == *index)
                    .expect("compiled indexed-row source index exists")
                    .clone();
                let index = index.clone();
                let scan = storage.scan_range("indices".to_owned(), start.clone(), end.clone());
                let storage = storage.clone();
                Box::pin(async move {
                    let entries = scan.await?;
                    load_indexed_rows(storage, table_schema, index_schema, index, entries).await
                })
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
    ) -> Option<Result<StorageRequestOutput, super::IvmRuntimeError>> {
        self.ready.remove(key)
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(super) fn drain_ready(
        &mut self,
    ) -> Result<
        BTreeMap<StorageRequestKey, StorageRequestOutput>,
        Box<(StorageRequestKey, super::IvmRuntimeError)>,
    > {
        let ready = std::mem::take(&mut self.ready);
        ready
            .into_iter()
            .map(|(key, value)| match value {
                Ok(value) => Ok((key, value)),
                Err(error) => Err(Box::new((key, error))),
            })
            .collect()
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.pending.len()
    }
}

async fn load_indexed_rows(
    storage: OwnedStorage<'_>,
    table: crate::schema::TableSchema,
    index_schema: crate::schema::IndexSchema,
    index_name: String,
    entries: Vec<KeyValue>,
) -> Result<StorageRequestOutput, super::IvmRuntimeError> {
    let index_descriptor = crate::db::index_record_descriptor();
    let mut primary_keys = Vec::with_capacity(entries.len());
    for (storage_key, persisted_record) in entries {
        let index_record = index_descriptor.bind(&persisted_record);
        let stored_value = index_record
            .get("value")
            .map_err(super::IvmRuntimeError::RecordEncoding)?;
        let primary_key = crate::db::persisted_index_primary_key(
            &table,
            &index_name,
            &index_schema,
            &storage_key,
            &stored_value,
        )
        .map_err(|_| super::IvmRuntimeError::InvalidPersistedIndex(index_name.clone()))?;
        primary_keys.push(primary_key);
    }
    let mut reads = primary_keys
        .into_iter()
        .map(|primary_key| {
            let read = storage.get(table.name.clone(), primary_key.clone());
            (primary_key, read, None)
        })
        .collect::<Vec<_>>();
    let rows = poll_fn(|cx| {
        let mut all_ready = true;
        for (_, read, output) in &mut reads {
            if output.is_some() {
                continue;
            }
            match read.as_mut().poll(cx) {
                Poll::Ready(Ok(Some(record))) => *output = Some(record),
                Poll::Ready(Ok(None)) => {
                    return Poll::Ready(Err(super::IvmRuntimeError::InvalidPersistedIndex(
                        index_name.clone(),
                    )));
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Pending => all_ready = false,
            }
        }
        if !all_ready {
            return Poll::Pending;
        }
        Poll::Ready(Ok(reads
            .iter_mut()
            .map(|(key, _, output)| {
                (
                    key.clone(),
                    output.take().expect("all indexed row reads are ready"),
                )
            })
            .collect()))
    })
    .await?;
    Ok(StorageRequestOutput::Rows(rows))
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

        let schema = DatabaseSchema::new([]);
        assert!(requests.request(key.clone(), &storage, &schema));
        assert!(!requests.request(key.clone(), &storage, &schema));
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
