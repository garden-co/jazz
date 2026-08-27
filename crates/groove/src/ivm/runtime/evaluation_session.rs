//! Owned work and request state for interruptible evaluation.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::schema::DatabaseSchema;
use crate::storage::{KeyValue, OwnedStorage, ScanRequest, StorageFuture};
use crate::{chunks::ChunkLease, chunks::ChunkRequest, chunks::OwnedChunkProvider};

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
    ScanPrefixLimit {
        family: String,
        prefix: Vec<u8>,
        max_items: usize,
    },
    IndexedRowsPrefix {
        table: String,
        index: String,
        prefix: Vec<u8>,
    },
    IndexedRowsPrefixLimit {
        table: String,
        index: String,
        prefix: Vec<u8>,
        max_items: usize,
    },
    IndexedRowsIntersection {
        table: String,
        index: String,
        prefix: Vec<u8>,
        intersections: Vec<(String, Vec<u8>)>,
    },
    IndexedRowsRange {
        table: String,
        index: String,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum EvaluationRequestKey {
    Storage(StorageRequestKey),
    Chunk(ChunkRequest),
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum StorageRequestOutput {
    Value(Option<Vec<u8>>),
    Rows(Vec<KeyValue>),
}

#[derive(Debug)]
pub(super) enum EvaluationRequestOutput {
    Storage(StorageRequestOutput),
    Chunk(LoadedChunk),
}

#[derive(Debug)]
pub(crate) struct LoadedChunk {
    bytes: LoadedChunkBytes,
}

#[derive(Debug)]
enum LoadedChunkBytes {
    Direct(bytes::Bytes),
    Leased(ChunkLease),
}

impl LoadedChunkBytes {
    fn bytes(&self) -> &bytes::Bytes {
        match self {
            Self::Direct(bytes) => bytes,
            Self::Leased(lease) => lease.bytes(),
        }
    }
}

#[derive(Default)]
pub(crate) struct EvaluationInputs {
    loaded: BTreeMap<EvaluationRequestKey, EvaluationRequestOutput>,
    missing: BTreeSet<EvaluationRequestKey>,
}

impl EvaluationInputs {
    pub(crate) fn release_chunks(&mut self) {
        self.loaded
            .retain(|key, _| !matches!(key, EvaluationRequestKey::Chunk(_)));
    }

    pub(crate) fn take_missing_chunks(&mut self) -> Vec<ChunkRequest> {
        let missing = self.take_missing();
        missing
            .into_iter()
            .filter_map(|request| match request {
                EvaluationRequestKey::Chunk(request) => Some(request),
                EvaluationRequestKey::Storage(_) => None,
            })
            .collect()
    }

    #[allow(dead_code)] // Also used by private large-write continuations.
    pub(crate) fn install_chunk(&mut self, request: ChunkRequest, bytes: bytes::Bytes) {
        self.loaded.insert(
            EvaluationRequestKey::Chunk(request.clone()),
            EvaluationRequestOutput::Chunk(LoadedChunk {
                bytes: LoadedChunkBytes::Direct(bytes),
            }),
        );
    }

    pub(crate) fn install_chunk_from_provider(&mut self, request: ChunkRequest, bytes: ChunkLease) {
        self.loaded.insert(
            EvaluationRequestKey::Chunk(request.clone()),
            EvaluationRequestOutput::Chunk(LoadedChunk {
                bytes: LoadedChunkBytes::Leased(bytes),
            }),
        );
    }

    #[allow(dead_code)] // Used by indirect scalar node steps introduced in phase 2.
    pub(crate) fn chunk(
        &mut self,
        request: ChunkRequest,
    ) -> Result<&bytes::Bytes, super::IvmRuntimeError> {
        let key = EvaluationRequestKey::Chunk(request);
        if !self.loaded.contains_key(&key) {
            self.missing.insert(key);
            return Err(super::IvmRuntimeError::EvaluationBlocked);
        }
        match self.loaded.get(&key).expect("loaded key checked") {
            EvaluationRequestOutput::Chunk(chunk) => Ok(chunk.bytes.bytes()),
            EvaluationRequestOutput::Storage(_) => Err(super::IvmRuntimeError::UnsupportedOperator),
        }
    }

    pub(super) fn rows(
        &mut self,
        key: StorageRequestKey,
    ) -> Result<&[KeyValue], super::IvmRuntimeError> {
        let key = EvaluationRequestKey::Storage(key);
        if !self.loaded.contains_key(&key) {
            self.missing.insert(key);
            return Err(super::IvmRuntimeError::EvaluationBlocked);
        }
        match self.loaded.get(&key).expect("loaded key checked") {
            EvaluationRequestOutput::Storage(StorageRequestOutput::Rows(rows)) => Ok(rows),
            EvaluationRequestOutput::Storage(StorageRequestOutput::Value(_)) => {
                Err(super::IvmRuntimeError::UnsupportedOperator)
            }
            EvaluationRequestOutput::Chunk(_) => Err(super::IvmRuntimeError::UnsupportedOperator),
        }
    }

    pub(super) fn install(
        &mut self,
        ready: BTreeMap<EvaluationRequestKey, EvaluationRequestOutput>,
    ) {
        self.loaded.extend(ready);
    }

    pub(super) fn take_missing(&mut self) -> BTreeSet<EvaluationRequestKey> {
        std::mem::take(&mut self.missing)
    }
}

type PendingRequest<'a> =
    StorageFuture<'a, Result<EvaluationRequestOutput, EvaluationRequestFailure>>;

#[derive(Debug)]
pub(super) struct EvaluationRequestFailure {
    pub(super) error: super::IvmRuntimeError,
    pub(super) publication_metadata_durability: bool,
}

impl From<super::IvmRuntimeError> for EvaluationRequestFailure {
    fn from(error: super::IvmRuntimeError) -> Self {
        Self {
            error,
            publication_metadata_durability: false,
        }
    }
}

impl From<crate::storage::Error> for EvaluationRequestFailure {
    fn from(error: crate::storage::Error) -> Self {
        super::IvmRuntimeError::from(error).into()
    }
}

/// One request registry is shared by every work entry in an evaluation
/// session. Equal semantic requests therefore have one future and one result.
pub(super) struct EvaluationRequests<'a> {
    pending: BTreeMap<EvaluationRequestKey, PendingRequest<'a>>,
    ready:
        BTreeMap<EvaluationRequestKey, Result<EvaluationRequestOutput, EvaluationRequestFailure>>,
}

impl<'a> EvaluationRequests<'a> {
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
        key: EvaluationRequestKey,
        storage: &OwnedStorage<'a>,
        chunks: Option<&OwnedChunkProvider>,
        schema: &DatabaseSchema,
    ) -> bool {
        if self.pending.contains_key(&key) || self.ready.contains_key(&key) {
            return false;
        }
        let future = match &key {
            EvaluationRequestKey::Storage(StorageRequestKey::Get { family, key }) => {
                let future = storage.get(family.clone(), key.clone());
                Box::pin(async move {
                    future
                        .await
                        .map(StorageRequestOutput::Value)
                        .map(EvaluationRequestOutput::Storage)
                        .map_err(Into::into)
                }) as PendingRequest<'a>
            }
            EvaluationRequestKey::Storage(StorageRequestKey::ScanRange { family, start, end }) => {
                let future = storage.scan(ScanRequest::range(
                    family.clone(),
                    start.clone(),
                    end.clone(),
                ));
                Box::pin(async move {
                    future
                        .await
                        .map(StorageRequestOutput::Rows)
                        .map(EvaluationRequestOutput::Storage)
                        .map_err(Into::into)
                }) as PendingRequest<'a>
            }
            EvaluationRequestKey::Storage(StorageRequestKey::ScanPrefix { family, prefix }) => {
                let future = storage.scan(ScanRequest::prefix(family.clone(), prefix.clone()));
                Box::pin(async move {
                    future
                        .await
                        .map(StorageRequestOutput::Rows)
                        .map(EvaluationRequestOutput::Storage)
                        .map_err(Into::into)
                }) as PendingRequest<'a>
            }
            EvaluationRequestKey::Storage(StorageRequestKey::ScanPrefixLimit {
                family,
                prefix,
                max_items,
            }) => {
                let future = storage.scan(
                    ScanRequest::prefix(family.clone(), prefix.clone()).with_max_items(*max_items),
                );
                Box::pin(async move {
                    future
                        .await
                        .map(StorageRequestOutput::Rows)
                        .map(EvaluationRequestOutput::Storage)
                        .map_err(Into::into)
                }) as PendingRequest<'a>
            }
            EvaluationRequestKey::Storage(StorageRequestKey::IndexedRowsPrefix {
                table,
                index,
                prefix,
            }) => {
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
                let scan = storage.scan(ScanRequest::prefix("indices".to_owned(), prefix.clone()));
                let storage = storage.clone();
                Box::pin(async move {
                    let entries = scan.await?;
                    load_indexed_rows(storage, table_schema, index_schema, index, entries)
                        .await
                        .map(EvaluationRequestOutput::Storage)
                        .map_err(Into::into)
                })
            }
            EvaluationRequestKey::Storage(StorageRequestKey::IndexedRowsPrefixLimit {
                table,
                index,
                prefix,
                max_items,
            }) => {
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
                let scan = storage.scan(
                    ScanRequest::prefix("indices".to_owned(), prefix.clone())
                        .with_max_items(*max_items),
                );
                let storage = storage.clone();
                Box::pin(async move {
                    let entries = scan.await?;
                    load_indexed_rows(storage, table_schema, index_schema, index, entries)
                        .await
                        .map(EvaluationRequestOutput::Storage)
                        .map_err(Into::into)
                })
            }
            EvaluationRequestKey::Storage(StorageRequestKey::IndexedRowsIntersection {
                table,
                index,
                prefix,
                intersections,
            }) => {
                let table_schema = schema
                    .table(table)
                    .expect("compiled intersected index source table exists")
                    .clone();
                let primary_index_schema = table_schema
                    .indices
                    .iter()
                    .find(|candidate| candidate.name == *index)
                    .expect("compiled intersected index source exists")
                    .clone();
                let index = index.clone();
                let prefix = prefix.clone();
                let intersections = intersections.clone();
                let storage = storage.clone();
                Box::pin(async move {
                    let mut entries = storage
                        .scan(ScanRequest::prefix("indices".to_owned(), prefix))
                        .await?;
                    let primary_keys = indexed_primary_keys(
                        &table_schema,
                        &index,
                        &primary_index_schema,
                        &entries,
                    )?;
                    let mut retained = primary_keys.iter().cloned().collect::<HashSet<_>>();
                    for (other_index, other_prefix) in intersections {
                        let other_schema = table_schema
                            .indices
                            .iter()
                            .find(|candidate| candidate.name == other_index)
                            .expect("compiled intersected index exists");
                        let other_entries = storage
                            .scan(ScanRequest::prefix("indices".to_owned(), other_prefix))
                            .await?;
                        let other_keys = indexed_primary_keys(
                            &table_schema,
                            &other_index,
                            other_schema,
                            &other_entries,
                        )?
                        .into_iter()
                        .collect::<HashSet<_>>();
                        retained.retain(|key| other_keys.contains(key));
                    }
                    entries = entries
                        .into_iter()
                        .zip(primary_keys)
                        .filter_map(|(entry, key)| retained.contains(&key).then_some(entry))
                        .collect();
                    load_indexed_rows(storage, table_schema, primary_index_schema, index, entries)
                        .await
                        .map(EvaluationRequestOutput::Storage)
                        .map_err(Into::into)
                })
            }
            EvaluationRequestKey::Storage(StorageRequestKey::IndexedRowsRange {
                table,
                index,
                start,
                end,
            }) => {
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
                let scan = storage.scan(ScanRequest::range(
                    "indices".to_owned(),
                    start.clone(),
                    end.clone(),
                ));
                let storage = storage.clone();
                Box::pin(async move {
                    let entries = scan.await?;
                    load_indexed_rows(storage, table_schema, index_schema, index, entries)
                        .await
                        .map(EvaluationRequestOutput::Storage)
                        .map_err(Into::into)
                })
            }
            EvaluationRequestKey::Chunk(request) => {
                let request = request.clone();
                match chunks {
                    Some(chunks) => {
                        let future = chunks.get_tracked(request.clone());
                        Box::pin(async move {
                            match future.await {
                                Ok(bytes) => Ok(EvaluationRequestOutput::Chunk(LoadedChunk {
                                    bytes: LoadedChunkBytes::Leased(bytes),
                                })),
                                Err(error) => {
                                    let (error, publication_metadata_durability) =
                                        error.into_parts();
                                    Err(EvaluationRequestFailure {
                                        error: super::IvmRuntimeError::Chunk(error),
                                        publication_metadata_durability,
                                    })
                                }
                            }
                        }) as PendingRequest<'a>
                    }
                    None => Box::pin(async {
                        Err(EvaluationRequestFailure::from(
                            super::IvmRuntimeError::Chunk(crate::chunks::ChunkError::Unavailable),
                        ))
                    }) as PendingRequest<'a>,
                }
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
        key: &EvaluationRequestKey,
    ) -> Option<Result<EvaluationRequestOutput, EvaluationRequestFailure>> {
        self.ready.remove(key)
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(super) fn drain_ready(
        &mut self,
    ) -> Result<
        BTreeMap<EvaluationRequestKey, EvaluationRequestOutput>,
        Box<(EvaluationRequestKey, EvaluationRequestFailure)>,
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
    let primary_keys = indexed_primary_keys(&table, &index_name, &index_schema, &entries)?;
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

fn indexed_primary_keys(
    table: &crate::schema::TableSchema,
    index_name: &str,
    index_schema: &crate::schema::IndexSchema,
    entries: &[KeyValue],
) -> Result<Vec<Vec<u8>>, super::IvmRuntimeError> {
    let index_descriptor = crate::db::index_record_descriptor();
    let mut primary_keys = Vec::with_capacity(entries.len());
    for (storage_key, persisted_record) in entries {
        let index_record = index_descriptor.bind(persisted_record);
        let stored_value = index_record
            .get("value")
            .map_err(super::IvmRuntimeError::RecordEncoding)?;
        let primary_key = crate::db::persisted_index_primary_key(
            table,
            index_name,
            index_schema,
            storage_key,
            &stored_value,
        )
        .map_err(|_| super::IvmRuntimeError::InvalidPersistedIndex(index_name.to_owned()))?;
        primary_keys.push(primary_key);
    }
    Ok(primary_keys)
}
#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::task::Context;

    use futures::task::noop_waker;

    use super::*;
    use crate::chunks::{ChunkError, ChunkFuture, ChunkProvider, ChunkRequest};
    use crate::storage::{TestStorage, TestStorageOperation};

    struct CountingChunkProvider {
        calls: Cell<usize>,
        bytes: bytes::Bytes,
    }

    impl ChunkProvider for CountingChunkProvider {
        fn get(&self, _request: ChunkRequest) -> ChunkFuture<'_, Result<bytes::Bytes, ChunkError>> {
            self.calls.set(self.calls.get() + 1);
            let bytes = self.bytes.clone();
            Box::pin(async move { Ok(bytes) })
        }
    }

    #[test]
    fn equal_requests_share_one_retained_future() {
        let (storage, control) = TestStorage::controlled(&["rows"]);
        control.pause_on(TestStorageOperation::Get);
        let storage = OwnedStorage::new(Rc::new(storage));
        let mut requests = EvaluationRequests::new();
        let key = EvaluationRequestKey::Storage(StorageRequestKey::Get {
            family: "rows".to_owned(),
            key: b"one".to_vec(),
        });

        let schema = DatabaseSchema::new([]);
        assert!(requests.request(key.clone(), &storage, None, &schema));
        assert!(!requests.request(key.clone(), &storage, None, &schema));
        assert_eq!(requests.pending_len(), 1);

        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert_eq!(requests.poll(&mut context), 0);
        assert_eq!(control.observed(), vec![TestStorageOperation::Get]);

        control.resume_operation(TestStorageOperation::Get);
        assert_eq!(requests.poll(&mut context), 1);
        assert!(matches!(
            requests.take(&key).unwrap().unwrap(),
            EvaluationRequestOutput::Storage(StorageRequestOutput::Value(None))
        ));
        assert_eq!(control.observed(), vec![TestStorageOperation::Get]);
    }

    // This is intentionally an internal mechanism test: request coalescing is
    // not publicly observable until indirect scalar operators are introduced.
    #[test]
    fn equal_chunk_requests_share_one_retained_future() {
        let (storage, _) = TestStorage::controlled(&["rows"]);
        let storage = OwnedStorage::new(Rc::new(storage));
        let chunk = bytes::Bytes::from_static(b"chunk");
        let provider = Rc::new(CountingChunkProvider {
            calls: Cell::new(0),
            bytes: chunk.clone(),
        });
        let chunks = OwnedChunkProvider::new(provider.clone());
        let mut requests = EvaluationRequests::new();
        let key = EvaluationRequestKey::Chunk(ChunkRequest {
            object_hash: crate::large_values::object_hash(&chunk).0,
            locator: crate::large_values::Locator::from_seed(b"opaque-locator"),
        });
        let schema = DatabaseSchema::new([]);

        assert!(requests.request(key.clone(), &storage, Some(&chunks), &schema));
        assert!(!requests.request(key.clone(), &storage, Some(&chunks), &schema));
        assert_eq!(provider.calls.get(), 0, "the future has not been polled");

        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert_eq!(requests.poll(&mut context), 1);
        assert_eq!(provider.calls.get(), 1);
        let EvaluationRequestOutput::Chunk(loaded) = requests.take(&key).unwrap().unwrap() else {
            panic!("expected chunk output");
        };
        assert_eq!(loaded.bytes.bytes(), &chunk);
    }
}
