//! Owned, pollable ordered-storage contract.
//!
//! The request is independent of the backend borrow. A database scheduler can
//! therefore retain a suspended request beside its storage handle without a
//! self-referential future. Immediate backends execute the same request during
//! its first poll; asynchronous backends may retain backend-specific state by
//! request id and wake the supplied task later.

use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use super::{Error, KeyValue, OrderedKvStorage, OwnedWriteOperation};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Stable process-local identity for one owned storage request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageRequestId(u64);

/// Owned half-open range or prefix scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedScanBounds {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Reverse,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedScanRequest {
    pub column_family: String,
    pub bounds: OwnedScanBounds,
    pub direction: ScanDirection,
}

impl OwnedScanRequest {
    pub fn prefix(column_family: impl Into<String>, prefix: impl Into<Vec<u8>>) -> Self {
        Self {
            column_family: column_family.into(),
            bounds: OwnedScanBounds::Prefix(prefix.into()),
            direction: ScanDirection::Forward,
        }
    }

    pub fn range(
        column_family: impl Into<String>,
        start: impl Into<Vec<u8>>,
        end: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            column_family: column_family.into(),
            bounds: OwnedScanBounds::Range {
                start: start.into(),
                end: end.into(),
            },
            direction: ScanDirection::Forward,
        }
    }

    pub fn reverse(mut self) -> Self {
        self.direction = ScanDirection::Reverse;
        self
    }
}

/// Operation payload retained by the scheduler across suspension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedStorageOperation {
    Get { column_family: String, key: Vec<u8> },
    Scan(OwnedScanRequest),
    Commit(Vec<OwnedWriteOperation>),
    Flush,
    Close,
}

/// One uniquely identified owned storage request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedStorageRequest {
    id: StorageRequestId,
    operation: OwnedStorageOperation,
}

impl OwnedStorageRequest {
    pub fn new(operation: OwnedStorageOperation) -> Self {
        Self {
            id: StorageRequestId(NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)),
            operation,
        }
    }

    pub fn id(&self) -> StorageRequestId {
        self.id
    }

    pub fn operation(&self) -> &OwnedStorageOperation {
        &self.operation
    }
}

/// Owned result of a storage request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedStorageResponse {
    Value(Option<Vec<u8>>),
    Rows(Vec<KeyValue>),
    Committed,
    Flushed,
    Closed,
}

/// Object-safe asynchronous ordered key/value storage.
///
/// A backend must associate any retained work with [`OwnedStorageRequest::id`]
/// and return the same terminal result exactly once. Requests may be
/// thread-affine; neither the backend nor its wake path is required to be
/// `Send`.
pub trait PollableOrderedKvStorage {
    fn poll_request(
        &mut self,
        request: &OwnedStorageRequest,
        context: &mut Context<'_>,
    ) -> Poll<Result<OwnedStorageResponse, Error>>;
}

/// Existing immediate backends inherit the object-safe pollable contract.
impl<S> PollableOrderedKvStorage for S
where
    S: OrderedKvStorage,
{
    fn poll_request(
        &mut self,
        request: &OwnedStorageRequest,
        _context: &mut Context<'_>,
    ) -> Poll<Result<OwnedStorageResponse, Error>> {
        Poll::Ready(match request.operation() {
            OwnedStorageOperation::Get { column_family, key } => {
                OrderedKvStorage::get(self, column_family, key).map(OwnedStorageResponse::Value)
            }
            OwnedStorageOperation::Scan(request) => {
                let mut rows = match &request.bounds {
                    OwnedScanBounds::Prefix(prefix) => {
                        OrderedKvStorage::prefix(self, &request.column_family, prefix)?
                    }
                    OwnedScanBounds::Range { start, end } => {
                        OrderedKvStorage::range(self, &request.column_family, start, end)?
                    }
                };
                if request.direction == ScanDirection::Reverse {
                    rows.reverse();
                }
                Ok(OwnedStorageResponse::Rows(rows))
            }
            OwnedStorageOperation::Commit(operations) => {
                let operations = operations
                    .iter()
                    .map(OwnedWriteOperation::as_write_operation)
                    .collect::<Vec<_>>();
                OrderedKvStorage::write_many(self, &operations)
                    .map(|()| OwnedStorageResponse::Committed)
            }
            OwnedStorageOperation::Flush => {
                OrderedKvStorage::flush_write_boundary(self).map(|()| OwnedStorageResponse::Flushed)
            }
            OwnedStorageOperation::Close => {
                OrderedKvStorage::close(self).map(|()| OwnedStorageResponse::Closed)
            }
        })
    }
}
