//! Demand-loaded synchronous storage view over an asynchronous durable store.

use std::cell::RefCell;
use std::collections::BTreeSet;

use super::async_ordered::{OwnedScanRequest, OwnedStorageOperation, OwnedStorageResponse};
use super::{
    ColumnFamilyName, Error, Key, MemoryStorage, OwnedWriteOperation, ReopenableStorage,
    ResidentStorage, ScanVisitor, Value, WriteOperation, apply_storage_delta,
};

/// Synchronous working set that reports exact missing storage inputs instead
/// of interpreting an unfilled cache as durable absence.
///
/// An async host catches [`Error::NotResident`], executes that owned request,
/// calls [`DemandLoadedStorage::admit`], and retries the same core operation.
/// Local writes update this cache synchronously and are protected from stale
/// in-flight fetches.
#[derive(Clone)]
pub struct DemandLoadedStorage {
    cache: MemoryStorage,
    state: std::rc::Rc<RefCell<DemandState>>,
}

#[derive(Clone, Default)]
struct DemandState {
    admissions: ResidentAdmissions,
    dirty_keys: BTreeSet<(String, Vec<u8>)>,
    pending_writes: Vec<OwnedWriteOperation>,
    pending_column_families: BTreeSet<String>,
}

/// Exact point admissions are indexed independently from range admissions.
///
/// Written keys become resident immediately and can grow without bound during
/// a process lifetime. Keeping them in the same linear list as semantic range
/// coverage made every later write search the entire write history. Range
/// admissions are normally few and need coverage comparisons, while point
/// admissions need only exact logarithmic lookup.
#[derive(Clone, Default)]
struct ResidentAdmissions {
    points: BTreeSet<(String, Vec<u8>)>,
    scans: Vec<OwnedScanRequest>,
}

impl ResidentAdmissions {
    fn insert(&mut self, operation: OwnedStorageOperation) {
        match operation {
            OwnedStorageOperation::Get { column_family, key } => {
                self.points.insert((column_family, key));
            }
            OwnedStorageOperation::Scan(scan) => {
                if !self.scans.contains(&scan) {
                    self.scans.push(scan);
                }
            }
            _ => {}
        }
    }

    fn covers(&self, requested: &OwnedStorageOperation) -> bool {
        match requested {
            OwnedStorageOperation::Get { column_family, key }
                if self.points.contains(&(column_family.clone(), key.clone())) =>
            {
                true
            }
            _ => self
                .scans
                .iter()
                .any(|admitted| covers(&OwnedStorageOperation::Scan(admitted.clone()), requested)),
        }
    }
}

impl DemandLoadedStorage {
    pub fn new(column_families: &[&str]) -> Self {
        let mut state = DemandState::default();
        state
            .pending_column_families
            .extend(column_families.iter().map(|name| (*name).to_owned()));
        Self {
            cache: MemoryStorage::new(column_families),
            state: std::rc::Rc::new(RefCell::new(state)),
        }
    }

    /// Begin an isolated, restartable storage transaction over the resident set.
    ///
    /// Reads observe the inputs admitted so far. Writes are applied only to the
    /// transaction's private working set and accumulated as one durable commit.
    /// The caller may discard the transaction after [`Error::NotResident`] and
    /// retry after admitting the requested input, or publish the successful
    /// transaction as its new resident view.
    ///
    /// This currently snapshots the resident memory store. Keeping that detail
    /// behind this boundary lets the implementation become copy-on-write later
    /// without changing restartable core operations.
    pub fn begin_transaction(&self) -> Result<Self, Error> {
        let column_families = self.cache.column_family_names().unwrap_or_default();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let fork = Self::new(&refs);
        for column_family in column_families {
            for (key, value) in self.cache.prefix(&column_family, &[])? {
                fork.cache.set(&column_family, &key, &value)?;
            }
        }
        let state = self.state.borrow();
        *fork.state.borrow_mut() = DemandState {
            admissions: state.admissions.clone(),
            dirty_keys: state.dirty_keys.clone(),
            pending_writes: Vec::new(),
            pending_column_families: state.pending_column_families.clone(),
        };
        Ok(fork)
    }

    /// Take writes applied synchronously to the resident working set.
    pub fn take_pending_writes(&self) -> Vec<OwnedWriteOperation> {
        std::mem::take(&mut self.state.borrow_mut().pending_writes)
    }

    /// Take column families that must exist before the transaction journal is
    /// durably committed.
    pub fn take_pending_column_families(&self) -> Vec<String> {
        std::mem::take(&mut self.state.borrow_mut().pending_column_families)
            .into_iter()
            .collect()
    }

    /// Admit one completed durable read into the working set.
    pub fn admit(
        &self,
        request: OwnedStorageOperation,
        response: OwnedStorageResponse,
    ) -> Result<(), Error> {
        match (&request, response) {
            (
                OwnedStorageOperation::Get { column_family, key },
                OwnedStorageResponse::Value(value),
            ) => {
                if !self
                    .state
                    .borrow()
                    .dirty_keys
                    .contains(&(column_family.clone(), key.clone()))
                {
                    match value {
                        Some(value) => self.cache.set(column_family, key, &value)?,
                        None => self.cache.delete(column_family, key)?,
                    }
                }
            }
            (OwnedStorageOperation::Scan(scan), OwnedStorageResponse::Rows(rows)) => {
                let dirty = self.state.borrow().dirty_keys.clone();
                for (key, value) in rows {
                    if !dirty.contains(&(scan.column_family.clone(), key.clone())) {
                        self.cache.set(&scan.column_family, &key, &value)?;
                    }
                }
            }
            (expected, actual) => {
                return Err(Error::Backend {
                    backend: "resident-cache",
                    message: format!(
                        "storage response {actual:?} does not satisfy demand {expected:?}"
                    ),
                });
            }
        }
        self.state.borrow_mut().admissions.insert(request);
        Ok(())
    }

    fn require(&self, request: OwnedStorageOperation) -> Result<(), Error> {
        if self.state.borrow().admissions.covers(&request) {
            Ok(())
        } else {
            Err(Error::NotResident {
                request: Box::new(request),
            })
        }
    }

    fn mark_dirty(&self, cf: &str, key: &[u8]) {
        let mut state = self.state.borrow_mut();
        state.dirty_keys.insert((cf.to_owned(), key.to_vec()));
        state.admissions.insert(OwnedStorageOperation::Get {
            column_family: cf.to_owned(),
            key: key.to_vec(),
        });
    }
}

fn covers(admitted: &OwnedStorageOperation, requested: &OwnedStorageOperation) -> bool {
    if admitted == requested {
        return true;
    }
    match (admitted, requested) {
        (
            OwnedStorageOperation::Scan(admitted),
            OwnedStorageOperation::Get { column_family, key },
        ) if admitted.column_family == *column_family => match &admitted.bounds {
            super::async_ordered::OwnedScanBounds::Prefix(prefix) => key.starts_with(prefix),
            super::async_ordered::OwnedScanBounds::Range { start, end } => {
                key.as_slice() >= start.as_slice() && key.as_slice() < end.as_slice()
            }
        },
        (OwnedStorageOperation::Scan(admitted), OwnedStorageOperation::Scan(requested))
            if admitted.column_family == requested.column_family =>
        {
            match (&admitted.bounds, &requested.bounds) {
                (
                    super::async_ordered::OwnedScanBounds::Prefix(admitted),
                    super::async_ordered::OwnedScanBounds::Prefix(requested),
                ) => requested.starts_with(admitted),
                (
                    super::async_ordered::OwnedScanBounds::Range {
                        start: admitted_start,
                        end: admitted_end,
                    },
                    super::async_ordered::OwnedScanBounds::Range {
                        start: requested_start,
                        end: requested_end,
                    },
                ) => requested_start >= admitted_start && requested_end <= admitted_end,
                _ => false,
            }
        }
        _ => false,
    }
}

impl ResidentStorage for DemandLoadedStorage {
    fn require_resident(&self, operation: &OwnedStorageOperation) -> Result<(), Error> {
        self.require(operation.clone())
    }

    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        self.require(OwnedStorageOperation::Get {
            column_family: cf.to_owned(),
            key: key.to_vec(),
        })?;
        self.cache.get(cf, key)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        self.cache.set(cf, key, value)?;
        self.mark_dirty(cf, key);
        self.state
            .borrow_mut()
            .pending_writes
            .push(OwnedWriteOperation::Set {
                cf: cf.to_owned(),
                key: key.to_vec(),
                value: value.to_vec(),
            });
        Ok(())
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        self.cache.delete(cf, key)?;
        self.mark_dirty(cf, key);
        self.state
            .borrow_mut()
            .pending_writes
            .push(OwnedWriteOperation::Delete {
                cf: cf.to_owned(),
                key: key.to_vec(),
            });
        Ok(())
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        self.require(OwnedStorageOperation::Scan(OwnedScanRequest::range(
            cf, start, end,
        )))?;
        self.cache.scan_range(cf, start, end, visit)
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        self.require(OwnedStorageOperation::Scan(OwnedScanRequest::prefix(
            cf, prefix,
        )))?;
        self.cache.scan_prefix(cf, prefix, visit)
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        self.require(OwnedStorageOperation::Scan(
            OwnedScanRequest::prefix(cf, prefix).reverse(),
        ))?;
        self.cache.scan_prefix_reverse(cf, prefix, visit)
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        for operation in operations {
            match operation {
                WriteOperation::Set { cf, key, value } => self.set(cf, key, value)?,
                WriteOperation::Delete { cf, key } => self.delete(cf, key)?,
                WriteOperation::Delta { cf, key, delta } => {
                    let current = self.cache.get(cf, key)?;
                    let value = apply_storage_delta(current.as_deref(), &delta.encode()?)?;
                    self.set(cf, key, &value)?;
                }
            }
        }
        Ok(())
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.cache.column_family_names()
    }
}

impl ReopenableStorage for DemandLoadedStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error> {
        let Self { cache, state } = self;
        state
            .borrow_mut()
            .pending_column_families
            .extend(column_families.iter().map(|name| (*name).to_owned()));
        Ok(Self {
            cache: cache.reopen(column_families)?,
            state,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resident_requirement_checks_admission_without_scanning_rows() {
        let storage = DemandLoadedStorage::new(&["rows"]);
        let request = OwnedStorageOperation::Scan(OwnedScanRequest::prefix("rows", b"item/"));

        assert!(matches!(
            storage.require_resident(&request),
            Err(Error::NotResident { .. })
        ));
        storage
            .admit(
                request.clone(),
                OwnedStorageResponse::Rows(vec![(b"item/1".to_vec(), b"one".to_vec())]),
            )
            .unwrap();

        storage.require_resident(&request).unwrap();
        assert_eq!(
            storage.get("rows", b"item/1").unwrap(),
            Some(b"one".to_vec())
        );
    }

    #[test]
    fn written_point_admissions_are_indexed_separately_from_ranges() {
        let storage = DemandLoadedStorage::new(&["rows"]);
        for index in 0_u32..10_000 {
            storage.set("rows", &index.to_be_bytes(), b"value").unwrap();
        }
        // Rewriting a resident key must not accumulate another admission, and
        // writes must never masquerade as loaded range coverage.
        storage.set("rows", &5_u32.to_be_bytes(), b"new").unwrap();

        let state = storage.state.borrow();
        assert_eq!(state.admissions.points.len(), 10_000);
        assert!(state.admissions.scans.is_empty());
    }
}
