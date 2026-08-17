//! Demand-loaded synchronous storage view over an asynchronous durable store.

use std::cell::RefCell;
use std::collections::BTreeSet;

use super::pollable::{OwnedScanRequest, OwnedStorageOperation, OwnedStorageResponse};
use super::{
    ColumnFamilyName, Error, Key, MemoryStorage, OrderedKvStorage, ScanVisitor, Value,
    WriteOperation, apply_storage_delta,
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

#[derive(Default)]
struct DemandState {
    admitted: Vec<OwnedStorageOperation>,
    dirty_keys: BTreeSet<(String, Vec<u8>)>,
}

impl DemandLoadedStorage {
    pub fn new(column_families: &[&str]) -> Self {
        Self {
            cache: MemoryStorage::new(column_families),
            state: std::rc::Rc::new(RefCell::new(DemandState::default())),
        }
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
        let mut state = self.state.borrow_mut();
        if !state.admitted.contains(&request) {
            state.admitted.push(request);
        }
        Ok(())
    }

    fn require(&self, request: OwnedStorageOperation) -> Result<(), Error> {
        if self.state.borrow().admitted.contains(&request) {
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
        let request = OwnedStorageOperation::Get {
            column_family: cf.to_owned(),
            key: key.to_vec(),
        };
        if !state.admitted.contains(&request) {
            state.admitted.push(request);
        }
    }
}

impl OrderedKvStorage for DemandLoadedStorage {
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
        Ok(())
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        self.cache.delete(cf, key)?;
        self.mark_dirty(cf, key);
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
