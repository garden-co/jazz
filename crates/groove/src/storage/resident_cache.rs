//! Demand-loaded synchronous storage view over an asynchronous durable store.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use super::async_ordered::{OwnedScanRequest, OwnedStorageOperation, OwnedStorageResponse};
use super::{
    ColumnFamilyName, Error, Key, MemoryStorage, OrderedKvStorage, OwnedWriteOperation,
    ReopenableStorage, ScanVisitor, Value, WriteOperation, apply_storage_delta,
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
    demand_collector: std::rc::Rc<RefCell<DemandCollector>>,
    transaction: Option<std::rc::Rc<ResidentTransaction>>,
}

#[derive(Default)]
struct DemandCollector {
    active: bool,
    seen: BTreeSet<DemandIdentity>,
    ordered: Vec<OwnedStorageOperation>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum DemandIdentity {
    Get(String, Vec<u8>),
    Scan(OwnedScanRequest),
}

impl DemandIdentity {
    fn from_operation(operation: &OwnedStorageOperation) -> Option<Self> {
        match operation {
            OwnedStorageOperation::Get { column_family, key } => {
                Some(Self::Get(column_family.clone(), key.clone()))
            }
            OwnedStorageOperation::Scan(scan) => Some(Self::Scan(scan.clone())),
            _ => None,
        }
    }
}

/// Undo journal for the node-open preparation transaction.
///
/// The resident cache is private to the unopened owner, so preparation may
/// update it in place. If preparation suspends or fails, dropping the last
/// transaction handle restores only the keys that preparation touched. A
/// successful durable node-open commit publishes the journal instead. This
/// keeps opening proportional to the demanded inputs and staged writes rather
/// than copying every admitted row into a disposable cache.
struct ResidentTransaction {
    cache: MemoryStorage,
    original_values: RefCell<OriginalValues>,
    published: std::cell::Cell<bool>,
}

type OriginalValues = BTreeMap<(String, Vec<u8>), Option<Value>>;

impl ResidentTransaction {
    fn record_original(&self, cf: &str, key: &[u8]) -> Result<(), Error> {
        if self.published.get() {
            return Ok(());
        }
        let identity = (cf.to_owned(), key.to_vec());
        if self.original_values.borrow().contains_key(&identity) {
            return Ok(());
        }
        let original = self.cache.get(cf, key)?;
        self.original_values.borrow_mut().insert(identity, original);
        Ok(())
    }

    fn publish(&self) {
        self.published.set(true);
        self.original_values.borrow_mut().clear();
    }
}

impl Drop for ResidentTransaction {
    fn drop(&mut self) {
        if self.published.get() {
            return;
        }
        for ((cf, key), value) in self.original_values.get_mut().iter().rev() {
            let result = match value {
                Some(value) => self.cache.set(cf, key, value),
                None => self.cache.delete(cf, key),
            };
            debug_assert!(
                result.is_ok(),
                "resident transaction rollback must be infallible"
            );
        }
    }
}

#[derive(Clone, Default)]
struct DemandState {
    admissions: ResidentAdmissions,
    inherited_dirty_keys: std::rc::Rc<BTreeSet<(String, Vec<u8>)>>,
    dirty_keys: BTreeSet<(String, Vec<u8>)>,
    uses_overlay: bool,
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
    inherited_points: std::rc::Rc<BTreeSet<(String, Vec<u8>)>>,
    points: BTreeSet<(String, Vec<u8>)>,
    inherited_exact_scans: std::rc::Rc<BTreeSet<OwnedScanRequest>>,
    exact_scans: BTreeSet<OwnedScanRequest>,
    inherited_prefixes: std::rc::Rc<BTreeSet<(String, Vec<u8>)>>,
    prefixes: BTreeSet<(String, Vec<u8>)>,
    inherited_ranges: std::rc::Rc<BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>>,
    ranges: BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl ResidentAdmissions {
    fn insert(&mut self, operation: OwnedStorageOperation, uses_overlay: bool) {
        match operation {
            OwnedStorageOperation::Get { column_family, key } => {
                if uses_overlay {
                    self.points.insert((column_family, key));
                } else {
                    std::rc::Rc::make_mut(&mut self.inherited_points).insert((column_family, key));
                }
            }
            OwnedStorageOperation::Scan(scan) => {
                let inserted = if uses_overlay {
                    self.exact_scans.insert(scan.clone())
                } else {
                    std::rc::Rc::make_mut(&mut self.inherited_exact_scans).insert(scan.clone())
                };
                if inserted {
                    match scan.bounds {
                        super::async_ordered::OwnedScanBounds::Prefix(prefix) => {
                            if uses_overlay {
                                self.prefixes.insert((scan.column_family, prefix));
                            } else {
                                std::rc::Rc::make_mut(&mut self.inherited_prefixes)
                                    .insert((scan.column_family, prefix));
                            }
                        }
                        super::async_ordered::OwnedScanBounds::Range { start, end } => {
                            let ranges = if uses_overlay {
                                &mut self.ranges
                            } else {
                                std::rc::Rc::make_mut(&mut self.inherited_ranges)
                            };
                            insert_range(ranges.entry(scan.column_family).or_default(), start, end);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn covers(&self, requested: &OwnedStorageOperation) -> bool {
        match requested {
            OwnedStorageOperation::Get { column_family, key }
                if self.points.contains(&(column_family.clone(), key.clone()))
                    || self
                        .inherited_points
                        .contains(&(column_family.clone(), key.clone())) =>
            {
                true
            }
            OwnedStorageOperation::Scan(scan)
                if self.exact_scans.contains(scan) || self.inherited_exact_scans.contains(scan) =>
            {
                true
            }
            OwnedStorageOperation::Get { column_family, key } => {
                self.prefix_covers(column_family, key) || self.range_covers(column_family, key, key)
            }
            OwnedStorageOperation::Scan(scan) => match &scan.bounds {
                super::async_ordered::OwnedScanBounds::Prefix(prefix) => {
                    self.prefix_covers(&scan.column_family, prefix)
                }
                super::async_ordered::OwnedScanBounds::Range { start, end } => {
                    self.range_covers(&scan.column_family, start, end)
                }
            },
            _ => false,
        }
    }

    fn prefix_covers(&self, column_family: &str, value: &[u8]) -> bool {
        (0..=value.len()).any(|length| {
            let identity = (column_family.to_owned(), value[..length].to_vec());
            self.prefixes.contains(&identity) || self.inherited_prefixes.contains(&identity)
        })
    }

    fn range_covers(&self, column_family: &str, start: &[u8], end: &[u8]) -> bool {
        [&*self.inherited_ranges, &self.ranges]
            .into_iter()
            .filter_map(|ranges| ranges.get(column_family))
            .any(|ranges| {
                ranges
                    .range(..=start.to_vec())
                    .next_back()
                    .is_some_and(|(_, admitted_end)| admitted_end.as_slice() >= end)
            })
    }
}

fn insert_range(ranges: &mut BTreeMap<Vec<u8>, Vec<u8>>, start: Vec<u8>, end: Vec<u8>) {
    if ranges
        .range(..=start.clone())
        .next_back()
        .is_some_and(|(_, admitted_end)| admitted_end >= &end)
    {
        return;
    }
    let contained = ranges
        .range(start.clone()..end.clone())
        .filter(|(_, admitted_end)| *admitted_end <= &end)
        .map(|(admitted_start, _)| admitted_start.clone())
        .collect::<Vec<_>>();
    for admitted_start in contained {
        ranges.remove(&admitted_start);
    }
    ranges.insert(start, end);
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
            demand_collector: Default::default(),
            transaction: None,
        }
    }

    pub(crate) fn begin_demand_collection(&self) {
        let mut collector = self.demand_collector.borrow_mut();
        collector.active = true;
        collector.seen.clear();
        collector.ordered.clear();
    }

    pub(crate) fn take_collected_demands(&self) -> Vec<OwnedStorageOperation> {
        let mut collector = self.demand_collector.borrow_mut();
        collector.active = false;
        collector.seen.clear();
        std::mem::take(&mut collector.ordered)
    }

    /// Begin an isolated, restartable storage transaction over the resident set.
    ///
    /// Reads observe the inputs admitted so far. Writes are applied only to the
    /// transaction's private working set and accumulated as one durable commit.
    /// The caller may discard the transaction after [`Error::NotResident`] and
    /// retry after admitting the requested input, or publish the successful
    /// transaction as its new resident view.
    ///
    pub fn begin_transaction(&self) -> Result<Self, Error> {
        let state = self.state.borrow();
        Ok(Self {
            cache: self.cache.clone(),
            state: std::rc::Rc::new(RefCell::new(DemandState {
                admissions: ResidentAdmissions {
                    inherited_points: std::rc::Rc::clone(&state.admissions.inherited_points),
                    points: BTreeSet::new(),
                    inherited_exact_scans: std::rc::Rc::clone(
                        &state.admissions.inherited_exact_scans,
                    ),
                    exact_scans: BTreeSet::new(),
                    inherited_prefixes: std::rc::Rc::clone(&state.admissions.inherited_prefixes),
                    prefixes: BTreeSet::new(),
                    inherited_ranges: std::rc::Rc::clone(&state.admissions.inherited_ranges),
                    ranges: BTreeMap::new(),
                },
                inherited_dirty_keys: std::rc::Rc::clone(&state.inherited_dirty_keys),
                dirty_keys: BTreeSet::new(),
                uses_overlay: true,
                pending_writes: Vec::new(),
                pending_column_families: state.pending_column_families.clone(),
            })),
            demand_collector: std::rc::Rc::clone(&self.demand_collector),
            transaction: Some(std::rc::Rc::new(ResidentTransaction {
                cache: self.cache.clone(),
                original_values: RefCell::new(BTreeMap::new()),
                published: std::cell::Cell::new(false),
            })),
        })
    }

    /// Publish a successfully persisted preparation transaction as the live
    /// resident cache. No row copy is required: the transaction already owns
    /// the exact in-place writes guarded by its undo journal.
    pub fn publish_transaction(&self) {
        if let Some(transaction) = &self.transaction {
            transaction.publish();
        }
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
                let state = self.state.borrow();
                let identity = (column_family.clone(), key.clone());
                if !state.dirty_keys.contains(&identity)
                    && !state.inherited_dirty_keys.contains(&identity)
                {
                    match value {
                        Some(value) => self.cache.set(column_family, key, &value)?,
                        None => self.cache.delete(column_family, key)?,
                    }
                }
            }
            (OwnedStorageOperation::Scan(scan), OwnedStorageResponse::Rows(rows)) => {
                let state = self.state.borrow();
                for (key, value) in rows {
                    let identity = (scan.column_family.clone(), key.clone());
                    if !state.dirty_keys.contains(&identity)
                        && !state.inherited_dirty_keys.contains(&identity)
                    {
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
        let uses_overlay = state.uses_overlay;
        state.admissions.insert(request, uses_overlay);
        Ok(())
    }

    fn require(&self, request: OwnedStorageOperation) -> Result<bool, Error> {
        if self.state.borrow().admissions.covers(&request) {
            Ok(true)
        } else if self.demand_collector.borrow().active {
            let mut collector = self.demand_collector.borrow_mut();
            if DemandIdentity::from_operation(&request)
                .is_some_and(|identity| collector.seen.insert(identity))
            {
                collector.ordered.push(request);
            }
            Ok(false)
        } else {
            Err(Error::NotResident {
                request: Box::new(request),
            })
        }
    }

    fn mark_dirty(&self, cf: &str, key: &[u8]) {
        let mut state = self.state.borrow_mut();
        let identity = (cf.to_owned(), key.to_vec());
        if state.uses_overlay {
            state.dirty_keys.insert(identity);
        } else {
            std::rc::Rc::make_mut(&mut state.inherited_dirty_keys).insert(identity);
        }
        let uses_overlay = state.uses_overlay;
        state.admissions.insert(
            OwnedStorageOperation::Get {
                column_family: cf.to_owned(),
                key: key.to_vec(),
            },
            uses_overlay,
        );
    }
}

impl OrderedKvStorage for DemandLoadedStorage {
    fn require_resident(&self, operation: &OwnedStorageOperation) -> Result<(), Error> {
        self.require(operation.clone()).map(|_| ())
    }

    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        if !self.require(OwnedStorageOperation::Get {
            column_family: cf.to_owned(),
            key: key.to_vec(),
        })? {
            return Ok(None);
        }
        self.cache.get(cf, key)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        if let Some(transaction) = &self.transaction {
            transaction.record_original(cf, key)?;
        }
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
        if let Some(transaction) = &self.transaction {
            transaction.record_original(cf, key)?;
        }
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
        if !self.require(OwnedStorageOperation::Scan(OwnedScanRequest::range(
            cf, start, end,
        )))? {
            return Ok(());
        }
        self.cache.scan_range(cf, start, end, visit)
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        if !self.require(OwnedStorageOperation::Scan(OwnedScanRequest::prefix(
            cf, prefix,
        )))? {
            return Ok(());
        }
        self.cache.scan_prefix(cf, prefix, visit)
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        if !self.require(OwnedStorageOperation::Scan(
            OwnedScanRequest::prefix(cf, prefix).reverse(),
        ))? {
            return Ok(());
        }
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
        let Self {
            cache,
            state,
            demand_collector,
            transaction,
        } = self;
        state
            .borrow_mut()
            .pending_column_families
            .extend(column_families.iter().map(|name| (*name).to_owned()));
        Ok(Self {
            cache: cache.reopen(column_families)?,
            state,
            demand_collector,
            transaction,
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
        assert_eq!(state.admissions.inherited_points.len(), 10_000);
        assert!(state.admissions.inherited_prefixes.is_empty());
        assert!(state.admissions.prefixes.is_empty());
        assert!(state.admissions.inherited_ranges.is_empty());
        assert!(state.admissions.ranges.is_empty());
    }

    /// This is an internal mechanism test because copying private residency
    /// metadata during a restartable node-open attempt has no public semantic
    /// signal other than nonlinear startup cost.
    #[test]
    fn restartable_transaction_does_not_copy_the_resident_point_history() {
        let storage = DemandLoadedStorage::new(&["rows"]);
        for index in 0_u32..10_000 {
            storage.set("rows", &index.to_be_bytes(), b"value").unwrap();
        }
        let inherited = std::rc::Rc::clone(&storage.state.borrow().admissions.inherited_points);
        let transaction = storage.begin_transaction().unwrap();
        assert!(std::rc::Rc::ptr_eq(
            &inherited,
            &transaction.state.borrow().admissions.inherited_points
        ));
    }

    /// This is an internal mechanism test because admission-index probes are
    /// deliberately invisible through the ordered-storage API.
    #[test]
    fn distinct_scan_admissions_do_not_make_later_covered_checks_linear() {
        let storage = DemandLoadedStorage::new(&["rows"]);
        for index in 0_u32..1_000 {
            let request =
                OwnedStorageOperation::Scan(OwnedScanRequest::prefix("rows", index.to_be_bytes()));
            storage
                .admit(request, OwnedStorageResponse::Rows(Vec::new()))
                .unwrap();
        }
        let covered_key = [999_u32.to_be_bytes().as_slice(), b"/child"].concat();
        storage.get("rows", &covered_key).unwrap();

        let state = storage.state.borrow();
        assert_eq!(state.admissions.inherited_prefixes.len(), 1_000);
        assert!(state.admissions.inherited_exact_scans.len() == 1_000);
    }

    #[test]
    fn node_open_transaction_rolls_back_only_touched_keys_and_publishes_in_place() {
        let storage = DemandLoadedStorage::new(&["rows"]);
        storage
            .admit(
                OwnedStorageOperation::Get {
                    column_family: "rows".to_owned(),
                    key: b"existing".to_vec(),
                },
                OwnedStorageResponse::Value(Some(b"before".to_vec())),
            )
            .unwrap();

        {
            let transaction = storage.begin_transaction().unwrap();
            transaction.set("rows", b"existing", b"after").unwrap();
            transaction.set("rows", b"new", b"value").unwrap();
            assert_eq!(
                storage.cache.get("rows", b"existing").unwrap(),
                Some(b"after".to_vec())
            );
        }
        assert_eq!(
            storage.cache.get("rows", b"existing").unwrap(),
            Some(b"before".to_vec())
        );
        assert_eq!(storage.cache.get("rows", b"new").unwrap(), None);

        {
            let transaction = storage.begin_transaction().unwrap();
            transaction.set("rows", b"existing", b"published").unwrap();
            transaction.publish_transaction();
            transaction.set("rows", b"after-open", b"live").unwrap();
            assert!(
                transaction
                    .transaction
                    .as_ref()
                    .unwrap()
                    .original_values
                    .borrow()
                    .is_empty(),
                "published runtime writes must not extend the node-open undo journal"
            );
        }
        assert_eq!(
            storage.cache.get("rows", b"existing").unwrap(),
            Some(b"published".to_vec())
        );
        assert_eq!(
            storage.cache.get("rows", b"after-open").unwrap(),
            Some(b"live".to_vec())
        );
    }
}
