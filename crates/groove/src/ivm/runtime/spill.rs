//! Spilling join arrangements to node-local scratch storage.
//!
//! An arrangement holds a full in-memory copy of every join input side, so
//! its size follows the data, not the query. Under memory pressure the
//! runtime can move an arrangement's committed contents into an immutable
//! *run* on scratch storage and keep only the keys touched since then in
//! memory, the way Postgres batches a hash join to temporary files instead
//! of growing past `work_mem`.
//!
//! A run is never modified after it is written. An arrangement reads it
//! through a per-run cache, overriding keys it has changed since the spill
//! with in-memory buckets (an empty bucket marks a key removed since the
//! spill). Because a run is immutable, every snapshot, staged evaluation copy
//! and registry checkpoint that shares it reads the same contents; the run's
//! scratch rows are deleted when the last of them drops it.
//!
//! Scratch storage is synchronous and not durable: it is the equivalent of
//! Postgres's temporary files, never part of the database. A node restarts
//! with empty scratch storage and rebuilds arrangements by hydration, as it
//! always has.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use bytes::Bytes;
use rustc_hash::FxHashMap as HashMap;

use super::join::{JoinBucket, JoinKey};

/// Key-value entries read from or written to [`SpillStorage`].
pub type SpillEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// Synchronous, ordered, non-durable key-value storage for spilled runs.
///
/// Implementations live outside Groove (a temporary RocksDB on native
/// servers, a map in tests). Reads happen on the evaluation thread, so a
/// backend should be local disk, not a network service.
pub trait SpillStorage {
    /// Write every entry. Entries of one call may be applied in any order;
    /// a run is only read after its writes return.
    fn write(&self, entries: SpillEntries) -> Result<(), String>;
    /// Up to `limit` entries whose key starts with `prefix` and sorts after
    /// `after` (from the start when `None`), in key order.
    fn scan_prefix(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<SpillEntries, String>;
    /// Remove every entry whose key starts with `prefix`.
    fn delete_prefix(&self, prefix: &[u8]) -> Result<(), String>;
}

/// In-process [`SpillStorage`] over an ordered map, for tests and tools.
#[derive(Debug, Default)]
pub struct MemorySpillStorage {
    entries: RefCell<std::collections::BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemorySpillStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored entries, for tests that check run cleanup.
    pub fn len(&self) -> usize {
        self.entries.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.borrow().is_empty()
    }
}

impl SpillStorage for MemorySpillStorage {
    fn write(&self, entries: SpillEntries) -> Result<(), String> {
        self.entries.borrow_mut().extend(entries);
        Ok(())
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<SpillEntries, String> {
        use std::ops::Bound;
        let start = match after {
            Some(after) => Bound::Excluded(after.to_vec()),
            None => Bound::Included(prefix.to_vec()),
        };
        Ok(self
            .entries
            .borrow()
            .range((start, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn delete_prefix(&self, prefix: &[u8]) -> Result<(), String> {
        self.entries
            .borrow_mut()
            .retain(|key, _| !key.starts_with(prefix));
        Ok(())
    }
}

/// The scratch storage a runtime spills to, and its run-id allocator.
pub(super) struct SpillContext {
    storage: Rc<dyn SpillStorage>,
    next_run: Cell<u64>,
}

impl std::fmt::Debug for SpillContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillContext")
            .field("next_run", &self.next_run.get())
            .finish_non_exhaustive()
    }
}

impl SpillContext {
    pub(super) fn new(storage: Rc<dyn SpillStorage>) -> Self {
        Self {
            storage,
            next_run: Cell::new(0),
        }
    }

    fn allocate_run(&self) -> u64 {
        let run = self.next_run.get();
        self.next_run.set(run + 1);
        run
    }
}

thread_local! {
    /// The first scratch-storage failure since the evaluator last checked.
    ///
    /// Bucket lookups are infallible by signature and run deep inside the
    /// join kernels. A failed scratch read cannot be answered correctly, so
    /// it is recorded here and the evaluator fails the node that read it
    /// (see [`take_spill_error`]) rather than letting a partial result out.
    /// The IVM runtime is single-threaded, so a thread-local is exact.
    static SPILL_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn record_spill_error(error: String) {
    SPILL_ERROR.with(|slot| {
        slot.borrow_mut().get_or_insert(error);
    });
}

/// Take the scratch-storage failure recorded since the last call, if any.
pub(super) fn take_spill_error() -> Option<String> {
    SPILL_ERROR.with(|slot| slot.borrow_mut().take())
}

/// An immutable spilled copy of an arrangement's committed buckets.
pub(super) struct SpilledRun {
    id: u64,
    context: Rc<SpillContext>,
    /// Buckets read so far, including keys the run does not contain.
    cache: RefCell<HashMap<JoinKey, Option<Rc<JoinBucket>>>>,
    rows: usize,
    bytes: usize,
}

impl std::fmt::Debug for SpilledRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpilledRun")
            .field("id", &self.id)
            .field("rows", &self.rows)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for SpilledRun {
    fn drop(&mut self) {
        // A failed delete leaks scratch rows until the scratch storage is
        // recreated; they are never read again, so results are unaffected.
        let _ = self.context.storage.delete_prefix(&run_prefix(self.id));
    }
}

/// Entries per scratch write and per scan page.
const SPILL_BATCH: usize = 4096;

impl SpilledRun {
    /// Write a new run holding `previous`'s buckets, replaced key by key by
    /// `resident` (an empty resident bucket removes the key).
    ///
    /// The previous run is streamed page by page, so re-spilling a large
    /// arrangement never loads its spilled part back into memory.
    pub(super) fn write_merged<'a>(
        context: &Rc<SpillContext>,
        previous: Option<&SpilledRun>,
        resident: impl IntoIterator<Item = (&'a JoinKey, &'a JoinBucket)> + Clone,
        is_resident: impl Fn(&[u8]) -> bool,
    ) -> Result<Rc<Self>, String> {
        let id = context.allocate_run();
        // Construct the run first, so a failed write still deletes whatever
        // part of it landed when the run drops.
        let mut run = Self {
            id,
            context: Rc::clone(context),
            cache: RefCell::default(),
            rows: 0,
            bytes: 0,
        };
        let mut batch = Vec::with_capacity(SPILL_BATCH);
        if let Some(previous) = previous {
            let prefix = run_prefix(previous.id);
            let mut after = None;
            loop {
                let page = context
                    .storage
                    .scan_prefix(&prefix, after.as_deref(), SPILL_BATCH)?;
                let Some((last, _)) = page.last() else { break };
                after = Some(last.clone());
                let complete = page.len() < SPILL_BATCH;
                for (entry, value) in &page {
                    let Some((key, record)) = decode_entry(&entry[prefix.len()..]) else {
                        return Err(format!("malformed entry in spilled run {}", previous.id));
                    };
                    if is_resident(key) {
                        continue;
                    }
                    run.push(&mut batch, key, record, value.clone())?;
                }
                if complete {
                    break;
                }
            }
        }
        for (key, bucket) in resident {
            for (record, weight) in bucket.iter() {
                if *weight != 0 {
                    run.push(&mut batch, key, record, weight.to_le_bytes().to_vec())?;
                }
            }
        }
        if !batch.is_empty() {
            context.storage.write(batch)?;
        }
        Ok(Rc::new(run))
    }

    fn push(
        &mut self,
        batch: &mut SpillEntries,
        key: &[u8],
        record: &[u8],
        weight: Vec<u8>,
    ) -> Result<(), String> {
        self.rows += 1;
        self.bytes += key.len() + record.len();
        batch.push((entry_key(self.id, key, record), weight));
        if batch.len() >= SPILL_BATCH {
            self.context.storage.write(std::mem::take(batch))?;
        }
        Ok(())
    }

    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Bytes of buckets currently held in the read cache.
    pub(super) fn cached_bytes(&self) -> usize {
        self.cache
            .borrow()
            .iter()
            .map(|(key, bucket)| {
                key.len()
                    + bucket.as_ref().map_or(0, |bucket| {
                        bucket.iter().map(|(record, _)| record.len()).sum()
                    })
            })
            .sum()
    }

    /// Visit every entry of the run whose key starts with `prefix`.
    fn scan(&self, prefix: &[u8], mut visit: impl FnMut(&[u8], i64)) -> Result<(), String> {
        let mut after = None;
        loop {
            let page = self
                .context
                .storage
                .scan_prefix(prefix, after.as_deref(), SPILL_BATCH)?;
            let Some((last, _)) = page.last() else {
                return Ok(());
            };
            after = Some(last.clone());
            let complete = page.len() < SPILL_BATCH;
            for (entry, value) in &page {
                visit(entry, decode_weight(value));
            }
            if complete {
                return Ok(());
            }
        }
    }

    /// The run's bucket for `key`, read from scratch storage on first use.
    pub(super) fn bucket(&self, key: &JoinKey) -> Option<Rc<JoinBucket>> {
        if let Some(cached) = self.cache.borrow().get(key) {
            return cached.clone();
        }
        let prefix = key_prefix(self.id, key);
        let mut records = Vec::new();
        let read = self.scan(&prefix, |entry, weight| {
            records.push((Bytes::copy_from_slice(&entry[prefix.len()..]), weight));
        });
        if let Err(error) = read {
            record_spill_error(format!(
                "reading spilled arrangement run {}: {error}",
                self.id
            ));
            return None;
        }
        let bucket = (!records.is_empty()).then(|| Rc::new(JoinBucket::from_entries(records)));
        self.cache.borrow_mut().insert(key.clone(), bucket.clone());
        bucket
    }

    /// Every bucket of the run, in key order. This reads the whole run; only
    /// a full re-evaluation needs it.
    pub(super) fn buckets(&self) -> Vec<(JoinKey, Rc<JoinBucket>)> {
        let mut grouped: Vec<(JoinKey, Vec<(Bytes, i64)>)> = Vec::new();
        let prefix = run_prefix(self.id);
        let mut malformed = false;
        let read = self.scan(&prefix, |entry, weight| {
            let Some((key, record)) = decode_entry(&entry[prefix.len()..]) else {
                malformed = true;
                return;
            };
            let record = Bytes::copy_from_slice(record);
            match grouped.last_mut() {
                Some((last, records)) if last.as_slice() == key => records.push((record, weight)),
                _ => grouped.push((JoinKey::from_slice(key), vec![(record, weight)])),
            }
        });
        let read = read.and_then(|()| {
            if malformed {
                Err("malformed entry".to_owned())
            } else {
                Ok(())
            }
        });
        if let Err(error) = read {
            record_spill_error(format!(
                "scanning spilled arrangement run {}: {error}",
                self.id
            ));
            return Vec::new();
        }
        grouped
            .into_iter()
            .map(|(key, records)| (key, Rc::new(JoinBucket::from_entries(records))))
            .collect()
    }

    /// Drop cached buckets, returning their memory. The run stays readable.
    pub(super) fn clear_cache(&self) {
        self.cache.borrow_mut().clear();
    }
}

/// What one [`super::IvmRuntime::spill_arrangements`] call did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ArrangementSpillReport {
    /// Arrangement bytes held in memory before and after the call.
    pub resident_bytes_before: usize,
    pub resident_bytes_after: usize,
    /// Arrangements written to new runs by this call.
    pub spilled_arrangements: usize,
}

impl super::IvmRuntime {
    /// Spill join arrangements to `storage` when asked to shed memory.
    ///
    /// Without spill storage every arrangement stays in memory, as before.
    /// Setting storage on a runtime that already has some keeps the old
    /// storage for existing runs, which stay readable until dropped.
    pub fn set_spill_storage(&mut self, storage: Rc<dyn SpillStorage>) {
        self.spill_context = Some(Rc::new(SpillContext::new(storage)));
    }

    /// Spill into the same storage as `other`, a runtime this one replaces.
    /// Run ids keep counting up, so runs `other` still holds never collide
    /// with this runtime's.
    pub(crate) fn share_spill_storage_with(&mut self, other: &Self) {
        self.spill_context = other.spill_context.clone();
    }

    /// Encoded bytes of join arrangements held in memory, including buckets
    /// cached from spilled runs.
    pub fn resident_arrangement_bytes(&self) -> usize {
        self.arrangement_states
            .values()
            .map(|state| state.value().encoded_bytes())
            .sum()
    }

    /// Move join arrangements to scratch storage, largest first, until the
    /// arrangement bytes held in memory are at most `target_bytes`.
    ///
    /// Read caches of runs already spilled are dropped first, since they are
    /// the cheapest memory to give back. Only root-scope arrangements spill:
    /// a recursive step rebuilds its child-scope arrangements every
    /// iteration. Results are unchanged; lookups of spilled keys read
    /// scratch storage instead of memory.
    pub fn spill_arrangements(
        &mut self,
        target_bytes: usize,
    ) -> Result<ArrangementSpillReport, super::IvmRuntimeError> {
        let mut report = ArrangementSpillReport {
            resident_bytes_before: self.resident_arrangement_bytes(),
            ..ArrangementSpillReport::default()
        };
        let Some(context) = self.spill_context.clone() else {
            report.resident_bytes_after = report.resident_bytes_before;
            return Ok(report);
        };
        let mut resident = report.resident_bytes_before;
        if resident > target_bytes {
            for state in self.arrangement_states.values() {
                if let Some(run) = state.value().spilled_run() {
                    run.clear_cache();
                }
            }
            resident = self.resident_arrangement_bytes();
        }
        if resident > target_bytes {
            let mut candidates = self
                .arrangement_states
                .iter()
                .filter(|(key, _)| key.scope == super::ScopeId::root())
                .map(|(key, state)| (state.value().encoded_bytes(), key.clone()))
                .filter(|(bytes, _)| *bytes > 0)
                .collect::<Vec<_>>();
            candidates.sort_by(|a, b| b.0.cmp(&a.0));
            for (bytes, key) in candidates {
                if resident <= target_bytes {
                    break;
                }
                let Some(state) = self.arrangement_states.get_mut(&key) else {
                    continue;
                };
                if state
                    .value_mut()
                    .spill(&context)
                    .map_err(super::IvmRuntimeError::ArrangementSpill)?
                {
                    report.spilled_arrangements += 1;
                    resident = resident.saturating_sub(bytes);
                }
            }
            resident = self.resident_arrangement_bytes();
        }
        report.resident_bytes_after = resident;
        Ok(report)
    }
}

// Scratch keys: run id, then the join key framed by its length, then the
// encoded record. Framing keeps one key's prefix from matching a longer key.
fn run_prefix(run: u64) -> Vec<u8> {
    run.to_be_bytes().to_vec()
}

fn key_prefix(run: u64, key: &[u8]) -> Vec<u8> {
    let mut prefix = run_prefix(run);
    prefix.extend_from_slice(&(key.len() as u32).to_be_bytes());
    prefix.extend_from_slice(key);
    prefix
}

fn entry_key(run: u64, key: &[u8], record: &[u8]) -> Vec<u8> {
    let mut entry = key_prefix(run, key);
    entry.extend_from_slice(record);
    entry
}

/// Split an entry key (with the run prefix removed) into join key and record.
fn decode_entry(entry: &[u8]) -> Option<(&[u8], &[u8])> {
    let len = u32::from_be_bytes(entry.get(..4)?.try_into().ok()?) as usize;
    let rest = entry.get(4..)?;
    Some((rest.get(..len)?, rest.get(len..)?))
}

fn decode_weight(value: &[u8]) -> i64 {
    value.try_into().map(i64::from_le_bytes).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(records: &[(&[u8], i64)]) -> JoinBucket {
        JoinBucket::from_entries(
            records
                .iter()
                .map(|(record, weight)| (Bytes::copy_from_slice(record), *weight))
                .collect(),
        )
    }

    fn contents(bucket: &JoinBucket) -> Vec<(Vec<u8>, i64)> {
        let mut records = bucket
            .iter()
            .map(|(record, weight)| (record.to_vec(), *weight))
            .collect::<Vec<_>>();
        records.sort();
        records
    }

    #[test]
    fn run_reads_back_buckets_without_prefix_collisions() {
        let storage = Rc::new(MemorySpillStorage::new());
        let context = Rc::new(SpillContext::new(storage.clone()));
        let short = JoinKey::from_slice(b"a");
        let long = JoinKey::from_slice(b"ab");
        let short_bucket = bucket(&[(b"r1", 1), (b"r2", 2)]);
        let long_bucket = bucket(&[(b"r3", 1)]);
        let run = SpilledRun::write_merged(
            &context,
            None,
            [(&short, &short_bucket), (&long, &long_bucket)],
            |_| false,
        )
        .expect("write run");

        assert_eq!(run.rows(), 3);
        assert_eq!(
            contents(&run.bucket(&short).expect("short key")),
            vec![(b"r1".to_vec(), 1), (b"r2".to_vec(), 2)]
        );
        assert_eq!(
            contents(&run.bucket(&long).expect("long key")),
            vec![(b"r3".to_vec(), 1)]
        );
        assert!(run.bucket(&JoinKey::from_slice(b"b")).is_none());

        let all = run.buckets();
        assert_eq!(
            all.iter().map(|(key, _)| key.to_vec()).collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"ab".to_vec()]
        );
        assert!(take_spill_error().is_none());
    }

    #[test]
    fn dropping_the_last_reference_deletes_the_run() {
        let storage = Rc::new(MemorySpillStorage::new());
        let context = Rc::new(SpillContext::new(storage.clone()));
        let key = JoinKey::from_slice(b"k");
        let contents = bucket(&[(b"r", 1)]);
        let run = SpilledRun::write_merged(&context, None, [(&key, &contents)], |_| false)
            .expect("write run");
        let shared = Rc::clone(&run);
        drop(run);
        assert_eq!(storage.len(), 1, "a shared run stays readable");
        drop(shared);
        assert!(storage.is_empty(), "the last owner deletes the run");
    }

    struct FailingStorage;

    impl SpillStorage for FailingStorage {
        fn write(&self, _entries: SpillEntries) -> Result<(), String> {
            Ok(())
        }
        fn scan_prefix(
            &self,
            _prefix: &[u8],
            _after: Option<&[u8]>,
            _limit: usize,
        ) -> Result<SpillEntries, String> {
            Err("disk gone".to_owned())
        }
        fn delete_prefix(&self, _prefix: &[u8]) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn failed_reads_are_reported_not_answered() {
        let context = Rc::new(SpillContext::new(Rc::new(FailingStorage)));
        let key = JoinKey::from_slice(b"k");
        let contents = bucket(&[(b"r", 1)]);
        let run = SpilledRun::write_merged(&context, None, [(&key, &contents)], |_| false)
            .expect("write run");
        assert!(run.bucket(&key).is_none());
        let error = take_spill_error().expect("the failed read is recorded");
        assert!(error.contains("disk gone"), "{error}");
        assert!(take_spill_error().is_none(), "taking the error clears it");
    }
}
