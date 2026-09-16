//! Diagnostic storage boundary counts; never use instrumented timing as baseline.
use jazz::groove::storage::*;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
thread_local! {
 static ACTIVE: Cell<bool> = const { Cell::new(false) };
 static COUNTS: RefCell<BTreeMap<String, usize>> = const { RefCell::new(BTreeMap::new()) };
}
pub fn start() {
    COUNTS.with(|c| c.borrow_mut().clear());
    ACTIVE.with(|v| v.set(true));
}
pub fn stop() -> serde_json::Value {
    ACTIVE.with(|v| v.set(false));
    COUNTS.with(|c| serde_json::json!(*c.borrow()))
}
fn add(role: &str, cf: &str, name: &str, count: usize) {
    if ACTIVE.with(Cell::get) {
        COUNTS.with(|c| {
            *c.borrow_mut()
                .entry(format!("{role}/{cf}/{name}"))
                .or_default() += count
        });
    }
}
fn count_writes(role: &str, operations: &[OwnedWriteOperation]) {
    add(role, "*", "write_batches", 1);
    for operation in operations {
        match operation {
            OwnedWriteOperation::Set { cf, key, value } => {
                add(role, cf, "write_entries", 1);
                add(role, cf, "write_key_bytes", key.len());
                add(role, cf, "write_value_bytes", value.len());
            }
            OwnedWriteOperation::Delete { cf, key } => {
                add(role, cf, "delete_entries", 1);
                add(role, cf, "write_key_bytes", key.len());
            }
        }
    }
}
struct Cursor<'a> {
    inner: StorageScan<'a>,
    role: &'static str,
    cf: String,
}
impl StorageCursor for Cursor<'_> {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>> {
        Box::pin(async move {
            let batch = self.inner.next_batch().await?;
            if let Some(rows) = &batch {
                add(self.role, &self.cf, "scan_rows", rows.len());
                add(
                    self.role,
                    &self.cf,
                    "read_key_bytes",
                    rows.iter().map(|r| r.0.len()).sum(),
                );
                add(
                    self.role,
                    &self.cf,
                    "read_value_bytes",
                    rows.iter().map(|r| r.1.len()).sum(),
                );
            }
            Ok(batch)
        })
    }
}
pub struct BudgetStorage {
    inner: BoxedStorage,
    role: &'static str,
}
pub fn wrap(inner: BoxedStorage, role: &'static str) -> BoxedStorage {
    if std::env::var_os("JAZZ_CUSTOMER_WORK_BUDGET").is_some() {
        BoxedStorage::new(BudgetStorage { inner, role })
    } else {
        inner
    }
}
impl OrderedKvStorage for BudgetStorage {
    fn compare_value(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<ValueComparison, Error>> {
        add(self.role, &cf, "compare_value_calls", 1);
        add(self.role, &cf, "comparison_bytes", expected.len());
        self.inner.compare_value(cf, key, expected)
    }

    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        let cf = request.cf.clone();
        add(self.role, &cf, "scan_calls", 1);
        Box::pin(async move {
            let inner = self.inner.scan(request).await?;
            Ok(Box::new(Cursor {
                inner,
                role: self.role,
                cf,
            }) as StorageScan<'_>)
        })
    }

    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        add(self.role, &cf, "get_calls", 1);
        add(self.role, &cf, "lookup_key_bytes", key.len());
        Box::pin(async move {
            let result = self.inner.get(cf.clone(), key).await?;
            if let Some(value) = &result {
                add(self.role, &cf, "read_value_bytes", value.len());
            }
            Ok(result)
        })
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        add(self.role, &cf, "put_if_absent_calls", 1);
        self.inner.put_if_absent(cf, key, value)
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        add(self.role, &cf, "compare_and_delete_calls", 1);
        self.inner.compare_and_delete(cf, key, expected)
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        add(self.role, &cf, "set_calls", 1);
        add(self.role, &cf, "write_entries", 1);
        add(self.role, &cf, "write_key_bytes", key.len());
        add(self.role, &cf, "write_value_bytes", value.len());
        self.inner.set(cf, key, value)
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        add(self.role, &cf, "delete_calls", 1);
        self.inner.delete(cf, key)
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.close()
    }

    fn set_write_flush_cadence(&self, every: usize) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.set_write_flush_cadence(every)
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.flush_write_boundary()
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        self.inner.approximate_class_bytes(cf)
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        add(self.role, &cf, "last_with_prefix_calls", 1);
        Box::pin(async move {
            let result = self.inner.last_with_prefix(cf.clone(), prefix).await?;
            if let Some((key, value)) = &result {
                add(self.role, &cf, "read_key_bytes", key.len());
                add(self.role, &cf, "read_value_bytes", value.len());
            }
            Ok(result)
        })
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: String,
        prefix: Vec<u8>,
        upper: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        add(self.role, &cf, "last_with_prefix_before_or_at_calls", 1);
        Box::pin(async move {
            let result = self
                .inner
                .last_with_prefix_before_or_at(cf.clone(), prefix, upper)
                .await?;
            if let Some((key, value)) = &result {
                add(self.role, &cf, "read_key_bytes", key.len());
                add(self.role, &cf, "read_value_bytes", value.len());
            }
            Ok(result)
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        count_writes(self.role, &operations);
        self.inner.write_many(operations)
    }

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        count_writes(self.role, &operations);
        self.inner.write_many_outcome(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl ReopenableStorage for BudgetStorage {
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            Ok(Self {
                inner: self.inner.reopen(column_families).await?,
                role: self.role,
            })
        })
    }
}
