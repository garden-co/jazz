# Native-First JazzClientCore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move shared `JazzClient` behavior into Rust through a new `JazzClientCore`, then migrate WASM, NAPI, React Native, and TypeScript wrappers onto that native contract.

**Architecture:** Add a Rust `client_core` layer above `RuntimeCore`. Bindings become host adapters that convert values, callbacks, and async surfaces while Rust owns write contexts, batches, transactions, query defaults, subscription state, schema alignment, transport auth, and stable client errors.

**Tech Stack:** Rust, `jazz-tools`, `RuntimeCore`, WASM via `wasm-bindgen`, Node via `napi-rs`, React Native via UniFFI, TypeScript/Vitest for wrapper parity.

---

## Scope Check

This is one sequential system, not independent feature work. The Rust core must land before the bindings can become thin. WASM, NAPI, React Native, and TypeScript migration are separate tasks in this single plan because each one depends on the same Rust contract.

## File Map

- Create `crates/jazz-tools/src/client_core/mod.rs`: public core module and `JazzClientCore` type.
- Create `crates/jazz-tools/src/client_core/config.rs`: `ClientConfig`, runtime flavor, storage mode, auth input, default tier logic.
- Create `crates/jazz-tools/src/client_core/error.rs`: stable `ClientError` and `ClientErrorCode`.
- Create `crates/jazz-tools/src/client_core/write.rs`: write context builder, `WriteHandleCore`, direct batch, transaction.
- Create `crates/jazz-tools/src/client_core/query.rs`: query option parsing/defaults and schema-aligned row helpers.
- Create `crates/jazz-tools/src/client_core/subscription.rs`: subscription handle state and callback-independent lifecycle logic.
- Create `crates/jazz-tools/src/client_core/tests.rs`: Rust contract tests for the shared client.
- Modify `crates/jazz-tools/src/lib.rs`: export `client_core`.
- Modify `crates/jazz-wasm/src/lib.rs`: export new WASM client binding.
- Create `crates/jazz-wasm/src/client.rs`: thin `WasmJazzClient` adapter.
- Modify `crates/jazz-napi/src/lib.rs`: add `NapiJazzClient` adapter while keeping `NapiRuntime` during migration.
- Modify `crates/jazz-rn/rust/src/lib.rs`: route RN client calls through `JazzClientCore`.
- Modify generated RN TypeScript only by running the existing RN binding generation command.
- Modify `packages/jazz-tools/src/runtime/client.ts`: wrap native client shape and remove migrated behavior.
- Modify `packages/jazz-tools/src/types/jazz-wasm.d.ts`: add new WASM client declarations.
- Add or update tests in `packages/jazz-tools/src/runtime/client.test.ts`, `packages/jazz-tools/src/runtime/napi.integration.test.ts`, and `packages/jazz-tools/src/react-native/db.test.ts`.

## Task 1: Add The Rust Client Core Shell

**Files:**

- Create: `crates/jazz-tools/src/client_core/mod.rs`
- Create: `crates/jazz-tools/src/client_core/config.rs`
- Create: `crates/jazz-tools/src/client_core/error.rs`
- Create: `crates/jazz-tools/src/client_core/tests.rs`
- Modify: `crates/jazz-tools/src/lib.rs`

- [ ] **Step 1: Write the failing compile smoke test**

Add `crates/jazz-tools/src/client_core/tests.rs`:

```rust
use super::*;
use crate::query_manager::types::{ColumnType, Schema, SchemaBuilder, TableName, TableSchema};
use crate::runtime_core::{NoopScheduler, RuntimeCore};
use crate::schema_manager::{AppId, SchemaManager};
use crate::storage::MemoryStorage;
use crate::sync_manager::SyncManager;

fn users_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn test_runtime(schema: Schema) -> RuntimeCore<MemoryStorage, NoopScheduler> {
    let app_id = AppId::from_name("client-core-test");
    let schema_manager =
        SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
    let mut runtime = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);
    runtime.immediate_tick();
    runtime
}

#[test]
fn client_core_wraps_runtime_and_exposes_schema() {
    let schema = users_schema();
    let client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("client-core-test", schema.clone()),
        test_runtime(schema),
    )
    .expect("client core should be constructed");

    assert!(client.current_schema().contains_key(&TableName::new("users")));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p jazz-tools client_core::tests::client_core_wraps_runtime_and_exposes_schema --features test-utils
```

Expected: FAIL because `client_core` and `JazzClientCore` do not exist.

- [ ] **Step 3: Add the core module shell**

Add `crates/jazz-tools/src/client_core/config.rs`:

```rust
use crate::query_manager::types::Schema;
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientRuntimeFlavor {
    BrowserMainThread,
    BrowserWorker,
    Node,
    ReactNative,
    Rust,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStorageMode {
    Memory,
    Persistent,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub app_id: String,
    pub schema: Schema,
    pub env: String,
    pub user_branch: String,
    pub storage_mode: ClientStorageMode,
    pub server_url: Option<String>,
    pub default_durability_tier: Option<DurabilityTier>,
    pub runtime_flavor: ClientRuntimeFlavor,
}

impl ClientConfig {
    pub fn memory_for_test(app_id: impl Into<String>, schema: Schema) -> Self {
        Self {
            app_id: app_id.into(),
            schema,
            env: "dev".to_string(),
            user_branch: "main".to_string(),
            storage_mode: ClientStorageMode::Memory,
            server_url: None,
            default_durability_tier: None,
            runtime_flavor: ClientRuntimeFlavor::Rust,
        }
    }
}
```

Add `crates/jazz-tools/src/client_core/error.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientErrorCode {
    InvalidConfig,
    InvalidSchema,
    InvalidQuery,
    WriteRejected,
    BatchRejected,
    UnsupportedRuntimeFeature,
    TransportError,
    AuthFailure,
    StorageError,
    RuntimeError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientError {
    pub code: ClientErrorCode,
    pub message: String,
    pub batch_id: Option<String>,
    pub table: Option<String>,
    pub object_id: Option<String>,
}

impl ClientError {
    pub fn new(code: ClientErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            batch_id: None,
            table: None,
            object_id: None,
        }
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ClientError {}
```

Add `crates/jazz-tools/src/client_core/mod.rs`:

```rust
pub mod config;
pub mod error;

#[cfg(test)]
mod tests;

pub use config::{ClientConfig, ClientRuntimeFlavor, ClientStorageMode};
pub use error::{ClientError, ClientErrorCode};

use crate::query_manager::types::Schema;
use crate::runtime_core::{RuntimeCore, Scheduler};
use crate::storage::Storage;

pub struct JazzClientCore<S: Storage, Sch: Scheduler> {
    config: ClientConfig,
    runtime: RuntimeCore<S, Sch>,
}

impl<S: Storage, Sch: Scheduler> JazzClientCore<S, Sch> {
    pub fn from_runtime_parts(
        config: ClientConfig,
        runtime: RuntimeCore<S, Sch>,
    ) -> Result<Self, ClientError> {
        Ok(Self { config, runtime })
    }

    pub fn current_schema(&self) -> &Schema {
        self.runtime.current_schema()
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn runtime(&self) -> &RuntimeCore<S, Sch> {
        &self.runtime
    }

    pub fn runtime_mut(&mut self) -> &mut RuntimeCore<S, Sch> {
        &mut self.runtime
    }
}
```

Modify `crates/jazz-tools/src/lib.rs`:

```rust
pub mod client_core;
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
cargo test -p jazz-tools client_core::tests::client_core_wraps_runtime_and_exposes_schema --features test-utils
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/client_core crates/jazz-tools/src/lib.rs
git commit -m "Add JazzClientCore shell"
```

## Task 2: Move Client Defaults And Error Mapping Into Rust

**Files:**

- Modify: `crates/jazz-tools/src/client_core/config.rs`
- Modify: `crates/jazz-tools/src/client_core/error.rs`
- Modify: `crates/jazz-tools/src/client_core/tests.rs`

- [ ] **Step 1: Write failing tests for default durability and error mapping**

Append to `crates/jazz-tools/src/client_core/tests.rs`:

```rust
use crate::sync_manager::DurabilityTier;

#[test]
fn browser_main_thread_defaults_reads_to_local() {
    let mut config = ClientConfig::memory_for_test("browser-default-test", users_schema());
    config.runtime_flavor = ClientRuntimeFlavor::BrowserMainThread;
    config.server_url = Some("https://example.test".to_string());

    assert_eq!(config.resolved_default_durability_tier(), DurabilityTier::Local);
}

#[test]
fn non_browser_server_clients_default_reads_to_edge() {
    let mut config = ClientConfig::memory_for_test("node-default-test", users_schema());
    config.runtime_flavor = ClientRuntimeFlavor::Node;
    config.server_url = Some("https://example.test".to_string());

    assert_eq!(
        config.resolved_default_durability_tier(),
        DurabilityTier::EdgeServer
    );
}

#[test]
fn explicit_default_durability_tier_wins() {
    let mut config = ClientConfig::memory_for_test("explicit-default-test", users_schema());
    config.runtime_flavor = ClientRuntimeFlavor::BrowserMainThread;
    config.server_url = Some("https://example.test".to_string());
    config.default_durability_tier = Some(DurabilityTier::GlobalServer);

    assert_eq!(
        config.resolved_default_durability_tier(),
        DurabilityTier::GlobalServer
    );
}

#[test]
fn client_error_preserves_stable_code_and_context() {
    let error = ClientError::new(ClientErrorCode::BatchRejected, "permission denied")
        .with_batch_id("abc123")
        .with_table("todos")
        .with_object_id("row1");

    assert_eq!(error.code, ClientErrorCode::BatchRejected);
    assert_eq!(error.batch_id.as_deref(), Some("abc123"));
    assert_eq!(error.table.as_deref(), Some("todos"));
    assert_eq!(error.object_id.as_deref(), Some("row1"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p jazz-tools client_core::tests --features test-utils
```

Expected: FAIL because `resolved_default_durability_tier`, `with_batch_id`, `with_table`, and `with_object_id` do not exist.

- [ ] **Step 3: Implement defaults and error context helpers**

In `crates/jazz-tools/src/client_core/config.rs`, add:

```rust
impl ClientConfig {
    pub fn resolved_default_durability_tier(&self) -> DurabilityTier {
        if let Some(tier) = self.default_durability_tier {
            return tier;
        }

        if self.runtime_flavor == ClientRuntimeFlavor::BrowserMainThread {
            return DurabilityTier::Local;
        }

        if self.server_url.is_some() {
            return DurabilityTier::EdgeServer;
        }

        DurabilityTier::Local
    }
}
```

In `crates/jazz-tools/src/client_core/error.rs`, add builder helpers:

```rust
impl ClientError {
    pub fn with_batch_id(mut self, batch_id: impl Into<String>) -> Self {
        self.batch_id = Some(batch_id.into());
        self
    }

    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    pub fn with_object_id(mut self, object_id: impl Into<String>) -> Self {
        self.object_id = Some(object_id.into());
        self
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p jazz-tools client_core::tests --features test-utils
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/client_core
git commit -m "Move JazzClient defaults into Rust"
```

## Task 3: Move Direct Writes And Write Context Creation Into Rust

**Files:**

- Create: `crates/jazz-tools/src/client_core/write.rs`
- Modify: `crates/jazz-tools/src/client_core/mod.rs`
- Modify: `crates/jazz-tools/src/client_core/tests.rs`

- [ ] **Step 1: Write failing tests for insert and explicit batch context**

Append to `crates/jazz-tools/src/client_core/tests.rs`:

```rust
use crate::object::ObjectId;
use crate::query_manager::types::Value;
use std::collections::HashMap;

fn user_insert_values(id: ObjectId, name: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("name".to_string(), Value::Text(name.to_string())),
    ])
}

#[test]
fn client_core_insert_seals_standalone_direct_write() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("standalone-insert-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let user_id = ObjectId::new();
    let result = client
        .insert("users", user_insert_values(user_id, "Alice"), None)
        .expect("insert should succeed");

    let record = client
        .local_batch_record(result.handle.batch_id)
        .expect("record load should succeed")
        .expect("standalone write should retain a local batch record");

    assert_eq!(result.row.id, result.row.id);
    assert!(record.sealed, "standalone direct writes should seal in Rust");
}

#[test]
fn direct_batch_uses_one_rust_generated_batch_id() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("direct-batch-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let mut batch = client.begin_direct_batch();
    let alice = batch
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .expect("first insert should succeed");
    let bob = batch
        .insert("users", user_insert_values(ObjectId::new(), "Bob"), None)
        .expect("second insert should succeed");
    let handle = batch.commit().expect("batch commit should seal");

    assert_eq!(alice.handle.batch_id, bob.handle.batch_id);
    assert_eq!(alice.handle.batch_id, handle.batch_id);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p jazz-tools client_core::tests --features test-utils
```

Expected: FAIL because `insert`, `local_batch_record`, and `begin_direct_batch` do not exist.

- [ ] **Step 3: Implement row/write result types and write context builder**

Add `crates/jazz-tools/src/client_core/write.rs`:

```rust
use std::collections::HashMap;

use crate::batch_fate::{BatchMode, LocalBatchRecord};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{SchemaHash, Value};
use crate::row_histories::BatchId;
use crate::runtime_core::{RuntimeCore, Scheduler};
use crate::storage::Storage;

use super::{ClientError, ClientErrorCode, JazzClientCore};

#[derive(Debug, Clone, PartialEq)]
pub struct ClientRow {
    pub id: ObjectId,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteHandleCore {
    pub batch_id: BatchId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriteResultCore {
    pub row: ClientRow,
    pub handle: WriteHandleCore,
}

#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    pub object_id: Option<ObjectId>,
    pub session: Option<Session>,
    pub attribution: Option<String>,
    pub updated_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct BatchContext {
    pub mode: BatchMode,
    pub batch_id: BatchId,
    pub target_branch_name: String,
}

impl BatchContext {
    fn new(mode: BatchMode, env: &str, schema_hash: &SchemaHash, user_branch: &str) -> Self {
        Self {
            mode,
            batch_id: BatchId::new(),
            target_branch_name: format!("{env}-{}-{user_branch}", &schema_hash.to_string()[..12]),
        }
    }
}

pub(crate) fn write_context(
    options: &WriteOptions,
    batch_context: Option<&BatchContext>,
) -> Option<WriteContext> {
    if options.session.is_none()
        && options.attribution.is_none()
        && options.updated_at.is_none()
        && batch_context.is_none()
    {
        return None;
    }

    let mut context = options
        .session
        .clone()
        .map(WriteContext::from_session)
        .unwrap_or_default();
    context.attribution = options.attribution.clone();
    context.updated_at = options.updated_at;

    if let Some(batch) = batch_context {
        context = context
            .with_batch_mode(batch.mode)
            .with_batch_id(batch.batch_id)
            .with_target_branch_name(batch.target_branch_name.clone());
    }

    Some(context)
}

impl<S: Storage, Sch: Scheduler> JazzClientCore<S, Sch> {
    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<WriteResultCore, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, None);
        let ((id, values), batch_id) = self
            .runtime_mut()
            .insert_with_id(table, values, options.object_id, context.as_ref())
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        self.runtime_mut()
            .seal_batch(batch_id)
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;

        Ok(WriteResultCore {
            row: ClientRow { id, values },
            handle: WriteHandleCore { batch_id },
        })
    }

    pub fn local_batch_record(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<LocalBatchRecord>, ClientError> {
        self.runtime()
            .local_batch_record(batch_id)
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))
    }

    pub fn begin_direct_batch(&mut self) -> DirectBatchCore<'_, S, Sch> {
        let schema_hash = SchemaHash::compute(self.current_schema());
        let context = BatchContext::new(
            BatchMode::Direct,
            &self.config().env,
            &schema_hash,
            &self.config().user_branch,
        );
        DirectBatchCore {
            client: self,
            context,
        }
    }
}

pub struct DirectBatchCore<'a, S: Storage, Sch: Scheduler> {
    client: &'a mut JazzClientCore<S, Sch>,
    context: BatchContext,
}

impl<'a, S: Storage, Sch: Scheduler> DirectBatchCore<'a, S, Sch> {
    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<WriteResultCore, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, Some(&self.context));
        let ((id, values), batch_id) = self
            .client
            .runtime_mut()
            .insert_with_id(table, values, options.object_id, context.as_ref())
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;
        Ok(WriteResultCore {
            row: ClientRow { id, values },
            handle: WriteHandleCore { batch_id },
        })
    }

    pub fn commit(self) -> Result<WriteHandleCore, ClientError> {
        self.client
            .runtime_mut()
            .seal_batch(self.context.batch_id)
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;
        Ok(WriteHandleCore {
            batch_id: self.context.batch_id,
        })
    }
}
```

Modify `crates/jazz-tools/src/client_core/mod.rs`:

```rust
pub mod write;
pub use write::{DirectBatchCore, WriteHandleCore, WriteOptions, WriteResultCore};
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p jazz-tools client_core::tests --features test-utils
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/client_core
git commit -m "Move JazzClient write context into Rust"
```

## Task 4: Add Transactions And Native Write Wait Outcomes

**Files:**

- Modify: `crates/jazz-tools/src/client_core/write.rs`
- Modify: `crates/jazz-tools/src/client_core/tests.rs`

- [ ] **Step 1: Write failing tests for transactions and batch wait checks**

Append to `crates/jazz-tools/src/client_core/tests.rs`:

```rust
#[test]
fn transaction_commit_returns_transactional_batch_handle() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("transaction-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let mut tx = client.begin_transaction();
    let inserted = tx
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .expect("transaction insert should succeed");
    let handle = tx.commit().expect("transaction commit should seal");

    assert_eq!(inserted.handle.batch_id, handle.batch_id);
    let record = client
        .local_batch_record(handle.batch_id)
        .unwrap()
        .expect("transaction record should exist");
    assert_eq!(record.mode, crate::batch_fate::BatchMode::Transactional);
    assert!(record.sealed);
}

#[test]
fn local_wait_check_succeeds_after_direct_batch_commit() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("local-wait-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let result = client
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    assert_eq!(
        client.check_batch_wait(result.handle.batch_id, DurabilityTier::Local),
        BatchWaitOutcome::Satisfied
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p jazz-tools client_core::tests --features test-utils
```

Expected: FAIL because `begin_transaction`, `check_batch_wait`, and `BatchWaitOutcome` do not exist.

- [ ] **Step 3: Implement transaction and wait outcome logic**

In `crates/jazz-tools/src/client_core/write.rs`, add:

```rust
use crate::batch_fate::BatchSettlement;
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchWaitOutcome {
    Pending,
    Satisfied,
    Rejected { code: String, reason: String },
    Missing,
}

fn tier_rank(tier: DurabilityTier) -> u8 {
    match tier {
        DurabilityTier::Local => 0,
        DurabilityTier::EdgeServer => 1,
        DurabilityTier::GlobalServer => 2,
    }
}

fn settlement_satisfies_tier(
    settlement: Option<&BatchSettlement>,
    tier: DurabilityTier,
) -> BatchWaitOutcome {
    match settlement {
        Some(BatchSettlement::Rejected { code, reason, .. }) => BatchWaitOutcome::Rejected {
            code: code.clone(),
            reason: reason.clone(),
        },
        Some(BatchSettlement::DurableDirect { confirmed_tier, .. })
        | Some(BatchSettlement::AcceptedTransaction { confirmed_tier, .. })
            if tier_rank(*confirmed_tier) >= tier_rank(tier) =>
        {
            BatchWaitOutcome::Satisfied
        }
        Some(_) | None => BatchWaitOutcome::Pending,
    }
}

impl<S: Storage, Sch: Scheduler> JazzClientCore<S, Sch> {
    pub fn begin_transaction(&mut self) -> TransactionCore<'_, S, Sch> {
        let schema_hash = SchemaHash::compute(self.current_schema());
        let context = BatchContext::new(
            BatchMode::Transactional,
            &self.config().env,
            &schema_hash,
            &self.config().user_branch,
        );
        TransactionCore {
            client: self,
            context,
        }
    }

    pub fn check_batch_wait(&self, batch_id: BatchId, tier: DurabilityTier) -> BatchWaitOutcome {
        let record = match self.runtime().local_batch_record(batch_id) {
            Ok(Some(record)) => record,
            Ok(None) => return BatchWaitOutcome::Missing,
            Err(error) => {
                return BatchWaitOutcome::Rejected {
                    code: "storage_error".to_string(),
                    reason: error.to_string(),
                };
            }
        };

        if tier == DurabilityTier::Local && record.sealed {
            return BatchWaitOutcome::Satisfied;
        }

        settlement_satisfies_tier(record.latest_settlement.as_ref(), tier)
    }
}

pub struct TransactionCore<'a, S: Storage, Sch: Scheduler> {
    client: &'a mut JazzClientCore<S, Sch>,
    context: BatchContext,
}

impl<'a, S: Storage, Sch: Scheduler> TransactionCore<'a, S, Sch> {
    pub fn insert(
        &mut self,
        table: &str,
        values: HashMap<String, Value>,
        options: Option<WriteOptions>,
    ) -> Result<WriteResultCore, ClientError> {
        let options = options.unwrap_or_default();
        let context = write_context(&options, Some(&self.context));
        let ((id, values), batch_id) = self
            .client
            .runtime_mut()
            .insert_with_id(table, values, options.object_id, context.as_ref())
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;
        Ok(WriteResultCore {
            row: ClientRow { id, values },
            handle: WriteHandleCore { batch_id },
        })
    }

    pub fn commit(self) -> Result<WriteHandleCore, ClientError> {
        self.client
            .runtime_mut()
            .seal_batch(self.context.batch_id)
            .map_err(|error| ClientError::new(ClientErrorCode::RuntimeError, error.to_string()))?;
        Ok(WriteHandleCore {
            batch_id: self.context.batch_id,
        })
    }
}
```

Update `crates/jazz-tools/src/client_core/mod.rs` exports:

```rust
pub use write::{
    BatchWaitOutcome, DirectBatchCore, TransactionCore, WriteHandleCore, WriteOptions,
    WriteResultCore,
};
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p jazz-tools client_core::tests --features test-utils
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/client_core
git commit -m "Add native JazzClient batch and transaction handles"
```

## Task 5: Move Query Defaults And Schema Alignment Into Client Core

**Files:**

- Create: `crates/jazz-tools/src/client_core/query.rs`
- Modify: `crates/jazz-tools/src/client_core/mod.rs`
- Modify: `crates/jazz-tools/src/client_core/tests.rs`

- [ ] **Step 1: Write failing tests for query defaults and aligned rows**

Append to `crates/jazz-tools/src/client_core/tests.rs`:

```rust
use crate::query_manager::query::Query;

#[test]
fn client_core_query_uses_config_default_tier() {
    let schema = users_schema();
    let mut config = ClientConfig::memory_for_test("query-default-test", schema.clone());
    config.runtime_flavor = ClientRuntimeFlavor::Node;
    config.server_url = Some("https://example.test".to_string());
    let client = JazzClientCore::from_runtime_parts(config, test_runtime(schema)).unwrap();

    let options = client.resolve_query_options(None);
    assert_eq!(options.tier, DurabilityTier::EdgeServer);
    assert_eq!(options.local_updates, crate::query_manager::manager::LocalUpdates::Immediate);
}

#[test]
fn client_core_query_returns_inserted_rows() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("query-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let inserted = client
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    let rows = futures::executor::block_on(client.query(Query::new("users"), None))
        .expect("query should succeed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, inserted.row.id);
    assert_eq!(rows[0].values, inserted.row.values);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p jazz-tools client_core::tests --features test-utils
```

Expected: FAIL because `query`, `resolve_query_options`, and `ClientQueryOptions` do not exist.

- [ ] **Step 3: Implement query defaults and query execution**

Add `crates/jazz-tools/src/client_core/query.rs`:

```rust
use crate::object::ObjectId;
use crate::query_manager::manager::LocalUpdates;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::Value;
use crate::runtime_core::{ReadDurabilityOptions, RuntimeCore, Scheduler};
use crate::storage::Storage;
use crate::sync_manager::{DurabilityTier, QueryPropagation};

use super::{ClientError, ClientErrorCode, JazzClientCore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQueryOptions {
    pub tier: DurabilityTier,
    pub local_updates: LocalUpdates,
    pub propagation: QueryPropagation,
    pub session: Option<Session>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRowCore {
    pub id: ObjectId,
    pub values: Vec<Value>,
}

impl<S: Storage, Sch: Scheduler> JazzClientCore<S, Sch> {
    pub fn resolve_query_options(
        &self,
        options: Option<ClientQueryOptions>,
    ) -> ClientQueryOptions {
        options.unwrap_or(ClientQueryOptions {
            tier: self.config().resolved_default_durability_tier(),
            local_updates: LocalUpdates::Immediate,
            propagation: QueryPropagation::Full,
            session: None,
        })
    }

    pub async fn query(
        &mut self,
        query: Query,
        options: Option<ClientQueryOptions>,
    ) -> Result<Vec<QueryRowCore>, ClientError> {
        let options = self.resolve_query_options(options);
        let future = self
            .runtime_mut()
            .query_with_propagation(
                query,
                options.session,
                ReadDurabilityOptions {
                    tier: Some(options.tier),
                    local_updates: options.local_updates,
                },
                options.propagation,
            );

        let rows = future
            .await
            .map_err(|error| ClientError::new(ClientErrorCode::InvalidQuery, format!("{error:?}")))?;

        Ok(rows
            .into_iter()
            .map(|(id, values)| QueryRowCore { id, values })
            .collect())
    }
}
```

Modify `crates/jazz-tools/src/client_core/mod.rs`:

```rust
pub mod query;
pub use query::{ClientQueryOptions, QueryRowCore};
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p jazz-tools client_core::tests --features test-utils
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/client_core
git commit -m "Move JazzClient query defaults into Rust"
```

## Task 6: Add Native Subscription Lifecycle In Client Core

**Files:**

- Create: `crates/jazz-tools/src/client_core/subscription.rs`
- Modify: `crates/jazz-tools/src/client_core/mod.rs`
- Modify: `crates/jazz-tools/src/client_core/tests.rs`

- [ ] **Step 1: Write failing subscription lifecycle test**

Append to `crates/jazz-tools/src/client_core/tests.rs`:

```rust
use std::sync::{Arc, Mutex};

#[test]
fn client_core_subscribe_and_unsubscribe_owns_runtime_handle() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("subscription-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_for_callback = Arc::clone(&seen);
    let handle = client
        .subscribe(Query::new("users"), None, move |delta| {
            seen_for_callback.lock().unwrap().push(delta);
        })
        .expect("subscription should be created");

    client
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .expect("insert should trigger subscription");
    client.runtime_mut().batched_tick();

    assert!(!seen.lock().unwrap().is_empty());

    client
        .unsubscribe(handle)
        .expect("unsubscribe should remove runtime subscription");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p jazz-tools client_core::tests::client_core_subscribe_and_unsubscribe_owns_runtime_handle --features test-utils
```

Expected: FAIL because `subscribe`, `unsubscribe`, and `SubscriptionCoreHandle` do not exist.

- [ ] **Step 3: Implement subscription lifecycle wrappers**

Add `crates/jazz-tools/src/client_core/subscription.rs`:

```rust
use std::collections::HashMap;

use crate::runtime_core::{RuntimeCore, Scheduler, SubscriptionDelta, SubscriptionHandle};
use crate::storage::Storage;

use super::{ClientError, ClientErrorCode, ClientQueryOptions, JazzClientCore};
use crate::query_manager::query::Query;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionCoreHandle(pub u64);

impl<S: Storage, Sch: Scheduler> JazzClientCore<S, Sch> {
    pub fn subscribe<F>(
        &mut self,
        query: Query,
        options: Option<ClientQueryOptions>,
        callback: F,
    ) -> Result<SubscriptionCoreHandle, ClientError>
    where
        F: Fn(SubscriptionDelta) + 'static,
    {
        let options = self.resolve_query_options(options);
        let handle = self
            .runtime_mut()
            .subscribe_with_durability_and_propagation(
                query,
                callback,
                options.session,
                crate::runtime_core::ReadDurabilityOptions {
                    tier: Some(options.tier),
                    local_updates: options.local_updates,
                },
                options.propagation,
            )
            .map_err(|error| ClientError::new(ClientErrorCode::InvalidQuery, format!("{error:?}")))?;

        Ok(SubscriptionCoreHandle(handle.0))
    }

    pub fn unsubscribe(&mut self, handle: SubscriptionCoreHandle) -> Result<(), ClientError> {
        self.runtime_mut()
            .unsubscribe(SubscriptionHandle(handle.0));
        Ok(())
    }
}
```

Modify `crates/jazz-tools/src/client_core/mod.rs`:

```rust
pub mod subscription;
pub use subscription::SubscriptionCoreHandle;
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p jazz-tools client_core::tests::client_core_subscribe_and_unsubscribe_owns_runtime_handle --features test-utils
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/client_core
git commit -m "Add native JazzClient subscription lifecycle"
```

## Task 7: Expose A Thin WASM Client Adapter

**Files:**

- Create: `crates/jazz-wasm/src/client.rs`
- Modify: `crates/jazz-wasm/src/lib.rs`
- Modify: `packages/jazz-tools/src/types/jazz-wasm.d.ts`
- Add test: `packages/jazz-tools/src/runtime/wasm-client-core.test.ts`

- [ ] **Step 1: Write failing TypeScript declaration/runtime smoke test**

Add `packages/jazz-tools/src/runtime/wasm-client-core.test.ts`:

```ts
import { describe, expect, it } from "vitest";

describe("WasmJazzClient binding shape", () => {
  it("is declared by jazz-wasm types", async () => {
    const wasm = await import("jazz-wasm");
    expect(typeof wasm.WasmJazzClient).toBe("function");
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/wasm-client-core.test.ts
```

Expected: FAIL because `WasmJazzClient` is not exported.

- [ ] **Step 3: Add a WASM adapter that delegates to `JazzClientCore`**

Add `crates/jazz-wasm/src/client.rs`:

```rust
use jazz_tools::client_core::{ClientConfig, ClientRuntimeFlavor, JazzClientCore};
use jazz_tools::query_manager::types::Value;
use jazz_tools::runtime_core::{NoopScheduler, RuntimeCore};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::MemoryStorage;
use jazz_tools::sync_manager::SyncManager;
use serde::Serialize;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WasmClientInsertResult {
    id: String,
    values: Vec<Value>,
    batch_id: String,
}

#[wasm_bindgen]
pub struct WasmJazzClient {
    inner: JazzClientCore<MemoryStorage, NoopScheduler>,
}

#[wasm_bindgen]
impl WasmJazzClient {
    #[wasm_bindgen(constructor)]
    pub fn new(schema_json: &str, app_id: &str, env: &str, user_branch: &str) -> Result<Self, JsError> {
        let runtime_schema = jazz_tools::binding_support::parse_runtime_schema_input(schema_json)
            .map_err(|error| JsError::new(&error))?;
        let schema = runtime_schema.schema;
        let app = AppId::from_string(app_id).unwrap_or_else(|_| AppId::from_name(app_id));
        let schema_manager =
            SchemaManager::new(SyncManager::new(), schema.clone(), app, env, user_branch)
                .map_err(|error| JsError::new(&format!("{error:?}")))?;
        let runtime = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);
        let mut config = ClientConfig::memory_for_test(app_id, schema);
        config.env = env.to_string();
        config.user_branch = user_branch.to_string();
        config.runtime_flavor = ClientRuntimeFlavor::BrowserMainThread;

        Ok(Self {
            inner: JazzClientCore::from_runtime_parts(config, runtime)
                .map_err(|error| JsError::new(&error.to_string()))?,
        })
    }

    #[wasm_bindgen]
    pub fn insert(&mut self, table: &str, values: JsValue) -> Result<JsValue, JsError> {
        let values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let result = self
            .inner
            .insert(table, values, None)
            .map_err(|error| JsError::new(&error.to_string()))?;
        let payload = WasmClientInsertResult {
            id: result.row.id.uuid().to_string(),
            values: result.row.values,
            batch_id: result.handle.batch_id.to_string(),
        };
        serde_wasm_bindgen::to_value(&payload)
            .map_err(|error| JsError::new(&format!("{error:?}")))
    }
}
```

Modify `crates/jazz-wasm/src/lib.rs`:

```rust
pub mod client;
pub use client::WasmJazzClient;
```

Update `packages/jazz-tools/src/types/jazz-wasm.d.ts`:

```ts
export class WasmJazzClient {
  constructor(schemaJson: string, appId: string, env: string, userBranch: string);
  insert(
    table: string,
    values: Record<string, unknown>,
  ): {
    id: string;
    values: unknown[];
    batchId: string;
  };
}
```

- [ ] **Step 4: Build WASM and rerun the smoke test**

Run:

```bash
pnpm --filter jazz-wasm build
pnpm --filter jazz-tools test -- src/runtime/wasm-client-core.test.ts
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-wasm/src/client.rs crates/jazz-wasm/src/lib.rs packages/jazz-tools/src/types/jazz-wasm.d.ts packages/jazz-tools/src/runtime/wasm-client-core.test.ts
git commit -m "Expose JazzClientCore through WASM"
```

## Task 8: Expose A Thin NAPI Client Adapter

**Files:**

- Modify: `crates/jazz-tools/src/client_core/mod.rs`
- Modify: `crates/jazz-tools/src/client_core/tests.rs`
- Modify: `crates/jazz-napi/src/lib.rs`
- Modify generated package declarations by running the existing NAPI build.
- Add test: `packages/jazz-tools/src/runtime/napi.client-core.test.ts`

- [ ] **Step 1: Write failing NAPI adapter test**

Add `packages/jazz-tools/src/runtime/napi.client-core.test.ts`:

```ts
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";

const schema: WasmSchema = {
  users: {
    columns: [
      { name: "id", type: { type: "Uuid" }, nullable: false },
      { name: "name", type: { type: "Text" }, nullable: false },
    ],
  },
};

let dataDir: string | undefined;

afterEach(async () => {
  if (dataDir) {
    await rm(dataDir, { recursive: true, force: true });
    dataDir = undefined;
  }
});

describe("NapiJazzClient", () => {
  it("delegates insert behavior to Rust client core", async () => {
    const { NapiJazzClient } = await loadNapiModule();
    dataDir = await mkdtemp(join(tmpdir(), "jazz-napi-client-core-"));
    const client = new NapiJazzClient(
      serializeRuntimeSchema(schema),
      "napi-client-core-test",
      "dev",
      "main",
      dataDir,
    );

    const result = client.insert("users", {
      id: { type: "Uuid", value: "00000000-0000-7000-8000-000000000001" },
      name: { type: "Text", value: "Alice" },
    });

    expect(result.batchId).toEqual(expect.any(String));
    expect(result.values[1]).toEqual({ type: "Text", value: "Alice" });
    client.close();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/napi.client-core.test.ts
```

Expected: FAIL because `NapiJazzClient` is not exported.

- [ ] **Step 3: Refactor `JazzClientCore` to support shared locked runtimes**

Append this Rust test to `crates/jazz-tools/src/client_core/tests.rs`:

```rust
#[test]
fn client_core_can_wrap_a_shared_runtime_handle() {
    let schema = users_schema();
    let runtime = std::sync::Arc::new(std::sync::Mutex::new(test_runtime(schema.clone())));
    let mut client = JazzClientCore::from_runtime_host(
        ClientConfig::memory_for_test("shared-runtime-test", schema),
        SharedRuntimeHost::new(runtime),
    )
    .expect("shared host should construct");

    let result = client
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .expect("insert should work through shared runtime host");

    assert_eq!(
        client.check_batch_wait(result.handle.batch_id, DurabilityTier::Local),
        BatchWaitOutcome::Satisfied
    );
}
```

Run:

```bash
cargo test -p jazz-tools client_core::tests::client_core_can_wrap_a_shared_runtime_handle --features test-utils
```

Expected: FAIL because `from_runtime_host` and `SharedRuntimeHost` do not exist.

Modify `crates/jazz-tools/src/client_core/mod.rs` so `JazzClientCore` stores a host instead of storing `RuntimeCore` directly:

```rust
use std::sync::{Arc, Mutex};

pub trait ClientRuntimeHost {
    type Storage: Storage;
    type Scheduler: Scheduler;

    fn with_runtime<T>(&self, f: impl FnOnce(&RuntimeCore<Self::Storage, Self::Scheduler>) -> T)
        -> T;
    fn with_runtime_mut<T>(
        &mut self,
        f: impl FnOnce(&mut RuntimeCore<Self::Storage, Self::Scheduler>) -> T,
    ) -> T;
}

pub struct OwnedRuntimeHost<S: Storage, Sch: Scheduler> {
    runtime: RuntimeCore<S, Sch>,
}

impl<S: Storage, Sch: Scheduler> OwnedRuntimeHost<S, Sch> {
    pub fn new(runtime: RuntimeCore<S, Sch>) -> Self {
        Self { runtime }
    }
}

impl<S: Storage, Sch: Scheduler> ClientRuntimeHost for OwnedRuntimeHost<S, Sch> {
    type Storage = S;
    type Scheduler = Sch;

    fn with_runtime<T>(&self, f: impl FnOnce(&RuntimeCore<S, Sch>) -> T) -> T {
        f(&self.runtime)
    }

    fn with_runtime_mut<T>(&mut self, f: impl FnOnce(&mut RuntimeCore<S, Sch>) -> T) -> T {
        f(&mut self.runtime)
    }
}

pub struct SharedRuntimeHost<S: Storage, Sch: Scheduler> {
    runtime: Arc<Mutex<RuntimeCore<S, Sch>>>,
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

pub struct JazzClientCore<H: ClientRuntimeHost> {
    config: ClientConfig,
    host: H,
}

impl<S: Storage, Sch: Scheduler> JazzClientCore<OwnedRuntimeHost<S, Sch>> {
    pub fn from_runtime_parts(
        config: ClientConfig,
        runtime: RuntimeCore<S, Sch>,
    ) -> Result<Self, ClientError> {
        Self::from_runtime_host(config, OwnedRuntimeHost::new(runtime))
    }
}

impl<H: ClientRuntimeHost> JazzClientCore<H> {
    pub fn from_runtime_host(config: ClientConfig, host: H) -> Result<Self, ClientError> {
        Ok(Self { config, host })
    }

    pub fn with_runtime<T>(
        &self,
        f: impl FnOnce(&RuntimeCore<H::Storage, H::Scheduler>) -> T,
    ) -> T {
        self.host.with_runtime(f)
    }

    pub fn with_runtime_mut<T>(
        &mut self,
        f: impl FnOnce(&mut RuntimeCore<H::Storage, H::Scheduler>) -> T,
    ) -> T {
        self.host.with_runtime_mut(f)
    }
}
```

Update the write/query/subscription implementations from earlier tasks to use:

```rust
self.with_runtime(|runtime| runtime.current_schema().clone())
self.with_runtime_mut(|runtime| {
    runtime.insert_with_id(table, values, options.object_id, context.as_ref())
})
```

Run:

```bash
cargo test -p jazz-tools client_core::tests --features test-utils
```

Expected: PASS.

- [ ] **Step 4: Add `NapiJazzClient` using `JazzClientCore`**

In `crates/jazz-napi/src/lib.rs`, add imports:

```rust
use jazz_tools::client_core::{
    ClientConfig, ClientRuntimeFlavor, JazzClientCore, SharedRuntimeHost,
};
```

Split `build_napi_runtime` into a reusable helper:

```rust
fn build_napi_core(
    env: Env,
    schema_json: String,
    app_id: String,
    jazz_env: String,
    user_branch: String,
    storage: Box<dyn Storage + Send>,
    tier: Option<String>,
) -> napi::Result<(Arc<Mutex<NapiCoreType>>, Schema)> {
    let runtime_schema = parse_runtime_schema_input(&schema_json)
        .map_err(|e| napi::Error::from_reason(format!("Invalid schema JSON: {}", e)))?;
    let schema = runtime_schema.schema;
    let declared_schema = schema.clone();
    let node_tiers = parse_node_durability_tier(tier)?;
    let mut sync_manager = SyncManager::new();
    if !node_tiers.is_empty() {
        sync_manager = sync_manager.with_durability_tiers(node_tiers);
    }
    let schema_manager = SchemaManager::new_with_policy_mode(
        sync_manager,
        schema,
        AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id)),
        &jazz_env,
        &user_branch,
        if runtime_schema.loaded_policy_bundle {
            jazz_tools::query_manager::types::RowPolicyMode::Enforcing
        } else {
            jazz_tools::query_manager::types::RowPolicyMode::PermissiveLocal
        },
    )
    .map_err(|e| napi::Error::from_reason(format!("Failed to create SchemaManager: {:?}", e)))?;

    let core = RuntimeCore::new(schema_manager, storage, NapiScheduler::new());
    let core_arc = Arc::new(Mutex::new(core));
    install_napi_scheduler(env, &core_arc)?;
    Ok((core_arc, declared_schema))
}

fn install_napi_scheduler(env: Env, core_arc: &Arc<Mutex<NapiCoreType>>) -> napi::Result<()> {
    let core_weak = Arc::downgrade(core_arc);
    let scheduled_flag = {
        let core_guard = core_arc.lock().map_err(|_| napi::Error::from_reason("lock"))?;
        core_guard.scheduler().scheduled.clone()
    };
    let core_ref_for_tsfn = core_weak.clone();
    let flag_for_tsfn = scheduled_flag;
    let tick_fn = env.create_function_from_closure("__groove_tick", move |_ctx| {
        flag_for_tsfn.store(false, Ordering::SeqCst);
        if let Some(core_arc) = core_ref_for_tsfn.upgrade()
            && let Ok(mut core) = core_arc.lock()
        {
            core.batched_tick();
        }
        Ok(())
    })?;
    let tsfn = tick_fn.build_threadsafe_function().weak::<true>().build()?;
    let mut core_guard = core_arc.lock().map_err(|_| napi::Error::from_reason("lock"))?;
    core_guard.scheduler_mut().set_core_ref(core_weak);
    core_guard.scheduler_mut().set_tsfn(tsfn);
    core_guard.persist_schema();
    Ok(())
}
```

Then make `build_napi_runtime` call `build_napi_core`, preserving the existing `NapiRuntime` surface:

```rust
let (core_arc, declared_schema) =
    build_napi_core(env, schema_json, app_id, jazz_env, user_branch, storage, tier)?;
Ok(NapiRuntime {
    core: core_arc,
    upstream_server_id: Mutex::new(None),
    declared_schema,
    subscription_queries: Mutex::new(HashMap::new()),
})
```

Add a new NAPI class near `NapiRuntime`:

```rust
type NapiJazzClientCore =
    JazzClientCore<SharedRuntimeHost<Box<dyn Storage + Send>, NapiScheduler>>;

#[napi]
pub struct NapiJazzClient {
    inner: Mutex<NapiJazzClientCore>,
    core: Arc<Mutex<NapiCoreType>>,
}

#[napi]
impl NapiJazzClient {
    #[napi(constructor)]
    pub fn new(
        env: Env,
        schema_json: String,
        app_id: String,
        jazz_env: String,
        user_branch: String,
        data_path: String,
    ) -> napi::Result<Self> {
        let (core, schema) = build_napi_core(
            env,
            schema_json.clone(),
            app_id.clone(),
            jazz_env.clone(),
            user_branch.clone(),
            Box::new(open_sqlite_storage(&data_path)?),
            None,
        )?;
        let mut config = ClientConfig::memory_for_test(app_id, schema);
        config.env = jazz_env;
        config.user_branch = user_branch;
        config.runtime_flavor = ClientRuntimeFlavor::Node;

        Ok(Self {
            inner: Mutex::new(
                JazzClientCore::from_runtime_host(config, SharedRuntimeHost::new(core.clone()))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            core,
        })
    }

    #[napi]
    pub fn insert(
        &self,
        table: String,
        #[napi(ts_arg_type = "Record<string, unknown>")] values: FfiRecordArg,
    ) -> napi::Result<serde_json::Value> {
        let mut client = self.inner.lock().map_err(|_| napi::Error::from_reason("lock"))?;
        let result = client
            .insert(&table, values.0, None)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(serde_json::json!({
            "id": result.row.id.uuid().to_string(),
            "values": result.row.values,
            "batchId": result.handle.batch_id.to_string(),
        }))
    }

    #[napi]
    pub fn close(&self) -> napi::Result<()> {
        let core = self.core.lock().map_err(|_| napi::Error::from_reason("lock"))?;
        core.with_storage(|storage| {
            storage.flush();
            storage.close()
        })
        .map_err(|error| napi::Error::from_reason(error.to_string()))?
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(())
    }
}
```

- [ ] **Step 5: Build NAPI and rerun the test**

Run:

```bash
pnpm --filter jazz-napi build:debug
pnpm --filter jazz-tools test -- src/runtime/napi.client-core.test.ts
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-napi/src/lib.rs crates/jazz-napi/index.d.ts packages/jazz-tools/src/runtime/napi.client-core.test.ts
git commit -m "Expose JazzClientCore through NAPI"
```

## Task 9: Wrap The Native Client Shape In TypeScript JazzClient

**Files:**

- Modify: `packages/jazz-tools/src/runtime/client.ts`
- Modify: `packages/jazz-tools/src/runtime/client.test.ts`

- [ ] **Step 1: Write failing wrapper test proving TypeScript no longer creates batch ids**

Append to `packages/jazz-tools/src/runtime/client.test.ts`:

```ts
it("delegates batch id creation to the native runtime", () => {
  const runtime = makeFakeRuntime();
  runtime.insert.mockReturnValue({
    id: "row-1",
    values: [],
    batchId: "native-batch-id",
  });

  const client = JazzClient.connectWithRuntime(runtime, makeContext());
  const result = client.create("todos", {});

  expect(result.batchId).toBe("native-batch-id");
  expect(runtime.sealBatch).toHaveBeenCalledWith("native-batch-id");
});
```

- [ ] **Step 2: Run test to verify current behavior**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/client.test.ts -t "delegates batch id creation"
```

Expected before migration: PASS for standalone writes. Add a second failing test for batches:

```ts
it("delegates explicit direct batch creation to native runtime when available", () => {
  const runtime = makeFakeRuntime() as Runtime & {
    beginDirectBatch: ReturnType<typeof vi.fn>;
  };
  runtime.beginDirectBatch = vi.fn(() => ({
    insert: vi.fn(() => ({
      id: "row-1",
      values: [],
      batchId: "native-batch-id",
    })),
    commit: vi.fn(() => ({ batchId: "native-batch-id" })),
  }));

  const client = JazzClient.connectWithRuntime(runtime, makeContext());
  const batch = client.beginBatch();
  batch.create("todos", {});
  const handle = batch.commit();

  expect(handle.batchId).toBe("native-batch-id");
  expect(runtime.beginDirectBatch).toHaveBeenCalled();
});
```

Expected: FAIL because `JazzClient` does not call `runtime.beginDirectBatch`.

- [ ] **Step 3: Extend the TypeScript runtime interface with native batch methods**

In `packages/jazz-tools/src/runtime/client.ts`, extend `Runtime`:

```ts
  beginDirectBatch?(): {
    insert(table: string, values: InsertValues, object_id?: string | null): DirectInsertResult;
    update(object_id: string, values: Record<string, Value>): DirectMutationResult;
    delete(object_id: string): DirectMutationResult;
    commit(): DirectMutationResult;
  };
  beginTransaction?(): {
    insert(table: string, values: InsertValues, object_id?: string | null): DirectInsertResult;
    update(object_id: string, values: Record<string, Value>): DirectMutationResult;
    delete(object_id: string): DirectMutationResult;
    commit(): DirectMutationResult;
  };
```

Update `beginBatchInternal` to prefer the native batch runtime when available:

```ts
beginBatchInternal(session?: Session, attribution?: string): DirectBatch {
  const nativeBatch = this.runtime.beginDirectBatch?.();
  if (nativeBatch && !session && attribution === undefined) {
    return DirectBatch.fromNative(this, nativeBatch);
  }
  return new DirectBatch(
    this,
    this.createBatchContext("direct"),
    this.resolveWriteSession(session, attribution),
    attribution,
  );
}
```

Add `DirectBatch.fromNative(client, nativeBatch)` in the `DirectBatch` class:

```ts
type NativeDirectBatch = NonNullable<Runtime["beginDirectBatch"]> extends () => infer T ? T : never;

export class DirectBatch {
  private committedHandle: WriteHandle | null = null;

  static fromNative(client: JazzClient, nativeBatch: NativeDirectBatch): DirectBatch {
    return new DirectBatch(client, null, undefined, undefined, nativeBatch);
  }

  constructor(
    private readonly client: JazzClient,
    private readonly batchContext: BatchWriteContext | null,
    private readonly session?: Session,
    private readonly attribution?: string,
    private readonly nativeBatch?: NativeDirectBatch,
  ) {}

  batchId(): string {
    if (this.nativeBatch) {
      throw new Error("Native direct batch id is available after the first write or commit");
    }
    return this.batchContext!.batchId;
  }

  commit(): WriteHandle {
    if (this.committedHandle) return this.committedHandle;
    if (this.nativeBatch) {
      const result = this.nativeBatch.commit();
      this.committedHandle = new WriteHandle(result.batchId, this.client);
      return this.committedHandle;
    }
    const handle = this.client.sealBatch(this.batchId());
    this.committedHandle = handle;
    return handle;
  }
}
```

Then update `create`, `update`, and `delete` to call the native batch when `this.nativeBatch` is set. The native path must not call `createBatchContext`.

- [ ] **Step 4: Run wrapper tests**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/client.test.ts -t "native runtime"
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/runtime/client.ts packages/jazz-tools/src/runtime/client.test.ts
git commit -m "Wrap native JazzClient batch APIs in TypeScript"
```

## Task 10: Move React Native Adapter To The Shared Contract

**Files:**

- Modify: `crates/jazz-rn/rust/src/lib.rs`
- Regenerate: `crates/jazz-rn/src/generated/jazz_rn.ts`
- Regenerate: `crates/jazz-rn/src/generated/jazz_rn-ffi.ts`
- Modify: `packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.ts`
- Modify: `packages/jazz-tools/src/react-native/db.test.ts`

- [ ] **Step 1: Write failing RN adapter test**

Append to `packages/jazz-tools/src/react-native/db.test.ts`:

```ts
it("uses native client batch methods when the RN binding exposes them", () => {
  const connectWithRuntimeSpy = vi.spyOn(JazzClient, "connectWithRuntime");
  const nativeRuntime = makeRuntimeStub({
    beginDirectBatch: vi.fn(() => ({
      insert: vi.fn(() => ({ id: "row-1", values: [], batchId: "native-rn-batch" })),
      update: vi.fn(() => ({ batchId: "native-rn-batch" })),
      delete: vi.fn(() => ({ batchId: "native-rn-batch" })),
      commit: vi.fn(() => ({ batchId: "native-rn-batch" })),
    })),
  });

  connectWithRuntimeSpy.mockReturnValue(nativeRuntime.client);

  const batch = nativeRuntime.client.beginBatch();
  batch.create("todos", {});
  const handle = batch.commit();

  expect(handle.batchId).toBe("native-rn-batch");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pnpm --filter jazz-tools test -- src/react-native/db.test.ts -t "native client batch"
```

Expected: FAIL because the RN adapter does not expose native batch methods.

- [ ] **Step 3: Route RN Rust methods through `JazzClientCore`**

In `crates/jazz-rn/rust/src/lib.rs`, replace duplicated write context/session parsing inside the client object with a `JazzClientCore<SqliteStorage, RnScheduler>` field. Add UniFFI methods matching the TypeScript runtime interface:

```rust
#[uniffi::export]
impl JazzClient {
    pub fn begin_direct_batch(&self) -> Result<Arc<JazzDirectBatch>, JazzRnError> {
        with_panic_boundary("JazzClient::begin_direct_batch", || {
            let mut inner = self.inner.lock().map_err(|_| JazzRnError::Internal {
                message: "lock".to_string(),
            })?;
            Ok(Arc::new(JazzDirectBatch::new(inner.begin_direct_batch())))
        })
    }
}
```

Expose direct batch `insert`, `update`, `delete`, and `commit` methods through generated bindings. Use the same JSON value encoding already used by the current RN runtime adapter.

- [ ] **Step 4: Regenerate RN bindings and rerun test**

Run:

```bash
pnpm --filter jazz-rn generate
pnpm --filter jazz-tools test -- src/react-native/db.test.ts -t "native client batch"
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-rn/rust/src/lib.rs crates/jazz-rn/src/generated packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.ts packages/jazz-tools/src/react-native/db.test.ts
git commit -m "Route React Native JazzClient through Rust core"
```

## Task 11: Remove Duplicated TypeScript Behavior After Parity

**Files:**

- Modify: `packages/jazz-tools/src/runtime/client.ts`
- Modify: `packages/jazz-tools/src/runtime/client.test.ts`
- Modify: `packages/jazz-tools/src/runtime/client-tests/schema-order.test.ts`
- Modify: `packages/jazz-tools/src/runtime/db.schema-order.test.ts`

- [ ] **Step 1: Add tests that guard Rust-owned behavior**

Add tests that fail if TypeScript recomputes native-owned values:

```ts
it("does not compute explicit batch target branches when native batch APIs exist", () => {
  const runtime = makeFakeRuntime() as Runtime & {
    beginTransaction: ReturnType<typeof vi.fn>;
    getSchemaHash: ReturnType<typeof vi.fn>;
  };
  runtime.getSchemaHash = vi.fn(() => {
    throw new Error("TypeScript should not request schema hash for native transaction batches");
  });
  runtime.beginTransaction = vi.fn(() => ({
    insert: vi.fn(() => ({ id: "row-1", values: [], batchId: "native-tx" })),
    update: vi.fn(() => ({ batchId: "native-tx" })),
    delete: vi.fn(() => ({ batchId: "native-tx" })),
    commit: vi.fn(() => ({ batchId: "native-tx" })),
  }));

  const client = JazzClient.connectWithRuntime(runtime, makeContext());
  const tx = client.beginTransaction();
  tx.create("todos", {});
  expect(tx.commit().batchId).toBe("native-tx");
});
```

- [ ] **Step 2: Run test to verify it fails while old fallback is still used**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/client.test.ts -t "does not compute explicit batch target"
```

Expected: FAIL if `createBatchContext` is still used for native transaction runtimes.

- [ ] **Step 3: Delete migrated TypeScript helpers**

Remove these helpers from `packages/jazz-tools/src/runtime/client.ts` after all runtimes expose the native method:

Delete the `BatchWriteContext` type, the `composeTargetBranchName` function, and the
`generateBatchId` function from `packages/jazz-tools/src/runtime/client.ts`.

Keep `normalizeUpdatedAt` until Rust owns timestamp validation for every write path.

- [ ] **Step 4: Run parity tests**

Run:

```bash
pnpm --filter jazz-tools test -- src/runtime/client.test.ts src/runtime/client-tests/schema-order.test.ts src/runtime/db.schema-order.test.ts
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/runtime/client.ts packages/jazz-tools/src/runtime/client.test.ts packages/jazz-tools/src/runtime/client-tests/schema-order.test.ts packages/jazz-tools/src/runtime/db.schema-order.test.ts
git commit -m "Remove duplicated JazzClient batch behavior"
```

## Task 12: Full Verification

**Files:**

- No code changes unless verification exposes a defect.

- [ ] **Step 1: Run Rust client-core tests**

Run:

```bash
cargo test -p jazz-tools client_core --features test-utils
```

Expected: PASS.

- [ ] **Step 2: Run binding-focused tests**

Run:

```bash
pnpm --filter jazz-wasm build
pnpm --filter jazz-napi build:debug
pnpm --filter jazz-tools test -- src/runtime/client.test.ts src/runtime/wasm-client-core.test.ts src/runtime/napi.client-core.test.ts src/react-native/db.test.ts
```

Expected: PASS.

- [ ] **Step 3: Run formatting and lint checks**

Run:

```bash
pnpm format:check
cargo clippy -p jazz-tools --features test-utils -- -D warnings
```

Expected: PASS.

- [ ] **Step 4: Run full project test if time allows before PR**

Run:

```bash
pnpm test
```

Expected: PASS. If this is too slow for local iteration, run it in CI and note the exact local subset that passed.

- [ ] **Step 5: Commit final fixes**

Only if Step 1-4 required small fixes:

```bash
git add <fixed-files>
git commit -m "Stabilize native JazzClientCore migration"
```

## Self-Review Notes

- Spec coverage: Rust-owned client behavior is covered in Tasks 1-6; WASM, NAPI, RN, and TS migration are covered in Tasks 7-11; verification is covered in Task 12.
- Deferred optional module loading remains outside this plan.
- The browser `Db` worker bridge is not redesigned; TypeScript keeps that coordination while calling the new native client shape.
- The NAPI adapter task explicitly splits runtime construction so `NapiRuntime` and `NapiJazzClient` share the same locked runtime handle without cloning it.
