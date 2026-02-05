# Synchronous Editing with Async Index Loading

## Design Goal

> **Eliminate all explicit index preloading APIs.** The system should bootstrap itself correctly when queries/subscriptions are created. Indices load on-demand, queries wait automatically, and new writes are visible immediately.

**This is an aggressive refactor. No backwards compatibility required.** We will remove (not deprecate) the explicit preloading APIs entirely (`load_indices()`, `reset_indices_for_cold_start()`, etc.).

## Problem Statement

For React/WASM use cases, a key invariant must hold:

> When updating a text field bound to an object that affects the same query populating the text field, the subscription callback must fire **synchronously** after the mutation.

This works when indices are fully loaded. It breaks when:

1. Index metadata hasn't loaded yet (`meta_loaded=false`)
2. Specific B-tree pages aren't loaded yet

**The common issue**: `pending_writes` is invisible to scans, scans don't signal incomplete data, and scans don't trigger loading of missing pages.

## Key Design Decisions

Based on investigation, these decisions guide the implementation:

1. **`is_ready()` requires root page loaded** - not just metadata
2. **Subscription callbacks simply don't fire while pending** - OutputNode holds back results until ready. Callbacks have no `pending` field; they just don't arrive. (This infrastructure already exists via MaterializeNode pattern)
3. **No timeouts** - queries wait as long as needed for indices to load
4. **Root pages are eagerly loaded** - along with metadata, when subscribing
5. **Pages are discovered during traversal** - can't know upfront what's needed, so scans must trigger loading recursively

## Architecture: Two Layers of Async Loading

The query graph has two distinct async loading mechanisms:

| Layer           | What's Loading                                | Current Status                                                          | Who Handles                 |
| --------------- | --------------------------------------------- | ----------------------------------------------------------------------- | --------------------------- |
| **Index Pages** | B-tree structure (meta, internal, leaf pages) | **Broken** - scans don't trigger loading, `scan_all()` doesn't traverse | IndexScanNode (to be fixed) |
| **Row Data**    | Object content bytes                          | **Working** - MaterializeNode tracks `pending_ids`, retries on settle   | MaterializeNode             |

Index page loading is _upstream_ of row materialization - you can't even know what ObjectIds to materialize without the index pages.

---

# Implementation Phases

## Phase 0: Red Tests (Test Infrastructure)

**Goal**: Create comprehensive E2E tests that verify all invariants. These tests establish a contract: they fail today (red) and progressively turn green as each phase is implemented.

### Test Infrastructure

Tests operate on `RuntimeCore<DelayedIoHandler>` where `DelayedIoHandler` is a new test handler that:

- Queues storage requests instead of processing immediately
- Allows controlled delivery of responses (simulating async storage)
- Uses the existing `TestDriver` for actual storage operations

```rust
// crates/groove/src/io_handler.rs (new test handler)

/// IoHandler that delays storage responses for testing async scenarios.
/// Unlike TestIoHandler which processes synchronously, this queues requests
/// and requires explicit `deliver_responses()` to process them.
pub struct DelayedIoHandler {
    driver: TestDriver,
    /// Requests waiting to be processed
    pending_requests: Vec<StorageRequest>,
    /// Responses ready to be delivered
    ready_responses: Vec<StorageResponse>,
    /// Track whether batched_tick was scheduled
    batched_tick_scheduled: bool,
}

impl DelayedIoHandler {
    pub fn new() -> Self {
        Self {
            driver: TestDriver::new(),
            pending_requests: Vec::new(),
            ready_responses: Vec::new(),
            batched_tick_scheduled: false,
        }
    }

    /// Process all pending requests through driver and queue responses.
    /// Does NOT deliver them yet - call deliver_responses() for that.
    pub fn process_pending(&mut self) {
        for request in self.pending_requests.drain(..) {
            let response = self.driver.process_one(&request);
            self.ready_responses.push(response);
        }
    }

    /// Take ready responses for parking in RuntimeCore.
    pub fn take_responses(&mut self) -> Vec<StorageResponse> {
        std::mem::take(&mut self.ready_responses)
    }

    /// Check if there are pending requests waiting.
    pub fn has_pending_requests(&self) -> bool {
        !self.pending_requests.is_empty()
    }

    /// Convenience: process pending and return responses in one call.
    pub fn flush(&mut self) -> Vec<StorageResponse> {
        self.process_pending();
        self.take_responses()
    }
}

impl IoHandler for DelayedIoHandler {
    fn send_storage_request(&mut self, request: StorageRequest) {
        self.pending_requests.push(request);
    }

    fn send_sync_message(&mut self, _message: OutboxEntry) {
        // No-op for local tests
    }

    fn schedule_batched_tick(&self) {
        // Track scheduling but don't auto-execute
    }

    fn take_pending_responses(&mut self) -> Vec<StorageResponse> {
        Vec::new() // Responses come via explicit flush()
    }
}
```

### Test Categories

Tests are organized by invariant. Each test documents:

- The invariant being tested
- Why it fails today (current behavior)
- Which phase will fix it

---

### Category A: Index Readiness and Pending State

**Invariant**: IndexScanNode must signal `pending=true` internally when index pages aren't loaded. This causes OutputNode to hold back results - subscribers simply don't receive callbacks until ready.

```rust
// crates/groove/src/runtime_core.rs (in #[cfg(test)] mod tests)

mod delayed_io_tests {
    use super::*;

    /// Helper: Create RuntimeCore with DelayedIoHandler (no auto-processing)
    fn create_delayed_runtime(schema: WasmSchema) -> RuntimeCore<DelayedIoHandler> {
        let handler = DelayedIoHandler::new();
        let schema_manager = SchemaManager::new(schema);
        RuntimeCore::new(schema_manager, handler)
    }

    /// Helper: Insert data and persist to storage, then create fresh runtime
    fn create_cold_start_runtime(
        schema: WasmSchema,
        setup: impl FnOnce(&mut RuntimeCore<TestIoHandler>),
    ) -> RuntimeCore<DelayedIoHandler> {
        // Phase 1: Create and populate with TestIoHandler (synchronous)
        let mut warm_runtime = {
            let handler = TestIoHandler::new(TestDriver::new());
            let schema_manager = SchemaManager::new(schema.clone());
            RuntimeCore::new(schema_manager, handler)
        };
        warm_runtime.schema_manager_mut().query_manager_mut().reset_indices_for_cold_start();
        for _ in 0..10 { warm_runtime.batched_tick(); }

        // Run setup (inserts, etc.)
        setup(&mut warm_runtime);
        for _ in 0..10 { warm_runtime.batched_tick(); }

        // Extract driver with persisted data
        let driver = warm_runtime.into_io_handler().into_driver();

        // Phase 2: Create fresh runtime with that driver's data
        let handler = DelayedIoHandler::with_driver(driver);
        let schema_manager = SchemaManager::new(schema);
        RuntimeCore::new(schema_manager, handler)
    }

    // ========================================================================
    // A1: is_ready() reflects actual loading state
    // ========================================================================

    #[test]
    fn a1_btree_index_not_ready_without_meta() {
        // Invariant: BTreeIndex::is_ready() == false when meta not loaded
        // Status: SHOULD PASS after Phase 1 (is_ready method doesn't exist yet)

        let index = BTreeIndex::new("users", "_id");

        // Today: no is_ready() method exists, only root_exists()
        // After Phase 1: is_ready() returns false
        assert!(!index.is_ready());
    }

    #[test]
    fn a1_btree_index_not_ready_without_root_page() {
        // Invariant: is_ready() == false even after meta loads if root page missing
        // Status: SHOULD PASS after Phase 1

        let mut index = BTreeIndex::new("users", "_id");

        // Load metadata that references a root page
        let meta = IndexMeta::new();
        index.process_meta_load(Some(meta.serialize()));

        // Meta is loaded, but root page is not
        assert!(index.meta_loaded);
        assert!(!index.is_ready()); // Should be false until root loads
    }

    #[test]
    fn a1_btree_index_ready_with_meta_and_root() {
        // Invariant: is_ready() == true once meta AND root page are loaded
        // Status: SHOULD PASS after Phase 1

        let mut index = BTreeIndex::new("users", "_id");
        index.process_meta_load(None); // New index = root auto-created

        assert!(index.is_ready());
    }

    // ========================================================================
    // A2: IndexScanNode signals pending when index not ready
    // ========================================================================

    #[test]
    fn a2_index_scan_returns_pending_when_not_ready() {
        // Invariant: scan() returns pending=true when index.is_ready() == false
        // Status: FAILS TODAY - IndexScanNode always returns pending=false
        // Fixed by: Phase 1

        let index = BTreeIndex::new("users", "_id");
        // Index has no meta loaded

        let mut indices = IndicesMap::default();
        indices.insert(("users".into(), "_id".into(), "main".into()), index);

        let mut node = IndexScanNode::new(
            "users", "_id",
            ScanCondition::All,
            test_descriptor(),
        );

        let ctx = SourceContext { indices: &indices };
        let delta = node.scan(&ctx);

        // TODAY: delta.pending == false (WRONG)
        // AFTER Phase 1: delta.pending == true (CORRECT)
        assert!(delta.pending, "IndexScanNode should signal pending when index not ready");
    }

    #[test]
    fn a2_index_scan_becomes_ready_after_loading() {
        // Invariant: After was_pending, scan forces full rescan and returns pending=false
        // Status: FAILS TODAY - no was_pending tracking exists
        // Fixed by: Phase 1

        let mut index = BTreeIndex::new("users", "_id");
        // Initially not ready

        let mut indices = IndicesMap::default();
        indices.insert(("users".into(), "_id".into(), "main".into()), index);

        let mut node = IndexScanNode::new(
            "users", "_id",
            ScanCondition::All,
            test_descriptor(),
        );

        let ctx = SourceContext { indices: &indices };

        // First scan: pending (not ready)
        let delta1 = node.scan(&ctx);
        assert!(delta1.pending);

        // Load the index
        indices.get_mut(&("users".into(), "_id".into(), "main".into()))
            .unwrap()
            .process_meta_load(None);

        // Second scan: should be ready now
        let ctx = SourceContext { indices: &indices };
        let delta2 = node.scan(&ctx);

        assert!(!delta2.pending, "Should be ready after index loads");
        // should have forced full rescan (last_delta_epoch cleared)
    }

    // ========================================================================
    // A3: OutputNode holds back pending results (internal test)
    // ========================================================================

    #[test]
    fn a3_output_node_holds_back_when_input_pending() {
        // Invariant: OutputNode does not produce output while input has pending=true
        // Status: PASSES TODAY (OutputNode infrastructure exists)
        // This is an INTERNAL test - the pending flag is internal to graph nodes.
        // Subscribers simply don't see callbacks; there's no pending field exposed.

        let mut node = make_output_node(OutputMode::Delta);

        let id = ObjectId::new();
        let tuple = make_tuple(id, 1, "Alice");

        // Internal: graph passes pending=true through nodes
        node.process(TupleDelta {
            pending: true,  // Internal flag, not exposed to subscribers
            added: vec![tuple],
            removed: vec![],
            updated: vec![],
        });

        // Internal state updated
        assert_eq!(node.current_tuples().len(), 1);

        // But nothing in output queue (no callback would fire)
        let deltas = node.take_tuple_deltas();
        assert!(deltas.is_empty(), "OutputNode should hold back - no callback fires");
    }
}
```

---

### Category B: Cold Start and Query Waiting

**Invariant**: Queries must wait for indices to load. Callbacks simply don't fire until ready.

```rust
mod cold_start_tests {
    use super::*;

    // ========================================================================
    // B1: One-shot query waits for index to load
    // ========================================================================

    #[test]
    fn b1_no_callback_while_index_loading() {
        // Invariant: Callback doesn't fire until index is ready
        // Status: FAILS TODAY - callback fires immediately with empty results
        // Fixed by: Phase 1 + Phase 3

        let schema = test_users_schema();
        let mut core = create_cold_start_runtime(schema.clone(), |warm| {
            warm.insert("users", encode_user("alice@test.com")).unwrap();
            warm.insert("users", encode_user("bob@test.com")).unwrap();
        });

        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let cb_clone = callbacks.clone();

        let query = Query::scan("users");
        let _handle = core.subscribe(query, move |delta| {
            cb_clone.lock().unwrap().push(delta);
        }, None).unwrap();

        // Run immediate_tick - graph settles but index not loaded
        core.immediate_tick();

        // Callback should NOT have fired yet
        // TODAY: callbacks.len() > 0 with empty results (WRONG)
        // AFTER Phase 1+3: callbacks.len() == 0 (CORRECT)
        let cbs = callbacks.lock().unwrap();
        assert!(cbs.is_empty(), "Callback should NOT fire while index loading");
    }

    #[test]
    fn b1_callback_fires_after_index_loads() {
        // Invariant: After index loads, callback fires with correct data
        // Status: FAILS TODAY - fires prematurely with empty
        // Fixed by: Phase 1 + Phase 2 + Phase 3

        let schema = test_users_schema();
        let mut core = create_cold_start_runtime(schema.clone(), |warm| {
            warm.insert("users", encode_user("alice@test.com")).unwrap();
        });

        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let cb_clone = callbacks.clone();

        let query = Query::scan("users");
        let _handle = core.subscribe(query, move |delta| {
            cb_clone.lock().unwrap().push(delta);
        }, None).unwrap();

        // Tick 1: Index not loaded - no callback yet
        core.immediate_tick();
        assert!(callbacks.lock().unwrap().is_empty(), "No callback before load");

        // Deliver storage responses (index loads)
        let responses = core.io_handler_mut().flush();
        for response in responses {
            core.park_storage_response(response);
        }

        // Tick 2: Process loaded index, re-settle
        core.immediate_tick();

        // Keep delivering until stable
        loop {
            let responses = core.io_handler_mut().flush();
            if responses.is_empty() { break; }
            for response in responses {
                core.park_storage_response(response);
            }
            core.immediate_tick();
        }

        // NOW callback should have fired with data
        let cbs = callbacks.lock().unwrap();
        assert!(!cbs.is_empty(), "Callback should fire after index loaded");
        assert!(!cbs.last().unwrap().added.is_empty(), "Should have found the user");
    }

    // ========================================================================
    // B2: Callback count verification
    // ========================================================================

    #[test]
    fn b2_exactly_one_callback_after_cold_start() {
        // Invariant: Cold start produces exactly ONE callback with complete data
        // Status: FAILS TODAY - multiple spurious callbacks
        // Fixed by: Phase 2 (needs dirty marking when index loads)

        let schema = test_users_schema();
        let mut core = create_cold_start_runtime(schema.clone(), |warm| {
            warm.insert("users", encode_user("alice@test.com")).unwrap();
        });

        let callback_count = Arc::new(Mutex::new(0usize));
        let count_clone = callback_count.clone();

        let query = Query::scan("users");
        let _handle = core.subscribe(query, move |_delta| {
            *count_clone.lock().unwrap() += 1;
        }, None).unwrap();

        // Run to completion
        loop {
            core.immediate_tick();
            let responses = core.io_handler_mut().flush();
            if responses.is_empty() { break; }
            for response in responses {
                core.park_storage_response(response);
            }
        }

        // Should have exactly 1 callback (the complete result)
        let count = *callback_count.lock().unwrap();
        assert_eq!(count, 1, "Should have exactly 1 callback, not {}", count);
    }
}
```

---

### Category C: Dirty Marking When Index Loads

**Invariant**: When an index finishes loading, affected subscriptions must re-settle.

```rust
mod dirty_marking_tests {
    // ========================================================================
    // C1: Subscriptions marked dirty when index meta loads
    // ========================================================================

    #[test]
    fn c1_subscription_marked_dirty_when_index_meta_loads() {
        // Invariant: After LoadIndexMeta response, subscriptions using that index get dirty
        // Status: FAILS TODAY - no dirty marking on meta load
        // Fixed by: Phase 2

        let schema = test_users_schema();
        let mut core = create_delayed_runtime(schema);

        // Subscribe to users query
        let query = Query::scan("users");
        let handle = core.subscribe(query, |_| {}, None).unwrap();

        // Initial tick
        core.immediate_tick();

        // Mark subscription clean manually (simulate settled state)
        // (This would need internal access or checking graph.is_dirty())

        // Now deliver the meta load response
        let responses = core.io_handler_mut().flush();
        for response in responses {
            core.park_storage_response(response);
        }

        // Process response
        core.immediate_tick();

        // Check: subscription should be dirty now
        // TODAY: Not marked dirty (WRONG)
        // AFTER Phase 2: Marked dirty for re-settlement
        let is_dirty = core.schema_manager()
            .query_manager()
            .get_subscription_graph(handle) // hypothetical accessor
            .is_dirty();

        assert!(is_dirty, "Subscription should be dirty after index loads");
    }

    // ========================================================================
    // C2: Only relevant subscriptions marked dirty
    // ========================================================================

    #[test]
    fn c2_only_relevant_subscriptions_marked_dirty() {
        // Invariant: Loading index for table X doesn't dirty subscriptions for table Y
        // Status: FAILS TODAY (no dirty marking at all)
        // Fixed by: Phase 2

        let schema = test_multi_table_schema(); // users + todos
        let mut core = create_delayed_runtime(schema);

        // Subscribe to both tables
        let users_handle = core.subscribe(Query::scan("users"), |_| {}, None).unwrap();
        let todos_handle = core.subscribe(Query::scan("todos"), |_| {}, None).unwrap();

        core.immediate_tick();

        // Deliver only users index meta
        // (Would need to filter responses or have separate control)

        core.immediate_tick();

        // Users subscription should be dirty, todos should not
        // (Implementation detail: graph.uses_index() method needed)
    }
}
```

---

### Category D: Sync Writes Visible via pending_writes

**Invariant**: New writes while index is loading are tracked and appear in first callback (when it finally fires).

```rust
mod pending_writes_tests {
    // ========================================================================
    // D1: New insert tracked while index loading
    // ========================================================================

    #[test]
    fn d1_insert_during_index_loading_appears_in_first_callback() {
        // Invariant: Insert while loading is held internally, appears when ready
        // Status: FAILS TODAY - pending_writes concept doesn't exist
        // Fixed by: Phase 4

        let schema = test_users_schema();
        let mut core = create_cold_start_runtime(schema.clone(), |warm| {
            warm.insert("users", encode_user("old@test.com")).unwrap();
        });

        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let cb_clone = callbacks.clone();

        let query = Query::scan("users");
        let _handle = core.subscribe(query, move |delta| {
            cb_clone.lock().unwrap().push(delta.added.len());
        }, None).unwrap();

        // Initial tick - index not loaded, no callback
        core.immediate_tick();
        assert!(callbacks.lock().unwrap().is_empty(), "No callback yet");

        // Insert new user WHILE index still loading
        core.insert("users", encode_user("new@test.com")).unwrap();
        core.immediate_tick();

        // Still no callback (index still loading)
        assert!(callbacks.lock().unwrap().is_empty(), "Still no callback");

        // Now load the index
        loop {
            let responses = core.io_handler_mut().flush();
            if responses.is_empty() { break; }
            for response in responses {
                core.park_storage_response(response);
            }
            core.immediate_tick();
        }

        // First callback contains BOTH old and new user
        let cbs = callbacks.lock().unwrap();
        assert_eq!(cbs.len(), 1, "Should have exactly 1 callback");
        assert_eq!(cbs[0], 2, "First callback should have both old and new user");
    }

    // ========================================================================
    // D2: Multiple inserts while pending are batched
    // ========================================================================

    #[test]
    fn d2_multiple_inserts_while_loading_appear_together() {
        // Invariant: All inserts during loading period appear in ONE callback
        // Status: FAILS TODAY - no pending_writes batching
        // Fixed by: Phase 4

        let schema = test_users_schema();
        let mut core = create_cold_start_runtime(schema.clone(), |_| {});

        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let cb_clone = callbacks.clone();

        let query = Query::scan("users");
        let _handle = core.subscribe(query, move |delta| {
            cb_clone.lock().unwrap().push(delta.added.len());
        }, None).unwrap();

        // Insert 3 users while index not loaded
        core.insert("users", encode_user("a@test.com")).unwrap();
        core.immediate_tick();
        assert!(callbacks.lock().unwrap().is_empty(), "No callback yet");

        core.insert("users", encode_user("b@test.com")).unwrap();
        core.immediate_tick();
        assert!(callbacks.lock().unwrap().is_empty(), "Still no callback");

        core.insert("users", encode_user("c@test.com")).unwrap();
        core.immediate_tick();
        assert!(callbacks.lock().unwrap().is_empty(), "Still no callback");

        // Load index
        loop {
            let responses = core.io_handler_mut().flush();
            if responses.is_empty() { break; }
            for response in responses {
                core.park_storage_response(response);
            }
            core.immediate_tick();
        }

        // ONE callback with 3 rows
        let cbs = callbacks.lock().unwrap();
        assert_eq!(cbs.len(), 1, "Should have exactly 1 callback");
        assert_eq!(cbs[0], 3, "Callback should contain all 3 users");
    }

    // ========================================================================
    // D3: pending_writes deduped with B-tree results
    // ========================================================================

    #[test]
    fn d3_no_duplicate_rows_in_callback() {
        // Invariant: Same row in pending_writes and B-tree appears once
        // Status: FAILS TODAY - no merging logic
        // Fixed by: Phase 4

        // Edge case: row in both pending_writes (just-completed insert)
        // AND the B-tree (from persistence). Can happen with sync storage.

        let schema = test_users_schema();
        let mut core = create_delayed_runtime(schema);

        // Insert user
        core.insert("users", encode_user("alice@test.com")).unwrap();

        let total_rows = Arc::new(Mutex::new(0usize));
        let rows_clone = total_rows.clone();

        let query = Query::scan("users");
        let _handle = core.subscribe(query, move |delta| {
            *rows_clone.lock().unwrap() += delta.added.len();
        }, None).unwrap();

        // Process everything
        core.immediate_tick();
        loop {
            let responses = core.io_handler_mut().flush();
            if responses.is_empty() { break; }
            for response in responses {
                core.park_storage_response(response);
            }
            core.immediate_tick();
        }

        // Total across all callbacks should be exactly 1 row
        let total = *total_rows.lock().unwrap();
        assert_eq!(total, 1, "Same row should not appear twice");
    }

    // ========================================================================
    // D4: Insert when is_ready() but specific leaf page not loaded
    // ========================================================================

    #[test]
    fn d4_insert_when_leaf_page_not_loaded() {
        // Invariant: Insert should be captured even when is_ready() but target
        //            leaf page is not loaded yet
        // Status: FAILS TODAY - insert returns Ok(false), write is lost
        // Fixed by: Phase 4 (pending_writes captures writes on PageNotLoaded too)
        //
        // Scenario: Index has meta + root loaded (is_ready() == true), but root
        // is an internal node pointing to leaf pages that aren't loaded yet.
        // An insert targeting an unloaded leaf should still be captured.

        let mut index = BTreeIndex::new("users", "score");

        // Set up meta: root is page 1, an internal node
        let mut meta = IndexMeta::new();
        meta.root_page_id = PageId(1);
        meta.next_page_id = 4;
        meta.entry_count = 20;
        index.process_meta_load(Some(meta.serialize()));

        // Load root page as an INTERNAL node pointing to leaves 2 and 3
        // Keys < 50 go to page 2, keys >= 50 go to page 3
        let root_internal = BTreePage::Internal {
            keys: vec![50i32.to_be_bytes().to_vec()],
            children: vec![PageId(2), PageId(3)],
        };
        index.process_page_load(PageId(1), Some(root_internal.serialize()));

        // Now index.is_ready() is true (meta + root loaded)
        assert!(index.is_ready());

        // But leaf pages 2 and 3 are NOT loaded
        // Try to insert a row with score=25 (would go to page 2)
        let row_id = ObjectId::new();
        let key = 25i32.to_be_bytes();
        let result = index.insert(&key, row_id);

        // Current: insert returns Ok(false), write NOT captured
        // THE INVARIANT: write should be in pending_inserts, visible in scan_all

        let scan_results = index.scan_all();
        assert!(scan_results.contains(&row_id),
            "Insert to unloaded leaf should be captured in pending_inserts");
    }

    // ========================================================================
    // D5: Update that moves row to unloaded page (delete + insert same row_id)
    // ========================================================================

    #[test]
    fn d5_update_moves_row_to_unloaded_page() {
        // Invariant: Update (delete old key + insert new key) for same row_id
        //            should keep the row in results, not delete it
        // Status: FAILS TODAY - pending_deletes removes row even if re-inserted
        // Fixed by: Phase 4 (pending_inserts "wins" over pending_deletes for same row_id)
        //
        // Scenario: Row exists with score=25 (page 2). Update changes score to 75
        // (would go to page 3). Neither page 2 nor 3 is loaded.
        // At index level: remove(25, row_id) + insert(75, row_id)
        // The row should still appear in scan_all() results.

        let mut index = BTreeIndex::new("users", "score");

        // Set up meta: root is page 1, an internal node
        let mut meta = IndexMeta::new();
        meta.root_page_id = PageId(1);
        meta.next_page_id = 4;
        meta.entry_count = 20;
        index.process_meta_load(Some(meta.serialize()));

        // Load root as internal node: keys < 50 → page 2, keys >= 50 → page 3
        let root_internal = BTreePage::Internal {
            keys: vec![50i32.to_be_bytes().to_vec()],
            children: vec![PageId(2), PageId(3)],
        };
        index.process_page_load(PageId(1), Some(root_internal.serialize()));

        assert!(index.is_ready());

        // Simulate an UPDATE: row moves from score=25 to score=75
        let row_id = ObjectId::new();
        let old_key = 25i32.to_be_bytes();
        let new_key = 75i32.to_be_bytes();

        // Delete old key (page 2 not loaded)
        let _ = index.remove(&old_key, row_id);
        // Insert new key (page 3 not loaded)
        let _ = index.insert(&new_key, row_id);

        // THE INVARIANT: Row should still appear in scan_all()
        // The delete of old_key should NOT remove the row since it was re-inserted
        let scan_results = index.scan_all();

        assert!(scan_results.contains(&row_id),
            "Update (delete + insert same row_id) should keep row in results. \
             pending_inserts should 'win' over pending_deletes for same ObjectId");
    }
}
```

---

### Category E: Page Discovery and Load Requirements

**Invariant**: Scans discover and request missing pages; settlement reports load requirements.

```rust
mod page_discovery_tests {
    // ========================================================================
    // E1: scan_all reports missing pages
    // ========================================================================

    #[test]
    fn e1_scan_all_reports_missing_meta() {
        // Invariant: scan_all returns missing:[Meta] when meta not loaded
        // Status: FAILS TODAY - scan_all returns empty vec
        // Fixed by: Phase 5

        let index = BTreeIndex::new("users", "_id");

        let result = index.scan_all();

        // TODAY: result.missing doesn't exist, returns empty vec
        // AFTER Phase 5: result.missing contains IndexLoadType::Meta
        assert!(result.missing.contains(&IndexLoadType::Meta));
    }

    #[test]
    fn e1_scan_all_reports_missing_root_page() {
        // Invariant: After meta loads, scan reports missing root page
        // Status: FAILS TODAY
        // Fixed by: Phase 5

        let mut index = BTreeIndex::new("users", "_id");

        // Create meta that references a root page
        let meta_bytes = create_meta_with_root(PageId(42)).serialize();
        index.process_meta_load(Some(meta_bytes));

        let result = index.scan_all();

        // Should report root page 42 as missing
        assert!(result.missing.iter().any(|m|
            matches!(m, IndexLoadType::Page(id) if id.0 == 42)
        ));
    }

    // ========================================================================
    // E2: Settlement returns load requirements
    // ========================================================================

    #[test]
    fn e2_settlement_returns_pending_with_load_requirements() {
        // Invariant: settle() returns Pending with index_loads_needed when pages missing
        // Status: FAILS TODAY - settle returns Success with empty results
        // Fixed by: Phase 5

        let schema = test_users_schema();
        let mut core = create_delayed_runtime(schema);

        let _handle = core.subscribe(Query::scan("users"), |_| {}, None).unwrap();

        // First settle - index not loaded
        core.immediate_tick();

        // Check that load requests were queued
        // (This tests that settlement requested the loads)
        assert!(core.io_handler().has_pending_requests(),
            "Settlement should have queued index load requests");
    }

    // ========================================================================
    // E3: Range scan reports missing sibling pages
    // ========================================================================

    #[test]
    fn e3_range_scan_reports_missing_siblings() {
        // Invariant: range_scan reports pages discovered via next_leaf pointer
        // Status: FAILS TODAY - range_scan silently stops
        // Fixed by: Phase 5

        let mut index = BTreeIndex::new("users", "score");
        index.process_meta_load(None);

        // Insert enough to create multiple leaves (65+ entries)
        for i in 0..70 {
            let row = ObjectId::new();
            index.insert(&i.to_be_bytes(), row).unwrap();
        }

        // Now simulate cold start: keep only first leaf loaded
        // (This requires internal manipulation for the test)

        // Range scan that spans multiple leaves
        let result = index.range_scan(
            &Bound::Included(0i32.to_be_bytes().to_vec()),
            &Bound::Included(100i32.to_be_bytes().to_vec()),
        );

        // TODAY: Returns only entries from first leaf, silently
        // AFTER Phase 5: Returns ScanResult with missing sibling pages
        assert!(!result.missing.is_empty(),
            "Should report missing sibling pages in range");
    }
}
```

---

### Category F: E2E Integration

**Invariant**: Complete flow from cold start to consistent data.

```rust
mod e2e_tests {
    // ========================================================================
    // F1: Full cold-start cycle
    // ========================================================================

    #[test]
    fn f1_cold_start_delivers_all_data() {
        // Invariant: Cold start eventually delivers all persisted data in ONE callback
        // Status: PASSES with current manual preloading
        // This test verifies the automatic version works after all phases

        let schema = test_users_schema();
        let mut core = create_cold_start_runtime(schema.clone(), |warm| {
            for i in 0..10 {
                warm.insert("users", encode_user(&format!("user{}@test.com", i))).unwrap();
            }
        });

        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let cb_clone = callbacks.clone();

        let query = Query::scan("users");
        let _handle = core.subscribe(query, move |delta| {
            cb_clone.lock().unwrap().push(delta.added.len());
        }, None).unwrap();

        // Run until stable
        let mut iterations = 0;
        loop {
            core.immediate_tick();
            let responses = core.io_handler_mut().flush();
            if responses.is_empty() { break; }
            for response in responses {
                core.park_storage_response(response);
            }
            iterations += 1;
            if iterations > 100 { panic!("Infinite loop in cold start"); }
        }

        let cbs = callbacks.lock().unwrap();
        assert_eq!(cbs.len(), 1, "Should have exactly 1 callback");
        assert_eq!(cbs[0], 10, "Should have all 10 users");
    }

    // ========================================================================
    // F2: Insert during cold-start visible in final result
    // ========================================================================

    #[test]
    fn f2_insert_during_cold_start_included() {
        // Invariant: Insert while loading appears together with old data
        // Status: FAILS TODAY
        // Fixed by: All phases working together

        let schema = test_users_schema();
        let mut core = create_cold_start_runtime(schema.clone(), |warm| {
            warm.insert("users", encode_user("old@test.com")).unwrap();
        });

        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let cb_clone = callbacks.clone();

        let query = Query::scan("users");
        let _handle = core.subscribe(query, move |delta| {
            cb_clone.lock().unwrap().push(delta.added.len());
        }, None).unwrap();

        // First tick - no callback yet
        core.immediate_tick();
        assert!(callbacks.lock().unwrap().is_empty(), "No callback yet");

        // Insert new user mid-loading
        core.insert("users", encode_user("new@test.com")).unwrap();

        // Run to completion
        loop {
            core.immediate_tick();
            let responses = core.io_handler_mut().flush();
            if responses.is_empty() { break; }
            for response in responses {
                core.park_storage_response(response);
            }
        }

        // ONE callback with both rows
        let cbs = callbacks.lock().unwrap();
        assert_eq!(cbs.len(), 1, "Should have exactly 1 callback");
        assert_eq!(cbs[0], 2, "Should have old and new user together");
    }

    // ========================================================================
    // F3: Sync edit invariant (the original motivation)
    // ========================================================================

    #[test]
    fn f3_sync_edit_fires_callback_synchronously() {
        // Invariant: After index loaded, insert triggers IMMEDIATE callback
        // Status: PASSES today (when index is ready)
        // This documents the invariant that must be preserved

        let schema = test_users_schema();

        // Create with synchronous TestIoHandler
        let mut core = {
            let handler = TestIoHandler::new(TestDriver::new());
            let schema_manager = SchemaManager::new(schema);
            RuntimeCore::new(schema_manager, handler)
        };
        core.schema_manager_mut().query_manager_mut().reset_indices_for_cold_start();
        for _ in 0..10 { core.batched_tick(); }

        let callback_count = Arc::new(Mutex::new(0usize));
        let count_clone = callback_count.clone();

        let query = Query::scan("users");
        let _handle = core.subscribe(query, move |delta| {
            if !delta.added.is_empty() {
                *count_clone.lock().unwrap() += 1;
            }
        }, None).unwrap();

        // Initial tick (empty result - may or may not fire callback)
        core.immediate_tick();
        let initial_count = *callback_count.lock().unwrap();

        // Insert - should fire callback synchronously (within immediate_tick)
        core.insert("users", encode_user("test@test.com")).unwrap();
        core.immediate_tick();

        let final_count = *callback_count.lock().unwrap();
        assert!(final_count > initial_count,
            "Callback must fire synchronously after insert when index ready");
    }
}
```

---

### Test Passingness by Phase

**Already passing** (verify invariants that work today):

- A3: OutputNode holds back pending results ✅
- F3: Sync edit fires callback immediately (when index ready) ✅

**Red tests** (fail today, turn green as phases complete):

| Test                                   | Phase 0 | Phase 1 | Phase 2 | Phase 3 | Phase 4 | Phase 5 |
| -------------------------------------- | :-----: | :-----: | :-----: | :-----: | :-----: | :-----: |
| A1 (is_ready methods)                  |   ❌    |   ✅    |   ✅    |   ✅    |   ✅    |   ✅    |
| A2 (scan returns pending)              |   ❌    |   ✅    |   ✅    |   ✅    |   ✅    |   ✅    |
| B1 (query waits)                       |   ❌    |   ❌    |   ❌    |   ✅    |   ✅    |   ✅    |
| B2 (no pending callbacks)              |   ❌    |   ❌    |   ✅    |   ✅    |   ✅    |   ✅    |
| C1 (dirty on meta load)                |   ❌    |   ❌    |   ✅    |   ✅    |   ✅    |   ✅    |
| C2 (only relevant dirty)               |   ❌    |   ❌    |   ✅    |   ✅    |   ✅    |   ✅    |
| D1 (insert while pending)              |   ❌    |   ❌    |   ❌    |   ❌    |   ✅    |   ✅    |
| D2 (batch pending writes)              |   ❌    |   ❌    |   ❌    |   ❌    |   ✅    |   ✅    |
| D3 (dedupe pending)                    |   ❌    |   ❌    |   ❌    |   ❌    |   ✅    |   ✅    |
| D4 (insert to unloaded leaf)           |   ❌    |   ❌    |   ❌    |   ❌    |   ✅    |   ✅    |
| D5 (update moves row to unloaded page) |   ❌    |   ❌    |   ❌    |   ❌    |   ✅    |   ✅    |
| E1 (scan reports missing)              |   ❌    |   ❌    |   ❌    |   ❌    |   ❌    |   ✅    |
| E2 (settlement returns loads)          |   ❌    |   ❌    |   ❌    |   ❌    |   ❌    |   ✅    |
| E3 (sibling pages)                     |   ❌    |   ❌    |   ❌    |   ❌    |   ❌    |   ✅    |
| F1 (cold start e2e)                    |   ❌    |   ❌    |   ❌    |   ✅    |   ✅    |   ✅    |
| F2 (insert during cold)                |   ❌    |   ❌    |   ❌    |   ❌    |   ✅    |   ✅    |

---

## Phase 1: Index Signals Pending State

**Goal**: IndexScanNode returns `pending=true` when index isn't ready.

**Tests passing after this phase**: A1, A2 (+ A3, F3 which already pass)

### Changes

| File             | Change                                                                                 |
| ---------------- | -------------------------------------------------------------------------------------- |
| `btree_index.rs` | Add `is_ready()` method: returns true only if meta AND root page loaded                |
| `btree_index.rs` | Add `ScanState` enum: `Ready(Vec<ObjectId>)` or `Pending`                              |
| `index_scan.rs`  | Add `was_pending: bool` field to track state transitions                               |
| `index_scan.rs`  | Return `pending=true` in TupleDelta when `!index.is_ready()` or scan returns `Pending` |

### Implementation

```rust
// btree_index.rs
pub fn is_ready(&self) -> bool {
    self.meta_loaded && self.pages.contains_key(&self.meta.root_page_id)
}

pub enum ScanState {
    Ready(Vec<ObjectId>),
    Pending,  // Pages still loading
}

// index_scan.rs
fn scan(&mut self, ctx: &SourceContext) -> TupleDelta {
    let Some(index) = ctx.indices.get(&key) else {
        return TupleDelta::new();
    };

    if !index.is_ready() {
        self.was_pending = true;
        return TupleDelta { pending: true, ..Default::default() };
    }

    // Transitioning from pending to ready - force full rescan
    if self.was_pending {
        self.was_pending = false;
        self.last_delta_epoch = None;
    }

    // ... existing scan logic ...
    TupleDelta { pending: false, ... }
}
```

### Unit Tests

```rust
#[test]
fn index_scan_returns_pending_when_meta_not_loaded() {
    let index = BTreeIndex::new("users", "_id");
    assert!(!index.is_ready());
    // IndexScanNode with this index should return pending=true
}

#[test]
fn index_scan_returns_pending_when_root_page_missing() {
    let mut index = BTreeIndex::new("users", "_id");
    index.process_meta_load(Some(meta_bytes));  // meta loaded
    assert!(!index.is_ready());  // but root page not loaded
}

#[test]
fn index_scan_returns_ready_when_root_loaded() {
    let mut index = BTreeIndex::new("users", "_id");
    index.process_meta_load(Some(meta_bytes));
    index.process_page_load(root_page_id, Some(page_bytes));
    assert!(index.is_ready());
}

#[test]
fn index_scan_forces_rescan_after_pending_to_ready_transition() {
    // Verify last_delta_epoch is cleared when was_pending transitions to false
}
```

---

## Phase 2: Mark Subscriptions Dirty When Index Loads

**Goal**: When index meta/pages load, affected subscriptions re-settle.

**Tests passing after this phase**: A1, A2, B2, C1, C2

### Changes

| File         | Change                                                                                      |
| ------------ | ------------------------------------------------------------------------------------------- |
| `manager.rs` | In `StorageResponse::LoadIndexMeta` handler, mark subscriptions dirty if index became ready |
| `manager.rs` | In `StorageResponse::LoadIndexPage` handler, same pattern                                   |
| `manager.rs` | Add `mark_subscriptions_dirty_for_index(table, column)` method                              |
| `graph.rs`   | Add `uses_index(table, column) -> bool` method                                              |

### Implementation

```rust
// manager.rs
StorageResponse::LoadIndexMeta { table, column, result } => {
    let key = (table.clone(), column.clone(), self.current_branch());
    if let Some(index) = self.indices.get_mut(&key) {
        let was_ready = index.is_ready();
        index.process_meta_load(result.as_ref().ok().and_then(|o| o.clone()));

        if !was_ready && index.is_ready() {
            self.mark_subscriptions_dirty_for_index(&table, &column);
        }
    }
}

StorageResponse::LoadIndexPage { table, column, page_id, result } => {
    // Same pattern - check is_ready() before and after
}

fn mark_subscriptions_dirty_for_index(&mut self, table: &str, column: &str) {
    for subscription in self.subscriptions.values_mut() {
        if subscription.graph.uses_index(table, column) {
            subscription.graph.mark_dirty();
        }
    }
}
```

### Unit Tests

```rust
#[test]
fn subscriptions_marked_dirty_when_index_meta_loads() {
    // Subscribe to query on users table
    // Verify subscription.graph is dirty after process_meta_load makes index ready
}

#[test]
fn subscriptions_marked_dirty_when_root_page_loads() {
    // Meta already loaded, root page loads
    // Verify dirty marking
}

#[test]
fn only_relevant_subscriptions_marked_dirty() {
    // Subscribe to users table and todos table
    // Load users index
    // Only users subscription should be dirty
}
```

---

## Phase 3: RuntimeCore Respects Pending

**Goal**: One-shot queries wait until ready. Subscription callbacks simply don't fire while pending.

**Tests passing after this phase**: A1, A2, B1, B2, C1, C2, F1

### Changes

| File              | Change                                                                                |
| ----------------- | ------------------------------------------------------------------------------------- |
| `runtime_core.rs` | In one-shot query handling, only resolve when `!update.delta.pending` (internal flag) |
| `runtime_core.rs` | Only invoke subscription callback when `!update.delta.pending`                        |
| `output.rs`       | Verify OutputNode holds back results when `pending=true` (already implemented)        |

### Implementation

```rust
// runtime_core.rs - One-shot queries
if let Some(pending_query) = self.pending_one_shot_queries.get_mut(&handle) {
    // Only resolve when not pending (internal state)
    if !update.delta.pending {
        if let Some(sender) = pending_query.sender.take() {
            let results = decode_rows(&update.delta);
            let _ = sender.send(Ok(results));
        }
        completed_one_shots.push(handle);
    }
    // If pending internally, keep waiting - don't resolve yet
}

// runtime_core.rs - Subscription callbacks
// Note: The delta delivered to callback has NO pending field.
// We simply don't call the callback at all while pending.
if !update.delta.pending {
    let callback_delta = SubscriptionDelta {
        handle,
        added: update.delta.added,  // No pending field in this struct
        removed: update.delta.removed,
        updated: update.delta.updated,
    };
    (state.callback)(callback_delta);
}
// If pending, callback is not called at all
```

### Unit Tests

```rust
#[test]
fn one_shot_query_does_not_resolve_while_loading() {
    // Create query on unloaded index
    // QueryFuture should NOT resolve yet (no callback, future pending)
    // Load index
    // QueryFuture resolves with data
}

#[test]
fn subscription_callback_not_invoked_while_loading() {
    // Create subscription on unloaded index
    // Verify callback count == 0 while loading
    // Load index
    // Verify callback count == 1 with complete data
}
```

---

## Phase 4: Scans Include pending_writes (with correct bookkeeping)

**Goal**: New writes via `pending_writes` are tracked internally and appear in the first successful result set, but are NOT delivered while the query is still pending.

**Tests passing after this phase**: A1, A2, B1, B2, C1, C2, D1, D2, D3, F1, F2

**Key principle**: If the overall query is pending (waiting for index pages), we hold back ALL results including new sync writes. But we must do correct internal bookkeeping so those writes appear in the first callback when pending clears. This mirrors how MaterializeNode accumulates changes while waiting for row data.

### Changes

| File             | Change                                                                             |
| ---------------- | ---------------------------------------------------------------------------------- |
| `btree_index.rs` | `scan_all()`, `lookup_exact()`, `range_scan()` merge results from `pending_writes` |
| `btree_index.rs` | Ensure duplicates are handled (same ID in B-tree and pending_writes)               |
| `index_scan.rs`  | Track `pending_writes` entries seen while pending, include in first Ready delta    |
| `output.rs`      | Accumulate deltas while `pending=true` (already exists for MaterializeNode)        |

### Implementation

```rust
// btree_index.rs
pub fn scan_all(&self) -> ScanResult {
    let mut results = Vec::new();

    // Always include pending_writes in results
    for write in &self.pending_writes {
        if let PendingWrite::Insert(_, row_id) = write {
            results.push(*row_id);
        }
    }

    // Check if B-tree is ready
    if !self.is_ready() {
        return ScanResult {
            ids: results,  // Has pending_writes
            state: ScanState::Pending,
        };
    }

    // Merge with B-tree results (dedupe)
    // ... traverse B-tree, add to results ...

    ScanResult {
        ids: results,
        state: ScanState::Ready,
    }
}

// index_scan.rs - track internally, only emit when ready
fn scan(&mut self, ctx: &SourceContext) -> TupleDelta {
    let result = index.scan_all();

    match result.state {
        ScanState::Pending => {
            // Track what we've seen for when we become ready
            self.held_ids = result.ids;
            self.was_pending = true;
            TupleDelta { pending: true, ..Default::default() }
        }
        ScanState::Ready => {
            // Include everything (held + new)
            let all_ids = if self.was_pending {
                self.was_pending = false;
                std::mem::take(&mut self.held_ids)  // First ready: emit all
            } else {
                result.ids  // Normal: just current results
            };
            TupleDelta { added: all_ids, pending: false, ... }
        }
    }
}
```

**Result**: New writes are tracked immediately but delivered together with old data when the query becomes ready. User sees one complete callback, not partial results.

### Unit Tests

```rust
#[test]
fn pending_writes_held_back_while_query_pending() {
    // Index not loaded
    // Insert new row (goes to pending_writes)
    // Scan returns pending=true, NO rows emitted yet
}

#[test]
fn pending_writes_included_in_first_ready_result() {
    // Index not loaded, insert row A
    // Scan 1: pending=true, no emit
    // Load index (has row B from before)
    // Scan 2: pending=false, emits both A and B
}

#[test]
fn multiple_pending_writes_accumulated() {
    // Index not loaded
    // Insert row A, insert row B, insert row C
    // All three should appear in first ready callback
}

#[test]
fn scan_merges_pending_writes_with_btree_deduped() {
    // Index loaded with row A
    // Insert row B (goes to pending_writes)
    // scan_all returns both A and B, no duplicates
}
```

---

## Phase 5: Graph Settlement Returns Load Requirements

**Goal**: Scans discover required pages during traversal and report them as load requirements, which get queued to outboxes.

**Tests passing after this phase**: ALL (A1, A2, B1, B2, C1, C2, D1, D2, D3, E1, E2, E3, F1, F2, F3)

### Background

Pages are discovered during traversal - you can't know upfront what's needed:

- `lookup_exact(key)`: Root → internal → target leaf (path discovered by traversing)
- `range_scan(min, max)`: Path to start + sibling chain (discovered via `next_leaf`)
- `scan_all()`: ALL leaf pages (requires traversing ALL internal pages)

**Current bugs**:

- `scan_all()` doesn't traverse - just iterates loaded pages!
- `range_scan()` silently stops on missing sibling pages
- No method reports what pages are needed

### Design: Settlement Returns Either Success or Pending with Load Requirements

Instead of making scan methods mutable, graph settlement returns a result type:

```rust
enum SettlementResult {
    Success {
        deltas: RowDelta,
    },
    Pending {
        deltas: RowDelta,  // May have partial results (pending_writes)
        index_loads_needed: Vec<IndexLoadRequest>,  // Pages to load
        object_loads_needed: Vec<ObjectId>,  // Rows to materialize
    },
}

struct IndexLoadRequest {
    table: String,
    column: String,
    request: IndexLoadType,
}

enum IndexLoadType {
    Meta,
    Page(PageId),
}
```

The `index_loads_needed` and `object_loads_needed` get put into outboxes for the storage layer to process.

### Changes

| File             | Change                                                                   |
| ---------------- | ------------------------------------------------------------------------ |
| `btree_index.rs` | Scan methods return `(results, missing_pages)` tuple                     |
| `btree_index.rs` | Fix `scan_all()` to actually traverse the tree and collect missing pages |
| `btree_index.rs` | Fix `range_scan()` to report missing sibling pages                       |
| `graph.rs`       | `settle()` returns `SettlementResult` instead of just `RowDelta`         |
| `manager.rs`     | Process `SettlementResult`, queue load requests to outbox                |

### Implementation

```rust
// btree_index.rs
pub struct ScanResult {
    pub ids: Vec<ObjectId>,
    pub missing: Vec<IndexLoadType>,  // What we need to complete
}

pub fn scan_all(&self) -> ScanResult {
    let mut ids = Vec::new();
    let mut missing = Vec::new();

    // Always include pending_writes
    for write in &self.pending_writes {
        if let PendingWrite::Insert(_, row_id) = write {
            ids.push(*row_id);
        }
    }

    if !self.meta_loaded {
        missing.push(IndexLoadType::Meta);
        return ScanResult { ids, missing };
    }

    // Traverse tree, collecting IDs and missing pages
    self.traverse_collecting(&mut ids, &mut missing);

    ScanResult { ids, missing }
}

fn traverse_collecting(&self, ids: &mut Vec<ObjectId>, missing: &mut Vec<IndexLoadType>) {
    let mut stack = vec![self.meta.root_page_id];

    while let Some(page_id) = stack.pop() {
        match self.pages.get(&page_id) {
            Some(PageState::Loaded(page)) => {
                match page {
                    BTreePage::Leaf { entries, next_leaf, .. } => {
                        for entry in entries {
                            ids.extend(&entry.row_ids);
                        }
                        if let Some(next) = next_leaf {
                            stack.push(*next);  // Continue sibling chain
                        }
                    }
                    BTreePage::Internal { children, .. } => {
                        stack.extend(children.iter().copied());
                    }
                }
            }
            Some(PageState::Loading) => {
                // Already requested, just note we're waiting
                missing.push(IndexLoadType::Page(page_id));
            }
            None => {
                missing.push(IndexLoadType::Page(page_id));
            }
        }
    }
}

// graph.rs
pub fn settle(&mut self, ...) -> SettlementResult {
    let mut index_loads_needed = Vec::new();
    let mut object_loads_needed = Vec::new();

    // ... process nodes ...

    for node in index_scan_nodes {
        let result = index.scan_all();  // or lookup_exact, range_scan
        index_loads_needed.extend(
            result.missing.into_iter().map(|m| IndexLoadRequest {
                table: node.table.clone(),
                column: node.column.clone(),
                request: m,
            })
        );
        // ... process result.ids ...
    }

    // ... materialize nodes may add to object_loads_needed ...

    if index_loads_needed.is_empty() && object_loads_needed.is_empty() {
        SettlementResult::Success { deltas }
    } else {
        SettlementResult::Pending {
            deltas,
            index_loads_needed,
            object_loads_needed,
        }
    }
}

// manager.rs - queue the load requests
match settlement_result {
    SettlementResult::Pending { index_loads_needed, object_loads_needed, .. } => {
        for req in index_loads_needed {
            self.queue_index_load(req);
        }
        for oid in object_loads_needed {
            self.queue_object_load(oid);
        }
    }
    _ => {}
}
```

**Benefits of this approach**:

- Keeps scan methods immutable (`&self`)
- Clear separation: "what do I need?" vs "go load it"
- Fits existing outbox pattern
- Easy to batch load requests
- Settlement is pure computation, I/O is separate

### Unit Tests

```rust
#[test]
fn scan_all_reports_missing_meta() {
    let index = BTreeIndex::new("users", "_id");
    let result = index.scan_all();
    assert!(result.missing.contains(&IndexLoadType::Meta));
}

#[test]
fn scan_all_reports_missing_root_page() {
    let mut index = BTreeIndex::new("users", "_id");
    index.process_meta_load(Some(meta_with_root_id));

    let result = index.scan_all();
    assert!(result.missing.iter().any(|m| matches!(m, IndexLoadType::Page(id) if *id == root_id)));
}

#[test]
fn scan_all_reports_missing_internal_and_leaf_pages() {
    // Set up index with root loaded, but children not
    // scan_all should report all missing children
}

#[test]
fn settlement_queues_index_load_requests() {
    // Graph with unloaded index
    // settle() returns Pending with index_loads_needed
    // Verify requests are queued to outbox
}

#[test]
fn settlement_returns_success_when_all_loaded() {
    // Graph with fully loaded index
    // settle() returns Success with deltas
}

#[test]
fn range_scan_reports_missing_sibling_pages() {
    // First leaf loaded, second not
    // range_scan should include second leaf in missing
}
```

---

## Phase 6: Remove Explicit Preloading APIs

**Goal**: Clean up now-unnecessary APIs.

**Tests passing**: All Phase 0 tests continue to pass (no new invariants, just API cleanup).

### Changes

| API                              | Location                      | Action     |
| -------------------------------- | ----------------------------- | ---------- |
| `load_indices()`                 | `WasmRuntime`, `TokioRuntime` | **Remove** |
| `reset_indices_for_cold_start()` | `QueryManager`, `BTreeIndex`  | **Remove** |
| `load_indices_from_driver()`     | Test helpers                  | **Remove** |

### What to Update

- Remove methods from runtime structs
- Update any tests that explicitly called these
- Remove TODO comments about `waitForIndices()`
- Update `client.ts` - remove the NOTE comment about loadIndices

---

## Phase 7: E2E Tests

**Goal**: Verify the complete flow works in realistic scenarios.

### IndexedDB Cold-Start Test (Critical)

This test doesn't exist today and would fail without the fixes:

```typescript
it("loads data from IndexedDB after reopen", async () => {
  // Phase 1: Create and populate
  const db1 = await createDb({ driver: indexedDbDriver() });
  db1.insert(app.todos, { title: "Test todo" });
  db1.insert(app.todos, { title: "Another todo" });
  await db1.shutdown();

  // Phase 2: Reopen and query immediately
  const db2 = await createDb({ driver: indexedDbDriver() });
  const todos = await db2.all(app.todos);

  expect(todos.length).toBe(2);
  expect(todos.map((t) => t.title)).toContain("Test todo");
});
```

### Synchronous Edit Cycle Test

```typescript
it("shows inserted row immediately in subscription", async () => {
  const db = await createDb({ driver: indexedDbDriver() });
  let callbackCount = 0;
  let lastAll: Todo[] = [];

  db.subscribeAll(app.todos, (delta) => {
    callbackCount++;
    lastAll = [...delta.all];
  });

  // Wait for initial callback (empty or loaded)
  await new Promise((r) => setTimeout(r, 100));
  const initialCount = callbackCount;

  // Insert should trigger immediate callback with new row
  const id = db.insert(app.todos, { title: "New todo" });

  // Callback should have fired synchronously (count increased)
  expect(callbackCount).toBeGreaterThan(initialCount);
  expect(lastAll.some((t) => t.id === id)).toBe(true);
});
```

### Mixed Scenario: Reopen + Edit

```typescript
it("new write during load appears in first callback", async () => {
  // Phase 1: Create initial data
  const db1 = await createDb({ driver: indexedDbDriver() });
  db1.insert(app.todos, { title: "Old todo" });
  await db1.shutdown();

  // Phase 2: Reopen, subscribe, then insert immediately
  const db2 = await createDb({ driver: indexedDbDriver() });
  let callbackCount = 0;
  let lastAll: Todo[] = [];

  db2.subscribeAll(app.todos, (delta) => {
    callbackCount++;
    lastAll = [...delta.all];
  });

  // Insert new todo immediately (while index still loading)
  db2.insert(app.todos, { title: "New todo" });

  // Wait for indices to load
  await new Promise((r) => setTimeout(r, 500));

  // Should have received exactly ONE callback with BOTH todos
  // (not: callback with empty, then callback with old, then callback with new)
  expect(callbackCount).toBe(1);
  expect(lastAll.length).toBe(2);
  expect(lastAll.map((t) => t.title)).toContain("Old todo");
  expect(lastAll.map((t) => t.title)).toContain("New todo");
});
```

---

## Why Existing Tests Pass (Context)

Existing cold-start tests pass because they avoid the async scenario:

| Test Suite             | Storage           | Why It Passes                                       |
| ---------------------- | ----------------- | --------------------------------------------------- |
| TypeScript E2E         | SQLite (sync)     | SQLite is blocking - no async gap                   |
| Rust cold-start        | TestDriver (sync) | Explicit `load_indices_from_driver()` + sync driver |
| IndexedDB driver tests | fake-indexeddb    | Driver-level only, no query tests                   |

**No tests exist for**: WASM + IndexedDB + DB reopen + query before indices load.

---

## Flow Diagrams

### Scenario: New insert, index not loaded

```
1. insert() → adds to pending_writes (instant)
2. mark_subscriptions_dirty() called
3. immediate_tick() → settle()
4. IndexScanNode: scan_all() returns {ids: [new_row], missing: [Meta]}
5. IndexScanNode tracks new_row internally, returns pending=true
6. SettlementResult::Pending { index_loads_needed: [Meta] }
7. Manager queues LoadIndexMeta to outbox
8. OutputNode holds back (pending=true) - no callback yet
9. [Async] Meta loads → mark subscriptions dirty
10. Re-settle: scan_all() returns {ids: [new_row], missing: [RootPage]}
11. Manager queues LoadIndexPage(root) to outbox
12. [Async] Root loads → mark subscriptions dirty
13. Re-settle: scan_all() returns {ids: [new_row, old_rows...], missing: []}
14. SettlementResult::Success { deltas with all rows }
15. Callback fires with complete results (new + old together)
```

### Scenario: DB reopen, query existing data

```
1. Subscribe to query
2. First settle() runs
3. IndexScanNode: scan_all() returns {ids: [], missing: [Meta]}
4. SettlementResult::Pending { index_loads_needed: [Meta] }
5. Manager queues LoadIndexMeta to outbox
6. OutputNode holds back - no callback
7. [Async] Meta loads → mark subscriptions dirty
8. Re-settle: scan_all() returns {ids: [], missing: [RootPage, Page2, Page3...]}
9. Manager queues all page loads to outbox
10. [Async] Pages load progressively → mark dirty each time
11. Re-settle each time, collecting more pages discovered
12. Eventually: scan_all() returns {ids: [all_rows], missing: []}
13. SettlementResult::Success { deltas }
14. Callback fires with complete data
```

### Scenario: Mixed - insert while index loading

```
1. Subscribe (index not loaded)
2. settle() → Pending, queues Meta load
3. [User inserts new row] → goes to pending_writes
4. mark_subscriptions_dirty() called
5. Re-settle: scan_all() returns {ids: [new_row], missing: [Meta]}
6. IndexScanNode tracks new_row internally (held_ids)
7. Still Pending - no callback yet
8. [Async] Meta loads, more pages needed
9. Multiple re-settles as pages load...
10. Finally: scan_all() returns {ids: [new_row, old_rows], missing: []}
11. IndexScanNode emits all held_ids + current ids
12. SettlementResult::Success
13. ONE callback with everything (new + old)
```
