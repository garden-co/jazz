# Making Tests More E2E

## Motivation

Unit-level sync tests construct "perfect" test data — hand-built `Commit` structs, empty metadata, pre-wired scopes — that exercises code paths in isolation but misses how those paths interact in production. When tests bypass the same layers that production traffic flows through, security and correctness gaps hide in the seams.

The fix: move most correctness tests to the **RuntimeCore** level, where they exercise the full stack with realistic data.

---

## Example 1: Row-Object Scope Bypass

### The bug

`process_from_client` has a fast-path for "system or row objects" — anything whose metadata contains `"table"` or `type=catalogue_schema` bypasses the scope check:

```rust
let is_system_or_row_object = metadata.as_ref().is_some_and(|m| {
    m.metadata.get("type").is_some_and(|t| t == "catalogue_schema" || t == "catalogue_lens")
        || m.metadata.contains_key("table")
});

if !is_system_or_row_object && !client.is_in_scope(*object_id, branch_name) {
    // queue for approval
    return;
}

if let Some(session) = &client.session {
    // queue for permission check (ReBAC)
} else {
    // No session — applied immediately
    self.apply_payload_from_client(...);
}
```

Intent: new row inserts can't be scope-checked (the server hasn't seen the object yet), so skip the scope gate and let ReBAC handle auth.

Reality: when the client has **no session** (permissive mode), the write is applied immediately with zero approval. A no-session client can write arbitrary row objects to any table.

### Why tests didn't catch it

Every auth test uses `metadata: HashMap::new()` or `metadata: None` in the payload. No test ever sends metadata with `"table"` set — which is what **every real row insert** does. The `is_system_or_row_object` path was completely untested.

The tests constructed synthetic payloads that didn't look like real traffic, so the bypass code was never exercised.

### The fix

Row objects not in scope should still go to `pending_updates` for upper-layer approval. The `is_system_or_row_object` flag should only suppress the scope check, not skip the pending queue entirely.

### Lesson

If these tests ran at RuntimeCore level — creating a schema, inserting a row through the client API, observing what the server does — the metadata would be realistic by construction. The bypass would have been caught immediately because the test row would have `"table"` metadata, and the test would assert that unapproved writes don't get applied.

---

## Example 2: PersistenceAck Without a Consumer (Phase 6a)

### The gap

Phase 6a added `PersistenceAck` to the sync protocol — when a server persists data, it sends `PersistenceAck { commit_id, tier }` back through the tier chain. The implementation is correct: acks are emitted, relayed, and arrive at the originating client.

But `RuntimeCore` — the public API layer — has zero references to `PersistenceAck`. There is no way for a caller to say "insert this row and tell me when it's persisted at tier X." The plumbing exists with no consumer.

### Why tests didn't catch it

The Phase 6a tests are surgical SyncManager-level tests: set up two or three connected `SyncManager` instances, pump messages, assert that `PersistenceAck` payloads arrive with the correct `commit_id` and `tier`. These tests pass — the sync protocol works.

But no test ever asks the question a real user would ask: "I inserted a row — is it persisted yet?" Because the tests operate below RuntimeCore, the missing API surface was invisible. The plan felt complete because every sync message was verified, even though no application-level behavior was tested.

### The fix

Phase 6c adds `insert_persisted()` / `update_persisted()` / `delete_persisted()` to RuntimeCore, returning a `Receiver<()>` that resolves when the requested tier confirms. Tests at the RuntimeCore level exercise the full path: insert → sync → server persists → ack relayed → receiver fires.

### Lesson

If Phase 6a had been planned around RuntimeCore-level tests — "insert a row, wait for persistence, assert the wait resolves" — the missing consumer API would have been obvious from the start. Testing sync messages in isolation verified the mechanism but not the feature. The spirit of the plan (durability guarantees for users) wasn't complete; only the letter (correct ack messages) was.
