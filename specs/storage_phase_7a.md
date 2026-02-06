# Phase 7a: BfTreeStorage + Native Integration

**Depends on**: IoHandler → Storage refactor (completed separately).

**Goal**: Import bf-tree-web, implement `BfTreeStorage`, make `jazz-rs::test_persistence` and `todo-server-rs::test_local_persistence` go green.

---

## Step 1: Import bf-tree-web as subtree

```bash
git subtree add --prefix=crates/bf-tree \
  git@github.com:garden-co/bf-tree-web.git main --squash
```

Add to workspace `Cargo.toml`:
```toml
members = [
  "crates/bf-tree",
  # ... existing
]
```

Add feature to `crates/groove/Cargo.toml`:
```toml
[features]
default = []
bftree = ["bf-tree"]

[dependencies]
bf-tree = { path = "../bf-tree", optional = true }
```

### Verification

```bash
cargo check -p bf-tree
```

**Risks to resolve here**: package name, WASM-only deps, Send/Sync of BfTree.

---

## Step 2: Implement BfTreeStorage

**File**: `crates/groove/src/storage/bftree.rs` (behind `#[cfg(feature = "bftree")]`)

In `storage/mod.rs`:
```rust
#[cfg(feature = "bftree")]
mod bftree;
#[cfg(feature = "bftree")]
pub use bftree::BfTreeStorage;
```

### Struct

```rust
pub struct BfTreeStorage {
    tree: BfTree,
}
```

No outbox, no scheduling — pure storage.

### Constructors

```rust
impl BfTreeStorage {
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError>
    pub fn memory(cache_size_bytes: usize) -> Result<Self, StorageError>
}
```

### Key encoding (private functions in bftree.rs)

All data in one bf-tree, keys are UTF-8 strings with hex-encoded binary parts:

```
"obj:{uuid}:meta"                                              → JSON metadata
"obj:{uuid}:br:{branch}:tips"                                  → JSON HashSet<CommitId>
"obj:{uuid}:br:{branch}:c:{commit_uuid}"                       → JSON Commit
"blob:{hex_hash}"                                               → raw bytes
"ack:{commit_uuid}"                                             → JSON PersistenceTier
"idx:{table}:{col}:{branch}:{hex_encoded_value}:{uuid}"        → empty
```

- `{uuid}` = UUID hex (32 chars, no dashes). Preserves sort order.
- `{hex_encoded_value}` = `hex::encode(encode_value(value))`. Preserves lexicographic order.
- `:` separator safe: schema names have no colons, all binary data is hex-encoded.

Reuse existing `encode_value()` function (`pub(crate)` in `storage/mod.rs`).

### bf-tree API Surface

```rust
// Constructors
BfTree::new(path, cache_size_byte) -> Result<Self, ConfigError>  // StdVfs (native)
BfTree::new(":memory:", cache_size_byte)                          // MemoryVfs

// All methods take &self (not &mut self) — internal concurrency control
tree.insert(key: &[u8], value: &[u8]) -> LeafInsertResult
tree.read(key: &[u8], out_buffer: &mut [u8]) -> LeafReadResult
tree.delete(key: &[u8])
tree.scan_with_end_key(start: &[u8], end: &[u8], ...) -> ScanIter
tree.scan_with_count(start: &[u8], count: usize, ...) -> ScanIter
```

### Storage implementation mapping

| Storage method | bf-tree operation | Notes |
|---|---|---|
| `create_object` | `tree.insert(obj_meta_key, json_metadata)` | |
| `load_object_metadata` | `tree.read(obj_meta_key, buf)` → deserialize | Handle not-found (LeafReadResult) |
| `append_commit` | insert commit + read-modify-write tips | Read existing tips, add commit_id, write back |
| `delete_commit` | delete commit + read-modify-write tips | |
| `set_branch_tails` | insert or delete tips key | None → delete |
| `load_branch` | scan commit prefix + read tips + read ack tiers | Assemble LoadedBranch |
| `store_blob` / `load_blob` / `delete_blob` | direct insert/read/delete | |
| `store_ack_tier` | insert ack key | |
| `index_insert` / `index_remove` | insert/delete with empty value | Existence is the signal |
| `index_lookup` | scan value prefix, parse object_ids from keys | |
| `index_range` | scan with start/end keys, filter for exact bounds | Handle Bound variants |
| `index_scan_all` | scan table/col/branch prefix | |

**Buffer sizing for `tree.read()`**: Start with 64KB buffer. Check `LeafReadResult` — if it indicates truncation/not-found, handle accordingly.

**`load_branch` ack_state population**: For each commit loaded, check `tree.read(ack_key(commit_id))` to get stored ack tiers, then populate `commit.ack_state`.

### Verification

```bash
cargo test -p groove --features bftree -- bftree
```

---

## Step 3: Integrate with jazz-rs

**`crates/jazz-rs/Cargo.toml`**:
```toml
groove = { path = "../groove", features = ["bftree"] }
```

**`crates/jazz-rs/src/client.rs`**:
```rust
use groove::storage::BfTreeStorage;

const CACHE_SIZE: usize = 64 * 1024 * 1024; // 64MB

pub struct JazzClient {
    runtime: TokioRuntime<BfTreeStorage>,
    // ... rest unchanged
}
```

In `JazzClient::connect()`:
```rust
let storage = BfTreeStorage::open(&context.data_dir, CACHE_SIZE)
    .map_err(|e| JazzError::Storage(e.to_string()))?;

let runtime = TokioRuntime::new(schema_manager, storage, move |entry| {
    // ... existing sync callback unchanged
});
```

---

## Step 4: Integrate with jazz-cli

**`crates/jazz-cli/Cargo.toml`**:
```toml
groove = { path = "../groove", features = ["bftree"] }
```

**`crates/jazz-cli/src/commands/server.rs`**: Same pattern. `ServerState.runtime` type becomes `TokioRuntime<BfTreeStorage>`. Propagate through routes.rs if needed.

---

## Step 5: Tests

### New BfTreeStorage tests (in `storage/bftree.rs`)

1. **`bftree_object_roundtrip`** — create_object + load_object_metadata, verify metadata matches
2. **`bftree_commit_roundtrip`** — append_commit + load_branch, verify commit + tips
3. **`bftree_index_ops`** — index_insert several entries, verify index_lookup, index_range, index_scan_all
4. **`bftree_persistence`** — BfTreeStorage::open(tempdir), insert data, drop, reopen same path, verify survival
5. **`bftree_with_runtime_core`** — RuntimeCore<BfTreeStorage, ...> end-to-end insert/query/update/delete

### Expected test status after Phase 7a

| Test | Before | After |
|------|--------|-------|
| `jazz-rs::test_crud_operations` | GREEN | GREEN |
| `jazz-rs::test_persistence` | **RED** | **GREEN** |
| `todo-server-rs::test_local_persistence` | **RED** | **GREEN** |
| All 571 groove tests | GREEN | GREEN |
| groove-tokio tests | GREEN | GREEN |
| New bftree_* tests | N/A | GREEN |

---

## Verification

```bash
# Step 1: bf-tree compiles
cargo check -p bf-tree

# Step 2: BfTreeStorage tests
cargo test -p groove --features bftree -- bftree

# Steps 3-4: Persistence goes green
cargo test -p jazz-rs
cargo test -p todo-server-rs

# Full suite
cargo test --workspace
cargo clippy --workspace -- -D warnings
```

---

## Files to Modify/Create

| File | Change |
|------|--------|
| `Cargo.toml` (workspace) | Add `crates/bf-tree` to members |
| `crates/groove/Cargo.toml` | Add `bftree` feature + optional `bf-tree` dep |
| `crates/groove/src/storage/bftree.rs` | **New**: BfTreeStorage + key encoding + tests |
| `crates/groove/src/storage/mod.rs` | Add conditional `mod bftree`, make `encode_value` pub(crate) |
| `crates/jazz-rs/Cargo.toml` | Add `features = ["bftree"]` to groove dep |
| `crates/jazz-rs/src/client.rs` | Use BfTreeStorage, update TokioRuntime type |
| `crates/jazz-cli/Cargo.toml` | Add `features = ["bftree"]` to groove dep |
| `crates/jazz-cli/src/commands/server.rs` | Use BfTreeStorage, update TokioRuntime type |
| `crates/jazz-cli/src/routes.rs` | Update ServerState type if needed |

---

## Risks

1. **BfTree Send/Sync** — If `BfTree` is `!Send`, can't wrap in `Arc<Mutex<...>>`. Verify after import.
2. **bf-tree read() buffer sizing** — Need to understand `LeafReadResult` semantics. Check API after import.
3. **bf-tree package name** — Might be `bf-tree`, `bf-tree-web`, or something else. Adjust dep name accordingly.
4. **bf-tree dependencies** — May pull in WASM-specific deps that break native builds. May need feature flags.
