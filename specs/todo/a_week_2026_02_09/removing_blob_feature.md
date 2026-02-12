# Removing the Blob Feature — TODO

Remove the blob abstraction entirely from the codebase. The blob system (content-addressed binary data with its own storage, sync, and permission paths) is being replaced by the relational `files`/`file_parts` pattern (see `../b_mvp/built_in_file_storage.md`). Before building the replacement, remove the old system to simplify the codebase.

## Why Now

The team is starting to work on the codebase. The blob feature is a significant surface area (storage, sync, object manager, tests) that adds cognitive load and will never be shipped — it's superseded by the relational design. Removing it now prevents anyone from building on dead code.

## Codebase Edits

### 1. Object Manager (`crates/groove/src/object_manager.rs`)

**Remove:**
- `BlobId` struct (~line 62-68)
- `BlobState` enum (~line 70-77)
- `BlobAssociation` struct (~line 111-117)
- `Error::BlobNotFound` variant (~line 87-88)
- `deleted_blobs` field from `TruncateResult::Success` (~line 94-98)
- `blobs: HashMap<ContentHash, BlobState>` field (~line 139)
- `blob_associations: HashMap<ContentHash, Vec<BlobAssociation>>` field (~line 141)
- `associate_blob()` method (~line 529-574)
- `load_blob()` method (~line 576-599)
- `put_blob()` method (~line 714-728)
- `get_blob()` method (~line 730-739)
- Blob dissociation/deletion logic in `truncate_branch()` (~line 899-1006) — keep truncation itself, just remove blob parts
- Blob section of `memory_size()` (~line 1087-1134)
- Any remaining blob TODO comments (~line 2289-2293)

**Update:**
- `TruncateResult::Success` — remove `deleted_blobs` field
- `truncate_branch()` — remove blob association cleanup, simplify return

### 2. Storage Trait (`crates/groove/src/storage/mod.rs`)

**Remove from trait definition (~line 57-195):**
- `store_blob()` method
- `load_blob()` method
- `delete_blob()` method

**Remove from MemoryStorage (~line 200+):**
- `blobs: HashMap<ContentHash, Vec<u8>>` field (~line 220)
- `store_blob()`, `load_blob()`, `delete_blob()` implementations (~line 431-443)
- `memory_storage_blob_storage()` test (~line 623-640)

### 3. BfTreeStorage (`crates/groove/src/storage/bftree.rs`)

**Remove:**
- `"blob:{hex_hash}"` key encoding (~line 12)
- `blob_key()` helper (~line 147-149)
- `store_blob()`, `load_blob()`, `delete_blob()` implementations (~line 556-569)
- `bftree_blob_roundtrip()` test (~line 867-878)

### 4. Sync Manager (`crates/groove/src/sync_manager.rs`)

**Remove:**
- `BlobId` import (~line 8)
- `SyncPayload::BlobRequest` variant (~line 221-222)
- `SyncPayload::BlobResponse` variant (~line 224-225)
- `SyncError::BlobAccessDenied` variant (~line 176-177)
- `SyncError::BlobNotFound` variant (~line 178-179)
- Server-side `BlobRequest` handler (~line 1353-1390)
- Client-side `BlobResponse` handler (~line 1182-1189)
- All blob-related match arms in message routing (~line 1210-1211, 1478)
- `blob_request_with_permission_returns_data()` test (~line 2821-2886)
- `blob_request_without_permission_returns_error()` test (~line 2888-2945)

### 5. RocksDB Driver (`crates/groove-rocksdb/src/lib.rs`)

**Remove:**
- `CF_BLOBS` column family (~line 29)
- `CF_BLOB_REFS` column family (~line 30)
- `store_blob()`, `load_blob()` methods (~line 548-559)
- `associate_blob()`, `load_blob_associations()`, `dissociate_and_maybe_delete_blob()` methods (~line 561-646)
- `test_driver_blob_operations()` test (~line 828-853)
- `test_driver_blob_associations()` test (~line 856-925)

### 6. Transport (`crates/jazz-transport/src/lib.rs`)

**Update:**
- Remove blob message variants from `SyncPayload` serialization/deserialization (if any transport-layer handling exists beyond the SyncManager routing)

### 7. WASM/NAPI Bindings

**Check and remove** any blob-related exports or methods in:
- `crates/groove-wasm/src/runtime.rs`
- `crates/groove-napi/src/lib.rs`

## Status-Quo Spec Edits

### `specs/status-quo/object_manager.md`

- Remove "Blob" section (~line 46-52) — the entire data model entry
- Remove `ContentHash` from Identifiers table (~line 61)
- Remove `store_blob()`, `load_blob()` from Key Storage methods list (~line 73)
- Remove "Blob Operations" table from Public API (~line 106-115)
- Remove `deleted_blobs` from truncation description (~line 119)
- Remove "Blob deduplication" from Design Decisions (~line 162)
- Remove blob test note (~line 170)
- Remove `BlobNotFound` from Error enum (~line 149)

### `specs/status-quo/sync_manager.md`

- Remove `BlobRequest` / `BlobResponse` from SyncPayload table (~line 69)
- Remove `BlobAccessDenied`, `BlobNotFound` from SyncError table (~line 84-85)
- Remove "blobs" from `/sync` description in the table if referenced

### `specs/status-quo/storage.md`

- Remove `get_blob()`, `put_blob()` from Operations list (~line 13)
- Remove "blobs" from trait description (~line 9)

### `specs/status-quo/http_transport.md`

- Remove "blobs" from `/sync` endpoint description (~line 32)

### `specs/status-quo/batched_tick_orchestration.md`

- Update Storage description to remove "blobs" (~line 13)

## Todo Spec Edits

### `specs/todo/c_launch/image_and_file_serving.md`

- Update "external blob store" reference (~line 19) — reword to "external object store (S3, R2)"

### `specs/todo/c_launch/upload_limits_and_rules.md`

- Update "binary blobs" reference (~line 21) — reword to "binary data" or "file parts"

### `specs/todo/c_launch/memory_profiling_accuracy.md`

- Update "Variable-length blob data" (~line 5) — reword to "Variable-length binary data"
- Update superseded note (~line 9)

### `specs/todo/c_launch/e2ee_per_column.md`

- Update "encrypted blob" references (~line 16, 19, 35) — reword to "encrypted binary data" or "encrypted file parts"

## Verification

After removal:
1. `cargo build` — no blob references remain in production code
2. `cargo test` — all remaining tests pass (blob tests are deleted, not broken)
3. `grep -r "blob" crates/` — only legitimate uses (e.g., variable names in unrelated code, if any)
4. Review all status-quo specs for stale blob references
