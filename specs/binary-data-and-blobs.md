# Binary Data and Blob Columns

## Implementation Status

**Not yet implemented** - This is a design proposal.

## Problem Statement

Currently, commit contents in objects are binary files first, with DB rows being one kind of encoded data. In practice, we only use rows, and would rather have binary columns that we can stream out of.

The current model:
- Objects store arbitrary binary content
- Rows are just one encoding format for that content
- No first-class support for binary data within row columns

The desired model:
- Rows are the primary unit of data
- Columns can be blobs or lists of blobs
- Blobs are content-addressed chunks that can be streamed

## Design Goals

1. **Blob columns**: Support `BLOB` column type that references content-addressed chunks
2. **Streaming**: Enable streaming reads/writes of blob data without loading entire content
3. **Lists of blobs**: Support `BLOB[]` for multiple attachments per row
4. **Clear ownership**: Blobs strongly belong to one row, simplifying permission semantics

## Proposed Design

### Column Types

```sql
CREATE TABLE documents (
    id,
    title STRING,
    content BLOB,           -- Single blob
    attachments BLOB[]      -- List of blobs
);
```

### Blob Identity and Addressing

Blobs are identified by their content hash, but their full qualified ID combines the owning row's ObjectId with the content hash:

```
<object_id>/<content_hash>
```

This makes ownership explicit and enables permission inheritance.

### Permission Semantics

Blobs inherit permissions from their owning row:
- **Sync permissions**: If a user can sync a row, they can sync its blobs
- **Creation permissions**: If a user can insert/update a row, they can attach blobs to it
- **No standalone access**: Blobs cannot be accessed without going through their row

### Storage Model

Blobs reuse the existing chunked content storage:
- Content > 1KB is automatically chunked (existing `ContentRef::Chunked`)
- Chunks are stored in `ContentStore`
- Row encoding stores a blob reference that internally may be one or many chunk IDs

### Row Encoding

The `BLOB` type is abstract from the user's perspective - they stream bytes in and out. Internally:

- 1 byte: presence flag (for nullable)
- If present: serialized blob reference (either inline bytes for small blobs, or a list of chunk hashes for large blobs)

The chunking is an internal implementation detail. Users always interact with `BLOB` as a streamable byte sequence.

For a `BLOB[]` column:
- 4 bytes: count (u32)
- For each blob: serialized blob reference

### Streaming API

```rust
// Writing a blob
let blob = db.create_blob(&object_id, stream).await?;
db.update(&object_id, |row| {
    row.set("content", blob);
});

// Reading a blob - always streamable regardless of internal chunk count
let stream = db.read_blob(&object_id, "content").await?;
```

## Open Questions

- [ ] Should blobs support partial updates (byte ranges)?
- [ ] How do blob changes affect row commit history? (New blob = new row commit?)
- [ ] Should we support blob deduplication across rows/tables?
- [ ] Migration path from current binary-first model to row-first model
- [ ] Maximum blob size limits (if any)?

## Migration from Current Model

The existing `ContentRef` infrastructure can be reused. Main changes:
1. Add `BLOB` and `BLOB[]` column types to schema
2. Extend row encoding to handle blob references
3. Add streaming APIs for blob read/write
4. Update sync protocol to handle blob chunks with row context
