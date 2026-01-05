# Sorted Chunk Indices

This spec describes a scalable index structure that supports indices larger than memory through sorted, chunked storage with binary search access.

## Problem

The current `RefIndex` stores all entries in a single `HashMap<ObjectId, HashSet<ObjectId>>`:
- Must deserialize the entire index to look up one key
- Panics when serialized size exceeds `INLINE_THRESHOLD` (1KB)
- Cannot scale to large datasets

## Goals

1. **Scale beyond memory**: Only load chunks needed for a query
2. **Efficient lookups**: O(log N) to find the right chunk
3. **Incremental updates**: Modify only affected chunks
4. **Query graph integration**: Work with sync evaluation via caching
5. **Future range queries**: Enable ORDER BY, WHERE col > X, etc.

## Data Structure

### Index Root Object

Each index has a root object (small, always loadable) containing:

```rust
struct IndexRoot {
    /// Number of entries across all chunks
    entry_count: u64,

    /// First key in each chunk (for binary search)
    /// boundaries[i] = first key in chunk i
    boundaries: Vec<IndexKey>,

    /// Content hashes for each chunk (for loading from ContentStore)
    chunk_hashes: Vec<ChunkHash>,

    /// Target entries per chunk (for split/merge decisions)
    target_chunk_size: usize,  // e.g., 64KB worth of entries
}

/// Key for sorted index entries
struct IndexKey {
    target_id: ObjectId,
    source_id: ObjectId,
}
```

### Index Chunks

Each chunk is stored in the `ContentStore` (content-addressed by hash):

```rust
struct IndexChunk {
    /// Sorted entries: (target_id, source_id) pairs
    /// Sorted by target_id (primary), then source_id (secondary)
    entries: Vec<(ObjectId, ObjectId)>,
}
```

Binary layout per entry: 16 bytes (target) + 16 bytes (source) = 32 bytes.
A 64KB chunk holds ~2000 entries.

### Visual Structure

```
Index Root Object (small, inline)
┌─────────────────────────────────────────────────────────┐
│ entry_count: 50000                                      │
│ boundaries: [(T_0, S_0), (T_100, S_x), (T_200, S_y)...] │
│ chunk_hashes: [hash_0, hash_1, hash_2, ...]             │
└─────────────────────────────────────────────────────────┘
           │              │              │
           ▼              ▼              ▼
      ┌─────────┐    ┌─────────┐    ┌─────────┐
      │ Chunk 0 │    │ Chunk 1 │    │ Chunk 2 │
      │ T_0-T_99│    │T_100-199│    │T_200-299│
      │ ~2000   │    │ ~2000   │    │ ~2000   │
      │ entries │    │ entries │    │ entries │
      └─────────┘    └─────────┘    └─────────┘
       (in ContentStore, loaded on demand)
```

## Operations

### Lookup: find_referencing(target_id)

```rust
async fn find_referencing(
    &self,
    target_id: ObjectId,
    root: &IndexRoot,
    store: &dyn ContentStore,
    cache: &ChunkCache,
) -> Vec<ObjectId> {
    // 1. Binary search boundaries to find chunk index (sync, no I/O)
    let chunk_idx = root.boundaries
        .binary_search_by(|key| key.target_id.cmp(&target_id))
        .unwrap_or_else(|i| i.saturating_sub(1));

    // 2. Load chunk if not cached (async)
    let chunk = cache
        .get_or_load(root.chunk_hashes[chunk_idx], store)
        .await;

    // 3. Binary search within chunk for first entry with target_id (sync)
    let start = chunk.entries
        .binary_search_by(|(t, _)| t.cmp(&target_id))
        .unwrap_or_else(|i| i);

    // 4. Collect all entries with matching target_id
    chunk.entries[start..]
        .iter()
        .take_while(|(t, _)| *t == target_id)
        .map(|(_, s)| *s)
        .collect()
}
```

**Complexity**: O(log C) to find chunk + O(log E) within chunk, where C = chunks, E = entries per chunk.

### Insert: add_reference(target_id, source_id)

```rust
async fn add_reference(
    &self,
    target_id: ObjectId,
    source_id: ObjectId,
    root: &mut IndexRoot,
    store: &dyn ContentStore,
    cache: &ChunkCache,
) {
    let key = IndexKey { target_id, source_id };

    // 1. Find chunk
    let chunk_idx = find_chunk_for_key(&root.boundaries, &key);

    // 2. Load and modify chunk
    let mut chunk = cache.get_or_load(root.chunk_hashes[chunk_idx], store).await;
    let insert_pos = chunk.entries.binary_search(&(target_id, source_id))
        .unwrap_or_else(|i| i);
    chunk.entries.insert(insert_pos, (target_id, source_id));

    // 3. Check if chunk needs splitting
    if chunk.entries.len() > root.target_chunk_size * 2 {
        let (left, right) = split_chunk(chunk);
        // Update root with new chunk and boundary
        // ... (details below)
    }

    // 4. Write modified chunk(s) to store
    let new_hash = store.put_chunk(chunk.to_bytes()).await;
    root.chunk_hashes[chunk_idx] = new_hash;
    root.entry_count += 1;

    // 5. Persist updated root
    write_root(root).await;
}
```

### Delete: remove_reference(target_id, source_id)

Similar to insert, but removes entry and potentially merges undersized chunks.

### Chunk Splitting

When a chunk exceeds `2 * target_chunk_size`:

```rust
fn split_chunk(chunk: IndexChunk) -> (IndexChunk, IndexChunk) {
    let mid = chunk.entries.len() / 2;
    let (left, right) = chunk.entries.split_at(mid);
    (
        IndexChunk { entries: left.to_vec() },
        IndexChunk { entries: right.to_vec() },
    )
}
```

Update root:
- Insert new boundary at split point
- Insert new chunk hash
- Update existing chunk hash

### Chunk Merging

When a chunk falls below `target_chunk_size / 2`, merge with neighbor if combined size allows.

## Query Graph Integration

The key challenge: the query graph evaluates synchronously, but chunk loading is async.

### Chunk Cache

```rust
struct ChunkCache {
    /// Loaded chunks by hash
    chunks: HashMap<ChunkHash, IndexChunk>,

    /// LRU tracking for eviction
    lru: LruList<ChunkHash>,

    /// Maximum cache size
    max_size: usize,
}

impl ChunkCache {
    /// Sync access - returns None if not cached
    fn get(&self, hash: &ChunkHash) -> Option<&IndexChunk> {
        self.chunks.get(hash)
    }

    /// Async load - loads if not cached
    async fn get_or_load(
        &self,
        hash: ChunkHash,
        store: &dyn ContentStore,
    ) -> &IndexChunk {
        if !self.chunks.contains_key(&hash) {
            let data = store.get_chunk(&hash).await.unwrap();
            let chunk = IndexChunk::from_bytes(&data);
            self.insert(hash, chunk);
        }
        self.chunks.get(&hash).unwrap()
    }
}
```

### Two-Phase Delta Evaluation

```rust
impl QueryGraph {
    async fn apply_delta(&mut self, delta: DeltaBatch, db: &Database, store: &dyn ContentStore) {
        // Phase 1: Analyze which chunks are needed (sync)
        let needed_chunks = self.analyze_needed_chunks(&delta, db);

        // Phase 2: Ensure all needed chunks are loaded (async)
        for (index_key, chunk_hash) in needed_chunks {
            if !self.chunk_cache.has(&chunk_hash) {
                self.chunk_cache.load(chunk_hash, store).await;
            }
        }

        // Phase 3: Evaluate with all chunks available (sync)
        self.evaluate_sync(&delta, db, &self.chunk_cache);
    }

    /// Determine which index chunks will be needed for this delta
    fn analyze_needed_chunks(&self, delta: &DeltaBatch, db: &Database) -> Vec<(IndexKey, ChunkHash)> {
        let mut needed = Vec::new();

        for node in &self.nodes {
            match node {
                QueryNode::Join { ref_column, .. } |
                QueryNode::ArrayAggregate { inner_ref_column, .. } => {
                    // Extract target_ids from delta that will trigger index lookups
                    for row_delta in delta.iter() {
                        if let Some(target_id) = extract_ref_value(row_delta, ref_column) {
                            let root = db.get_index_root(ref_column);
                            let chunk_idx = find_chunk_for_target(&root.boundaries, target_id);
                            needed.push((ref_column.clone(), root.chunk_hashes[chunk_idx]));
                        }
                    }
                }
                _ => {}
            }
        }

        needed.sort();
        needed.dedup();
        needed
    }
}
```

### Sync Evaluation with Cache

```rust
fn evaluate_sync(&self, delta: DeltaBatch, db: &Database, cache: &ChunkCache) -> DeltaBatch {
    // ... existing evaluation logic ...

    // When find_referencing is needed:
    let find_ref = |table: &str, column: &str, target_id: ObjectId| -> Vec<Row> {
        let root = db.get_index_root(table, column);
        let chunk_idx = find_chunk_for_target(&root.boundaries, target_id);
        let chunk_hash = root.chunk_hashes[chunk_idx];

        // This is guaranteed to be cached because of Phase 2
        let chunk = cache.get(&chunk_hash).expect("chunk should be pre-loaded");

        // Binary search within chunk
        lookup_in_chunk(chunk, target_id)
            .map(|source_id| db.get_row(table, source_id))
            .collect()
    };

    // ... continue evaluation ...
}
```

### Incremental Chunk Updates

When an index chunk changes (due to insert/update/delete), the query graph can handle it incrementally:

```rust
/// Called when a chunk's content changes
fn on_chunk_updated(
    &mut self,
    index_key: &IndexKey,
    old_chunk: &IndexChunk,
    new_chunk: &IndexChunk,
) {
    // Compute diff
    let added: Vec<_> = new_chunk.entries.iter()
        .filter(|e| !old_chunk.entries.contains(e))
        .collect();
    let removed: Vec<_> = old_chunk.entries.iter()
        .filter(|e| !new_chunk.entries.contains(e))
        .collect();

    // Convert to deltas and propagate through graph
    for (target_id, source_id) in added {
        // This is like a new reference being added
        self.propagate_index_delta(index_key, target_id, source_id, DeltaType::Add);
    }
    for (target_id, source_id) in removed {
        self.propagate_index_delta(index_key, target_id, source_id, DeltaType::Remove);
    }
}
```

## Chunk Boundaries in Incremental Computation

The chunk boundaries themselves can be leveraged:

```rust
/// A query graph can "subscribe" to specific chunk ranges
struct ChunkSubscription {
    index_key: IndexKey,
    /// Range of target_ids this subscription covers
    target_range: (ObjectId, ObjectId),
    /// Chunk hashes currently loaded for this range
    loaded_chunks: Vec<ChunkHash>,
}
```

When a delta arrives:
1. Check if any subscribed range covers the delta's target_ids
2. If yes and chunk is loaded: evaluate immediately
3. If yes but chunk not loaded: queue delta, load chunk, then evaluate
4. If no subscription covers it: create new subscription, load chunk

This enables **lazy loading**: only load chunks for data patterns actually accessed.

## Migration from Current RefIndex

### Phase 1: Parallel Operation
- Keep current `RefIndex` working
- Build sorted chunk index alongside
- Verify consistency between them

### Phase 2: Switchover
- Route lookups to sorted chunk index
- Keep `RefIndex` as fallback

### Phase 3: Cleanup
- Remove old `RefIndex` code
- Migrate existing data

## Performance Characteristics

| Operation | Current RefIndex | Sorted Chunk Index |
|-----------|------------------|-------------------|
| Lookup | O(1) hash + deserialize all | O(log C + log E) + load 1 chunk |
| Insert | Deserialize all, modify, serialize all | Load 1 chunk, modify, write 1 chunk |
| Memory | Must fit entire index | Only cached chunks |
| Sync | Entire index per sync | Only changed chunks |

Where C = number of chunks, E = entries per chunk.

## Future Extensions

### Range Queries

```sql
SELECT * FROM issues WHERE project > 'P100' ORDER BY project
```

With sorted chunks:
1. Binary search for starting chunk
2. Scan chunks in order
3. Stop when past range end

### Composite Indices

For multi-column indices, extend `IndexKey`:
```rust
struct IndexKey {
    values: Vec<Value>,  // Multiple column values
}
```

Sort lexicographically, same chunk structure applies.

### Covering Indices

Store additional column values in index entries to avoid row lookups:
```rust
struct IndexEntry {
    key: IndexKey,
    source_id: ObjectId,
    covered_values: Vec<Value>,  // Cached column values
}
```

## Implementation Plan

1. **Define data structures**: `IndexRoot`, `IndexChunk`, serialization
2. **Implement basic operations**: lookup, insert, delete with splitting
3. **Add ChunkCache**: LRU cache with sync/async access
4. **Integrate with query graph**: Two-phase evaluation
5. **Add incremental chunk updates**: Delta propagation for chunk changes
6. **Migration**: Parallel operation, switchover, cleanup
7. **Testing**: Scale tests with indices larger than memory

## Open Questions

1. **Chunk size tuning**: What's the optimal chunk size? Tradeoff between I/O granularity and binary search depth.
2. **Cache eviction policy**: LRU? LFU? Workload-dependent?
3. **Concurrent modifications**: How to handle multiple writers to the same chunk?
4. **Transaction integration**: How do chunk updates participate in multi-row transactions?

## Relationship to Transactions

When multi-row transactions are implemented, index updates should be:
- **Buffered**: Index changes are held in transaction-local state
- **Applied atomically**: On commit, all index chunks are updated together
- **Rolled back**: On abort, buffered changes are discarded

This ensures index consistency with table data within a transaction boundary.
