# Streaming and Persistence Design

## Problem Statement

Current implementation assumes all commits and content fit in memory. This breaks for:
- Large files (GB+)
- Long history (millions of commits)
- Memory-constrained devices (mobile, browser)

## Design Decisions

### Object Loading States

An object can be in one of four states:

1. **Metadata only**: Just know the object exists, its ID, type prefix, frontier commit IDs
2. **Frontier loaded**: Latest state (frontier commits + their content) in memory, history on disk/remote
3. **Partial history**: Some commits in memory, rest on demand
4. **Fully loaded**: Everything in memory (current behavior, for small objects)

### Content Storage Architecture

Separate commit graph structure from content:

```
Commit {
    parents: Vec<CommitId>,
    content: ContentRef,  // Either inline or hash reference
    author: String,
    timestamp: u64,
    meta: Option<...>,
}

enum ContentRef {
    Inline(Box<[u8]>),        // For small content (<= ~100 bytes)
    Chunked(Vec<ChunkHash>),  // For larger content
}
```

**Inline threshold**: 1KB. Below this, store content directly to avoid hash lookup overhead. Most simple table rows (single record) will be inline.

### Content-Defined Chunking (CDC)

For large content, use CDC to split into chunks. This enables:
- Deduplication across similar content
- Streaming within a single snapshot
- Efficient delta sync

#### How Rolling Hash CDC Works

Basic principle: compute a rolling hash at each byte position, emit a chunk boundary when `hash % divisor == 0`.

```rust
// Simplified CDC
for each byte position:
    rolling_hash = update_hash(rolling_hash, byte)
    if rolling_hash % divisor == 0:
        emit chunk boundary
```

The divisor controls average chunk size (e.g., divisor=8192 → ~8KB average chunks).

#### Algorithm Options

| Algorithm | Speed | Dedup Quality | Notes |
|-----------|-------|---------------|-------|
| Rabin fingerprint | ~100 MB/s | Excellent | Polynomial division, slow |
| Gear hash | ~1 GB/s+ | Good | Lookup table + XOR/shift |
| FastCDC | ~1 GB/s+ | Excellent | Gear + optimizations |

**Recommendation**: FastCDC (Gear-based with optimizations).

Key FastCDC optimizations:
1. **Gear hash**: Just table lookup + XOR + shift (very fast)
2. **Cut-point skipping**: Don't compute hash for first `min_chunk_size` bytes (~2x speedup)
3. **Normalized distribution**: Tighter chunk size variance

```rust
// FastCDC pseudocode
fn chunk(data: &[u8], min: usize, avg: usize, max: usize) -> Vec<Range> {
    let mask = avg - 1;  // Power-of-2 average sizes
    let mut pos = 0;

    while pos < data.len() {
        let start = pos;
        pos += min;  // Skip minimum - no hash computation needed

        let mut hash = 0u64;
        while pos < start + max && pos < data.len() {
            hash = (hash >> 1) + GEAR_TABLE[data[pos] as usize];
            if hash & mask == 0 { break; }  // Found boundary
            pos += 1;
        }
        emit_chunk(start..pos);
    }
}
```

#### Chunk Size Parameters

- **Minimum**: 2KB-4KB (prevents tiny chunks)
- **Average**: Configurable per object (stored in object metadata if non-default)
- **Maximum**: 4x average (bounds worst case)

Default average: 8KB (good for most structured data).
For large binary files: 32-64KB (set on object creation).

### Storage Interface

```rust
trait ContentStore {
    /// Get chunk by hash, returns None if not found
    async fn get_chunk(&self, hash: &ChunkHash) -> Option<Vec<u8>>;

    /// Store chunk, returns its hash
    async fn put_chunk(&self, data: &[u8]) -> ChunkHash;

    /// Check if chunk exists
    async fn has_chunk(&self, hash: &ChunkHash) -> bool;

    /// Stream chunks for content
    fn stream_content(&self, chunks: &[ChunkHash]) -> impl Stream<Item = Vec<u8>>;
}

trait CommitStore {
    /// Get commit metadata (without content)
    async fn get_commit_meta(&self, id: &CommitId) -> Option<CommitMeta>;

    /// Store commit
    async fn put_commit(&self, commit: &Commit);

    /// Get frontier for a branch
    async fn get_frontier(&self, object_id: u128, branch: &str) -> Vec<CommitId>;

    /// List commits (for partial loading)
    fn list_commits(&self, object_id: u128, branch: &str) -> impl Stream<Item = CommitId>;
}
```

### Sync Implications

Default sync behavior:
- **Frontier commits**: Full sync (metadata + content)
- **Historical commits**: Metadata only, content on demand

This optimizes for the common case (latest state) while allowing history access when needed.

### Implications for Current Abstractions

`Commit` changes:
```rust
struct Commit {
    parents: Vec<CommitId>,
    content: ContentRef,      // Was: Box<[u8]>
    author: String,
    timestamp: u64,
    meta: Option<BTreeMap<String, String>>,
}
```

`Branch` becomes lighter:
- Stores frontier only in memory by default
- Queries commits on demand from CommitStore
- Can cache recently accessed commits

`Object` loading modes:
- Constructor specifies desired loading level
- Can upgrade (load more) or downgrade (evict) as needed

### Loading State Transitions

- **Implicit loading**: Access triggers lazy load (no explicit `load_full()` calls)
- **Global memory budget**: System auto-evicts objects to lower loading states under memory pressure
- **LRU-based eviction**: Least recently accessed objects evicted first

## Open Questions

1. **Cache eviction details**: Pure LRU, or weighted by object size?

2. **Prefetching**: When streaming, should we prefetch next chunks?

3. **Compression**: Compress before or after chunking? (After is more common - chunks compress independently)

## Sources

- [FastCDC Paper (USENIX ATC'16)](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia)
- [Intro to Content-Defined Chunking](https://joshleeb.com/posts/content-defined-chunking.html)
- [Rabin CDC Implementation](https://github.com/fd0/rabin-cdc)
