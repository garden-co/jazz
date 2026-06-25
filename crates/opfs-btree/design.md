# opfs-btree Design

`opfs-btree` is a single-writer, crash-consistent, embedded B+tree key-value
store. It is the cross-platform storage engine used by Jazz core. The same
code runs on native filesystems (`StdFile`), in the browser on top of an OPFS
synchronous access handle (`OpfsFile`), and against an in-memory buffer for
tests (`MemoryFile`).

The public surface is small: open a tree over a file, then `get`, `put`,
`delete`, `range`, and the two durability operations `flush_wal` and
`checkpoint`.

This document describes how the engine works today, to make the code easier to
follow.

## Contents

- [The storage abstraction](#the-storage-abstraction)
- [File format](#file-format)
  - [Overall layout](#overall-layout)
  - [Superblock](#superblock)
  - [Page header](#page-header)
  - [Leaf pages](#leaf-pages)
  - [Internal pages](#internal-pages)
  - [Overflow values and blob extents](#overflow-values-and-blob-extents)
  - [Freelist pages](#freelist-pages)
- [In-memory state](#in-memory-state)
  - [Page cache and eviction](#page-cache-and-eviction)
  - [Free bitmap](#free-bitmap)
  - [Leaf hint cache](#leaf-hint-cache)
- [Operations](#operations)
- [Page allocation](#page-allocation)
- [Durability and crash consistency](#durability-and-crash-consistency)
  - [The two durability operations](#the-two-durability-operations)
  - [WAL format](#wal-format)
  - [Recovery](#recovery)
  - [Checksums](#checksums)
  - [Invariants](#invariants)
- [Concurrency model](#concurrency-model)
- [Format versioning](#format-versioning)

## The storage abstraction

Everything sits on the `SyncFile` trait (`file.rs`):

```rust
pub trait SyncFile {
    fn len(&self) -> Result<u64, BTreeError>;
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError>;
    fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError>;
    fn truncate(&self, len: u64) -> Result<(), BTreeError>;
    fn flush(&self) -> Result<(), BTreeError>;
}
```

The trait is a narrow synchronous read/write/truncate/flush interface, so the
OPFS `FileSystemSyncAccessHandle` — which only offers blocking reads and writes
from inside a Web Worker — is a first-class backend.

`flush` is the durability barrier: it maps to `fsync`/`sync_all` natively and to
the OPFS handle's `flush()` in the browser. The engine assumes that a `flush`
which returns successfully has made all preceding writes durable, and that
writes after a `flush` may be reordered with respect to it only up to the next
`flush`. The correctness of recovery depends only on this barrier, never on the
ordering of individual `write_all_at` calls within a batch.

Three implementations ship:

| Implementation | Where                             | Notes                                                                                                                                                 |
| -------------- | --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OpfsFile`     | wasm32, inside a dedicated Worker | Wraps a `FileSystemSyncAccessHandle`; opens with exponential-backoff retry because a refreshed page's old worker may still hold the exclusive handle. |
| `StdFile`      | native                            | Positional `read_exact_at`/`write_all_at` (`FileExt`), `set_len`, `sync_all`.                                                                         |
| `MemoryFile`   | tests                             | A `Vec<u8>` behind `Rc<RefCell<…>>`.                                                                                                                  |

OPFS charges a high fixed cost per read/write call, so backend-specific tuning is
selected at compile time. For example, WAL writes and replay reads are coalesced
into 64-page runs on wasm32 (where per-call overhead dominates) but issued
per-page natively (where the extra buffer copy would be the larger cost).

## File format

All integers are little-endian. The unit of storage is a fixed-size **page**,
configurable at open time. The page size must be a power of two and at least
4 KiB; the default is 16 KiB.

### Overall layout

```
 page 0      page 1        page 2 ............. (total_pages-1)   total_pages ........ EOF
┌──────────┬──────────┬───────────────────────────────────────┬───────────────────────────┐
│ super-   │ super-   │            home region                 │         WAL tail          │
│ block A  │ block B  │  (B+tree, overflow, freelist pages)    │  (appended commit frames) │
└──────────┴──────────┴───────────────────────────────────────┴───────────────────────────┘
```

- **Pages 0 and 1** are the two superblock slots. They are reserved and never
  allocated to tree data. Page ids therefore start at 2.
- **The home region**, pages `2 .. total_pages`, holds every page at its
  permanent ("home") location. After a checkpoint, the home region reflects the
  committed state exactly.
- **The WAL tail**, pages `total_pages ..`, holds write-ahead-log commit records
  appended past the home region. It exists only between checkpoints and is
  truncated away by the next checkpoint.

`total_pages` (the boundary between the home region and the WAL tail) lives in
the active superblock. The in-memory `persisted_pages` tracks how many pages the
file physically contains, including any WAL tail.

### Superblock

Each slot is one page but only the first ~84 bytes are meaningful
(`superblock.rs`):

| Offset | Size | Field                                     |
| ------ | ---- | ----------------------------------------- |
| 0      | 8    | magic `OPFSBT01`                          |
| 8      | 4    | format version (`1`)                      |
| 12     | 4    | page size                                 |
| 16     | 8    | generation                                |
| 24     | 8    | root page id (`0` = empty tree)           |
| 32     | 8    | freelist head page id (`0` = no freelist) |
| 40     | 8    | total pages                               |
| 48     | 32   | reserved (zeroed)                         |
| 80     | 4    | xxh3 checksum of bytes `0..80`            |

Two slots provide atomic commit. The slot with the **higher generation** that
also passes magic, version, page-size, and checksum validation is the active
one. A checkpoint always writes the _inactive_ slot and flushes; only once that
write is durable does the higher generation make it the active slot. A crash
mid-write leaves the old slot intact and still selected, so the database simply
rolls back to the previous checkpoint.

A brand-new (zero-length) file is bootstrapped by writing an identical
generation-1 superblock to both slots with an empty root and `total_pages = 2`.

### Page header

Every home-region and WAL data page begins with a 24-byte header (`page.rs`):

| Offset | Size | Field                                                     |
| ------ | ---- | --------------------------------------------------------- |
| 0      | 4    | magic `OPPG`                                              |
| 4      | 1    | kind (`1`=internal, `2`=leaf, `3`=overflow, `4`=freelist) |
| 5      | 3    | leaf data-start hint (u24; see [Leaf pages](#leaf-pages)) |
| 8      | 8    | next page id (`0` = none)                                 |
| 16     | 4    | item count                                                |
| 20     | 4    | xxh3 checksum of the whole page (with this field zeroed)  |
| 24     | …    | payload                                                   |

`next page id` chains leaf pages (for range scans), overflow pages, and freelist
pages. The checksum covers the entire page; it is the basis of corruption
detection on every disk read and torn-write detection during WAL replay.

The checksum is **deferred** during in-place mutation: a hot mutation zeroes the
checksum field and the page is marked dirty, and the real checksum is computed
once per dirty page just before it is written (on WAL flush or checkpoint). This
avoids re-hashing a page on every key touched between flushes.

### Leaf pages

Leaf pages use a **slotted-page** layout so that point updates and deletes can
be done in place without re-encoding the page:

```
┌─────────┬──────────────────────────────┬───────────────┬────────────────────┐
│ header  │ slot directory →             │   free space  │           ← data   │
│ 24 B    │ [key_off,key_len,value_off]…  │               │  keys + value cells│
└─────────┴──────────────────────────────┴───────────────┴────────────────────┘
                grows forward                                  grows backward
```

- The **slot directory** grows forward from the start of the payload. Each slot
  is 12 bytes: key offset (u32), key length (u32), value offset (u32). Slots are
  kept sorted by key, so lookups are a binary search over the directory and
  scans are a linear walk.
- **Key and value bytes** grow backward from the end of the payload. The u24
  _data-start hint_ in the header records where the live data region currently
  begins, so an insert can find free space in O(1) instead of scanning every
  slot. The hint is advisory; it is re-derived from the slots whenever it looks
  stale.
- A **value cell** is tagged: tag `0` is an inline value (`u32` length + bytes);
  tag `1` is an overflow reference (`u64` head page id + `u32` total length).

In-place upsert writes the new value into free space (reusing the slot in place
when the value shrinks or fits), shifts the directory to insert a new slot, and
updates the hint. When free space runs out, the page is **compacted** in place
(data re-packed against the end, dead bytes reclaimed). Only if it still does
not fit does the operation report `NeedSplit` and fall back to the recursive
split path. Delete shifts the directory down and re-derives the hint when the
removed entry held the data-region minimum.

Leaf pages are never merged on delete. A leaf can become empty (and is then
freed only if it is the root); otherwise underfull leaves are tolerated. This
keeps deletes cheap and, importantly, makes the leaf-hint cache safe (a leaf's
key span can only shrink-from-the-right via split, never shift via merge).

### Internal pages

Internal pages route a descent to a child. The payload is the leftmost child id
(`u64`) followed by a directory of 16-byte slots, each holding key offset (u32),
key length (u32), and the right child id (u64) for that separator. Keys are
sorted; `internal_child_for_key` binary-searches the directory in a single pass
that validates the slot region's bounds once up front, so each probe is two
loads and a comparison.

### Overflow values and blob extents

Values larger than `overflow_threshold` (default 4 KiB) are not stored inline.
Instead they are written to a contiguous run of **overflow pages** ("a blob
extent"), and the leaf stores only a head-page-id + total-length reference.

Overflow pages are _raw_: the value bytes are copied straight into the page body
with no slot directory and no per-page header parsing on the value path (they
are tracked as `blob_pages` in memory and validated only by length). Because an
extent is a contiguous page run, a large value can be read or written with a
single coalesced I/O call rather than one per page. Values at least 128 KiB are
read directly from disk into the result buffer, bypassing the page cache
entirely, when none of the extent's pages are dirty.

When an existing overflow value is overwritten and the new value needs no more
pages than the old one, the extent is rewritten in place and any trailing pages
are freed; otherwise a fresh extent is allocated and the old one freed.

### Freelist pages

Freed pages are recorded so they can be reused. On disk the freelist is a linked
list of **freelist pages**, each holding an array of `u64` page ids plus a
`next` pointer; the head is named by the superblock. In memory the freelist is
not a list at all but a [bitmap](#free-bitmap); the on-disk pages are just its
serialized form, rebuilt on every flush from the current free set.

## In-memory state

A single `OpfsBTree` owns all mutable state. Nothing here is shared across
threads.

### Page cache and eviction

Pages are decoded lazily and cached as raw bytes in a `HashMap<PageId, Vec<u8>>`.
Reads that miss the cache are served by `read_page_run_from_disk`, which on the
OPFS backend coalesces up to `read_coalesce_pages` consecutive pages into one
read call and validates each before caching it.

The cache is bounded by `cache_bytes`. Eviction is approximate-LRU with a few
refinements:

- **Access recency** is a monotonically increasing epoch stamped on every touch,
  rather than a linked list.
- **Pinned pages are never evicted.** Dirty pages and WAL-resident pages (pages
  whose newest bytes live only in the cache or the WAL tail, not yet at their
  home location) must stay resident — evicting one would let a later miss read a
  stale home-location copy. The root page and the page currently being operated
  on are also protected.
- **Kind-based priority.** When choosing victims, overflow and freelist pages go
  first, then leaves, then internal pages. Internal pages can optionally be
  pinned entirely (`pin_internal_pages`).
- Eviction selects the _k_ worst victims in a single pass using a bounded max
  heap, and the trigger/resting levels are computed from the pinned-page count
  so a cache legitimately full of dirty pages does not rescan on every insert.

### Free bitmap

`FreeBitmap` (`free_bitmap.rs`) is a `Vec<u64>` where set bit _i_ means page _i_
is free, plus an O(1) free count. It supports the two allocation policies
directly:

- `take_highest` for single-page allocation (reuse the highest free id).
- `find_run` / `take_run` for extent allocation (reuse the lowest contiguous run
  long enough), with run clearing done by word-level masking rather than
  bit-by-bit.

Trailing empty words are trimmed so the "highest free id" scan stays tight after
high pages are released.

### Leaf hint cache

`LeafHintCache` (`leaf_hint.rs`) is a 4-slot MRU cache of recently visited
leaves, each remembered with its cached `[first_key, last_key]` span. Point
lookups, the put fast path, and range starts consult it to **skip a
root-to-leaf descent** when a hinted leaf provably covers (or, for ranges,
floors) the key.

The cache is a pure data structure: it never touches the page cache, and a
candidate is always re-validated against the leaf's _live_ bytes before it is
trusted, so a stale span can only cost a wasted check, never a wrong answer.
Multiple slots let a workload that interleaves several key regions (e.g.
separate logical tables in one tree) keep a hint per region. The "is this slot
worth checking?" prefilter is a couple of byte comparisons, so misses are cheap.
WAL replay clears the cache wholesale; freeing a page forgets its slot.

## Operations

- **Lookup** (`get`): try the leaf-hint cache; on a miss, descend from the root
  via `raw_descend_step` (one header parse + binary search per level) to the
  leaf, binary-search the leaf, and resolve inline or overflow values.
- **Insert** (`put`): a fast path upserts an inline-sized value directly into a
  hinted leaf with no descent. Otherwise `insert_recursive` descends, upserts
  in place, and — only when the leaf no longer fits even after compaction —
  splits. Splits propagate upward; a split at the root creates a new root. Split
  points are chosen near the center but adjusted outward until both halves fit.
- **Delete** (`delete`): descend, delete in place. An empty root leaf collapses
  the tree to empty; other underfull leaves are left as-is (no merging).
- **Range scan** (`range`): resolve the starting leaf (hint-aware), then walk the
  leaf chain via each leaf's `next` pointer, emitting `[start, end)` up to a
  limit. A visited-set guards against a corrupt chain that loops.

### Split locality

When a split allocates the new right page, `alloc_page_near` prefers a free page
within a small window of the original, or extends the file if the original was
the last page. Keeping siblings physically close improves the odds that a
range-scan read run picks up the next leaf in the same I/O.

## Page allocation

- Single-page allocation reuses the highest free page id, or extends
  `total_pages` if none is free.
- Extent allocation reuses the lowest contiguous free run of the needed length,
  or extends `total_pages`.
- Splits use the near-allocation policy above.
- Freed pages (deleted overflow extents, collapsed roots, replaced extents, old
  freelist meta-pages) are returned to the bitmap.

The free set is **sanitized** before each serialization: reserved pages (0, 1),
ids at or beyond `total_pages`, and any id that is actually resident/live are
dropped. The freelist meta-pages that host the serialized freelist are taken off
the top of the free set so the lower ids they leave behind remain free.

## Durability and crash consistency

The committed state of the database is whatever the active superblock plus the
durably-flushed WAL tail describe. Two operations advance durability, and they
have different costs and meanings.

### The two durability operations

**`flush_wal` — make the current logical state durable (cheap).** It appends one
commit record (header + frames + commit page) describing all dirty pages to the
WAL tail, then `flush`es the file. It bumps the in-memory generation but **does
not rotate the superblock** and does not touch home locations. After it returns,
the change will survive a crash because recovery will replay the WAL. Dirty
pages become "WAL-resident" (pinned in cache until the next checkpoint).

**`checkpoint` — fold the WAL into the home region (expensive).** It writes every
dirty and WAL-resident page to its **home location**, `flush`es, then writes the
new superblock to the inactive slot (with the new `root`, `freelist_head`, and
`total_pages`) and `flush`es again — this superblock swap is the atomic commit
point — and finally truncates the WAL tail away. Home-location writes are
coalesced into runs of consecutive page ids. After a checkpoint the home region
is self-contained and the WAL is empty.

This separation lets a hot write path commit durably many times via cheap WAL
appends, and amortize the cost of rewriting home locations across an occasional
checkpoint.

### WAL format

A commit record is a run of pages appended at `persisted_pages` (`wal.rs`):

```
┌────────────┬───────────┬───────────┬───────────┬───────────┬─────┬────────────┐
│ header     │ frame[0]  │ frame[0]  │ frame[1]  │ frame[1]  │ ... │ commit     │
│ page       │ meta page │ data page │ meta page │ data page │     │ page       │
└────────────┴───────────┴───────────┴───────────┴───────────┴─────┴────────────┘
```

- The **header page** (magic `OPFSWJ01`) carries the generation, the new root /
  freelist-head / total-pages, and the frame count, with its own checksum.
- Each frame is **two pages**: a **meta page** (magic `OPFSWF01`) with the target
  page id, blob/freelist flags, and the checksum of the data page; followed by
  the **data page** itself (the verbatim home-page image).
- The **commit page** (magic `OPFSWC01`) repeats the generation and frame count
  with a checksum. Its presence and validity is what makes the record atomic: a
  record is only applied during replay if the header, every frame, _and_ the
  commit page all validate.

A commit of _n_ frames is therefore `2 + 2n` pages.

### Recovery

`OpfsBTree::open` reconstructs state:

1. Read both superblock slots; pick the higher-generation slot that validates.
   A zero-length file is bootstrapped instead. A non-empty file with no valid
   superblock is reported as corrupt.
2. Load the root page (validating its kind) and the on-disk freelist into the
   bitmap, then sanitize the free set.
3. **Replay the WAL.** Starting at the superblock's `total_pages` (the home/WAL
   boundary), read commit records in order. Each fully-valid record is applied:
   its frames are installed into the cache as WAL-resident pages, and `root`,
   `freelist_head`, and `total_pages` are advanced to the record's header. The
   first record that fails to validate — a torn or partial tail from a crash
   mid-`flush_wal` — stops replay, and the tail from that point is **truncated**.

The atomicity argument: a `flush_wal` is durable only after its `flush` returns,
and the commit page is written last within the record. A crash before the
`flush` completes can leave a record whose commit page is missing or whose data
fails its checksum; replay detects this and discards the partial record,
rolling back to the last fully-committed state. A crash during a `checkpoint`
either leaves the old superblock selected (rollback to the previous checkpoint,
WAL still present and replayable) or, once the new superblock is durable, leaves
the new home region authoritative with a now-redundant tail that the truncate
step (or a future checkpoint) removes.

### Checksums

All integrity checks use xxh3, truncated to its low 32 bits (`checksum.rs`).
xxh3 is chosen for speed, not cryptographic strength; the checksums guard
against corruption and torn writes, not adversarial tampering. A streaming
`Hasher` is available, but pages are small enough that the one-shot path is the
common case. Every page read from disk is checksum-validated; WAL data pages are
validated against the checksum recorded in their meta page.

### Invariants

- A page at its home location, in the committed state, is never partially
  overwritten in place in a way that could be observed after a crash without a
  superblock rotation — new data lands in the WAL tail or in newly allocated
  pages first, and the home region is only rewritten _before_ the superblock
  that names the new `total_pages` is made durable.
- Reserved pages 0 and 1 are never written as data pages
  (`validate_writable_page_id`).
- The leaf chain is acyclic; range scans and freelist loads defensively detect
  cycles and report corruption rather than looping.
- `flush` is a true durability barrier (see [The storage abstraction](#the-storage-abstraction)).
  The engine does not depend on the relative order of writes within a batch,
  only on barriers.

## Concurrency model

There is **one** writer and **no** concurrent readers. `OpfsBTree` holds
`&mut self` for every operation, including `get` (which may load and cache
pages). State is held behind `Rc`/`RefCell`, not `Arc`/`Mutex` — the engine is
deliberately not `Send`/`Sync`. On the browser this matches reality: the OPFS
sync access handle is exclusive to one worker. Coordination with other tabs or
threads, if any, happens above this layer.

Consequently there is no MVCC, no read transaction, no savepoint, and no
page-reclamation epoch machinery: operations never interleave.

## Format versioning

The superblock carries a `format version`, currently `1`. A superblock whose
version does not match is rejected as corrupt rather than migrated. The page,
WAL header, WAL frame, and WAL commit structures each carry their own magic
strings (`OPPG`, `OPFSWJ01`, `OPFSWF01`, `OPFSWC01`) and, for the WAL, an
embedded format version, so a future on-disk change can be detected
unambiguously at each layer.
