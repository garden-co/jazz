# Batch Branches and Prefix-Indexed Storage — TODO (Launch)

Replace today's "one branch with many concurrent tips" model with many linear batches under a shared branch prefix.

This is a precursor to the transactions-first MVP and also a storage redesign for the case where a single object may accumulate millions of branches over time.

Related:

- [Object Manager](../../status-quo/object_manager.md)
- [Storage](../../status-quo/storage.md)
- [HTTP/SSE Transport Protocol](../../status-quo/http_transport.md)
- [Transactions-first system design (MVP)](../a_mvp/transactions_first_system_design.md)
- [Transactions-first system design (later stages)](../c_later/transactions_first_system_design.md)
- [Storage Compression Strategy](./storage_compression_strategy.md)

## Summary

Today an object branch can have multiple concurrent tips ("twigs") inside one branch DAG.

Proposed model:

- Each write batch gets its own branch.
- Branch histories are internally linear after their first commit.
- A full branch ID becomes `env-schemaHash-userBranch-batchId`.
- The shared prefix `env-schemaHash-userBranch-` is the unit of causal convergence.
- Starting a new batch creates a root merge commit whose parents are the known leaf heads in that prefix.

This moves concurrency from "many tips inside one branch" to "many batches under one prefix".

The storage and memory model should then optimize for:

- very many batches per object
- cheap cold load of batch metadata without loading all commits
- append-heavy writes with low IOPS cost
- fast retrieval of "heads in a prefix that do not have child batches"

## Goals

- Eliminate ambiguous multi-tip semantics inside a branch.
- Preserve git-like causality for object history.
- Make millions of historical batches per object practical.
- Optimize for IOPS-bound storage, not maximum raw throughput.
- Support delta-compressed commit/snapshot storage.
- Support stream compression on the wire.
- Make this query the fastest path:

`set of branch heads within a env-schemaHash-userBranch- prefix that do not have child branches`

## Non-Goals

- Full globally consistent transaction semantics. This proposal only makes them easier to build.
- Solving every pathological "millions of concurrent leaf writers" case in v1.
- Preserving the current internal representation of `BranchName` strings in hot paths.

## Proposed Model

### 1. Prefixes and Batches

Split branch identity into:

- `BranchPrefix = (env, schema_hash, user_branch)`
- `BatchId = device-generated identifier for one batch of writes`

Externally, the full branch ID remains string-shaped:

`env-schemaHash-userBranch-batchId`

Internally, treat the prefix and batch as separate fields. Do not key hot-path memory or storage structures by the full concatenated string.

### 2. Batch History Shape

Each batch is a linear commit history:

`root_merge -> c1 -> c2 -> c3`

The first commit is special:

- it starts a new batch
- it merges the currently known leaf heads in the same prefix

Every later commit in the batch has exactly one parent: the previous commit in that batch.

Using the known leaf heads is enough to capture the full known prefix frontier: every non-leaf commit is already reachable from some leaf head, so adding all historical commits as direct parents would add cost without adding causal information.

### 3. Concurrency Semantics

Concurrent editing no longer means "multiple tips in one branch". It means "multiple batches exist in the same prefix".

When a new batch starts, the current leaf heads it references stop being leaves once the new batch is persisted. The new batch head becomes a leaf.

This gives us a direct mapping:

- old model: concurrent tips
- new model: concurrent batches

## Identifier Strategy

Use three identifiers:

- `BranchPrefixId`: interned ID for `(env, schema_hash, user_branch)`
- `BatchId`: globally unique, time-sortable 128-bit ID
- `BatchOrd`: dense per-`(object, prefix)` ordinal assigned on first sight

### BatchId

Use a UUIDv7-style 128-bit ID or equivalent monotonic time-sortable format.

Rationale:

- new batches insert in roughly time order, which helps disk locality
- IDs remain unique across devices without coordination
- the text form can still be embedded into the public branch string

Prefer a binary canonical form in storage and memory, with a compact text encoding only at API boundaries.

### BatchOrd

`BatchOrd` is the hot-path identifier used in manifests, bitmaps, and parent lists.

Rationale:

- much smaller than storing full batch strings repeatedly
- faster for bitmap/set operations
- enables dense packed arrays and varint parent encodings

## Disk Layout

The disk layout should be prefix-centric, not commit-key-scan-centric.

### Per-Object Layout

For each object, store:

- object metadata
- prefix catalog
- per-prefix batch catalog
- per-prefix leaf index
- per-batch commit segments
- optional per-batch head snapshot references

### Prefix Catalog

The prefix catalog maps `BranchPrefixId` to the prefixes present on the object.

This allows cold-loading "what prefixes exist here?" without scanning every commit key.

### Batch Catalog

For each `(object, prefix)`, store a packed `BatchMeta` table keyed by `BatchOrd`:

- `batch_id`
- `root_commit_id`
- `head_commit_id`
- `first_timestamp`
- `last_timestamp`
- `parent_range`
- `head_snapshot_ref` or inline small snapshot
- `tail_segment_ptr`
- flags (`sealed`, `truncated`, etc.)

Parent batch references should live in a shared varint-packed arena:

- `ParentBatchOrdArena`

Each batch stores only a range into that arena.

### Leaf Index

For each `(object, prefix)`, maintain the set of batch ordinals whose heads have no child batches:

- roaring bitmap, bitset, or similarly compact leaf set
- optional `child_count` per batch for easy maintenance

This is the primary read index for the hot query.

### Commit Segments

Do not store one KV entry per commit for the hot path.

Instead, store append-oriented per-batch segments, for example:

- one tail segment for the current writable batch
- sealed immutable segments once they reach a target size

Suggested encoding:

- segment header
- first record is a checkpoint snapshot or full row image
- later records are delta-compressed against prior state in that batch

Because the storage target is IOPS-bound, fewer larger reads are better than many tiny point reads.

### Compression

Use a two-tier compression strategy:

- hot writable tail segments: LZ4 or light zstd
- sealed cold segments: zstd

This keeps write amplification low while still getting good long-term density.

## Snapshot Layout

The main read optimization is to avoid replaying an entire batch just to read its head.

Store or derive:

- a current head snapshot per batch
- periodic base snapshots inside sealed segments
- deltas between snapshots for intermediate commits

Possible implementation options:

- inline very small head snapshots in `BatchMeta`
- store head snapshots in a shared snapshot blob store with content addressing
- intern repeated field names and repeated scalar blobs

The exact snapshot codec can evolve later as long as `head_commit_id` and `head_snapshot_ref` stay cheap to load.

## Wire Layout

The current transport is binary framing around JSON payloads. For batch-heavy sync, move toward binary records plus stream compression.

Suggested wire properties:

- stream-level zstd compression for long-running sync sessions
- connection dictionaries for repeated `object_id`, `prefix_id`, `author`, and `batch_ref` values
- record types such as `new_batch`, `append_commits`, `batch_truncate`, `snapshot_blob`, `leaf_delta`

Stream compression matters more than per-message compression because the same prefixes, authors, and object IDs repeat heavily during sync.

## In-Memory Layout

Keep only prefix manifests and active leaf/head state hot by default.

Suggested structures:

- interned `BranchPrefixId`
- slab or struct-of-arrays storage for `BatchMeta`
- roaring bitmap or dense bitset for leaf batches
- compressed commit segments kept compressed in cache until needed
- decompressed cache only for writable tail segments and recently-read head snapshots

Useful interning opportunities:

- prefix components
- authors
- metadata keys
- repeated snapshot field names
- repeated small immutable values

The key memory rule is: loading an object with many historical batches must not require materializing every commit node.

## Fast Path: Leaf Heads by Prefix

This query should be the best-supported path in the system:

`leaf_heads(object_id, branch_prefix)`

Implementation target:

1. load the prefix manifest
2. load the leaf bitmap for that prefix
3. map each `BatchOrd` to `head_commit_id` and optionally `head_snapshot_ref`

That should be enough to answer the query without:

- scanning all commits
- walking ancestry
- decoding old sealed segments
- loading unrelated prefixes

Time complexity should be proportional to the number of leaf batches, not the total historical batch count.

## Write Path

Starting a batch:

1. allocate `BatchId`
2. look up current leaf set for the prefix
3. create the root merge commit from those known leaf heads
4. allocate `BatchOrd`
5. append `BatchMeta`
6. remove parent batches from the leaf set
7. insert the new batch into the leaf set

Appending within a batch:

- append to the writable tail segment
- update `head_commit_id`, `last_timestamp`, and optional inline head snapshot

Sealing:

- when a tail segment crosses a size threshold, compress and seal it

## Interaction with Transactions-first MVP

This model gives a natural frontier for mixed direct and explicit-transaction writes:

1. direct writes on `Optional` tables are ordinary general-branch batches
2. explicit txs write intents on tx-private branches such as `tx/<tx_id>`
3. the root merge commit of a batch captures the prefix frontier the writer observed

Later, the tx authority can validate a candidate explicit tx against that captured frontier, including both direct writes and prior accepted tx merges, and then:

1. accept it by materializing authoritative general-branch merge commits tagged with `tx_id`
2. reject it and force retry on a newer frontier
3. reject direct writes entirely for tables marked `Required`

In other words, this proposal does not implement `TxDecision` or tx visibility gating, but it creates the shared object-history substrate for both direct writes and tx-backed writes.

## Migration Direction

A practical migration path:

1. introduce the `prefix + batch` naming model at the API boundary
2. treat current concurrent tips as separate batches during load/sync translation
3. add the leaf index and prefix manifest
4. move commit persistence from per-commit entries to per-batch segments
5. switch wire sync to binary record streams with dictionaries and compression

This allows incremental adoption instead of a single all-at-once rewrite.

## Open Questions

- Exact text encoding for `BatchId` in public branch names.
- Whether `BatchOrd` should be stable forever or allow periodic compaction/reassignment.
- Whether parent batch refs should be stored as `BatchOrd`, `CommitId`, or both.
- How aggressively to inline tiny head snapshots in `BatchMeta`.
- Thresholds for sealing segments and for snapshot checkpoints.
- Whether we need a separate compact summary structure when a prefix frontier becomes unusually wide.
