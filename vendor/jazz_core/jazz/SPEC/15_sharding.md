# jazz — Specification · 15. Sharding

Sharding is **exploratory**. This chapter establishes vocabulary, sketches the
intended design shape, and records the questions that must be answered before
shard ownership becomes part of the committed architecture. It does not specify
implemented shard behavior (`INV-SHARD-1`).

## 15.1 "Partition" is already taken

The term *partition* already has a precise meaning in jazz: it names a physical,
per-logical-table / per-schema-version groove storage table used by migration
lenses (ch. 10) and branch overlays (ch. 11). Those storage partitions are
registered in `jazz_partitions` / `jazz_branch_partitions` and are durable across
reopen.

Shard ownership needs a separate concept. A shard ownership key identifies where
data is placed for authority and routing; it is not the same thing as an
existing storage partition. For that reason, this chapter always distinguishes
**schema-version storage partition** from **shard ownership partition** and does
not use the bare word *partition* for shard placement. The existing partition
machinery is useful analogy and support, but it is not itself shard placement
(its invariants live in ch. 10 / ch. 11).

## 15.2 The likely-v1 sketch (not committed)

The likely first sharded design assigns ordinary data to shard ownership
partitions while keeping the globally shared coordination surface small. The
probable shape is:

- **Placement.** Every non-global row is assigned to a schema-declared shard
  ownership key, likely a reference path to a root (workspace/org/warehouse).
- **Mergeable authority.** Mergeable transactions require permission
  evaluation rather than total ordering, so per-shard authority follows the same
  shape as edge mergeable authority (ch. 9).
- **Exclusive authority.** **Exclusive transactions are single-shard only** in
  v1; cross-shard exclusives are forbidden first. S4's per-warehouse cap
  discipline (appendix B) is exactly this single-shard-serialization shape.
- **Shard-cores + a tiny global catalogue.** Each shard-core is the authority for
  its shard ownership partitions. A small global catalogue/sequencer retains
  schema versions, lenses, policy bundles, and the partition-ownership map.
- **Per-shard settle positions.** Settle streams and snapshots become per-shard
  `(shard, seq)` vectors rather than one `GlobalSeq` line — generalizing the
  `Snapshot`/`GlobalSeq` cuts of ch. 5 and ch. 11.
- **Cross-shard via subscriptions.** Shard-core ↔ shard-core subscriptions carry
  permission closures and query assembly; edges subscribe to every shard-core a
  downstream shape touches.
- **Rebalancing is a handoff.** Because history is append-only and self-contained
  *per partition*, moving a partition between shard-cores is "ship its history,
  flip ownership in the catalogue" — no in-place state surgery. Ownership must not
  flip before the new owner can serve it, and in-flight work must drain.

## Open questions (the actual deliverable)

The whole design is open, and demand should be validated before committing to
it. A possible **S10-shaped benchmark** is a multi-shard scale-out of the S1/S4
workloads, using per-partition rate caps and cross-shard percentage as the dial.
The load-bearing unknowns are:

- 🔶 **Placement model** (`INV-SHARD-7`, open) — ref-path-to-root vs explicit key
  column; rootless/global lookup tables (replicate-everywhere vs a global
  partition class); and what happens when a row's root changes (move /
  cross-shard transfer / forbidden).
- 🔶 **Cross-shard exclusives** (`INV-SHARD-8`, `INV-SHARD-9`; open) — how long they
  stay forbidden and what replaces them (2PC, a global ordering lane,
  deterministic pre-ordering); how a single-shard write validates a predicate
  read-set whose shape spans shards.
- 🔶 **Global catalogue/sequencer** (`INV-SHARD-10`, open) — retained for schema/lens/
  policy/ownership unless explicitly replaced.
- 🔶 **Per-shard positions** (`INV-SHARD-11`, open) — how `at(position)` and
  `at_time(t)` resolve across shards (independent per-shard with documented skew
  vs a cut protocol); how branch bases spanning shards work.
- 🔶 **Multi-shard result assembly** (`INV-SHARD-12`, `INV-SHARD-13`; open) — who
  assembles (edge / coordinator / scatter-gather), where joins/aggregation
  happen, and what completeness evidence composes per-shard result sets;
  permission-closure latency/staleness across shards.
- 🔶 **Rebalancing handoff** (`INV-SHARD-14`, open) — the protocol for open
  subscriptions and pending fates during partition ownership transfer.
- 🔶 **Intra-shard availability** — orthogonal but multiplied: each shard-core
  picks consensus replication or restore-from-durable-log.
- 🔶 The design is sharded authority; the implementation has a singleton global
  core that is history-complete, has exclusive authority, and maintains a single
  `GlobalSeq` line.
