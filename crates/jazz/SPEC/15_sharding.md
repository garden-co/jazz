# jazz — Specification · 15. Sharding

## Overview

Sharding is **exploratory**. This chapter establishes vocabulary, sketches the
intended design shape, and records the questions that must be answered before
shard ownership becomes part of the committed architecture. It does not specify
implemented shard behavior (`INV-SHARD-1`).

Invariant digest:

- `INV-SHARD-1`: Chapter 15 MUST NOT describe sharded core as implemented; conforming current implementations MUST treat sharding as an exploratory/open design area. This is a guidance/process anchor, not runtime conformance.
- `INV-SHARD-7`: A sharded implementation MUST assign every non-global row to a schema-declared shard ownership key and MUST specify behavior for rows without a natural root and rows whose ownership key changes.
- `INV-SHARD-8`: Exclusive transactions MUST be single-shard unless a cross-shard serialization mechanism is explicitly specified.
- `INV-SHARD-9`: A cross-shard exclusive transaction MUST NOT be accepted without validation evidence for every shard touched by its row and predicate read-sets.
- `INV-SHARD-10`: A sharded design MUST retain a global catalogue/sequencer for schema versions, lenses, policy bundles, and the ownership map unless it explicitly specifies an equivalent replacement.
- `INV-SHARD-11`: A sharded design MUST define settled positions as per-shard positions or vectors and MUST specify how at(position) and at_time(t) resolve across shards.
- `INV-SHARD-12`: A multi-shard subscription result MUST NOT be marked complete unless completeness evidence has been obtained for every shard contributing to the result.
- `INV-SHARD-13`: Cross-shard permission closures MUST be obtained through shard-core subscriptions or an explicitly equivalent mechanism before a shard-core assigns a fate that depends on remote-shard policy data.
- `INV-SHARD-14`: Rebalancing MUST NOT flip partition ownership in the catalogue until the destination shard-core has the partition history needed to serve that ownership and the protocol has defined treatment of in-flight fates/subscriptions.

## Details

### 15.1 Storage lineages are not shard ownership

Jazz uses durable `PhysicalTableId` lineages and schema-declared canonical
branch keys for application storage (ch. 10 / ch. 11). These identities
survive reopen and are distinct from shard placement; a branch coordinate is
logical history identity, not a placement assignment.

Shard ownership needs a separate concept. A shard ownership key identifies where
data is placed for authority and routing; it is not the same thing as an
existing physical storage lineage. This chapter therefore uses **shard ownership
partition** explicitly for placement and does not conflate it with physical or
branch-key storage identity.

**Implementation status.** Shard ownership, shard cores, and cross-shard
transactions are not implemented. The current core has one global settlement timeline and
authority model; this chapter therefore specifies safety constraints on any
future sharded design rather than current sharded behavior.

### 15.2 The likely-v1 sketch (not committed)

The following is a candidate design shape, not a committed implementation. It
assigns ordinary data to shard ownership partitions while keeping the globally
shared coordination surface small:

- **Placement.** Every non-global row is assigned to a schema-declared shard
  ownership key, likely a reference path to a root (workspace/org/warehouse).
- **Mergeable authority.** Mergeable transactions require permission
  evaluation rather than total ordering, so per-shard authority follows the same
  shape as edge mergeable authority (ch. 9).
- **Exclusive authority.** **Exclusive transactions are single-shard only**
  unless an explicit cross-shard serialization mechanism is specified. S4's
  per-warehouse cap discipline (appendix B) is exactly this
  single-shard-serialization shape.
- **Shard-cores + a tiny global catalogue.** Each shard-core is the authority for
  its shard ownership partitions. A small global catalogue/sequencer retains
  schema versions, lenses, policy bundles, and the partition-ownership map.
- **Per-shard settle positions.** Each logical shard has its own packed-u64 HLC
  register and committed frontier. Shard identity is routing context derived
  from immutable row identity/ownership values, not bits inside `GlobalTime`.
  Exact multi-shard snapshots carry per-shard `(logical shard, GlobalTime)`
  frontiers, while a future wall-time query may apply one requested physical
  time to every participating shard.
- **Cross-shard via subscriptions.** Shard-core ↔ shard-core subscriptions carry
  permission closures and query assembly; edges subscribe to every shard-core a
  downstream shape touches.
- **Rebalancing is a handoff.** Because history is append-only and self-contained
  _per partition_, moving a partition between shard-cores is "ship its history,
  flip ownership in the catalogue" — no in-place state surgery. Ownership must not
  flip before the new owner can serve it, and in-flight work must drain.

The former sharding sketch's rendezvous/top-2 replication and two-level
subscription ideas are treated as candidate mechanisms for the same open design.
They do not supersede the authority and completeness questions below.

## Open Questions

- 🔶 [#1781](https://github.com/garden-co/jazz/issues/1781) — Sharding design.
