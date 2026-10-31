# jazz — Specification · 15. Sharding

## Overview

Sharding is **exploratory**. This chapter establishes vocabulary, sketches the
intended design shape, and records the questions that must be answered before
shard ownership becomes part of the committed architecture. It does not specify
implemented shard behavior (`INV-SHARD-1`).

Invariant digest:

- `INV-SHARD-1`: Chapter 15 MUST NOT describe sharded core as implemented; conforming current implementations MUST treat sharding as an exploratory/open design area. This is a guidance/process anchor, not runtime conformance.
- `INV-SHARD-7`: A sharded implementation MUST assign every non-global row to a schema-declared shard ownership key and MUST specify behavior for rows without a natural root and rows whose ownership key changes.
- `INV-SHARD-8`: Serializable transactions MUST be single-shard unless a cross-shard serialization mechanism is explicitly specified.
- `INV-SHARD-9`: A cross-shard serializable transaction MUST NOT be accepted without validation evidence for every shard touched by its row and predicate read-sets.
- `INV-SHARD-10`: A sharded design MUST retain a global catalogue/sequencer for schema versions, lenses, policy bundles, and the ownership map unless it explicitly specifies an equivalent replacement.
- `INV-SHARD-11`: A sharded design MUST define settled positions as per-shard positions or vectors and MUST specify how at(position) and at_time(t) resolve across shards.
- `INV-SHARD-12`: A multi-shard subscription result MUST NOT be marked complete unless completeness evidence has been obtained for every shard contributing to the result.
- `INV-SHARD-13`: Cross-shard permission closures MUST be obtained through shard-core subscriptions or an explicitly equivalent mechanism before a shard-core assigns a fate that depends on remote-shard policy data.
- `INV-SHARD-14`: Rebalancing MUST NOT flip partition ownership in the catalogue until the destination shard-core has the partition history needed to serve that ownership and the protocol has defined treatment of in-flight fates/subscriptions.
- `INV-SHARD-15`: A store opened without a clock MUST take no shard role: its positions count up by one, and observing, holding and fixing positions MUST be refused as not supported (§15.3).
- `INV-SHARD-16`: A shard's commit positions MUST be hybrid-clock timestamps: each above every position it handed out or observed and no earlier than its clock; a position more than `HLC_MAX_OFFSET_MS` ahead of its clock MUST be observed only up to its clock plus that offset (§15.3).
- `INV-SHARD-17`: While a hold stands, the position a shard serves as settled MUST stay below its lowest hold (§15.3).
- `INV-SHARD-18`: A fixed cross-shard part MUST land at its fixed position, which is at or above its hold; a part with a row written above that position MUST be refused, not landed under the newer version (§15.3).

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
  evaluation at the owning Core. How this authority composes across shards
  remains exploratory; there is no intermediary authority between relays and
  Core (ch. 9).
- **Serializable authority.** **Serializable transactions are single-shard only**
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
  permission closures and query assembly. Routing a client query to all of its
  owning shard-cores remains part of the future design.
- **Rebalancing is a handoff.** Because history is append-only and self-contained
  _per partition_, moving a partition between shard-cores is "ship its history,
  flip ownership in the catalogue" — no in-place state surgery. Ownership must not
  flip before the new owner can serve it, and in-flight work must drain.

The former sharding sketch's rendezvous/top-2 replication and two-level
subscription ideas are treated as candidate mechanisms for the same open design.
They do not supersede the authority and completeness questions below.

### 15.3 The shard role (engine API)

The jazz engine offers the calls a sharded deployment's gateway makes of the
store serving one shard, as an API with no wire of its own
(`jazz::db::shard`, `Db::shard()`). It is the engine side of the per-query
visibility design (SPEC 8 §8.4): the gateway, its protocol to the shards and
the shard ownership above are outside this repository and stay exploratory
(`INV-SHARD-1`).

- **Clock.** A store opened with `DbConfig::with_clock` takes the shard role.
  Its commit positions are hybrid-clock timestamps: the wall clock's ms since
  `HLC_EPOCH_MS` above `HLC_COUNTER_BITS` bits of counter, so they fit a JS
  number and the positions of every shard compare by time. Each position is
  above every one the store handed out or observed, and no earlier than its
  clock (`INV-SHARD-16`). A store opened without a clock counts positions up
  by one and refuses every shard call below (`INV-SHARD-15`).
- **Observe.** `observe(position)` closes the store at `position`: nothing it
  commits later lands at or below it. A gateway closes a quiet shard so its
  settles reach a position another shard named. A position more than
  `HLC_MAX_OFFSET_MS` ahead of the store's clock is observed only up to the
  clock plus that offset, since that peer's clock (or claim) cannot be
  trusted further; `observe` returns the position it observed, and the caller
  observes the rest once the store's clock has caught up. A `fix` above that
  bound is refused.
- **Hold, fix, release.** A cross-shard commit lands at one position on every
  shard it writes. The gateway `hold`s a position for the part on each shard
  (`CommitRef`, the writer's node and transaction number), then `fix`es each
  part at the highest hold before the part arrives. The part then lands at
  its fixed position, under commits the shard accepted since its hold
  (`INV-SHARD-18`). While a hold stands, `served_through` (what the store
  serves its sessions as settled) stays below `lowest_hold` (`INV-SHARD-17`),
  so no session is told it is settled past a position a part may still take.
  `release` drops a refused commit's hold. Holding a part again returns its
  standing hold, a part is fixed only once it is held, and a resent part that
  already landed holds nothing.
- **Known limit.** A hold does not block other writers of the part's rows yet:
  a part whose row the shard wrote above its fixed position is refused as not
  supported, instead of landing below a newer version of its row.

## Open Questions

- 🔶 [#1781](https://github.com/garden-co/jazz/issues/1781) — Sharding design.
