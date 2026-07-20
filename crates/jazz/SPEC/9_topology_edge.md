# jazz — Specification · 9. Topology & the edge tier

## Overview

Tiers in jazz are roles within the single sync protocol defined in ch. 8. They
are distinguished by trust: which node may assign fates, enforce permissions, and
stand behind durability. This chapter defines that trust ladder and the topology
that follows from it, building on transactions (ch. 3), merging (ch. 4), and sync
(ch. 8).

Fate authority is a host-wired role, not a property inferred from data or node
contents. The core accept path and the edge-authority ingest entry point are the
places that assign fates. A node receiving an unfated commit unit through an
ordinary sync application path remains a non-authority receiver for that unit:
it stages or parks the unit pending a remote fate and does not create merge
versions merely because it has the payload (`INV-TX-23`).

Invariant digest:

- `INV-EDGE-1`: A PeerRole::Relay link MUST use AuthorId::SYSTEM as its link identity and MUST NOT terminate a client identity.
- `INV-EDGE-2`: A relay MUST store/forward TxKind::Mergeable and TxKind::Exclusive commit units as Fate::Pending with DurabilityTier::Local and MUST NOT assign an authority fate.
- `INV-EDGE-3`: An edge-client link MUST terminate exactly one client author identity as PeerRole::EdgeClient { identity }, and downstream reads on that link MUST use that identity fo...
- `INV-EDGE-4`: An edge MUST NOT assign a mergeable fate until the needed permission-scope subscription has delivered an initial settled result; before that, the transaction MUST rema...
- `INV-EDGE-5`: Edge-local fate assignment MUST support only TxKind::Mergeable; an edge MUST NOT use the edge mergeable path to assign fate for TxKind::Exclusive.
- `INV-EDGE-6`: TxKind::Exclusive acceptance MUST be decided by core, the serialization point; edge authority MUST NOT make exclusive acceptance final.
- `INV-EDGE-7`: Once a transaction reaches Fate::Accepted, later stale Fate::Pending updates MUST NOT regress its fate.
- `INV-EDGE-8`: Edge acceptance of a mergeable transaction MUST be a final authorization outcome; core MUST NOT re-evaluate or reject it solely because policy changed concurrently aft...
- `INV-EDGE-9`: A cancelled or missing permission scope MUST NOT satisfy the edge permission gate; after restart, deferred edge-fate gates and retained scope refs are absent until cli...
- `INV-EDGE-10`: After a permission scope has first settled, an edge MAY continue accepting mergeables against stale scope state unless a configured staleness horizon says otherwise; t...
- `INV-EDGE-11`: Fate and durability MUST remain separate axes: edge-accepted does not imply DurabilityTier::Global; receivers MUST raise observed durability only from explicit durabil...
- `INV-EDGE-12`: Topology v1 MUST be star-shaped: edges connect upstream to core; edges MUST NOT sync with other edges as peers for authority or merge coordination.
- `INV-EDGE-13`: Resubmitting the same commit unit through another edge MUST be idempotent by TxId when the payload matches, and conflicting payloads with the same TxId MUST be rejecte...
- `INV-EDGE-14`: An edge cache MUST NOT evict fate-pending units, permission-scope results currently backing edge acceptance, parked commit families, large-value op metadata, or edge-a...
- `INV-EDGE-15`: Edge refetch after eviction MUST use payload-inventory resubscribe rather than assuming the edge has complete history.
- `INV-EDGE-16`: Duplicate merges of the same concurrent mergeable frontier MUST be legal (identical cells); when independent edge merges diverge, an upstream tier MUST reconcile them...
- `INV-EDGE-17`: An edge permission-scope subscription MUST be keyed by (policyshape, writerclaim) — the write policy's query shape bound to the writer's claim("sub") — and MUST NOT hy...
- `INV-EDGE-18`: Overlapping (policyshape, writerclaim) scopes MUST resolve to a single upstream subscription whose settled result satisfies every acceptance gate that depends on it; t...
- `INV-LOWER-20`: RLS policy declarations MUST be valid Jazz query shapes; read policy MUST lower through the query engine as part of the policy-composed read graph, while write-time ac...
- `INV-RLS-18`: An uploaded commit unit MUST be authorized under the authenticated link identity: a Session link's madeby MUST equal that identity or be rejected, while a TrustedBacke...
- `INV-TX-23`: Fate authority MUST be structurally wired by the host. Applying a bare unfated commit unit on a non-authority sync path MUST stage or park it pending remote fate; it M...

## Details

### 9.1 The role ladder

Trust is the axis:

- **client** — untrusted; no fate authority; local preview only.
- **relay** — semi-trusted passthrough/cache; never assigns fates or enforces
  per-user permissions; forwards opaquely under `AuthorId::SYSTEM`.
- **edge** — operator-trusted; may finally decide _mergeable_ fates and enforces
  read/write policy for the identities it terminates.
- **core** — operator-trusted; the exclusive-transaction authority and global
  ordering point; history-complete.

Only the core is history-complete. Every downstream node (relay, edge, client)
may hold partial or evicted history, and no protocol step may assume otherwise
(ch. 1, principle 4).

### 9.2 Topology

The topology separates responsibility by placing trusted edge service between
clients and the history-complete core. Clients connect to a relay or edge for
local service and policy narrowing; edges connect upstream to core for global
durability and ordering. The core remains the sole authority for exclusive
transactions, while mergeable fate authority belongs to the first trusted edge on
the upstream path.

Each capability belongs to the tier that can safely exercise it:

| capability                    | authority / behavior                                                                |
| ----------------------------- | ----------------------------------------------------------------------------------- |
| mergeable fate authority      | first upstream trusted edge; edge-final for edge-accepted mergeables (`INV-EDGE-8`) |
| exclusive fate authority      | core                                                                                |
| read narrowing / write-policy | edge enforces for the identities it terminates                                      |
| durability tiers offered      | `Local`, `Edge`, `Global`                                                           |
| eviction                      | edge cache eviction (`INV-EDGE-14`, target)                                         |
| topology                      | star: clients/edges ↔ core (`INV-EDGE-12`, target)                                  |

The four-tier tests exercise the role shapes: relay store-and-forward, edge
identity termination, and edge fate deferral. Normal committed units outside the
partial edge-mergeable path still rely on core as the authority until the
remaining edge capabilities are complete.

The canonical alpha-replacement conformance and benchmark topology is:

```text
client main thread (in-memory)
  ↔ client worker relay (OPFS)
  ↔ edge (RocksDB)
  ↔ core (RocksDB)
```

This topology is a deployment shape over the single protocol and API surface. The
main-thread client owns immediate UI-local state, the worker relay owns durable
browser persistence and tab sharing, the edge terminates client identities and
hydrates permission scopes, and core remains history-complete. Scenario smoke
benches may collapse this into in-process nodes while preserving the same role
boundaries; browser OPFS and worker ownership are integrability concerns, not
alternate semantics.

### 9.3 Relays

Relays provide unopinionated transport and caching. A relay link uses
`PeerRole::Relay` with identity `AuthorId::SYSTEM` (`INV-EDGE-1`) and forwards
both mergeable and exclusive commit units without deciding their outcome: stored
units remain `Fate::Pending` / `DurabilityTier::Local`, and the relay assigns no
fate (`INV-EDGE-2`).

A relay may cache encrypted read-side data at rest, but it never enforces
permissions and never accepts or rejects a transaction. The default browser
architecture is a shared-worker relay, where one worker relays for all tabs in
the browser. Server-deployed relays are the exception.

### 9.4 The edge-client boundary

The edge-client boundary is where the system binds a link to a user identity and
applies the last-hop policy view. An edge-client link terminates exactly one
client `AuthorId` as `PeerRole::EdgeClient { identity }`, and downstream reads on
that link are policy-composed for that identity (`INV-EDGE-3`, ch. 7).

Upstream commit-unit uploads on a normal session link are authorized under the
same terminated identity: `made_by` must match the terminated identity unless the
serving link is explicitly trusted as a backend. For a backend link, policy is
evaluated under the backend link identity and `made_by` is stored only as
attribution (`INV-RLS-18`, ch. 7). This is where per-user read narrowing happens:
the last hop to the client.

### 9.5 Mergeable fate authority

Mergeable transactions are decided at the first upstream trusted edge. Before an
edge assigns a fate for `TxKind::Mergeable`, it must have enough policy data to
authorize the writer against the affected policy scope. The gate is strict: an
edge must not assign a mergeable fate until a **settled permission-scope
subscription** covers the writer and affected policy data — otherwise it
registers/hydrates the scope and defers (`INV-EDGE-4`, ch. 8).

After the first settled result, stale scope data may be used for acceptance,
bounded only by an optional staleness-horizon knob (default off/unbounded). A
cancelled scope, or a scope missing after restart, no longer satisfies the gate;
validation defers until the scope rehydrates.

Deferred edge-fate gate state is in-memory by design. Restart drops deferred
fate entries and their retained permission-scope subscription refs; recovery is
the client's outbox redelivering any unit that has not received fate at its
target tier. By contrast, once an edge has assigned an edge-tier accepted fate,
that transaction and its row versions are durable edge state and survive restart
without client redelivery (`INV-EDGE-9`).

`TxKind::Exclusive` acceptance is **core-only** — the single serialization point
(`INV-EDGE-6`, ch. 3). An edge may locally early-reject a provable conflict but
never _accepts_ an exclusive transaction. Fate never regresses: once `Accepted`,
a later stale `Pending` update is ignored (`INV-EDGE-7`).

**Scope granularity.** The permission-scope subscription that gates acceptance is
keyed by `(policy_shape, writer_claim)` — the narrow slice of policy data that the
write policy reads _for that writer_ — not a whole-table scope (`INV-EDGE-17`).
Because a write policy is itself a jazz query shape (`INV-LOWER-20`), binding it
to the writer's `claim("sub")` narrows hydration to exactly the rows the policy
would read for that writer. An edge therefore holds only the policy data for the
identities it terminates, rather than every tenant's data.

The acceptance gate, the defer/rehydrate bookkeeping, and the eviction pin set
(§9.8) all index on this key. Overlapping scopes — many writers whose policies
read the same row — share **one** upstream subscription through sync-level
work-dedup (ch. 8): the edge registers a covering scope once and fans its settled
result to every gate entry that needs it (`INV-EDGE-18`). A whole-table scope is
deliberately _not_ offered; it would force an edge to hydrate unbounded unrelated
data and is exactly the pathological cost this tier exists to avoid.

> **Edge-final mergeable fate.** An edge mergeable fate is _final_: when core
> receives an edge-accepted mergeable, it performs structural admission checks
> and assigns the global settle position, but does not re-run write-policy
> authorization or re-judge the merge (`INV-EDGE-8`; `INV-EDGE-5`
> mergeable-only).

### 9.6 Fate and durability are separate (across tiers)

Acceptance answers whether a transaction has a final fate; durability answers
where the accepted data is safely stored. Edge acceptance is therefore not the
same as global durability: only an observed `DurabilityTier::Global` means the
write reached core/global durability (`INV-EDGE-11`, ch. 3).

Fate finality and storage durability are independent. An edge-final write can
still be lost if edge storage is destroyed before it syncs upstream.

### 9.7 Star topology

Edges form a star around core. They connect to core and do not sync with each
other (`INV-EDGE-12`, target). Client mobility across edges needs nothing
special: resubmitting a transaction to another edge is idempotent by `TxId`
(`INV-EDGE-13`, ch. 8), and two edges accepting concurrent mergeables is ordinary
merging (ch. 4).

Duplicate merges of the same concurrent frontier are legal because they carry
identical cells. When independent edge merges diverge, an upstream tier
reconciles them by folding over the de-duplicated raw head set rather than
re-merging the merged values, so `Counter` never double-counts a shared ancestor
(`INV-EDGE-16`; ch. 4, "Merging merges"). Nothing enforces the _absence_
of edge↔edge sync at the transport layer; the star is a deployment contract, not
a wire check.

### 9.8 Eviction and refetch

An edge is a cache, so it may shed cold state — but only the regenerable kind.
Cold globally-accepted row versions, large-value content extent bytes, and
materialized checkpoint bytes are evictable. The pin set is never evictable:
large-value op metadata a serving node needs for membership checks (ch. 12),
fate-pending units, edge-accepted versions not yet globally durable (not
refetchable from core until they reach `Global`, §9.6), the scope results backing
an acceptance gate (§9.5), and parked families (`INV-EDGE-14`, `INV-EDGE-15`).

Refetch of evicted state is a **payload-inventory resubscribe**. The edge
re-registers the scope and receives only what its payload inventory no longer
holds (ch. 8). The v1 trigger is an optional edge byte budget: absent budget
means eviction is disabled. When metered cache bytes exceed the budget, the edge
evicts to a fixed low-water mark using write/settle recency as the LRU
approximation: least-recently-written unpinned row versions first. Direct
large-value content/checkpoint bytes remain regenerable cold content under the
same budget, while metadata and all pin roots survive.

### 9.9 Subsumed topology and server notes

The former alpha transport and sync-manager notes are now represented as role
semantics. The server shell terminates carriers and admission, then hands
authorized links to `Node`/peer state; it does not own a parallel query,
transaction, or sync engine. CORS, WebSocket paths, health endpoints, quota
limits, and dashboard or deployment configuration are shell/product concerns
around this role ladder.

Client and edge cache limits are topology policy. Storage may evict cold
coverage only when doing so preserves fate-pending units, authority evidence,
large-value content needed by accepted rows, and enough resume/catalogue state to
refetch accurately.

## Open Questions

### Open questions

- 🔶 **Server shell responsibilities.** The production server should be a small
  shell around `Node`: listener setup, auth admission, storage configuration,
  health/metrics, protocol version reporting, migration status, and shutdown.
  It must not introduce a second transaction/query/sync engine. Decide which
  pieces live in a `jazz-server` crate/package versus examples while topology is
  still stabilizing.
- 🔶 **Topology conformance matrix.** Run the same black-box scenarios across
  client-only, browser shared-worker relay, Node client, edge, relay, and core
  deployments. The matrix should cover mergeable/exclusive writes, RLS,
  subscription deltas, large values, branches/lenses, reconnect/resume, and
  protocol mismatch. Differences should be role configuration, not alternate
  semantics.
- 🔶 **Staleness horizon** (`INV-EDGE-10`, target) — a config knob (default
  off/unbounded): once a scope has delivered its first settled result, how stale
  its data may be before acceptance must re-gate. Decided in prose; no config type
  or enforcement point yet.
- ✅ **Restart/rehydration** (`INV-EDGE-9`) — deferred edge-fate gates are
  in-memory by design; restart recovery is client outbox redelivery for unfated
  units, while edge-accepted units survive in edge storage.
- 🔶 **True read-recency LRU** — v1 eviction uses write/settle recency because it
  is already present in history. True least-recently-read eviction would require
  per-read metadata writes; that is a product-data decision, not a correctness
  requirement for the v1 byte-budget trigger.
- 🔶 **Serving while disconnected** — decided (2026-07-02): a disconnected edge
  **keeps serving edge-tier state**, including accepting mergeable
  transactions where it is the fate authority for the scope — edge-tier
  durability and mergeable semantics are eventually-consistent by design, so
  upstream connectivity is not a serving precondition for them. Claims at
  `Global` tier (reads or durability waits requiring global settlement) are
  what a disconnected edge cannot satisfy: those defer or carry an explicit
  staleness/unsettled marker rather than being served as fresh. This narrows
  the staleness-horizon knob above to global-tier claims.
- 🔶 **Topology and edge role completion** — the design is a star of clients and
  edges around core (`INV-EDGE-12`, target), with the first upstream trusted edge
  as mergeable fate authority and `Edge` as a durability tier. The implementation
  still collapses the ordinary path to client ↔ core for many committed units:
  core plays every upstream role, remains the sole fate authority outside the
  partial edge-mergeable path, and the offered durability tiers are `Local`
  (client) and `Global` (core).
- 🔶 **Edge read/write enforcement** — the design is that an edge enforces
  read/write policy for the identities it terminates. The implementation still
  relies on core for general read narrowing and write-policy enforcement, while
  edge-client links narrow under the terminated identity.
- 🔶 **Client TTL configuration.** The old hardcoded client-state TTL should
  become an explicit cache/staleness option with clear interaction between
  local, edge, and global durability tiers.
- 🔶 **CORS and admission routes.** Server shells must accept normal browser
  authorization headers and preflight behavior without weakening admission; this
  belongs in shell conformance, not the core protocol.
- 🔶 **Edge-local catalogue pruning.** When an edge reconnects with stale
  catalogue state, core replay must be able to remove edge-local catalogue rows
  absent from the authoritative catalogue without deleting unrelated local
  state.
- 🔶 **Edge transaction authorities.** Future edge authority placement must say
  which scopes an edge may decide, how leases or ownership move, and how that
  composes with sharding.
