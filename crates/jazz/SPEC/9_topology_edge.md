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

- `INV-EDGE-1`: A `PeerRole::Relay` link MUST use `AuthorSubject::SYSTEM` as its link identity and MUST NOT terminate a client identity.
- `INV-EDGE-2`: A relay MUST store/forward `TxKind::Mergeable` and `TxKind::Exclusive` commit units as `Fate::Pending` with `DurabilityTier::Local` and MUST NOT assign an authority fate.
- `INV-EDGE-3`: An edge-client link MUST terminate exactly one client author identity as `PeerRole::ClientLink { identity }`, and downstream reads on that link MUST use that identity for policy composition.
- `INV-EDGE-4`: An edge MUST NOT assign a mergeable fate until the needed permission-scope subscription has delivered an initial settled result; before that, the transaction MUST remain outside edge history in in-memory deferred-admission state. Once settled, an authorized transaction is ingested and edge-accepted exactly once; a denied transaction is rejected without ingestion.
- `INV-EDGE-5`: Edge-local fate assignment MUST support only `TxKind::Mergeable`; an edge MUST NOT use the edge mergeable path to assign fate for `TxKind::Exclusive`.
- `INV-EDGE-6`: `TxKind::Exclusive` acceptance MUST be decided by core, the serialization point; edge authority MUST NOT make exclusive acceptance final.
- `INV-EDGE-7`: Once a transaction reaches `Fate::Accepted`, later stale `Fate::Pending` updates MUST NOT regress its fate.
- `INV-EDGE-8`: Edge acceptance of a mergeable transaction MUST be a final authorization outcome; core MUST NOT re-evaluate or reject it solely because policy changed concurrently after the edge's settled permission basis.
- `INV-EDGE-9`: A cancelled or missing permission scope MUST NOT satisfy the edge permission gate; after restart, deferred edge-fate gates and retained scope refs are absent until client outbox redelivery reopens the gate, while already edge-accepted units MUST survive from edge storage without redelivery.
- `INV-EDGE-10`: An edge MAY use a previously settled permission scope to accept a mergeable transaction only as permitted by its configured freshness policy; the default policy MUST permit unbounded freshness.
- `INV-EDGE-11`: Fate and durability MUST remain separate axes: edge-accepted does not imply `DurabilityTier::Global`; receivers MUST raise observed durability only from explicit durability claims.
- `INV-EDGE-12`: Edge authority and merge coordination MUST route upstream through core rather than directly between edges.
- `INV-EDGE-13`: Resubmitting the same commit unit through another edge MUST be idempotent by `TxId` when the payload matches, and conflicting payloads with the same `TxId` MUST be rejected as `ConflictingCommitUnit`.
- `INV-EDGE-14`: An edge cache MUST NOT evict fate-pending units, permission-scope results currently backing edge acceptance, parked commit families, or edge-accepted versions not yet globally durable.
- `INV-EDGE-15`: After eviction, an edge MUST recover required payloads through resubscription without assuming complete local history.
- `INV-EDGE-16`: Duplicate merges of the same concurrent mergeable frontier MUST be legal (identical cells); when independent edge merges diverge, an upstream tier MUST reconcile them by folding over the de-duplicated raw head set (not by re-merging merged values), so `Counter` never double-counts a shared ancestor.
- `INV-EDGE-17`: An edge permission-scope subscription MUST be keyed by `(policy_shape, writer_claim)` — the write policy's query shape bound to the writer's `claim("user")` — and MUST NOT hydrate a whole-table scope. A public-write table (no write policy) opens no scope and settles immediately.
- `INV-EDGE-18`: An edge MUST share a settled permission-scope subscription among all dependent acceptance gates it can satisfy.
- `INV-EDGE-19`: A dynamically catalogued serving authority MUST NOT accept an uploaded commit unit until an authority has published a permissions head selecting its write schema and table policies. If no head is published, it MUST reject the unit with `permissions_head_missing`, rather than silently accepting it.
- `INV-EDGE-20`: A worker's relay-authority source identity MUST be used only for selected authority-membership handoffs; it MUST NOT create a second projection of ordinary Edge reads or deliver one transaction through conflicting view bundles.
- `INV-LOWER-20`: RLS policy declarations MUST be valid Jazz query shapes; read policy MUST lower through the query engine as part of the policy-composed read graph, while write-time ac...
- `INV-RLS-18`: An uploaded commit unit MUST be authorized under the authenticated link identity: a Session link's madeby MUST equal that identity or be rejected, while a TrustedBacke...
- `INV-TX-23`: Fate authority MUST be structurally wired by the host. Applying a bare unfated commit unit on a non-authority sync path MUST stage or park it pending remote fate; it M...

## Details

### 9.1 The role ladder

Trust is the axis:

- **client** — untrusted; no fate authority; local preview only.
- **relay** — semi-trusted passthrough/cache; never assigns fates or enforces
  per-user permissions; forwards opaquely under `AuthorSubject::SYSTEM`.
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
| eviction                      | edge cache eviction (`INV-EDGE-14`)                                                 |
| topology                      | authority and merge coordination route through core (`INV-EDGE-12`)                 |

**Implementation status (verified 2026-07-27).** The four-tier tests exercise
edge identity termination and edge fate deferral
(`edge_peer_terminates_client_identity_and_relays_upstream` and
`edge_defers_mergeable_fate_until_permission_scope_settles` in
`crates/jazz/tests/four_tier.rs`). The ordinary committed-unit path remains
core-authoritative outside the partial edge-mergeable path; general edge
read/write enforcement likewise still relies on core. Edge-client links do
narrow reads under the terminated identity. These are rollout status, not
topology semantics.

The canonical alpha-replacement conformance and benchmark topology is:

```text
client main thread (in-memory)
  ↔ client worker relay (IndexedDB)
  ↔ edge (RocksDB)
  ↔ core (RocksDB)
```

This topology is a deployment shape over the single protocol and API surface. The
main-thread client owns immediate UI-local state, the worker relay owns durable
browser persistence and tab sharing, the edge terminates client identities and
hydrates permission scopes, and core remains history-complete. Scenario smoke
benches may collapse this into in-process nodes while preserving the same role
boundaries; browser IndexedDB and worker ownership are integrability concerns, not
alternate semantics.

For persistent browsers, the physical IndexedDB name has one durable, non-secret
owner identity: app, environment, and authentication scope. The first worker to
open an explicitly selected name atomically pins that identity beside the page
store manifest. The same owner may release and reopen it across worker restarts;
an incompatible owner fails before receiving a page-store handle or mutating a
page. The marker is canonical JSON `{version: 1, appId, env, auth}`. `auth` is
either `{kind: "anonymous"}`, `{kind: "system"}`, or
`{kind: "principal", authMode, user}`, where `user` is the normal canonical
`session.user` JSON encoding of the exact `[issuer, subject]` pair. It contains
neither tokens, secrets, expiry nor claims, and is never hashed, truncated or
otherwise replaced with a collision-prone surrogate. Deleting the entire browser
namespace is the explicit ownership-transfer operation. This physical ownership
is distinct from a foreground replica/node ID, which remains per live client,
and from credentials, which are never persisted as the ownership marker.

The owner marker alone does not establish _live_ worker ownership: rolling
assets and generation retry can name distinct SharedWorker realms for the same
physical root. Before a realm opens or recovers that root's foreground-lease
pool, it MUST hold one origin-wide physical-root liveness lock and durably claim
an opaque worker epoch beside the manifest. A live predecessor therefore makes
a successor fail/retry operation-scoped rather than retiring its leases. Once
the browser releases a dead predecessor's lock, a successor may replace its
epoch; stale cleanup may delete only the epoch it claimed. A running persistent
browser Db likewise MUST reject a principal-changing auth update before it
reaches the worker; same-principal credential refresh remains allowed, while a
user switch requires shutdown and reopening (or explicit storage reset).

The main-thread client is deliberately non-durable: its authored transactions
start at `Pending`/`None`. Each live foreground owns an exclusive leased
`NodeUuid` and mints its own transaction identities locally; its clean handoff
may reuse that identity only after the worker durably records the runtime-owned
HLC high-water, while an unclean termination retires it. The worker relay persists the unchanged commit unit and
returns `Pending`/`Local`; that durability acknowledgement does not assign fate.
The relay forwards later Edge/Global durability and authority fates back over the
same client-worker link. A worker without an upstream can therefore satisfy
`Local` waits while Edge/Global waits remain unavailable.

The worker may give an upstream authority receipt an internal
`RelayAuthoritySession` source identity. That source is a topology-local
discriminator, never an application option, persisted field, or policy input:
it prevents a worker-owned authority receipt from being confused with ordinary
Global coverage for the same shape and bindings. It is selected only when a
downstream Edge handoff must reuse authority membership rather than re-evaluate
the worker cache: a nonzero window (whose offset must not be applied twice) or
a read-policy-scoped exact-ID read (whose cached row must not survive read
membership revocation). A write-only policy does not scope read membership and
therefore does not select this source. Other Edge reads keep the ordinary relay path. In particular, adding a worker
authority source MUST NOT create a second result projection or cause the same
transaction to be delivered through incompatible view bundles.

### 9.3 Relays

Relays provide unopinionated transport and caching. A relay link uses
`PeerRole::Relay` with identity `AuthorSubject::SYSTEM` (`INV-EDGE-1`) and forwards
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
client `AuthorSubject` as `PeerRole::ClientLink { identity }`, and downstream reads on
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

After the first settled result, an edge may use scope data only under its
configured freshness policy; the default permits unbounded freshness
(`INV-EDGE-10`). A cancelled scope, or a scope missing after restart, no longer
satisfies the gate; validation defers until the scope rehydrates.

**Implementation status (2026-07-27).** The freshness policy has no configuration
type or enforcement point yet. The existing gate behavior is covered by
`edge_defers_mergeable_fate_until_permission_scope_settles`
(`crates/jazz/tests/four_tier.rs`).

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
to the writer's `claim("user")` narrows hydration to exactly the rows the policy
would read for that writer. An edge therefore holds only the policy data for the
identities it terminates, rather than every tenant's data.

The acceptance gate, defer/rehydrate bookkeeping, and eviction pin set (§9.8)
index on this key. A settled scope is shared among every acceptance gate it can
satisfy (`INV-EDGE-18`).

**Implementation status (verified 2026-07-27).** The implementation shares
exact-key scopes; covering-scope subsumption is not implemented. Exact-key
sharing and last-dependent release are covered by
`edge_deduplicates_scope_subscription_for_repeated_deferred_units` and
`edge_releases_scope_subscription_after_last_deferred_unit_resolves`
(`crates/jazz/tests/four_tier.rs`).

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

A disconnected edge continues to serve edge-tier state, including mergeable
transactions for scopes where it is the fate authority. Requests that require
global settlement defer or carry an explicit unsettled/staleness marker; upstream
connectivity is not a precondition for edge-tier service.

The server shell owns one upstream connector for the edge lifecycle. An
established link ends when the target transport's socket pump reports closure,
even when no semantic frame is in flight; the shell disconnects that peer state
before reconnecting. Bootstrap/open failures, clean closure, native terminal
failures, and transient I/O/backpressure failures retry with exponential delay
from 100 ms capped at 5 s, with the failure streak reset after 30 seconds of
stable connection. Semantic wire/catalogue failures and local catalogue-install
failures are fatal for that connector generation: retry stops and `/health`
reports the upstream failure.
Shutdown wins races with bootstrap, connect, connected wait, and backoff, joins
the owned connector before storage close, and forbids a late dynamic shell or
ready-generation publication.

### 9.7 Star topology

Edge authority and merge coordination route upstream through core rather than
directly between edges (`INV-EDGE-12`). Client mobility across edges needs
nothing special: resubmitting a transaction to another edge is idempotent by `TxId`
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
Cold globally-accepted row versions are evictable. The pin set is never
evictable: fate-pending units, edge-accepted versions not yet globally durable (not
refetchable from core until they reach `Global`, §9.6), the scope results backing
an acceptance gate (§9.5), and parked families (`INV-EDGE-14`, `INV-EDGE-15`).

After eviction, an edge recovers required payloads through resubscription rather
than assuming complete local history (`INV-EDGE-15`).

**Implementation status (verified 2026-07-27).** The current recovery path
forgets evicted payload coverage and rehydrates; it is covered by
`peer_eviction_forgets_payload_coverage_for_rehydrate`.
The optional byte budget and
write/settle-recency eviction policy are implementation details, not topology
invariants.

### 9.9 Subsumed topology and server notes

The former alpha transport and sync-manager notes are now represented as role
semantics. The server shell terminates carriers and admission, then hands
authorized links to `Node`/peer state; it does not own a parallel query,
transaction, or sync engine. CORS, WebSocket paths, health endpoints, quota
limits, and dashboard or deployment configuration are shell/product concerns
around this role ladder.

`/health` is an operational readiness endpoint, not a liveness probe. It
returns `503 Service Unavailable` while shutdown is in progress, when an Edge
upstream has failed fatally, or when an Edge has no runtime eligible for client
sessions; those outcomes use the existing shutdown or upstream-failure response
bodies, except that the missing Edge runtime reports
`{"status":"not_ready","component":"runtime"}`. A fixed or offline Edge with a
ready runtime remains healthy, and Core health semantics are unchanged.

**Dynamic-catalogue bootstrap.** Publishing a schema alone does not make a
dynamically catalogued serving authority ready to serve session writes. Before
it accepts an uploaded commit unit, an authority MUST publish a permissions head
that selects the write schema and its table policies (`INV-EDGE-19`). Until that
head exists, the authority MUST reject the unit as
`Fate::Rejected(RejectionReason::MalformedCommit("permissions_head_missing: no
published permissions head"))`; it MUST neither silently accept the write nor
defer it as though a policy basis existed. Publishing the first permissions head
rehydrates live session views under the selected policy.

Client and edge cache limits are topology policy. Storage may evict cold
coverage only when doing so preserves fate-pending units, authority evidence,
enough resume/catalogue state to refetch accurately.

## Open Questions

- 🔶 [#1778](https://github.com/garden-co/jazz/issues/1778) — Server shell responsibilities and admission routes.
- 🔶 [#1785](https://github.com/garden-co/jazz/issues/1785) — Topology cache, TTL, eviction, and edge catalogue lifecycle.
