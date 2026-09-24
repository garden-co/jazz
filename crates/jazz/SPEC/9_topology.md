# jazz — Specification · 9. Core, clients, and local relays

## Overview

Core is the only authority. It authorizes reads and writes, decides the final
fate of both mergeable and exclusive transactions, and acknowledges global
durability. Clients retain authorized data and optimistic local edits. Local
browser and native persistence relays remain non-authoritative parts of a
client; there is no semantic server edge between a client and Core.

A connection gateway may route bytes to Core. It does not ingest transaction
history, run queries, cache permission scopes, assign fates, or acknowledge
durability. Sharding and distributed query execution are outside this change.

## Details

### 9.1 Authority and durability

- A client-authored transaction starts pending. Persisting it locally raises its
  durability to Local without accepting it.
- Core authenticates the session and evaluates current write policy before
  accepting either transaction kind. Exclusive transactions additionally require
  their normal serialized conflict checks.
- A successful Core acknowledgement establishes acceptance and Global
  durability. Rejection follows the existing rollback/dependency rules.
- Replaying the same transaction identity and unchanged payload is idempotent;
  a different payload under that identity is a conflict. Retrying never mints
  replacement authorship or transaction identities.
- A non-authority receiving an unfated commit unit stages or parks it. Possessing
  bytes is never authorization or permission to assign a fate (`INV-TX-23`).

Local-first reads use retained data and local edits. Remote reads require a
fresh Core-confirmed supporting set; local-first-unless-empty is local-first
but may hold an empty opening for the first remote view while a remote can
answer (ch. 13). No intermediate
server can substitute a locally computed result for Core confirmation.

Core supplies query supporting rows and handles extra-local-row reconciliation:
readable newer versions are delivered, while access loss is reported without
revealing hidden policy evidence. Clients still evaluate queries locally over
these inputs. Reconnect, complete-set replacement, negative evidence and
connection-scoped receipt rules from chapter 8 remain in force.

### 9.2 Existing persisted durability

The record encoding keeps its established discriminants: None is 0, Local is 1,
legacy Edge is 2, and Global is 3. New records never emit 2. Readers interpret
legacy 2 as Local; Global must not be renumbered to 2.

A legacy accepted transaction with durability 2 and no Core global position is
pending under the new authority model. Preserve its authored commit unit,
transaction ID, author and row versions. The normal locally-authored pending
replay path resubmits it to Core, including after restart without an active
query. Core may acknowledge an already-known identical unit, authorize and
accept a new unit, or reject it through ordinary transaction reconciliation.
The old edge acceptance is not evidence of present Core authorization
(`INV-SYNC-47`).
Retired edge-authority publications are rejected even on privileged links;
authenticating the sender cannot reinstate the removed admission shortcut.

Already globally confirmed transactions retain their fate and global position.
Rejected transactions remain rejected. Reopening must not upload another
account's cached history as the current account's authored edits. This migration
cannot recover bytes absent from the local store; it does not invent credentials
or convert cached remote transactions into local authorship.

### 9.3 Encoder and authorization trust

Core-produced representations are trusted on the downstream path. Client uploads
are untrusted at Core and obey chapter 8's bounded decoding and per-connection
failure containment rules. Relaying client bytes does not turn them into trusted
Core output. Encoder trust never replaces session authorization.

### 9.4 Local persistence and identity isolation

For persistent browsers, the physical IndexedDB name has one durable, non-secret
owner identity: app, environment, and authentication scope. The first worker to
open an explicitly selected name atomically pins that identity beside the page
store manifest. The same owner may release and reopen it across worker restarts;
an incompatible owner fails before receiving a page-store handle or mutating a
page. The marker is canonical JSON `{version: 1, appId, env, auth}`. `auth` is
`{kind: "account", account: accountUUID, registry: canonicalRegistryURL}` for
public account contexts. The acting issuer/subject scopes the live attachment
but does not split durable storage for identities linked to that account.
Internal anonymous/system and principal scopes remain explicit separate kinds;
a principal scope uses the exact canonical identity spelling, never an intern
handle. The owner marker contains neither tokens, secrets, expiry nor provider
claims. It is never replaced with a collision-prone surrogate. Deleting the entire browser
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
The relay forwards later Global durability and authority fates back over the
same client-worker link. A worker without an upstream can therefore satisfy
`Local` waits while Global waits remain unavailable.

The browser worker is a **scope-isolated client relay**. Its physical store and
every foreground attached to it belong to exactly one durable authentication
scope: the same app, environment, and canonical authentication identity named
by the owner marker above. Credential refresh within that identity is allowed;
a different identity cannot attach to or reuse the store. This isolation is a
capability boundary, not permission authority: the worker neither evaluates
read policy nor assigns write fate.

The worker may retain an upstream authority's result membership under an
internal authority-result source identity. That identity is assigned by the
topology, never supplied by an application or accepted from an untrusted wire
caller. It means “the upstream authority selected these members for this exact
policy binding”; it does **not** mean that the worker may act as that authority.

The physical worker-to-upstream link remains an authenticated relay transport
with no permission subject, but each
foreground Global request on it carries an immutable delegated-session
binding created from the worker's admitted same-scope foreground. The receiving
core, not the worker, resolves that binding to trusted claims and performs
policy composition. A generic relay message without such an admitted binding
remains unbound and cannot request a user's narrowed result. The worker
cannot invent, alter, or refresh delegated claims through application query or
wire fields (`INV-EDGE-24`).

This delegation exists only inside a closed, server-issued relay capability.
After authenticating the requested scope-isolated relay attachment, the server
binds the canonical foreground identity and claims to that exact durable scope
and mints a fresh admission epoch. An inbound delegated request is valid only
while that capability is live and only when its binding, scope, and epoch match
exactly. A disconnect, authority switch, or transport replacement retires the
epoch. Reconnect and process-local transport resume MUST mint a new epoch and
re-admit the preserved binding; persisted result state, a stale receipt, or a
replayed `(identity, claims)` snapshot never constitutes live admission.

Every retained authoritative result has a collision-free identity containing:

- the canonical query shape, binding, and read-view identity; and
- the exact immutable policy binding selected from the authenticated session,
  including its canonical claims rather than a truncated or hash-only alias.

That complete identity keys all mutable and durable result state: member sets,
facts, generations, known-state cursors, reset/open/deferred state, repair
bookkeeping, persistence, and relay-source selection. A live authority receipt
is narrower still: it additionally belongs to the selected authority connection
epoch and exact usage-site subscription, and is retired on disconnect,
authority switch, or detachment. Persisted members or cursors never become a
live receipt merely by reopening (`INV-SYNC-30`). Two sessions or
claims revisions may share content-addressed payloads, but they MUST NOT share a
mutable authoritative result merely because their application query is equal
(`INV-EDGE-22`).

The worker uses authoritative membership for every Global observation it
serves to a foreground. This is not limited to windows or exact-ID reads:
unbounded queries, joins, includes, and ordinary reads have the same boundary.
The worker may evaluate the application query locally over that exact member
source to construct the requested projection, but it MUST use client-local
lowering and MUST NOT compose or re-run read policy. Adding an internal
authority-result source MUST NOT create a second public projection or deliver
one transaction through incompatible view bundles (`INV-EDGE-20`).

Local observations use the worker's retained authorized knowledge plus the
foreground's optimistic local writes. An upstream addition makes newly
delivered material part of that retained knowledge and MUST wake affected Local
subscriptions. An upstream removal changes future authoritative Global
membership, but does not retroactively redact material already delivered to the
scope-isolated store; Local may continue to expose it (`INV-RLS-6`).
`Propagation::LocalOnly` prevents asking upstream and does not change these
Local semantics. `LocalFirstUnlessEmpty` (formerly `RemoteIfPossible`, now a
deprecated alias) is a Local read throughout; only an empty opening may wait
for the first authority view, only while the link is live or within the
attempt window, and an `offset > 0` window reads the strict remote view
(ch. 13). The gate lives in the core `Db`; it never runs a second concurrent
probe query.

Every worker-to-foreground read, subscription, and row-version repair uses an
explicit client-local serving context. Client-local lowering is its own
policy-free query mode; it MUST NOT impersonate `AuthorSubject::SYSTEM`, select
trusted serving, manufacture authority, or change authorship.

### 9.5 Relays

Relays provide unopinionated transport and caching. A relay link uses
`PeerRole::Relay` with an explicit relay transport capability and no permission
subject (`INV-EDGE-1`), and forwards
both mergeable and exclusive commit units without deciding their outcome: stored
units remain `Fate::Pending` / `DurabilityTier::Local`, and the relay assigns no
fate (`INV-EDGE-2`).

A relay may cache read-side data at rest, but it never enforces permissions and
never accepts or rejects a transaction. Relay transport authority is not an
authenticated user, row author, or permission identity. `SYSTEM` remains
reserved for an actual trusted internal principal that deliberately bypasses
policy; relay transport MUST NOT acquire that bypass merely by carrying data.

A scope-isolated client relay may answer an exactly same-scope foreground repair
without re-evaluating policy only for a version recorded in that scope's
previously delivered authorized result/payload closure, or for a same-scope
authored pending version. Mere physical co-location is insufficient: hidden
policy evidence, catalogue internals, and unrelated cached rows are not thereby
foreground-repairable. Revocation remains forward-looking for material already
delivered (`INV-RLS-6`, `INV-EDGE-23`). A multiplexed relay is stricter. It may
deduplicate immutable payload bytes, but before serving a principal it MUST
prove the requested version belongs to that principal's exact authorized
payload closure—not merely that the same row is currently a result member—or
forward the repair upstream. Possession of cached bytes alone is not a
cross-scope capability.

The default browser architecture is a shared-worker relay isolated to one auth
scope, where one worker relays for all same-scope tabs. A server-deployed or
otherwise multiplexed relay MUST preserve the policy-scoped result identities
defined above rather than relying on physical-store isolation. Native/RN client
relays have the same semantic boundary even when their storage-owner and
lifecycle mechanism differs from the browser's IndexedDB marker.

### 9.6 The Core/client boundary

The core-client boundary is where the system binds a link to a user identity and
applies the last-hop policy view. A core-client link terminates exactly one
client `AuthorSubject` as `PeerRole::ClientLink { identity }`, and downstream reads on
that link are policy-composed for that identity (`INV-EDGE-3`, ch. 7).

Upstream commit-unit uploads on a normal session link are authorized under the
same terminated identity: `made_by` must match the terminated identity unless the
serving link is explicitly trusted as a backend. For a backend link, policy is
evaluated under the backend link identity and `made_by` is stored only as
attribution (`INV-RLS-18`, ch. 7). This is where per-user read narrowing happens:
the last hop to the client.

A commit relayed from a scope-isolated client is different from both cases. The
relay transport has no permission subject, so the receiving fate authority
proves the commit under the immutable foreground binding in the live,
server-issued relay capability. Only that proof may enable terminal admission;
it is internal state and is never accepted from the wire. `Transaction.made_by`
remains authorship rather than permission evidence, and neither `SYSTEM`, a
persisted result, a stale admission epoch, nor a mutable subscriber-claims field
may substitute for or widen the capability. Disconnect or transport replacement
invalidates the proof and requires authorization under the freshly admitted
epoch (`INV-EDGE-25`).

This applies to both mergeable and exclusive transactions. Exclusive read-set
validation is an additional core admission requirement, not an alternative to
write authorization. For example, a foreground may submit an exclusive
todo/check/note insertion through its worker: all three write policies must
pass under its admitted session before core validates and accepts the bundle.
If the note is forbidden, none of the otherwise permitted siblings is admitted.

### 9.7 Server readiness

A dynamically catalogued Core must have a published permissions head selecting
its write schema and policies before accepting uploads (`INV-EDGE-19`). Missing
permissions must produce the existing actionable permissions-head error rather
than silently accepting a write. Schema and permission publication, account
registry operations, and query authorization all terminate at Core.

The historical `INV-EDGE` identifiers retained above describe client-relay and
session boundaries; their names do not imply a remaining semantic edge role.
The former edge-final acceptance, stale permission-scope authorization and
edge cache lifecycle requirements are retired by this architecture.

## Open Questions

- 🔶 [#1778](https://github.com/garden-co/jazz/issues/1778) — Server shell responsibilities and admission routes.
