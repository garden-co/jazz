# Pitch: Sync Protocol Reliability

## Problem

The current sync protocol has no delivery guarantees. Messages can be lost, arrive out of order, or be silently rejected — and no participant ever knows. Every write spawns an async task, so even consecutive writes race. The protocol must be redesigned with reliability as a foundational property, and must work identically across all transports (HTTP+SSE, postMessage, future WebSocket).

## Solution

### Core idea: reliable delivery, strategy-agnostic ordering

Jazz already has content-addressed commits (BLAKE3), parent references, and full-row snapshots per commit. The protocol uses content-addressing for reconciliation and idempotent delivery — but deliberately keeps ordering out of the reliability stack.

### Two design pillars:

1. **At-least-once unordered delivery** — the reliability stack guarantees every message arrives at least once, but makes no ordering promises. Three reconciliation layers (Outbox → TipFrontier → HashExchange) cover every reconnection scenario, from brief hiccup to total memory loss.
2. **Content-addressed gating** — consumers decide when to act by checking for specific CommitIds, never by relying on arrival order or sequence numbers. Commit ordering gates on parent presence (strategy-specific). Query settlement gates on a coverage manifest of result CommitIds. Both use the same principle: "do I have this content?" not "did everything before N arrive?"

---

### Pillar 1: At-least-once unordered delivery

The reliability stack (Outbox + TipFrontier + HashExchange) guarantees that every message reaches its destination at least once. It does **not** guarantee the order in which messages arrive. This is a deliberate separation of concerns:

- **Reliability** = every message is delivered (at-least-once, idempotent). Owned by the reliability stack.
- **Gating** = consumers decide when to act on delivered messages. Owned by the consumers (ObjectManager, QueryManager).

#### Layered reconciliation

The reliability stack achieves at-least-once delivery through three reconciliation layers, tried in order. Each falls through to the next when it can't cover the situation.

```
┌─────────────────────────────────────────────────┐
│  Layer 1: Outbox (fast path)                    │
│  Client remembers unacked commits → replay them │
├─────────────────────────────────────────────────┤
│  Layer 2: Tip Frontier (normal path)            │
│  Exchange tip CommitIds per (object, branch)    │
│  → diff → send what's missing                   │
├─────────────────────────────────────────────────┤
│  Layer 3: Hash Exchange (cold start)            │
│  Exchange object IDs + 8-byte short hashes      │
│  → identify missing commits → send them         │
└─────────────────────────────────────────────────┘
```

##### Layer 1 — Outbox

Every commit goes to a persistent outbox **in the Worker** when created. It stays there until the server confirms **persistence** — not transport receipt. On reconnect, the Worker replays all unacked commits.

This is the core reliability contract: **a commit leaves the outbox only when the server has durably persisted it.** If the server crashes between receiving and persisting, the Worker still has the commit and will replay it. The outbox lives in the Worker's persistent storage (OPFS) — it survives tab refreshes, browser restarts, and connection drops.

```rust
struct Outbox {
    /// Commits awaiting server persistence confirmation
    unacked: Vec<(ObjectId, BranchName, Commit)>,
}

impl Outbox {
    fn enqueue(&mut self, object_id: ObjectId, branch: BranchName, commit: Commit) {
        self.unacked.push((object_id, branch, commit));
    }

    /// Drain only on PersistenceAck — never on transport receipt
    fn ack(&mut self, persisted_ids: &HashSet<CommitId>) {
        self.unacked.retain(|(_, _, c)| !persisted_ids.contains(&c.id()));
    }

    fn replay(&self) -> Vec<SyncPayload> {
        // Group by object, topological sort, emit ObjectUpdated payloads
        // ...
    }
}
```

Receiving the same CommitId twice is always a no-op — `receive_commit()` already checks for existence and returns early. At-least-once delivery is the contract; idempotent application is the invariant.

This fixes **Gap 2** (outbox drained before delivery) and **Gap 5** (asymmetric reconnect). The outbox is only drained on persistence ack, and reconnect replays both directions.

##### Layer 2 — Tip Frontier

Each peer maintains a compact **tip frontier**: the current tip CommitId(s) per (object, branch), **scoped to objects in active query subscriptions only**. Objects neither side is subscribed to are excluded — there's no reason to track or reconcile them. This keeps the frontier compact and proportional to what the peer actually cares about. On reconnect (or periodically as a heartbeat), peers exchange frontiers. The diff tells each side what the other is missing.

```rust
/// Compact frontier — one entry per (object, branch)
struct TipFrontier {
    tips: HashMap<(ObjectId, BranchName), SmallVec<[CommitId; 2]>>,
}

/// Reconnect handshake
struct SyncHandshake {
    client_id: ClientId,
    frontier: TipFrontier,
}
```

Reconciliation: if my tip for `(obj_A, main)` is `C5` and the peer's is `C3`, I walk the DAG from `C5` back to `C3` and send the missing commits `C4, C5`. If the tips diverge (different branches of the DAG), both sides send their divergent commits.

This replaces `sent_tips` — same shape, but **verified** rather than optimistically assumed. `sent_tips` today says "I think I sent this." The tip frontier says "I know you have this, because you told me." `sent_tips` tracking can be removed.

This fixes **Gap 3** (lost message poisons incremental sync). Instead of `sent_tips` optimistically assuming delivery, the frontier exchange detects gaps and triggers retransmission.

It also provides **liveness during live sessions**: a periodic heartbeat includes the current frontier. If a commit was dropped in transit, the next heartbeat catches it.

##### Layer 3 — Hash Exchange (cold start)

When the client has zero memory of prior sync (storage wipe, fresh device, corrupted state), it can't use the outbox or tip frontier. Fall back to content-addressed reconciliation:

1. **Exchange object IDs**: "I have objects A, B, C" / "I'm missing B, send it."
2. **Per shared object, exchange 8-byte short commit hashes**: truncated BLAKE3 hashes. "For object A, I have `a1f3e7b2`, `c9d0f1a3`" / "I'm missing `c9d0f1a3`, send it."

```rust
/// 8-byte truncated BLAKE3 hash — virtually zero collision risk
type ShortHash = [u8; 8];

fn short_hash(commit_id: &CommitId) -> ShortHash {
    commit_id.0[..8].try_into().unwrap()
}

struct HashExchange {
    object_id: ObjectId,
    tip_hashes: Vec<ShortHash>,
    /// All known commit hashes for this object (compact)
    commit_hashes: Vec<ShortHash>,
}
```

This is the most expensive path but works from nothing. It's the ultimate fallback — the content-addressed data model makes it possible without any protocol state.

#### Server acknowledgment

The server acknowledges commits via `PersistenceAck` — the same mechanism that already exists for durability tiers. This unifies the acknowledgment path: there is no separate "transport ack." A commit is either persisted or it isn't. The client doesn't care about intermediate states.

`PersistenceAck` serves double duty: it confirms durability (resolving `_persisted()` watchers) **and** drains the outbox. This is intentional — the outbox contract is "kept until durably persisted," which is exactly what `PersistenceAck` guarantees. No separate transport-level ack exists.

**Multi-tier ack contract:** each hop's outbox is drained by the PersistenceAck from the _next_ tier in the chain. The client's outbox drains when Worker acks. The Worker's outbox drains when EdgeServer acks. EdgeServer's outbox drains when GlobalServer acks. Every hop is independently reliable — if the edge→global link drops a commit, the edge's outbox retains it and replays on reconnect.

```
Client ──► Worker ──► EdgeServer ──► GlobalServer
  outbox      outbox      outbox
  drains on   drains on   drains on
  Worker ack  Edge ack    Global ack
```

```rust
/// Server → client: these commits have been durably persisted
/// (existing PersistenceAck, now also used to drain the outbox)
struct PersistenceAck {
    object_id: ObjectId,
    branch_name: BranchName,
    confirmed_commits: HashSet<CommitId>,
    tier: DurabilityTier,
}

/// Server → client: these commits were rejected
struct CommitReject {
    object_id: ObjectId,
    commit_id: CommitId,
    reason: RejectReason,  // PermissionDenied, SchemaViolation, etc.
}
```

When a commit is rejected, it is removed from the outbox — replaying it would produce the same rejection. The rejection is surfaced to the app developer as an error event. Additionally, **the rejected commit is rolled back from local state**: it is removed from the branch, and affected query subscriptions are re-settled. The app sees the row revert to its previous state (or disappear if it was a create). Optimistic local display was wrong — the rollback corrects it.

This fixes **Gap 4** (server response ignored). The client receives explicit ack/reject for every commit and can act on both.

#### Transport trait

The protocol is transport-agnostic. Any transport just moves bytes:

```rust
trait Transport: Send + 'static {
    async fn send(&mut self, msg: &[u8]) -> Result<()>;
    async fn recv(&mut self) -> Result<Vec<u8>>;
}
```

Implementations:

- **`HttpTransport`** — POST for send, SSE for recv (current model, cleaned up)
- **`PostMessageTransport`** — client↔worker bridge, structured clone transfer
- Future: `WebSocketTransport`, `TcpTransport`, etc.

---

### Pillar 2: Content-addressed gating

Consumers never rely on arrival order or sequence numbers. Instead, they gate on **content** — specific CommitIds that must be present before proceeding. This applies at two levels: commit ordering (per-commit gate in ObjectManager) and query settlement (per-query gate in QueryManager).

#### Commit ordering

The ObjectManager gates on parent CommitIds. This is merge-strategy-specific:

- The current DAG-based model needs causal ordering (parent-before-child).
- A future commutative CRDT strategy (counters, sets) needs no ordering at all — any arrival order produces the same result.
- An OT or rebasing strategy might rewrite parent references, making DAG-based ordering meaningless.
- A compaction strategy might absorb intermediate parents, causing a parent-based buffer to deadlock.

Baking ordering into the reliability stack would couple it to one merge strategy. Instead, the ObjectManager applies a **strategy-specific commit gate** that decides when a commit is ready to process:

```rust
/// Strategy-specific readiness check — called by ObjectManager, not the reliability stack.
/// The reliability stack delivers commits; the ObjectManager decides when to process them.
trait CommitReadiness {
    /// Returns true if this commit can be processed now given the current branch state.
    fn is_ready(&self, commit: &Commit, branch: &Branch) -> bool;
}

/// Current DAG-based strategy: process when all parents are present.
struct CausalReadiness;

impl CommitReadiness for CausalReadiness {
    fn is_ready(&self, commit: &Commit, branch: &Branch) -> bool {
        commit.parents.iter().all(|p| branch.commits.contains_key(p))
    }
}
```

The ObjectManager holds a per-branch pending buffer for commits that aren't ready yet:

```rust
/// Lives in ObjectManager, not the reliability stack.
struct PendingCommits {
    /// Commits waiting for their readiness condition to be met.
    pending: HashMap<CommitId, Commit>,
}

impl PendingCommits {
    fn receive(
        &mut self,
        commit: Commit,
        branch: &Branch,
        readiness: &dyn CommitReadiness,
    ) -> Vec<Commit> {
        if readiness.is_ready(&commit, branch) {
            let mut ready = vec![commit];
            // Flush any pending commits that are now unblocked
            loop {
                let newly_ready: Vec<_> = self.pending
                    .values()
                    .filter(|c| readiness.is_ready(c, branch))
                    .map(|c| c.id())
                    .collect();
                if newly_ready.is_empty() { break; }
                for id in newly_ready {
                    if let Some(c) = self.pending.remove(&id) {
                        ready.push(c);
                    }
                }
            }
            ready
        } else {
            self.pending.insert(commit.id(), commit);
            vec![]
        }
    }
}
```

```
Arrives: C3, C1, C2  (out of order from concurrent spawns)

C3 arrives → CausalReadiness: parent C2 missing → pending
C1 arrives → CausalReadiness: root, no parents → process → flush: nothing unblocked
C2 arrives → CausalReadiness: parent C1 present → process → flush: C3 unblocked → process

ObjectManager processes: C1, C2, C3 (causal order)
```

This fixes **Gap 1** (cross-payload ordering) at the right layer — the merge strategy enforces its own ordering requirements, not the transport or the reliability stack.

If Jazz moves to a commutative merge strategy in the future, `CausalReadiness` is replaced with a strategy that returns `true` unconditionally — no buffering, no latency penalty, no reliability-stack changes.

#### Query settlement

QuerySettled tells the client: "your query result is complete — you can render." But it's a temporal claim: "everything I sent before this message is your data." That claim requires ordering between "before" and "this message." With unordered delivery, there is no "before."

The current implementation uses sequence numbers for this. The server assigns monotonic `seq` to each SyncUpdate and stamps QuerySettled with `through_seq = N`, meaning "all ObjectUpdateds with seq ≤ N are the data for this query." The client gates on having received all messages up to N. This breaks with unordered delivery — there is no "up to N" when messages arrive as 5, 2, 8, 1, 3.

The fix follows the same principle as commit ordering: replace positional gating with content-addressed gating. Instead of "everything before sequence N" (a watermark), QuerySettled carries a **coverage manifest** — "your result consists of exactly these commits" (a checklist). The client checks off each item as it arrives. When all items are present → query is settled.

The server already knows the answer. When it evaluates a query, it knows which (ObjectId, BranchName) pairs match and what their current tips are. Today it throws that information away and encodes it indirectly as a sequence number. Instead, it sends it directly:

```rust
QuerySettled {
    query_id: QueryId,
    tier: DurabilityTier,
    /// The tip frontier that defines this settled result.
    /// Query is settled when the client has all of these commits locally.
    coverage: Vec<(ObjectId, BranchName, SmallVec<[CommitId; 2]>)>,
}
```

The QueryManager holds query subscriptions pending until every entry in the coverage list is present in local storage. Each time an ObjectUpdated is processed and new commits arrive, re-check pending queries. When all coverage entries are satisfied → settled.

```rust
impl QueryManager {
    fn check_query_settlement(&mut self, query_id: QueryId) -> bool {
        let Some(pending) = self.pending_settlements.get(&query_id) else {
            return false;
        };
        pending.coverage.iter().all(|(object_id, branch_name, tips)| {
            tips.iter().all(|tip| {
                self.object_manager
                    .get(*object_id)
                    .and_then(|obj| obj.branches.get(branch_name))
                    .map(|branch| branch.commits.contains_key(tip))
                    .unwrap_or(false)
            })
        })
    }
}
```

This composes with CausalReadiness: CausalReadiness gates individual commits on parent presence (per-commit gate). Coverage gates query results on commit presence (per-query gate). They operate at different levels, independently.

**After initial settlement**, live updates flow directly to the client's subscriptions via ObjectUpdated. No new QuerySettled is needed. The coverage gate is one-time — it prevents premature rendering of a partial result on first load.

**Empty result**: coverage is `[]`. Query is immediately settled. Nothing to wait for.

**What this replaces**:

- `through_seq` on QuerySettled → replaced by `coverage`
- `seq` on SyncUpdate → no longer needed, removed entirely
- The browser client already drops both fields (`sync-transport.ts:680` drops `seq`, `inbox.rs:423` discards `through_seq`). This change makes the protocol match reality — those fields were dead weight.

**Size**: one entry per matching (ObjectId, BranchName), ~50-70 bytes each. Typical query (10-100 rows) → 500 bytes to 5 KB. Large query (10,000 rows) → ~500 KB, one-time message. Acceptable. If size proves problematic for very large queries, paginated settlement is an option — but that's optimization, not design.

---

### Architecture

The reliability stack sits above the transport. Content-addressed gating sits above the reliability stack, in ObjectManager and QueryManager.

```
┌────────────────────────────────────────────────┐
│               SyncManager                      │
│         (scoping, queries — unchanged)         │
└──────────────────┬─────────────────────────────┘
                   │ SyncPayload
┌──────────────────▼─────────────────────────────┐
│          Content-Addressed Gating              │
│                                                │
│  ObjectManager: PendingCommits + CommitReadi-  │
│    ness (per-commit, strategy-specific)        │
│  QueryManager: coverage manifest               │
│    (per-query, checks CommitId presence)       │
└──────────────────┬─────────────────────────────┘
                   │ messages (unordered)
┌──────────────────▼─────────────────────────────┐
│           Reliability Stack                    │
│  Outbox (at-least-once delivery)               │
│  TipFrontier (gap detection + reconciliation)  │
│  HashExchange (cold start)                     │
└──────────────────┬─────────────────────────────┘
                   │ bytes
           ┌───────┼────────┐
           ▼       ▼        ▼
         HTTP   postMsg   (future)
```

---

### Communication Flows

#### Flow 1: Happy path — alice writes, bob sees it

```
alice          alice's Worker       Server          bob's Worker         bob
  │                │                  │                  │                │
  │  write("hello")│                  │                  │                │
  │──postMessage──►│                  │                  │                │
  │                │  persist(C1)     │                  │                │
  │                │  outbox: [C1]    │                  │                │
  │◄─ack──────────│                  │                  │                │
  │  UI update     │                  │                  │                │
  │                │── HTTP [C1] ───►│                  │                │
  │                │                  │  persist(C1)     │                │
  │                │                  │  scope: bob      │                │
  │                │                  │── SSE [C1] ────►│                │
  │                │                  │                  │  persist(C1)   │
  │                │                  │                  │──postMessage──►│
  │                │                  │                  │                │  UI update
  │                │◄─ Ack [C1] ─────│                  │                │
  │                │  outbox: []      │                  │                │
```

#### Flow 2: Out-of-order arrival — strategy-specific ordering in ObjectManager

```
alice          alice's Worker       Server
  │                │                  │
  │  write("a") → C1                 │
  │  write("b") → C2 (parent: C1)   │
  │  write("c") → C3 (parent: C2)   │
  │                │                  │
  │  (concurrent spawns — arrive at server out of order)
  │──[C3]────────►│── [C3] ────────►│
  │──[C1]────────►│── [C1] ────────►│
  │──[C2]────────►│── [C2] ────────►│
  │                │                  │
  │                │                  │  Reliability stack delivers C3, C1, C2
  │                │                  │  (at-least-once, no ordering promise)
  │                │                  │
  │                │                  │  ObjectManager + CausalReadiness:
  │                │                  │  C3 → parent C2 missing → pending
  │                │                  │  C1 → root → process, persist
  │                │                  │  C2 → parent C1 ok → process
  │                │                  │       → flush: C3 unblocked
  │                │                  │       process C3 → persist
  │                │                  │
  │                │                  │  Processed in order: C1, C2, C3
  │                │                  │
  │                │◄─ Ack [C1,C2,C3]│
  │◄─ Ack ────────│                  │
  │  outbox.ack(C1, C2, C3)         │
```

#### Flow 3: Connection drop — outbox replay + frontier handshake

```
alice's Worker                Server
  │                            │
  │── [C1] ──────────────────► │  persist(C1)
  │◄── Ack [C1] ──────────────│
  │  outbox.ack(C1)            │
  │                            │
  │── [C2] ──────────────────► │  persist(C2)
  │                            │  (ack for C2 never reaches worker)
  │── [C3] ──────────────────X │  (lost! connection drops)
  │                            │
  ╳ ── Worker↔Server lost ──── ╳
  │                            │
  │  outbox = [C2, C3]         │  has: C1, C2
  │  (C2 persisted but unacked)│  (C3 never arrived)
  │                            │
  ╳ ── reconnect ──────────── ╳
  │                            │
  │── SyncHandshake ─────────► │
  │   frontier: {obj_A: C3}    │   frontier: {obj_A: C2}
  │                            │
  │── replay [C2, C3] ───────►│  C2: already exists, no-op
  │                            │  C3: new, persist
  │                            │
  │◄── Ack [C2, C3] ──────────│
  │  outbox.ack(C2, C3)        │
  │  outbox is now empty       │
```

Note: alice's UI was never affected. The client↔Worker link (postMessage) stayed up the whole time. alice saw her writes immediately. The outbox replay is purely Worker↔Server.

#### Flow 4: Cold start — hash exchange from nothing

```
alice's Worker (fresh)        Server
  │                            │
  │  (no outbox, no frontier,  │  has objects:
  │   no local state)          │  obj_A (C1→C2→C3)
  │                            │  obj_B (C1→C2)
  │                            │
  │── SyncHandshake ─────────► │
  │   frontier: {}             │  frontier: {obj_A: C3,
  │                            │             obj_B: C2}
  │                            │
  │   (empty frontier →        │
  │    fall through to Layer 3)│
  │                            │
  │── HashExchange ──────────► │
  │   object_ids: []           │
  │                            │
  │◄── HashExchange ──────────│
  │   object_ids: [obj_A,      │
  │                obj_B]      │
  │   obj_A hashes: [C1,C2,C3]│
  │   obj_B hashes: [C1,C2]   │
  │                            │
  │   (worker has none of them)│
  │                            │
  │◄── [obj_A: C1,C2,C3] ────│  all commits, topo sorted
  │◄── [obj_B: C1,C2] ───────│
  │                            │
  │  persist all               │
  │  forward to alice via      │
  │  postMessage               │
  │  frontier now matches      │
```

#### Flow 5: Server rejects a commit — rollback optimistic state

```
alice          alice's Worker       Server
  │                │                  │
  │  write("secret")                  │
  │──postMessage──►│                  │
  │                │  persist(C4)     │
  │                │  outbox: [C4]    │
  │◄─ack──────────│                  │
  │  UI shows "secret"               │
  │                │── [C4] ────────►│
  │                │                  │  permission check: DENIED
  │                │◄─ CommitReject ──│
  │                │   {C4, denied}   │
  │                │                  │
  │                │  outbox.remove(C4)
  │                │  rollback C4     │
  │◄─ rollback C4─│                  │
  │                │                  │
  │  re-settle queries               │
  │  UI reverts: "secret" gone       │
  │  surface error to app            │
```

#### Flow 6: Multi-tier — edge persists, edge→global drops

```
alice        Worker          EdgeServer       GlobalServer
  │            │                │                 │
  │  write→C5  │                │                 │
  │──[C5]────►│                │                 │
  │            │  persist(C5)   │                 │
  │            │  outbox: [C5]  │                 │
  │◄─ Ack[C5]─│                │                 │
  │            │──[C5]────────►│                 │
  │            │                │  persist(C5)    │
  │            │                │  outbox: [C5]   │
  │            │◄─ Ack[C5]─────│                 │
  │            │  outbox: []    │                 │
  │            │                │──[C5]──────X    │ (lost!)
  │            │                │                 │
  │            │                │  outbox: [C5]   │ (retained)
  │            │                │                 │
  │            │                ╳── reconnect ───╳│
  │            │                │                 │
  │            │                │── Handshake ───►│
  │            │                │  frontier:{C5}  │  frontier:{C4}
  │            │                │                 │
  │            │                │──[C5] replay──►│
  │            │                │                 │  persist(C5)
  │            │                │◄─ Ack[C5]──────│
  │            │                │  outbox: []     │
  │            │                │                 │
  │            │  (all tiers    │                 │
  │            │   now durable) │                 │
```

#### Flow 7: Bidirectional — alice and bob both write while server is down

Alice and bob each have their own local Worker. The client↔Worker link (postMessage)
never breaks. "Offline" = Worker↔Server link is down.

```
alice     alice's Worker       Server       bob's Worker      bob
  │            │                  │               │             │
  │  write→C_a1                  │                        write→C_b1
  │  write→C_a2                  │                        write→C_b2
  │  (UI updated                 │                  (UI updated
  │   immediately)               │                   immediately)
  │──[C_a1,C_a2]►│              │              │◄[C_b1,C_b2]──│
  │            │  persist local   │   persist local  │           │
  │            │                  │               │             │
  │            ╳── server down ──╳── server down─╳             │
  │            │                  │               │             │
  │            │  outbox:         │          outbox:│            │
  │            │  [C_a1, C_a2]   │     [C_b1, C_b2]            │
  │            │                  │               │             │
  │   (alice and bob continue    │    (both see their own       │
  │    using the app locally)    │     writes via local Worker) │
  │            │                  │               │             │
  │            ╳── server up ────╳── server up ──╳             │
  │            │                  │               │             │
  │            │── Handshake ───►│               │             │
  │            │   frontier:      │               │             │
  │            │   {obj: C_a2}    │               │             │
  │            │                  │   frontier:    │             │
  │            │                  │   {obj: C_old} │             │
  │            │                  │               │             │
  │            │── replay ──────►│               │             │
  │            │   [C_a1, C_a2]  │  persist both  │             │
  │            │◄─ Ack ──────────│               │             │
  │            │  outbox: []      │               │             │
  │            │                  │               │             │
  │            │                  │◄── Handshake ─│             │
  │            │                  │   frontier:    │             │
  │            │                  │   {obj: C_b2}  │             │
  │            │                  │               │             │
  │            │                  │◄── replay ────│             │
  │            │                  │  [C_b1, C_b2]  │             │
  │            │                  │  persist both  │             │
  │            │                  │── Ack ────────►│             │
  │            │                  │               │  outbox: []  │
  │            │                  │               │             │
  │            │                  │  (server now has all 4)     │
  │            │                  │  scope: forward to peers    │
  │            │                  │               │             │
  │            │◄─ [C_b1,C_b2] ──│               │             │
  │◄───────────│                  │── [C_a1,C_a2]►│             │
  │            │                  │               │────────────►│
  │            │                  │               │             │
  │  both see all 4 commits      │               │             │
  │  DAG: C_old ─→ C_a1 → C_a2  │  (diverged tips,           │
  │              └→ C_b1 → C_b2  │   LWW merge)               │
```

#### Flow 8: Query settlement — coverage manifest gates rendering

Bob subscribes to a query matching 3 objects. The server sends data and settlement in
any order — the coverage manifest ensures bob doesn't render until all data has arrived.

```
bob's Worker                 Server
  │                            │
  │── QuerySubscription ─────►│
  │   query: "tasks where      │
  │    project = X"            │
  │                            │  evaluate query:
  │                            │  matches obj_A (tip: C3),
  │                            │          obj_B (tip: C7),
  │                            │          obj_C (tip: C2)
  │                            │
  │  (server sends all 4 messages — reliability stack
  │   delivers them in arbitrary order)
  │                            │
  │◄── QuerySettled ───────────│  coverage: [(obj_A, C3),
  │    (arrives first!)        │            (obj_B, C7),
  │                            │            (obj_C, C2)]
  │                            │
  │  QueryManager checks:      │
  │  missing C3, C7, C2        │
  │  → query stays pending     │
  │                            │
  │◄── ObjectUpdated [obj_A] ──│  commits: [C1, C2, C3]
  │  have C3 now → re-check    │
  │  still missing C7, C2      │
  │                            │
  │◄── ObjectUpdated [obj_C] ──│  commits: [C1, C2]
  │  have C2 now → re-check    │
  │  still missing C7          │
  │                            │
  │◄── ObjectUpdated [obj_B] ──│  commits: [C5, C6, C7]
  │  have C7 now → re-check    │
  │  all coverage satisfied    │
  │  → query SETTLED           │
  │                            │
  │──postMessage──►bob          │
  │  "query ready, render"     │
```

Note: if ObjectUpdateds had arrived before QuerySettled, the coverage check
would pass immediately when QuerySettled arrives — same result, different order.

---

### How each gap is resolved

| Gap                          | Root cause                                      | Fix                                                                                                                                                              |
| ---------------------------- | ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Cross-payload ordering    | Spawns race, no cross-payload ordering          | Two content-addressed gates: PendingCommits + CausalReadiness in ObjectManager (per-commit), coverage manifest in QueryManager (per-query). No sequence numbers. |
| 2. Outbox drained early      | Fire-and-forget POST                            | Outbox: retain until server persistence ack                                                                                                                      |
| 3. Lost message poisons sync | No loss detection, `sent_tips` assumes delivery | TipFrontier: periodic exchange detects gaps, triggers resend                                                                                                     |
| 4. Response body ignored     | POST response discarded                         | PersistenceAck drains outbox; CommitReject surfaces errors                                                                                                       |
| 5. Asymmetric reconnect      | Send side has no replay                         | Outbox replay + TipFrontier handshake on reconnect — both directions                                                                                             |

---

## Rabbit Holes

- **PendingCommits liveness.** If a parent commit is permanently lost (sender crashed, never retransmitted), dependent commits sit in the ObjectManager's pending buffer forever. Need a timeout: if a commit has been pending for N seconds, request the missing parent explicitly via the tip frontier exchange. This timeout is strategy-specific — a CausalReadiness strategy needs it; a commutative strategy never buffers, so never hits it. Get this timeout right — too short causes unnecessary requests, too long causes visible stalls.

- **Tip frontier size.** One entry per (object, branch) — compact by design. But in a session subscribing to thousands of objects, the frontier exchange could still be large. Consider delta-encoding: only exchange entries that changed since the last handshake. Start with full exchange — optimize only if size proves to be a problem.

- **Outbox persistence.** The outbox lives in the Worker's persistent storage (OPFS), alongside commits. This is settled — but the implementation must ensure outbox writes are atomic with commit writes. If the Worker crashes between persisting a commit and adding it to the outbox, the commit exists but won't be replayed. Use a single transaction for both.

- **8-byte hash collisions.** At 8 bytes, birthday-problem collision risk is ~1 in 2^32 per object. Negligible in practice, but a collision would cause a commit to be skipped during hash exchange. Consider: should the protocol verify full hashes after short-hash matching, as a correctness check?

- **Theoretical concern: shared data/control channel.** Data and control messages (ObjectUpdated, QuerySubscription, PersistenceAck, Error) share the same path. A backlog of large data payloads could delay time-sensitive control messages. No concrete incident observed — validate during implementation and consider channel splitting if it proves real.

- **PersistenceAck latency.** Draining the outbox on persistence ack means the outbox stays fuller than a transport-ack design. If persistence is slow (disk I/O, replication), the outbox grows. This is the correct trade-off (reliability over memory), but monitor outbox size under load.

## No-gos

- **No transport migration.** HTTP+SSE stays for network, postMessage stays for worker. The Transport trait enables future migration but this project doesn't change wire transports.

- **No conflict resolution changes.** LWW-by-row merge semantics are unchanged. The DAG is preserved for future merge algorithms, but this project doesn't implement them. Merge-conflict notification is tracked separately (`todo/ideas/2_launch/concurrent-merge-notification.md`).

- **No message prioritization or channel splitting.** Data and control messages share one path. If the theoretical head-of-line blocking concern proves real, channel splitting is future work.

- **No backward compatibility.** Breaking protocol change. Old clients cannot talk to new servers. Acceptable — the framework is pre-production.

- **No end-to-end encryption.** Reliability guarantees only; security is orthogonal.

- **No flow control or backpressure.** Outbox persistence provides a crude buffer, but explicit "slow down" signaling is out of scope.

- **No commit compaction.** Storage optimization (compacting old commits to DAG-only metadata) is a separate concern. None of the five reliability gaps require it. Tracked as a future project.

## Testing Strategy

Integration-first at the RuntimeCore/SyncManager level. Realistic fixtures with human actors.

- **Causal ordering**: alice sends 100 commits via concurrent spawns. bob's subscription reflects them in causal order. A `ChaoticTransport` mock randomly reorders, delays, and duplicates messages at the transport level. The reliability stack delivers them unordered; ObjectManager + CausalReadiness processes them in parent-before-child order regardless. Separately: verify that replacing CausalReadiness with a no-op readiness (always ready) still converges under a commutative merge strategy — proving the reliability stack is strategy-agnostic.

- **Outbox retention and replay**: alice sends 5 commits while transport is stalled. Verify all 5 remain in outbox. Unstall — verify delivery via PersistenceAck. Then: alice sends 10, connection drops after server persists 6. On reconnect, alice replays 4 unacked commits. bob sees all 10.

- **Idempotent delivery**: alice replays commits the server already has (reconnect scenario). Verify `receive_commit()` returns early for all duplicates — no double-processing, no duplicate notifications.

- **Tip frontier reconciliation**: alice and server exchange tip frontiers. Server detects alice is missing 3 commits (its tips are ahead). Server walks the DAG back from its tips to alice's tips, sends the missing commits. alice confirms receipt. Verify no spurious duplicates.

- **Cold start hash exchange**: alice connects with empty state (fresh device). Exchanges object IDs with server, then 8-byte commit hashes per object. Receives all missing commits. Verify alice converges to server state.

- **PersistenceAck drains outbox**: alice sends 5 commits. Server persists 3 and sends PersistenceAck. Verify outbox shrinks to 2. Server persists remaining 2 and acks. Verify outbox is empty.

- **CommitReject handling**: alice sends a commit that the server rejects (PermissionDenied). Verify alice receives CommitReject, commit is removed from the outbox (not retried), and the rejection is observable by the app developer.

- **Multi-tier edge→global loss**: alice writes through Worker → EdgeServer → GlobalServer. Edge persists and acks to Worker. But the edge→global link drops the commit. Verify: edge's outbox retains the commit, edge replays to global on reconnect, global eventually persists and acks back through the chain. This is the scenario from `1_problem.md` Gap 3 — a commit durably stored at one tier but lost between tiers.

- **Bidirectional reliability**: alice writes, server writes (via bob), both directions lose messages via `ChaoticTransport`. Verify both sides converge to the same state.

- **Worker bridge parity**: Same test suite runs against `HttpTransport` and `PostMessageTransport`. Behavior must be identical.

- **Query settlement with coverage manifest**: bob subscribes to a query matching 5 objects. Server sends QuerySettled (with coverage) before all ObjectUpdateds arrive. Verify query stays pending until all coverage entries are satisfied. Then: same test but QuerySettled arrives last — verify immediate settlement. Use `ChaoticTransport` to randomize arrival order across 100 runs — settlement must be correct regardless of order.

- **Query settlement — empty result**: bob subscribes to a query matching 0 objects. Server sends QuerySettled with empty coverage. Verify query settles immediately.

- **Query settlement — live updates after initial settlement**: bob's query is settled. alice writes a new row matching the query. Verify bob receives the ObjectUpdated directly via subscription — no new QuerySettled needed, no re-gating.

- **Enable the ignored test**: `subscription_reflects_final_state_after_rapid_bulk_updates` should pass with the new protocol. This is the primary acceptance criterion.
