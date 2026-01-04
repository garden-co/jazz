# Multi-Row & Multi-Table Transactions

## Overview

This document describes an opt-in transaction system layered on top of Jazz's eventually-consistent, per-row sync model. Transactions provide multi-row atomicity with serializable isolation when needed, while preserving the default local-first behavior for most operations.

## Design Principles

1. **Opt-in complexity**: Most edits remain transaction-free (eventually consistent)
2. **Edges as resolvers**: Core never validates transactions, only manages leases
3. **No lease revocation**: Leases expire naturally (TTL-based), never forcibly revoked
4. **Sync-confirmed commits**: Transactions only succeed after server confirmation

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           CORE                                   │
│                    (Lease Authority Only)                        │
│                                                                  │
│   - Grants/renews leases                                        │
│   - Tracks lease ownership                                      │
│   - NEVER resolves transactions                                 │
│   - Sharded by key range for scalability                        │
└─────────────────────────────────────────────────────────────────┘
        │                    │                    │
        │ lease grants       │                    │
        ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Edge EU    │◄──►│   Edge US    │◄──►│   Edge JP    │
│              │    │              │    │              │
│ Leases: A,B,C│    │ Leases: D,E,F│    │ Leases: X,Y,Z│
│              │    │              │    │              │
│ Resolves txns│    │ Resolves txns│    │ Resolves txns│
│ for A,B,C    │    │ for D,E,F    │    │ for X,Y,Z    │
│              │    │              │    │              │
│ Lease Cache: │    │ Lease Cache: │    │ Lease Cache: │
│  D,E,F→US    │    │  A,B,C→EU    │    │  A,B,C→EU    │
│  X,Y,Z→JP    │    │  X,Y,Z→JP    │    │  D,E,F→US    │
└──────────────┘    └──────────────┘    └──────────────┘
```

### Component Responsibilities

| Component | Role | In Transaction Hot Path? |
|-----------|------|--------------------------|
| Core | Lease authority only | No (only on cache miss) |
| Edges | Resolve transactions, hold data | Yes |
| Lease cache | Track who owns what | Yes (avoids Core) |

---

## Two Modes of Operation

### Mode 1: No Transaction (Default)

Most edits bypass transactions entirely. Current Jazz behavior:
- Write locally, sync eventually
- Per-row conflict resolution via merge strategies
- Fast, optimistic, offline-capable

```rust
// Single-row edit - no transaction, eventually consistent
db.update("users", user_id, &[("name", "Alice")])?;
// Commits locally, syncs async
```

### Mode 2: Explicit Transaction (Sync-Confirmed)

When strong consistency is needed across rows:

```rust
// Multi-row transaction - sync-confirmed
let tx = db.begin_transaction().await?;

// Read (tracked in read set for validation)
let user = tx.read("users", user_id)?;
let account = tx.read("accounts", account_id)?;

// Write (buffered until commit)
tx.update("users", user_id, &[("balance", new_balance)])?;
tx.insert("audit_log", &[("user", user_id), ("action", "transfer")])?;

// Commit - sends to edge(s), waits for confirmation
tx.commit().await?;
```

---

## MVCC Using Commit Graphs

Each row already has a commit graph. We extend this for cross-row snapshots.

### Hybrid Logical Clocks (HLC)

HLC provides causality ordering without synchronized clocks:

```rust
struct HybridTimestamp {
    physical: u64,  // Wall clock (milliseconds)
    logical: u16,   // Counter for same-millisecond events
}
```

**Properties:**
- If event A caused event B, then `HLC(A) < HLC(B)`
- Stays close to real time (bounded drift)
- Total ordering for any two events

### Snapshot Reads

A transaction reads a consistent snapshot across rows:

```
Row A: [commit@T1] → [commit@T5] → [commit@T10]
Row B: [commit@T2] → [commit@T7]

Transaction at snapshot T6: sees A@T5, B@T2
```

### Read-Your-Writes

Pending writes visible within the transaction:

```rust
let tx = db.begin_transaction().await?;
tx.update("users", user_id, &[("name", "Bob")])?;

// See own pending changes
let user = tx.read("users", user_id)?;  // sees "Bob"

// Others still see old value until commit
```

---

## Optimistic Concurrency Control (OCC)

Primary concurrency mechanism (optimized for low contention).

### Read Set and Write Set

```rust
struct Transaction {
    start_ts: HybridTimestamp,

    // What we read and at which version
    read_set: HashMap<(String, ObjectId), CommitId>,

    // Buffered writes (not applied until commit)
    write_set: HashMap<(String, ObjectId), WriteOp>,
}

enum WriteOp {
    Insert(Row),
    Update(Vec<(String, Value)>),
    Delete,
}
```

### Validation Flow

```rust
// During transaction
fn read(&mut self, table: &str, id: ObjectId) -> Option<Row> {
    // Check write set first (read-your-writes)
    if let Some(pending) = self.write_set.get(&(table, id)) {
        return self.apply_pending(pending);
    }

    // Read from database, track version
    let (row, commit_id) = self.db.read_with_version(table, id)?;
    self.read_set.insert((table.to_string(), id), commit_id);
    Some(row)
}

// At commit time (on edge)
fn validate(&self, tx: &Transaction) -> Result<(), Conflict> {
    for ((table, id), version_read) in &tx.read_set {
        let current = self.current_version(table, id);
        if current != version_read {
            return Err(Conflict::VersionMismatch { table, id });
        }
    }
    Ok(())
}
```

---

## Lease System

### Lease Structure

```rust
struct Lease {
    row_id: ObjectId,
    holder: EdgeId,
    granted_at: HybridTimestamp,
    expires_at: HybridTimestamp,
}
```

### Core Lease Management

```rust
impl Core {
    // Grant lease (if available or expired)
    fn request_lease(&self, edge: EdgeId, row_id: ObjectId) -> LeaseResult {
        match self.leases.get(&row_id) {
            None => self.grant(edge, row_id),
            Some(lease) if lease.expired() => self.grant(edge, row_id),
            Some(lease) if lease.holder == edge => self.renew(edge, row_id),
            Some(_) => LeaseResult::Unavailable {
                retry_after: time_until_expiry,
            },
        }
    }

    // Renew existing lease
    fn renew_lease(&self, edge: EdgeId, row_id: ObjectId) -> LeaseResult;

    // Query ownership (for cache misses)
    fn who_owns(&self, row_id: ObjectId) -> Option<LeaseInfo>;

    // Never: revoke, transfer, or resolve transactions
}
```

### No Lease Revocation

Leases are never forcibly revoked. They only expire naturally.

**Benefits:**
- Simpler protocol (no revocation coordination)
- Edge can rely on lease for full TTL
- No distributed consensus needed for revocation

**Trade-off:**
- Edge crash blocks rows until lease expires
- TTL = maximum unavailability window

### Lease Cache on Edges

Edges cache lease ownership to avoid Core lookups:

```rust
struct LeaseCache {
    // row_id → (owner, expiry)
    entries: HashMap<ObjectId, CachedLease>,
}

impl LeaseCache {
    fn lookup(&self, row_id: ObjectId) -> Option<EdgeId> {
        self.entries.get(&row_id)
            .filter(|c| !c.expired())
            .map(|c| c.owner)
    }
}
```

Cache populated via:
- Core responses to lease requests
- Responses from other edges during transactions
- Background gossip (optional)

---

## Transaction Flow

### Single-Edge Transaction (Fast Path)

All rows leased to the coordinating edge:

```
Client ──▶ Edge EU
           │
           │ tx touches {A, B}
           │ I hold leases for both
           │
           └──▶ Validate locally
           └──▶ Commit locally
           └──▶ Done

Latency: ~1-5ms
```

### Multi-Edge Transaction

Rows span multiple edges:

```
Client ──▶ Edge EU (coordinator)
           │
           │ tx touches {A, D}
           │ A: I hold lease
           │ D: Cache says Edge US holds lease
           │
           ├──▶ Edge US: Validate(D, read_set)
           │         │
           │         └──▶ Check version, mark pending (5s TTL)
           │         └──▶ Return: OK
           │
           ├──▶ Local: Validate(A)
           │
           │ All valid?
           │
           ├──▶ Edge US: Commit(D, writes)
           ├──▶ Local: Commit(A, writes)
           │
           └──▶ Done

Latency: ~10-50ms (cross-edge RTT)
```

### Cache Miss

```
Client ──▶ Edge EU
           │
           │ tx touches {A, Q}
           │ A: I hold lease
           │ Q: Not in cache
           │
           └──▶ Core: who_owns(Q)?
                    │
                    └──▶ Edge JP, expires T+30s
           │
           │ (update cache)
           │
           └──▶ Continue with Edge JP...

Latency: ~50-100ms (Core lookup + cross-edge)
```

---

## Validation Protocol

### Validate-Then-Commit (Simplified 2PC)

```rust
impl Edge {
    async fn commit_multi_edge(&self, tx: Transaction) -> Result<(), TxError> {
        let partitions = self.partition_by_owner(&tx);

        // Phase 1: Validate all (parallel)
        let validations = partitions.iter()
            .map(|(edge, rows)| edge.validate(rows, &tx.read_set));

        let results = futures::join_all(validations).await;

        if results.iter().any(|r| r.is_err()) {
            // Nothing committed, just return error
            return Err(TxError::Conflict);
        }

        // Phase 2: Commit all (parallel)
        let commits = partitions.iter()
            .map(|(edge, rows)| edge.commit(rows, &tx.write_set));

        futures::join_all(commits).await;
        Ok(())
    }
}
```

### Pending State with Timeout

```rust
impl Edge {
    fn validate(&self, req: ValidateRequest) -> ValidateResult {
        // Check versions
        for (row_id, version) in &req.read_set {
            if self.current_version(row_id) != version {
                return ValidateResult::Conflict;
            }
        }

        // Mark pending (soft lock with timeout)
        self.pending.insert(req.tx_id, PendingTx {
            rows: req.rows.clone(),
            expires: now() + PENDING_TIMEOUT,  // e.g., 5 seconds
        });

        ValidateResult::Ok
    }

    // Background: expired pending state auto-clears
    fn cleanup_expired(&self) {
        self.pending.retain(|_, p| !p.expired());
    }
}
```

### Coordinator Crash Recovery

If coordinator crashes between validate and commit:
- Pending state on participant edges expires (5s timeout)
- No commits happened, no inconsistency
- Client retries get fresh transaction

If coordinator crashes after partial commits:
- Some edges committed, some didn't
- Options:
  1. Log commit intent before sending commits, replay on recovery
  2. Accept rare inconsistency, application-level reconciliation

---

## Backpressure

Edges signal overload without touching leases:

```rust
impl Edge {
    fn handle_validate(&self, req: ValidateRequest) -> ValidateResult {
        if self.pending.len() > MAX_PENDING {
            return ValidateResult::Overloaded {
                retry_after_ms: 100,
            };
        }

        self.do_validate(req)
    }
}

impl Coordinator {
    async fn validate_with_backpressure(&self, edge: EdgeId, req: ValidateRequest) {
        let mut delay = Duration::from_millis(10);

        loop {
            match edge.validate(req.clone()).await {
                ValidateResult::Overloaded { retry_after_ms } => {
                    sleep(Duration::from_millis(retry_after_ms)).await;
                    delay = min(delay * 2, MAX_BACKOFF);
                }
                result => return result,
            }
        }
    }
}
```

---

## Availability Characteristics

### Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Edge crash | Rows blocked until lease TTL | Wait for expiry |
| Edge overload | Transactions timeout | Backpressure, retry |
| Network partition | Multi-edge txns fail | Single-edge txns continue |
| Core down | No new leases/renewals | Edges continue until lease expiry |

### Lease TTL Trade-offs

| TTL | Crash Recovery | Renewal Overhead |
|-----|----------------|------------------|
| 5s | Fast (5s max outage) | High |
| 30s | Slow (30s max outage) | Medium |
| 5min | Very slow | Low |

**Recommendation**: 10-30s TTL with renewal at TTL/3 intervals.

### CAP Trade-offs

```
┌───────────────────────────────┐   ┌────────────────────────────────┐
│      Single-Edge Txns         │   │       Multi-Edge Txns          │
│                               │   │                                │
│ ✓ Available if edge is up     │   │ ✗ Needs ALL edges available   │
│ ✓ Partition tolerant          │   │ ✗ Fails on partition          │
│ ✓ Fast (local)                │   │ ✗ Slower (network)            │
└───────────────────────────────┘   └────────────────────────────────┘
```

---

## Transaction Guarantees

| Property | Guarantee |
|----------|-----------|
| Atomicity | All writes succeed or none do |
| Consistency | Application invariants maintained |
| Isolation | Serializable (no concurrent txn sees partial state) |
| Durability | Commit returns only after edge(s) persist |

---

## Prior Art

| System | Similarity |
|--------|------------|
| **FoundationDB** | Single-threaded resolvers (our edges), OCC validation |
| **Google Chubby** | Lease-based distributed locks (our Core) |
| **CockroachDB** | HLC timestamps for MVCC |
| **Spanner** | Sync-confirmed commits, strong consistency |

---

## Core Replication (Future)

If Core HA is needed, options:

1. **Raft consensus**: Each Core shard replicated via Raft
2. **Managed database**: Use DynamoDB/Spanner for lease table
3. **Single Core**: Accept occasional downtime (edges continue with existing leases)

---

## Implementation Phases

1. **Phase 1**: Local multi-row transactions (single node)
2. **Phase 2**: HLC timestamps (replace wall clock)
3. **Phase 3**: Distributed OCC (cross-edge validation)
4. **Phase 4**: Lease system (Core + edge leases)
5. **Phase 5**: Backpressure and monitoring

---

## Open Questions

1. **Conflict granularity**: Row-level vs column-level?
2. **Read-only transactions**: Skip validation entirely?
3. **Deadlock handling** (if pessimistic path added later): Detection vs prevention?
