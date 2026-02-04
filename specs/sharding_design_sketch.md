# Sharding Design Sketch

Status: Design exploration (not yet implemented)

## Problem Statement

We want to support a multi-tier architecture:

```
         core-server (multiple, auto-scaling)
              /\
              ||
              \/
         edge-server (multiple, auto-scaling)
              /\
              ||
              \/
            client
```

With **shared storage shards** behind the core servers:

```
┌─────────┐  ┌─────────┐  ┌─────────┐
│ Core-1  │  │ Core-2  │  │ Core-3  │
└────┬────┘  └────┬────┘  └────┬────┘
     │           │           │
     └───────────┼───────────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
┌───┴───┐  ┌────┴────┐  ┌────┴────┐
│Shard-1│  │ Shard-2 │  │ Shard-3 │
└───────┘  └─────────┘  └─────────┘
```

**The challenge**: Indices act as a source of truth for enumeration (all rows in table, filtered rows). With multiple core servers having overlapping but different views, how do we maintain consistency?

**Constraints**:

- Index updates should be locally consistent on write (if core-1 writes a row, core-1's view must immediately reflect it)
- Global index consistency can be eventual
- Tables can have billions of rows
- No external dependencies (Kafka, Redis, etc.) - we build the coordination ourselves
- No user-supplied partition keys - everything should be auto-tuning
- Objects should not move between shards once written

## Architecture Decision: Smart Shards

We chose **smart shards with local indices** over replicated indices at cores:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Core Servers                                  │
│         (query coordinators, shard map cache)                    │
└─────────────────────────────────────────────────────────────────┘
                    ↕ fan-out queries / receive deltas
┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
│ Shard-1 │  │ Shard-2 │  │ Shard-3 │  │ Shard-4 │
│ objects │  │ objects │  │ objects │  │ objects │
│ indices │  │ indices │  │ indices │  │ indices │
└─────────┘  └─────────┘  └─────────┘  └─────────┘
```

**Rationale**:

- No N-way index replication between cores
- Index size scales with shard count, not core server count
- Core servers can be truly stateless/autoscaling
- Clear ownership: each shard owns indices for its data

## Query Fan-out Strategy

### Full Fan-out with Local Filtering

For queries, we accept full fan-out to all shards but make empty responses cheap:

```
Query for (app, table):
1. Core fans out to all N shards
2. Each shard checks: "do I have any rows for (app, table)?"
   - If no: respond "empty" immediately (O(1) lookup)
   - If yes: execute query, return results
3. Core merges non-empty results
```

**Why this works**:

- "Do I have data for table X?" is O(1) (hash set of known tables, derived from index)
- Empty response is trivial (no data, no serialization)
- Parallel fan-out: latency ≈ slowest shard, not sum of shards
- No coordination, no registry, no synchronization

**Cost model**:

```
N = 1000 shards
Small table (100 rows): ~100 shards respond "empty", ~1-10 return data
Large table (1B rows): ~1000 shards each return ~1M rows
```

The overhead for small tables is N network round-trips with ~1 byte payload - acceptable at modern network speeds.

## Adaptive Sharding: Slot-Based Table Growth

### The Problem with Uniform Hashing

If we hash `(app, table, object_id)` uniformly, even small tables spread across many shards. We want:

- Small tables: concentrated on few shards
- Large tables: spread across many shards
- No object movement as tables grow

### Solution: Sequential Slot Filling

Every `(app, table)` starts on one "home" shard and expands as needed:

```
Table lifecycle:

1. New table: all writes → shard(app, table, 0) = "home"
2. Home fills up → rejects with "full, max_id=X"
3. Core updates shard map: [(slot_0, max=X), (slot_1, current)]
4. New writes → shard(app, table, 1)
5. Repeat as table grows
```

**Properties**:

- Small tables: exactly 1 shard (optimal)
- Large tables: proportional shards (optimal)
- No object movement (writes stay where they landed)
- Self-healing (stale cores get "full" rejection, update, retry)

### Shard Assignment with Rendezvous Hashing

To ensure stability when adding/removing shard servers, we use **rendezvous hashing** (highest random weight):

```rust
fn get_shard(app: AppId, table: TableId, slot: u32) -> ShardId {
    let mut best_shard = None;
    let mut best_score = 0;

    for shard in all_shards {
        let score = hash(app, table, slot, shard.id);
        if score > best_score {
            best_score = score;
            best_shard = Some(shard);
        }
    }

    best_shard.unwrap()
}
```

**Properties**:

- Adding a shard: only keys where new shard scores highest move to it (~1/N fraction)
- Removing a shard: only keys on that shard redistribute
- Deterministic, no central coordinator needed

### Home Shard as Coordinator

The home shard (slot 0) acts as coordinator for the table's shard map:

```
When shard N becomes full:
  1. Shard N marks self as full, records max_id
  2. Shard N notifies home shard: "slot N full, max_id = X"
  3. Home shard updates shard map
  4. Home shard notifies subscribed cores: "slot N closed, slot N+1 now current"

Discovery (cold start):
  Core asks home shard: "give me shard map for (app, table)"
  Home returns: [(slot_0, max_X0), (slot_1, max_X1), (slot_2, current)]
  Single round-trip
```

### Handling Concurrent Writes at Threshold

When a shard becomes "full":

```
Shard reports: "full, max_id = X"

Write A (id < X): accepted (in-flight concurrent write)
Write B (id > X): rejected → "full, max=X" → core retries on next slot
```

UUIDv7's time-ordering makes this natural - concurrent writes have nearby IDs.

## Replication: Top-2 Rendezvous Hashing

Each slot has a primary and replica, determined by taking the top-2 scoring shards:

```rust
fn get_shards(app: AppId, table: TableId, slot: u32) -> (ShardId, ShardId) {
    let mut scores: Vec<(ShardId, u64)> = all_shards
        .iter()
        .map(|s| (s.id, hash(app, table, slot, s.id)))
        .collect();

    scores.sort_by(|a, b| b.1.cmp(&a.1));  // descending

    (scores[0].0, scores[1].0)  // primary, replica
}
```

### Write Path with Replication

```rust
fn write(app: AppId, table: TableId, object: Object) -> Result<()> {
    let slot = current_slot(app, table);
    let (primary, replica) = get_shards(app, table, slot);

    // Option A: Sync replication (strong consistency)
    let result_p = primary.write(&object)?;
    let result_r = replica.write(&object)?;

    // Option B: Primary + async replica (lower latency)
    let result = primary.write(&object)?;
    primary.replicate_async(replica, &object);

    // Handle "full" response
    if let Err(Full { max_id }) = result {
        update_shard_map(app, table, slot, max_id);
        // Retry on next slot
    }

    Ok(())
}
```

### Read Path with Replication

```rust
fn query(app: AppId, table: TableId, query: Query) -> Results {
    let shard_map = get_shard_map(app, table);
    let mut results = vec![];

    for slot_info in shard_map {
        let (primary, replica) = get_shards(app, table, slot_info.slot);

        // Primary preferred, fallback to replica
        let slot_results = primary.query(&query)
            .or_else(|| replica.query(&query));

        results.extend(slot_results);
    }

    merge_results(results)
}
```

### Failure Handling

**Primary fails**:

- Core detects timeout, fails over to replica
- Replica serves reads and accepts writes
- When primary recovers: sync from replica, resume role

**Replica fails**:

- Primary continues serving
- Writes not replicated (degraded mode)
- When replica recovers: sync from primary, resume role

**Both fail**:

- Slot unavailable
- Queries return partial results (other slots work)
- Writes to that slot fail

## Subscription Model: Two-Level Subscriptions

### The Problem

With subscriptions, we can't determine relevant shards at subscribe time - new data may be written to a new slot that didn't exist when we subscribed.

### Solution: Table-Level + Query-Level Subscriptions

**Level 1** (always present on all shards):

- "Core C is interested in table T"
- Very cheap: just a `(core_id, table_id)` pair

**Level 2** (only on shards with data):

- Full query details: predicates, projections
- Sent on-demand when shard first gets data for table

### Subscribe Flow

```
1. Core subscribes to query on (app, table)

2. If first query for this table:
   Core → all shards: "I'm interested in (app, table)"
   Shards store: interested_cores[(app, table)].insert(core_id)

3. For shards in current shard map (have data):
   Core → shard: "Here are my queries for this table"
   Shard stores full query details

4. When shard first gets data for (app, table):
   Shard → core: "I now have data, send me your queries"
   Core → shard: full query details

5. When table expands (new slot):
   Home shard → cores: "New slot N for (app, table)"
   Cores subscribe to new slot's primary/replica
```

### Unsubscribe Flow

```
1. Core removes query locally

2. If last query for this table:
   Core → all shards: "No longer interested in (app, table)"

3. If not last query:
   Core → shards with data: "Remove query Q"
   (Level 1 interest remains)
```

## Shard Migration on Cluster Changes

When adding/removing shard servers, some slots need to migrate to new primaries/replicas.

### Epoch-Based Migration

Each slot assignment has an epoch number:

```rust
struct SlotAssignment {
    slot: (AppId, TableId, u32),
    epoch: u64,
    primary: ShardId,
    replica: ShardId,
}
```

### Migration State Machine

```
┌─────────────┐
│  Copying    │  New shard receiving data from old
└──────┬──────┘
       │ caught up
       ▼
┌─────────────┐
│  DualWrite  │  Cores write to both old and new
└──────┬──────┘
       │ quiet period (no old-epoch writes)
       ▼
┌─────────────┐
│  Draining   │  Old shard rejects writes, serves reads
└──────┬──────┘
       │ quiet period (no requests)
       ▼
┌─────────────┐
│  Complete   │  Old shard data deleted
└─────────────┘
```

### Migration Flow

**Phase 1: Copying**

```
1. Cluster membership changes (new shard added)
2. Recalculate rendezvous hash: identify affected slots
3. For each affected slot:
   - New primary subscribes to old primary's replication stream
   - New primary receives all existing data + ongoing writes
```

**Phase 2: DualWrite**

```
1. New primary caught up (replication lag ≈ 0)
2. Home shard broadcasts: "slot X entering DualWrite, epoch 1→2"
3. Cores update shard map, write to BOTH old and new
4. Old shard tracks: last write with epoch 1
```

**Phase 3: Draining**

```
1. Old shard observes: no epoch-1 writes for T seconds
2. Old shard transitions to Draining:
   - Rejects writes with redirect to new primary
   - Still serves reads (for very stale cores)
3. Old shard tracks: last read for this slot
```

**Phase 4: Complete (safe to delete)**

```
1. Old shard observes: no requests for this slot for T seconds
2. Old shard deletes data
3. Old shard notifies home: "migration complete"
```

### Request Routing with Epochs

```rust
fn handle_write(req: WriteRequest) -> Result<(), Error> {
    let my_epoch = self.get_epoch(req.slot);

    match req.epoch.cmp(&my_epoch) {
        Equal => self.do_write(req.object),

        Less => Err(Redirect {
            new_primary: self.successor(req.slot),
            new_epoch: my_epoch,
        }),

        Greater => Err(EpochAhead { my_epoch }),
    }
}
```

### Safety Property

**Old shard only deletes after observing silence**:

- No old-epoch writes for T1 (e.g., 5 minutes)
- No requests at all for T2 (e.g., 1 hour)

**Any late request gets redirected** - no data loss, just a retry.

### Configuration

```yaml
migration:
  catchup_timeout: 30s # max time for new shard to sync
  dual_write_quiet_period: 5m # no old-epoch writes before Draining
  drain_quiet_period: 1h # no requests before delete
  max_migration_duration: 24h # absolute maximum before forced delete
```

## Shard API Summary

```rust
trait Shard {
    // Object operations
    fn write(&self, object: Object, epoch: u64) -> Result<(), WriteError>;
    fn read(&self, object_id: ObjectId) -> Option<Object>;

    // Query operations
    fn query(&self, app: AppId, table: TableId, query: Query) -> QueryResult;
    fn subscribe(&self, app: AppId, table: TableId, query: Query) -> Subscription;

    // Table interest (Level 1 subscription)
    fn register_interest(&self, core: CoreId, app: AppId, table: TableId);
    fn unregister_interest(&self, core: CoreId, app: AppId, table: TableId);

    // Shard map (home shard only)
    fn get_shard_map(&self, app: AppId, table: TableId) -> Vec<SlotInfo>;
    fn notify_slot_full(&self, app: AppId, table: TableId, slot: u32, max_id: ObjectId);

    // Replication
    fn replicate_stream(&self, app: AppId, table: TableId, slot: u32) -> Stream<Object>;

    // Migration
    fn get_epoch(&self, slot: SlotId) -> u64;
    fn start_migration(&self, slot: SlotId, new_epoch: u64, new_primary: ShardId);
}
```

## Core Server State

```rust
struct CoreServer {
    // Cached shard maps (can be rebuilt from home shards)
    shard_maps: HashMap<(AppId, TableId), Vec<SlotInfo>>,

    // Active subscriptions
    subscriptions: HashMap<QueryId, Subscription>,

    // Table interests (for Level 1 subscriptions)
    interested_tables: HashSet<(AppId, TableId)>,
}

struct SlotInfo {
    slot: u32,
    primary: ShardId,
    replica: ShardId,
    max_id: Option<ObjectId>,  // None if current slot
    epoch: u64,
}
```

## Open Questions

1. **"Full" threshold**: Row count? Storage bytes? Both?

2. **Hash collision handling**: What if `hash(app, table, n)` and `hash(app, table, n+1)` map to same shard? Skip to next index, or accept multiple slots on same shard?

3. **Query pushdown**: How much query logic lives in shards vs cores? Can shards evaluate complex predicates, or just index scans?

4. **Cross-slot queries**: Queries that span slot boundaries (e.g., time ranges) need to merge results. How do we handle ordering guarantees?

5. **Home shard failure**: What if the home shard (coordinator) fails? Need replica to take over coordinator role.

6. **Subscription consistency**: During migration, how do we ensure no missed updates? Dual-subscribe during DualWrite phase?
