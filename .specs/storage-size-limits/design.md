# Design: Storage Size Limits with Intelligent Eviction

## Overview

This design implements configurable storage size limits with a hybrid LRU + size-based eviction strategy. The system enforces a hard storage limit, ensuring the database never exceeds the configured maximum. It integrates with the existing `GarbageCollector` and `UnsyncedCoValuesTracker` infrastructure to coordinate memory and storage lifecycle.

### Key Design Principles

1. **Hard limit enforcement** - Storage must NEVER exceed `maxStorageBytes`
2. **Check before write** - Validate space availability before storing
3. **Protect active data** - Never evict in-memory or unsynced CoValues
4. **Reuse existing infrastructure** - Leverage `GarbageCollector.lastAccessed` and `UnsyncedCoValuesTracker`
5. **Minimize overhead** - Efficient size tracking, batched operations

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              LocalNode                                        │
│  ┌─────────────────┐  ┌─────────────────────┐  ┌──────────────────────────┐  │
│  │ GarbageCollector│  │ StorageEvictionMgr  │  │ UnsyncedCoValuesTracker  │  │
│  │ (in-memory GC)  │  │ (storage eviction)  │  │ (sync tracking)          │  │
│  │                 │  │                     │  │                          │  │
│  │ tracks:         │  │ enforces:           │  │ tracks:                  │  │
│  │ lastAccessed    │◄─┤ - hard limit        │──►│ - unsynced CoValue IDs   │  │
│  │                 │  │ - eviction scoring  │  │                          │  │
│  └────────┬────────┘  └──────────┬──────────┘  └──────────────────────────┘  │
│           │                      │                                            │
│           │ unmount callback     │ evict/store                                │
│           ▼                      ▼                                            │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │                         StorageAPI                                     │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │   │
│  │  │                    Eviction Metadata                             │  │   │
│  │  │  - last_accessed (persisted from verified.lastAccessed)         │  │   │
│  │  │  - size_bytes (per CoValue)                                     │  │   │
│  │  └─────────────────────────────────────────────────────────────────┘  │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │   │
│  │  │                    CoValue Data                                  │  │   │
│  │  │  - coValues, sessions, transactions, signatureAfter             │  │   │
│  │  └─────────────────────────────────────────────────────────────────┘  │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Components

### 1. StorageEvictionManager

**Location:** `packages/cojson/src/storage/StorageEvictionManager.ts`

**Responsibilities:**
- Monitor storage size and enforce hard limits
- Calculate eviction scores (LRU + size hybrid)
- Coordinate with GarbageCollector and UnsyncedCoValuesTracker
- Trigger proactive and immediate eviction

```typescript
interface StorageEvictionConfig {
  maxStorageBytes: number;          // Hard limit (default: 5GB)
  softThresholdRatio: number;       // Proactive eviction threshold (default: 0.8)
  ageWeight: number;                // Weight for age in eviction score (default: 0.8 = 80%)
  sizeWeight: number;               // Weight for size in eviction score (default: 0.2 = 20%)
  evictionIntervalMs: number;       // Background check interval (default: 5 min)
  evictionBatchSize: number;        // Max CoValues to evict per batch (default: 100)
}

class StorageEvictionManager {
  private config: StorageEvictionConfig;
  private storage: StorageAPI;
  private inMemoryCoValues: Map<RawCoID, CoValueCore>;  // Reference to LocalNode.coValues
  private interval: ReturnType<typeof setInterval> | undefined;
  
  constructor(
    storage: StorageAPI,
    inMemoryCoValues: Map<RawCoID, CoValueCore>,
    config: Partial<StorageEvictionConfig>
  );
  
  // Start background eviction checks
  start(): void;
  
  // Stop background eviction
  stop(): void;
  
  // Check if a write of given size can proceed, evict if necessary
  // Returns true if write can proceed, throws StorageLimitExceededError if not
  ensureSpaceForWrite(bytesToWrite: number): Promise<boolean>;
  
  // Get current storage size
  getCurrentStorageSize(): Promise<number>;
  
  // Trigger immediate eviction to free at least `bytesNeeded`
  evictToFreeSpace(bytesNeeded: number): Promise<number>;
  
  // Get eviction candidates sorted by score (highest first)
  getEvictionCandidates(): Promise<EvictionCandidate[]>;
  
  // Calculate eviction score for a CoValue (uses logarithmic formula)
  calculateEvictionScore(candidate: EvictionCandidate): number;
  
  // Persist lastAccessed when CoValue is unmounted from memory
  persistLastAccessed(id: RawCoID, lastAccessed: number): void;
}

interface EvictionCandidate {
  id: RawCoID;
  lastAccessed: number;
  sizeBytes: number;
}

// Note: No error class - system gracefully degrades to memory-only mode (see Error Handling section)
```

### 2. Extended StorageAPI Interface

**Location:** `packages/cojson/src/storage/types.ts`

Add new methods to support eviction:

```typescript
export interface StorageAPI {
  // ... existing methods ...
  
  // Get current database size in bytes
  getStorageSize(): Promise<number> | number;
  
  // Get eviction metadata for a CoValue
  getEvictionMetadata(id: RawCoID): Promise<EvictionMetadata | undefined> | EvictionMetadata | undefined;
  
  // Update eviction metadata (lastAccessed and/or sizeBytes)
  updateEvictionMetadata(id: RawCoID, metadata: Partial<EvictionMetadata>): Promise<void> | void;
  
  // Get all eviction candidates (CoValues with metadata, excluding unsynced)
  getEvictionCandidates(excludeIds: Set<RawCoID>): Promise<EvictionCandidate[]> | EvictionCandidate[];
  
  // Delete a CoValue and all associated data (for eviction)
  deleteCoValue(id: RawCoID): Promise<void> | void;
  
  // Batch delete multiple CoValues
  deleteCoValues(ids: RawCoID[]): Promise<void> | void;
}

interface EvictionMetadata {
  lastAccessed: number;  // Unix timestamp ms
  sizeBytes: number;
}
```

### 3. Extended GarbageCollector

**Location:** `packages/cojson/src/GarbageCollector.ts`

Add callback to persist `lastAccessed` when unmounting:

```typescript
export class GarbageCollector {
  private readonly interval: ReturnType<typeof setInterval>;
  private onUnmount?: (id: RawCoID, lastAccessed: number) => void;  // NEW

  constructor(
    private readonly coValues: Map<RawCoID, CoValueCore>,
    private readonly garbageCollectGroups: boolean,
    onUnmount?: (id: RawCoID, lastAccessed: number) => void,  // NEW
  ) {
    this.onUnmount = onUnmount;
    // ... existing code ...
  }

  collect() {
    const currentTime = this.getCurrentTime();
    for (const coValue of this.coValues.values()) {
      const { verified } = coValue;

      if (!verified?.lastAccessed) {
        continue;
      }

      const timeSinceLastAccessed = currentTime - verified.lastAccessed;

      if (timeSinceLastAccessed > GARBAGE_COLLECTOR_CONFIG.MAX_AGE) {
        // NEW: Persist lastAccessed before unmounting
        if (this.onUnmount) {
          this.onUnmount(coValue.id, verified.lastAccessed);
        }
        coValue.unmount(this.garbageCollectGroups);
      }
    }
  }
}
```

---

## Data Models

### SQLite Schema Addition

**Location:** `packages/cojson/src/storage/sqlite/sqliteMigrations.ts`

```typescript
export const migrations: Record<number, string[]> = {
  // ... existing migrations ...
  
  5: [
    `CREATE TABLE IF NOT EXISTS covalue_eviction_metadata (
      covalue_id TEXT PRIMARY KEY,
      last_accessed INTEGER NOT NULL,
      size_bytes INTEGER NOT NULL
    );`,
    `CREATE INDEX IF NOT EXISTS idx_eviction_last_accessed 
     ON covalue_eviction_metadata(last_accessed);`,
    `CREATE INDEX IF NOT EXISTS idx_eviction_size 
     ON covalue_eviction_metadata(size_bytes);`,
  ],
};
```

### IndexedDB Schema Addition

**Location:** `packages/cojson-storage-indexeddb/src/idbNode.ts`

```typescript
// In onupgradeneeded, add version check:
if (ev.oldVersion <= 5) {
  const evictionMetadata = db.createObjectStore("evictionMetadata", {
    keyPath: "coValueId",
  });
  evictionMetadata.createIndex("byLastAccessed", "lastAccessed");
  evictionMetadata.createIndex("bySizeBytes", "sizeBytes");
}
```

---

## Algorithms

### 1. Storage Size Calculation

**SQLite:**
```typescript
function getStorageSize(db: SQLiteDatabaseDriver): number {
  const result = db.get<{ page_count: number; page_size: number }>(
    "SELECT (SELECT page_count FROM pragma_page_count) as page_count, " +
    "(SELECT page_size FROM pragma_page_size) as page_size"
  );
  return (result?.page_count ?? 0) * (result?.page_size ?? 0);
}
```

**IndexedDB:**
```typescript
async function getStorageSize(): Promise<number> {
  const estimate = await navigator.storage.estimate();
  return estimate.usage ?? 0;
}
```

### 2. Eviction Score Calculation (Logarithmic Approach)

**Why Logarithmic Scoring?**

We use logarithmic scaling instead of normalization for several important reasons:

1. **No database queries required** - Normalization requires computing MIN/MAX across all candidates, which means either:
   - Iterating through all CoValues (expensive for large databases)
   - Running SQL aggregation queries (adds latency before each eviction decision)

2. **Natural scale compression** - Logarithms compress both age (milliseconds) and size (bytes) into comparable ranges:
   - Age: 1 minute (4.8) → 1 week (8.8) - range of ~4
   - Size: 1 KB (3.0) → 1 GB (9.0) - range of ~6
   
3. **Stable weights** - With normalization, the effective weight changes based on the data distribution. With log scaling, 80% age weight always means 80%.

4. **Efficient computation** - Each score is computed independently, enabling parallel processing and early termination.

```typescript
function calculateEvictionScore(
  candidate: EvictionCandidate,
  ageWeight: number = 0.8,   // 80% weight for age (recency)
  sizeWeight: number = 0.2,  // 20% weight for size
): number {
  const ageMs = Date.now() - candidate.lastAccessed;
  
  // Use log10 to compress values into comparable ranges:
  // - log10(1 day in ms) ≈ 7.9
  // - log10(1 MB) ≈ 6.0
  // This puts both metrics in roughly the same scale (3-9)
  const ageScore = Math.log10(Math.max(ageMs, 1));
  const sizeScore = Math.log10(Math.max(candidate.sizeBytes, 1));
  
  // Weighted sum - higher score = evict first
  return (ageWeight * ageScore) + (sizeWeight * sizeScore);
}
```

**Logarithmic Scale Reference:**

| Value | Raw | log10 |
|-------|-----|-------|
| 1 minute | 60,000 ms | 4.8 |
| 1 hour | 3,600,000 ms | 6.6 |
| 1 day | 86,400,000 ms | 7.9 |
| 1 week | 604,800,000 ms | 8.8 |
| 1 KB | 1,024 bytes | 3.0 |
| 1 MB | 1,048,576 bytes | 6.0 |
| 100 MB | 104,857,600 bytes | 8.0 |
| 1 GB | 1,073,741,824 bytes | 9.0 |

**Example with default weights (0.8 age, 0.2 size):**

| CoValue | Age | Size | Age Score | Size Score | Total |
|---------|-----|------|-----------|------------|-------|
| A | 2 days | 10 MB | 0.8 × 8.2 = 6.6 | 0.2 × 7.0 = 1.4 | **8.0** |
| B | 1 day | 100 MB | 0.8 × 7.9 = 6.3 | 0.2 × 8.0 = 1.6 | **7.9** |
| C | 1 hour | 1 GB | 0.8 × 6.6 = 5.3 | 0.2 × 9.0 = 1.8 | **7.1** |

→ **A is evicted first** (score 8.0) - age dominates with 80% weight
→ Large but recent CoValues (C) are preserved longer

### 3. Tiered Eviction Candidate Selection

The system uses a tiered priority for eviction to maximize safety:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Eviction Priority Tiers                          │
│                                                                      │
│  Tier 1 (SAFEST): NOT in memory + synced                            │
│  - Data exists in storage only (not actively used)                   │
│  - Can be re-fetched from server if needed later                    │
│                                                                      │
│  Tier 2 (SAFE): IN memory + synced                                  │
│  - Data exists in memory (app continues working)                    │
│  - Data exists on server (can recover after crash)                  │
│  - Only evict from storage, keep in memory                          │
│                                                                      │
│  Tier 3 (NEVER): Unsynced (in-memory or not)                        │
│  - Data would be LOST if app crashes                                │
│  - NEVER evict until synced to server                               │
└─────────────────────────────────────────────────────────────────────┘
```

```typescript
interface TieredEvictionCandidate extends EvictionCandidate {
  tier: 1 | 2;  // Tier 3 (unsynced) is never returned
  inMemory: boolean;
}

async function getEvictionCandidates(
  storage: StorageAPI,
  inMemoryCoValues: Map<RawCoID, CoValueCore>,
): Promise<TieredEvictionCandidate[]> {
  // Get unsynced IDs - these are NEVER evictable
  const unsyncedIds = new Set<RawCoID>();
  await new Promise<void>(resolve => {
    storage.getUnsyncedCoValueIDs((ids) => {
      for (const id of ids) {
        unsyncedIds.add(id);
      }
      resolve();
    });
  });
  
  // Get in-memory IDs (with loaded state)
  const inMemoryIds = new Set<RawCoID>();
  for (const [id, core] of inMemoryCoValues) {
    if (core.loadingState !== "unknown") {
      inMemoryIds.add(id);
    }
  }
  
  // Get all candidates from storage (excluding unsynced only)
  const allCandidates = await storage.getEvictionCandidates(unsyncedIds);
  
  // Assign tiers and sort
  const tieredCandidates: TieredEvictionCandidate[] = allCandidates.map(c => ({
    ...c,
    inMemory: inMemoryIds.has(c.id),
    tier: inMemoryIds.has(c.id) ? 2 : 1,
  }));
  
  // Sort by: tier ASC (1 before 2), then score DESC (within each tier)
  tieredCandidates.sort((a, b) => {
    if (a.tier !== b.tier) return a.tier - b.tier;
    return calculateEvictionScore(b) - calculateEvictionScore(a);
  });
  
  return tieredCandidates;
}
```

### 4. Size Tracking on Store

```typescript
function calculateTransactionSize(tx: Transaction): number {
  // Estimate as JSON string length (close to actual storage size)
  return JSON.stringify(tx).length;
}

function updateSizeOnStore(
  storage: StorageAPI,
  id: RawCoID,
  newTransactions: Transaction[],
): void {
  const currentMetadata = storage.getEvictionMetadata(id);
  const addedSize = newTransactions.reduce(
    (sum, tx) => sum + calculateTransactionSize(tx), 
    0
  );
  
  storage.updateEvictionMetadata(id, {
    sizeBytes: (currentMetadata?.sizeBytes ?? 0) + addedSize,
    lastAccessed: Date.now(),
  });
}
```

---

## Integration Points

### 1. LocalNode Initialization

**Location:** `packages/cojson/src/localNode.ts`

```typescript
class LocalNode {
  garbageCollector: GarbageCollector | undefined = undefined;
  storageEvictionManager: StorageEvictionManager | undefined = undefined;  // NEW
  
  enableGarbageCollector(opts?: { garbageCollectGroups?: boolean }) {
    if (this.garbageCollector) {
      return;
    }

    // Create onUnmount callback for storage eviction
    const onUnmount = this.storageEvictionManager
      ? (id: RawCoID, lastAccessed: number) => {
          this.storageEvictionManager!.persistLastAccessed(id, lastAccessed);
        }
      : undefined;

    this.garbageCollector = new GarbageCollector(
      this.coValues,
      opts?.garbageCollectGroups ?? false,
      onUnmount,  // NEW
    );
  }

  setStorage(storage: StorageAPI, evictionConfig?: Partial<StorageEvictionConfig>) {
    this.storage = storage;
    this.syncManager.setStorage(storage);
    
    // NEW: Initialize eviction manager if limits are configured
    if (evictionConfig?.maxStorageBytes && evictionConfig.maxStorageBytes !== Infinity) {
      this.storageEvictionManager = new StorageEvictionManager(
        storage,
        this.coValues,
        evictionConfig,
      );
      this.storageEvictionManager.start();
    }
  }

  async gracefulShutdown(): Promise<unknown> {
    this.garbageCollector?.stop();
    this.storageEvictionManager?.stop();  // NEW
    await this.syncManager.gracefulShutdown();
    return this.storage?.close();
  }
}
```

### 2. Store Operation with Limit Check

**Location:** `packages/cojson/src/storage/storageSync.ts` and `storageAsync.ts`

```typescript
// In store() method, add limit check before storing:

async store(msg: NewContentMessage, correctionCallback: CorrectionCallback) {
  // NEW: Check storage limit before storing
  if (this.evictionManager) {
    const writeSize = this.estimateWriteSize(msg);
    await this.evictionManager.ensureSpaceForWrite(writeSize);
  }
  
  // Proceed with existing store logic
  return this.storeSingle(msg, correctionCallback);
}

private estimateWriteSize(msg: NewContentMessage): number {
  let size = 0;
  
  // Header size (only for new CoValues)
  if (msg.header) {
    size += JSON.stringify(msg.header).length;
  }
  
  // Transaction sizes
  for (const sessionData of Object.values(msg.new)) {
    for (const tx of sessionData.newTransactions) {
      size += JSON.stringify(tx).length;
    }
  }
  
  return size;
}
```

### 3. Load Operation with Metadata Restore

**Location:** `packages/cojson/src/storage/storageSync.ts` and `storageAsync.ts`

```typescript
// In loadCoValue(), restore lastAccessed after loading:

loadCoValue(
  id: string,
  callback: (data: NewContentMessage) => void,
  done?: (found: boolean) => void,
) {
  const coValueRow = this.dbClient.getCoValue(id);

  if (!coValueRow) {
    done?.(false);
    return;
  }

  // NEW: Get eviction metadata to restore lastAccessed
  const metadata = this.dbClient.getEvictionMetadata(id);
  
  // ... existing load logic ...

  // NEW: Update lastAccessed in storage (we're accessing it now)
  this.dbClient.updateEvictionMetadata(id, {
    lastAccessed: Date.now(),
    sizeBytes: metadata?.sizeBytes ?? this.calculateCoValueSize(coValueRow),
  });

  done?.(true);
}
```

---

## Error Handling

### Graceful Degradation to Memory-Only Mode

The system **does NOT throw errors** when storage is full. Instead, it gracefully degrades:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Storage Pressure Handling                         │
│                                                                      │
│  1. Try to evict Tier 1 candidates (not in memory, synced)          │
│     ↓ If not enough space freed                                     │
│  2. Try to evict Tier 2 candidates (in memory, synced)              │
│     - Data stays in memory, only removed from storage               │
│     - Safe because data exists on server                            │
│     ↓ If still not enough (only unsynced data remains)              │
│  3. Graceful degradation to memory-only mode                        │
│     - Skip storage write                                            │
│     - Data stays in memory + syncs to server                        │
│     - Log warning for debugging                                     │
│     - Resume storage when sync completes                            │
└─────────────────────────────────────────────────────────────────────┘
```

```typescript
// In StorageEvictionManager.ensureSpaceForWrite():
async ensureSpaceForWrite(bytesToWrite: number): Promise<boolean> {
  const currentSize = await this.getCurrentStorageSize();
  const availableSpace = this.config.maxStorageBytes - currentSize;
  
  if (bytesToWrite <= availableSpace) {
    return true;  // Enough space, proceed with store
  }
  
  // Try to evict (will try Tier 1 first, then Tier 2)
  const bytesNeeded = bytesToWrite - availableSpace;
  const freedBytes = await this.evictToFreeSpace(bytesNeeded);
  
  if (freedBytes >= bytesNeeded) {
    return true;  // Eviction succeeded, proceed with store
  }
  
  // Only unsynced data remains - graceful degradation
  logger.warn("Storage limit reached, operating in memory-only mode", {
    currentSize,
    maxSize: this.config.maxStorageBytes,
    bytesNeeded: bytesToWrite,
    freedBytes,
    reason: "Only unsynced CoValues remain - waiting for sync to complete",
  });
  
  return false;  // Signal to skip storage write, keep in memory only
}
```

**Store operation handling:**
```typescript
// In StorageApiSync/StorageApiAsync.store():
async store(msg: NewContentMessage, correctionCallback: CorrectionCallback) {
  if (this.evictionManager) {
    const writeSize = this.estimateWriteSize(msg);
    const canStore = await this.evictionManager.ensureSpaceForWrite(writeSize);
    
    if (!canStore) {
      // Skip storage write - data remains in memory and will sync to server
      // This is NOT an error - the system continues to function normally
      return;
    }
  }
  
  // Proceed with storage write
  return this.storeSingle(msg, correctionCallback);
}
```

**Why this approach works:**
- **Tier 1 eviction**: Safest - data not in use, exists on server
- **Tier 2 eviction**: Safe - app keeps working (in memory), data recoverable from server
- **Memory-only fallback**: Only for unsynced data - preserves data integrity
- **Self-healing**: Once sync completes, unsynced → synced → evictable → storage resumes
- **No user-facing errors**: Application continues to work seamlessly

---

## Testing Strategy

### Unit Tests

1. **StorageEvictionManager**
   - Eviction score calculation with various age/size combinations
   - Candidate filtering (excludes in-memory and unsynced)
   - Batch eviction respects batch size limits
   - Hard limit enforcement blocks writes when necessary

2. **Storage Size Tracking**
   - SQLite PRAGMA returns correct size
   - Size updates on store
   - Size calculation for existing CoValues

3. **GarbageCollector Integration**
   - onUnmount callback fires with correct lastAccessed
   - lastAccessed persisted to storage

### Integration Tests

1. **End-to-end Eviction Flow**
   - Fill storage to limit
   - Verify eviction triggers
   - Verify correct CoValues are evicted (by score)
   - Verify protected CoValues are not evicted

2. **Limit Enforcement**
   - Write that would exceed limit triggers eviction
   - Write rejected when all candidates protected
   - StorageLimitExceededError contains correct info

3. **Recovery After Eviction**
   - Evicted CoValue can be re-fetched from server
   - Re-fetched CoValue has fresh lastAccessed

### Performance Tests

1. **Large Database**
   - Eviction with 10,000+ CoValues
   - PRAGMA query performance
   - Batch eviction doesn't block main thread

---

## Configuration Defaults

```typescript
const DEFAULT_EVICTION_CONFIG: StorageEvictionConfig = {
  maxStorageBytes: 5 * 1024 * 1024 * 1024,  // 5 GB
  softThresholdRatio: 0.8,                    // 80% = 4 GB (proactive eviction starts here)
  ageWeight: 0.8,                             // 80% weight for age (recency)
  sizeWeight: 0.2,                            // 20% weight for size
  evictionIntervalMs: 5 * 60 * 1000,          // 5 minutes
  evictionBatchSize: 100,                     // Max 100 CoValues per batch
};
```

**Weight Rationale:**
- `ageWeight: 0.8` (80%) - Prioritizes recency, ensuring frequently accessed CoValues stay cached
- `sizeWeight: 0.2` (20%) - Gives a boost to larger items, making eviction more space-efficient
- These weights mean: a 10× older CoValue scores ~1.8 points higher; a 10× larger CoValue scores ~0.2 points higher

---

## File Changes Summary

| File | Type | Changes |
|------|------|---------|
| `packages/cojson/src/storage/StorageEvictionManager.ts` | NEW | Core eviction logic with tiered priority |
| `packages/cojson/src/storage/types.ts` | MODIFY | Add eviction methods to StorageAPI |
| `packages/cojson/src/storage/storageSync.ts` | MODIFY | Add limit checks, metadata tracking |
| `packages/cojson/src/storage/storageAsync.ts` | MODIFY | Add limit checks, metadata tracking |
| `packages/cojson/src/storage/sqlite/client.ts` | MODIFY | Implement eviction methods |
| `packages/cojson/src/storage/sqlite/sqliteMigrations.ts` | MODIFY | Add migration v5 |
| `packages/cojson/src/storage/sqliteAsync/client.ts` | MODIFY | Implement eviction methods |
| `packages/cojson-storage-indexeddb/src/idbClient.ts` | MODIFY | Implement eviction methods |
| `packages/cojson-storage-indexeddb/src/idbNode.ts` | MODIFY | Add schema migration |
| `packages/cojson/src/GarbageCollector.ts` | MODIFY | Add onUnmount callback |
| `packages/cojson/src/localNode.ts` | MODIFY | Initialize eviction manager |
| `packages/cojson/src/config.ts` | MODIFY | Add eviction config |
