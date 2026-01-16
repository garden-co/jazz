# Design: Storage Size Limits with Intelligent Eviction

## Overview

This design implements configurable storage size limits with a hybrid LRU + size-based eviction strategy. The system enforces a hard storage limit, ensuring the database never exceeds the configured maximum. It integrates with the existing `GarbageCollector` and `UnsyncedCoValuesTracker` infrastructure to coordinate memory and storage lifecycle.

### Key Design Principles

1. **Hard limit enforcement** - Storage must NEVER exceed `maxStorageBytes`
2. **Check before write** - Validate space availability before storing
3. **Protect active data** - Never evict in-memory or unsynced CoValues
4. **Separate persistence timestamps** - Use `Date.now()` epoch timestamps for storage eviction (independent from GarbageCollector's `performance.now()` tracking)
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
│  │ lastAccessed    │  │ - hard limit        │──►│ - unsynced CoValue IDs   │  │
│  │ (perf.now())    │  │ - eviction scoring  │  │                          │  │
│  └─────────────────┘  └──────────┬──────────┘  └──────────────────────────┘  │
│                                  │                                            │
│                                  │ evict/store                                │
│                                  ▼                                            │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │                         StorageAPI                                     │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │   │
│  │  │                    Eviction Metadata                             │  │   │
│  │  │  - last_accessed_epoch (Date.now() - independent from GC)       │  │   │
│  │  │  - size_bytes (per CoValue)                                     │  │   │
│  │  └─────────────────────────────────────────────────────────────────┘  │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │   │
│  │  │                    CoValue Data                                  │  │   │
│  │  │  - coValues, sessions, transactions, signatureAfter             │  │   │
│  │  └─────────────────────────────────────────────────────────────────┘  │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Important:** The `last_accessed_epoch` in storage uses `Date.now()` (Unix epoch milliseconds), which is independent from the `GarbageCollector.lastAccessed` field that uses `performance.now()` (milliseconds since page load). This separation ensures eviction scoring works correctly across app restarts.

---

## Flow Diagrams

### 1. Store Operation Flow

This flow is triggered when new content needs to be stored:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           STORE OPERATION FLOW                               │
└─────────────────────────────────────────────────────────────────────────────┘

  store(msg) called
        │
        ▼
┌───────────────────┐
│ Estimate write    │
│ size from msg     │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐     ┌──────────────────────────────────────────────────┐
│ evictionManager.  │     │                                                  │
│ ensureSpaceFor    │────►│  ┌─────────────────────────────────────────┐    │
│ Write(writeSize)  │     │  │ Get current storage size                │    │
└─────────┬─────────┘     │  │ (PRAGMA page_count * page_size)         │    │
          │               │  └───────────────┬─────────────────────────┘    │
          │               │                  ▼                               │
          │               │  ┌─────────────────────────────────────────┐    │
          │               │  │ availableSpace = maxBytes - currentSize │    │
          │               │  └───────────────┬─────────────────────────┘    │
          │               │                  ▼                               │
          │               │         writeSize <= availableSpace?             │
          │               │              /           \                       │
          │               │           YES             NO                     │
          │               │            │               │                     │
          │               │            ▼               ▼                     │
          │               │    return true    ┌───────────────────┐         │
          │               │         │         │ evictToFreeSpace  │         │
          │               │         │         │ (bytesNeeded)     │         │
          │               │         │         └─────────┬─────────┘         │
          │               │         │                   │                    │
          │               │         │                   ▼                    │
          │               │         │         freedBytes >= needed?          │
          │               │         │              /           \             │
          │               │         │           YES             NO           │
          │               │         │            │               │           │
          │               │         │            ▼               ▼           │
          │               │         │     return true    return false       │
          │               │         │            │        (memory-only)     │
          │               └─────────┼────────────┼───────────────┼──────────┘
          │                         │            │               │
          ▼                         ▼            ▼               ▼
┌───────────────────┐        ┌─────────────────────────┐  ┌──────────────┐
│ canStore = true?  │◄───────┤     canStore = true     │  │canStore=false│
└─────────┬─────────┘        └─────────────────────────┘  └──────┬───────┘
          │                                                       │
    ┌─────┴─────┐                                                 │
   YES          NO ◄──────────────────────────────────────────────┘
    │            │
    ▼            ▼
┌─────────┐  ┌─────────────────────────────┐
│ Store   │  │ Skip storage write          │
│ to DB   │  │ (data stays in memory,      │
│         │  │  syncs to server normally)  │
└────┬────┘  └─────────────────────────────┘
     │
     ▼
┌─────────────────────────────┐
│ Update eviction metadata:   │
│ - lastAccessedEpoch = now   │
│ - sizeBytes += newSize      │
└─────────────────────────────┘
```

### 2. Eviction Process Flow

This flow shows how `evictToFreeSpace` works:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EVICTION PROCESS FLOW                              │
└─────────────────────────────────────────────────────────────────────────────┘

  evictToFreeSpace(bytesNeeded) called
        │
        ▼
┌───────────────────────────────────────────────────────────────┐
│                    GET EVICTION CANDIDATES                     │
│                                                                │
│  1. Get unsynced IDs from UnsyncedCoValuesTracker (in-memory) │
│  2. Get in-memory IDs from LocalNode.coValues                  │
│  3. Combine into protectedIds set                              │
│  4. Query storage for candidates NOT in protectedIds           │
│  5. Calculate eviction score for each candidate                │
│  6. Sort by score DESC (highest = evict first)                 │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────┐
│ candidates.length │
│ > 0 ?             │
└─────────┬─────────┘
          │
    ┌─────┴─────┐
   YES          NO
    │            │
    │            ▼
    │    ┌─────────────────────────────┐
    │    │ return 0                    │
    │    │ (no candidates to evict,    │
    │    │  graceful degradation)      │
    │    └─────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│                    EVICTION LOOP                             │
│                                                              │
│   freedBytes = 0                                             │
│   batch = []                                                 │
│                                                              │
│   for candidate in candidates:                               │
│       │                                                      │
│       ▼                                                      │
│   ┌───────────────────────────┐                              │
│   │ batch.push(candidate.id)  │                              │
│   │ freedBytes += sizeBytes   │                              │
│   └─────────────┬─────────────┘                              │
│                 │                                            │
│                 ▼                                            │
│       batch.length >= batchSize?                             │
│            /           \                                     │
│         YES             NO                                   │
│          │               │                                   │
│          ▼               │                                   │
│   ┌──────────────┐       │                                   │
│   │ Flush batch  │       │                                   │
│   │ (delete from │◄──────┘                                   │
│   │  storage)    │                                           │
│   └──────┬───────┘                                           │
│          │                                                   │
│          ▼                                                   │
│   freedBytes >= bytesNeeded?                                 │
│        /           \                                         │
│     YES             NO                                       │
│      │               │                                       │
│      ▼               └──────► continue loop                  │
│   BREAK                                                      │
│                                                              │
└──────────────────────────────────┬───────────────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │ Flush remaining batch    │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │ return freedBytes        │
                    └──────────────────────────┘
```

### 3. Delete CoValue Flow

This flow shows what happens when evicting a single CoValue:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DELETE COVALUE FLOW                                  │
└─────────────────────────────────────────────────────────────────────────────┘

  deleteCoValue(id) called
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                  DELETE FROM DATABASE                        │
│                                                              │
│  1. DELETE FROM covalue_eviction_metadata WHERE id = ?       │
│  2. DELETE FROM transactions WHERE ses IN (session_ids)      │
│  3. DELETE FROM signatureAfter WHERE ses IN (session_ids)    │
│  4. DELETE FROM sessions WHERE coValue = ?                   │
│  5. DELETE FROM coValues WHERE id = ?                        │
│                                                              │
│  (All in single transaction for consistency)                 │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│              CLEAR FROM KNOWN STATE CACHE                    │
│                                                              │
│  knownStates.clearKnownState(id)                            │
│    - Remove from knownStates Map                            │
│    - Resolve any pending waitForSync requests               │
│                                                              │
└───────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│  CoValue is now fully evicted:                               │
│  - Not in storage                                           │
│  - Not in known state cache                                 │
│  - Can be re-fetched from server on next access             │
└─────────────────────────────────────────────────────────────┘
```

### 4. Background Eviction Flow

This flow runs periodically (default: every 5 minutes):

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      BACKGROUND EVICTION FLOW                                │
└─────────────────────────────────────────────────────────────────────────────┘

  setInterval callback (every evictionIntervalMs)
        │
        ▼
┌───────────────────────────────────┐
│ Get current storage size          │
└─────────────────┬─────────────────┘
                  │
                  ▼
┌───────────────────────────────────┐
│ currentSize > softThreshold?      │
│ (softThreshold = max * 0.8)       │
└─────────────────┬─────────────────┘
                  │
            ┌─────┴─────┐
           YES          NO
            │            │
            │            ▼
            │     ┌──────────────┐
            │     │ Do nothing   │
            │     │ (enough      │
            │     │  headroom)   │
            │     └──────────────┘
            │
            ▼
┌───────────────────────────────────────────┐
│ Proactive eviction:                        │
│ bytesToFree = currentSize - softThreshold │
│ evictToFreeSpace(bytesToFree)             │
└───────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────┐
│ Log eviction stats:                        │
│ - Bytes freed                             │
│ - CoValues evicted                        │
│ - New storage size                        │
└───────────────────────────────────────────┘
```

### 5. Overall System Integration

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SYSTEM INTEGRATION                                  │
└─────────────────────────────────────────────────────────────────────────────┘

                        ┌─────────────────────┐
                        │     LocalNode       │
                        └──────────┬──────────┘
                                   │
           ┌───────────────────────┼───────────────────────┐
           │                       │                       │
           ▼                       ▼                       ▼
┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐
│  GarbageCollector   │ │ StorageEvictionMgr  │ │UnsyncedCoValues     │
│  (in-memory GC)     │ │ (storage eviction)  │ │Tracker              │
│                     │ │                     │ │                     │
│ - Unmounts CoValues │ │ - Checks protectedI │ │ - Tracks unsynced   │
│   from memory after │ │   from both GC and  │◄│   CoValue IDs       │
│   MAX_AGE           │ │   UnsyncedTracker   │ │ - Used to protect   │
│ - Makes CoValues    │ │ - Evicts only safe  │ │   from eviction     │
│   eligible for      │ │   candidates        │ │                     │
│   storage eviction  │ │                     │ │                     │
└─────────────────────┘ └──────────┬──────────┘ └─────────────────────┘
                                   │
                                   ▼
                        ┌─────────────────────┐
                        │     StorageAPI      │
                        │  (SQLite/IndexedDB) │
                        │                     │
                        │ - store()           │
                        │ - load()            │
                        │ - deleteCoValue()   │
                        │ - getStorageSize()  │
                        │ - getEvictionMeta() │
                        └─────────────────────┘

LIFECYCLE OF A COVALUE:

  Created ──► In Memory ──► Synced to Server ──► Unmounted by GC ──► Evicted
     │            │               │                    │                │
     │            │               │                    │                │
     ▼            ▼               ▼                    ▼                ▼
  Protected   Protected      Protected            EVICTABLE         Gone from
  (unsynced)  (in-memory)    (in-memory)     (not in memory,       storage
                                               synced)              (can re-fetch)
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
  private unsyncedTracker: UnsyncedCoValuesTracker;     // Reference to in-memory tracker
  private interval: ReturnType<typeof setInterval> | undefined;
  
  constructor(
    storage: StorageAPI,
    inMemoryCoValues: Map<RawCoID, CoValueCore>,
    unsyncedTracker: UnsyncedCoValuesTracker,
    config: Partial<StorageEvictionConfig>
  );
  
  // Start background eviction checks
  start(): void;
  
  // Stop background eviction
  stop(): void;
  
  // Check if a write of given size can proceed, evict if necessary
  // Returns true if write can proceed, false to skip storage (memory-only mode)
  ensureSpaceForWrite(bytesToWrite: number): Promise<boolean>;
  
  // Get current storage size
  getCurrentStorageSize(): Promise<number>;
  
  // Trigger immediate eviction to free at least `bytesNeeded`
  evictToFreeSpace(bytesNeeded: number): Promise<number>;
  
  // Get eviction candidates sorted by score (highest first)
  // Only returns CoValues that are NOT in memory and ARE synced
  getEvictionCandidates(): Promise<EvictionCandidate[]>;
  
  // Calculate eviction score for a CoValue (uses logarithmic formula)
  calculateEvictionScore(candidate: EvictionCandidate): number;
}

interface EvictionCandidate {
  id: RawCoID;
  lastAccessedEpoch: number;  // Unix timestamp ms (Date.now())
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
  
  // Update eviction metadata (lastAccessedEpoch and/or sizeBytes)
  updateEvictionMetadata(id: RawCoID, metadata: Partial<EvictionMetadata>): Promise<void> | void;
  
  // Get all eviction candidates (CoValues with metadata, excluding unsynced)
  getEvictionCandidates(excludeIds: Set<RawCoID>): Promise<EvictionCandidate[]> | EvictionCandidate[];
  
  // Delete a CoValue and all associated data (for eviction)
  // Also clears the entry from StorageKnownState.knownStates
  deleteCoValue(id: RawCoID): Promise<void> | void;
  
  // Batch delete multiple CoValues
  // Also clears entries from StorageKnownState.knownStates
  deleteCoValues(ids: RawCoID[]): Promise<void> | void;
}

interface EvictionMetadata {
  lastAccessedEpoch: number;  // Unix timestamp ms from Date.now()
  sizeBytes: number;
}
```

### 3. GarbageCollector (No Changes Required)

**Location:** `packages/cojson/src/GarbageCollector.ts`

The `GarbageCollector` remains unchanged. It uses `performance.now()` for in-memory tracking, which is appropriate for its purpose (tracking which CoValues to unmount from memory).

The storage eviction system uses a **separate** `lastAccessedEpoch` field (using `Date.now()`) that is updated on store and load operations, independent of the GarbageCollector. This separation ensures:

1. **Correct persistence** - `Date.now()` timestamps work correctly across app restarts
2. **No coupling** - Storage eviction doesn't depend on GC timing or behavior
3. **Simpler implementation** - No need to modify the existing GarbageCollector

### 4. StorageKnownState Integration

**Location:** `packages/cojson/src/storage/knownState.ts`

When a CoValue is evicted, we must also clear its entry from `StorageKnownState.knownStates`. This ensures:

1. **Correct sync behavior** - The system won't think data is still in storage when it's been evicted
2. **Clean re-fetch** - When the CoValue is requested again, it will be properly loaded from server
3. **No stale waitForSync** - Pending sync requests for evicted CoValues won't hang

```typescript
// Add method to StorageKnownState class:
class StorageKnownState {
  // ... existing methods ...
  
  /**
   * Clear known state for an evicted CoValue.
   * Called when a CoValue is deleted from storage.
   */
  clearKnownState(id: string): void {
    this.knownStates.delete(id);
    
    // Also clear any pending waitForSync requests
    const requests = this.waitForSyncRequests.get(id);
    if (requests) {
      // Resolve pending requests (they'll re-fetch from server)
      for (const request of requests) {
        request.resolve();
      }
      this.waitForSyncRequests.delete(id);
    }
  }
  
  /**
   * Batch clear known states for multiple evicted CoValues.
   */
  clearKnownStates(ids: string[]): void {
    for (const id of ids) {
      this.clearKnownState(id);
    }
  }
}
```

**Usage in StorageApiSync/StorageApiAsync:**
```typescript
deleteCoValue(id: RawCoID): void {
  // Delete from database tables
  this.dbClient.deleteCoValue(id);
  
  // Clear from knownStates cache
  this.knownStates.clearKnownState(id);
}

deleteCoValues(ids: RawCoID[]): void {
  // Delete from database tables
  this.dbClient.deleteCoValues(ids);
  
  // Clear from knownStates cache
  this.knownStates.clearKnownStates(ids);
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
      last_accessed_epoch INTEGER NOT NULL,
      size_bytes INTEGER NOT NULL
    );`,
    `CREATE INDEX IF NOT EXISTS idx_eviction_last_accessed_epoch 
     ON covalue_eviction_metadata(last_accessed_epoch);`,
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
  evictionMetadata.createIndex("byLastAccessedEpoch", "lastAccessedEpoch");
  evictionMetadata.createIndex("bySizeBytes", "sizeBytes");
}
```

---

## Algorithms

### 1. Storage Size Calculation

**SQLite:**
```typescript
function getStorageSize(db: SQLiteDatabaseDriver): number {
  const result = db.get<{ size_bytes: number }>(
    "SELECT (SELECT page_count FROM pragma_page_count) * (SELECT page_size FROM pragma_page_size) as size_bytes"
  );
  return result?.size_bytes ?? 0;
}
```

**IndexedDB:**
```typescript
async function getStorageSize(): Promise<number> {
  const estimate = await navigator.storage.estimate();
  
  // Try to get IndexedDB-specific usage if available (Chrome/Chromium only)
  // Falls back to total origin usage (includes Cache API, etc.)
  const usageDetails = (estimate as { usageDetails?: { indexedDB?: number } }).usageDetails;
  
  return usageDetails?.indexedDB ?? estimate.usage ?? 0;
}
```

**Note:** `usageDetails.indexedDB` is more accurate but only available in Chromium browsers. Safari and Firefox don't support it, so we fall back to `estimate.usage` which includes all origin storage (IndexedDB, Cache API, etc.). This means the limit may be less precise on non-Chromium browsers.

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
  // lastAccessedEpoch is a Unix timestamp from Date.now()
  const ageMs = Date.now() - candidate.lastAccessedEpoch;
  
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

### 3. Eviction Candidate Selection

The system uses strict criteria to determine which CoValues can be safely evicted:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Eviction Eligibility                             │
│                                                                      │
│  EVICTABLE: NOT in memory AND synced                                │
│  - Data exists in storage only (not actively used)                   │
│  - Data has been synced to server (can be re-fetched if needed)     │
│  - Safe to delete from local storage                                │
│                                                                      │
│  PROTECTED (NEVER evict):                                           │
│  - IN memory: Would break incremental storage writes                │
│  - Unsynced: Data would be LOST if not on server                    │
└─────────────────────────────────────────────────────────────────────┘
```

**Why we don't evict in-memory CoValues:**

Evicting storage for a CoValue that's still in memory would break the incremental storage model. The storage layer assumes it can append transactions to existing sessions. If we delete storage while the CoValue is in memory:
1. Next store would try to find existing session → not found
2. Would create a new session starting from current index
3. Would lose all transaction history before that point

This would cause data corruption when the CoValue is later reloaded from storage.

```typescript
async function getEvictionCandidates(
  storage: StorageAPI,
  inMemoryCoValues: Map<RawCoID, CoValueCore>,
  unsyncedTracker: UnsyncedCoValuesTracker,
): Promise<EvictionCandidate[]> {
  // Get unsynced IDs from in-memory tracker (authoritative source)
  const unsyncedIds = new Set<RawCoID>(unsyncedTracker.getAll());
  
  // Get in-memory IDs (with loaded state)
  const inMemoryIds = new Set<RawCoID>();
  for (const [id, core] of inMemoryCoValues) {
    if (core.loadingState !== "unknown") {
      inMemoryIds.add(id);
    }
  }
  
  // Combine all protected IDs
  const protectedIds = new Set<RawCoID>([...unsyncedIds, ...inMemoryIds]);
  
  // Get candidates from storage, excluding all protected IDs
  const candidates = await storage.getEvictionCandidates(protectedIds);
  
  // Sort by eviction score (highest first = evict first)
  candidates.sort((a, b) => 
    calculateEvictionScore(b) - calculateEvictionScore(a)
  );
  
  return candidates;
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
    lastAccessedEpoch: Date.now(),  // Always use Date.now() for persistence
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
  
  // GarbageCollector remains unchanged - no modifications needed
  enableGarbageCollector(opts?: { garbageCollectGroups?: boolean }) {
    if (this.garbageCollector) {
      return;
    }

    this.garbageCollector = new GarbageCollector(
      this.coValues,
      opts?.garbageCollectGroups ?? false,
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
        this.syncManager.unsyncedTracker,  // Pass the in-memory tracker
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

### 3. Load Operation with Metadata Update

**Location:** `packages/cojson/src/storage/storageSync.ts` and `storageAsync.ts`

```typescript
// In loadCoValue(), update lastAccessedEpoch after loading:

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

  // ... existing load logic ...

  // NEW: Update lastAccessedEpoch in storage (we're accessing it now)
  const metadata = this.dbClient.getEvictionMetadata(id);
  this.dbClient.updateEvictionMetadata(id, {
    lastAccessedEpoch: Date.now(),  // Always use Date.now() for persistence
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
│  1. Try to evict eligible candidates (not in memory + synced)       │
│     ↓ If not enough space freed                                     │
│  2. Graceful degradation to memory-only mode                        │
│     - Skip storage write                                            │
│     - Data stays in memory + syncs to server                        │
│     - Log warning for debugging                                     │
│     - Resume storage when space becomes available                   │
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
  
  // Try to evict (only CoValues not in memory and synced)
  const bytesNeeded = bytesToWrite - availableSpace;
  const freedBytes = await this.evictToFreeSpace(bytesNeeded);
  
  if (freedBytes >= bytesNeeded) {
    return true;  // Eviction succeeded, proceed with store
  }
  
  // No more evictable candidates - graceful degradation
  logger.warn("Storage limit reached, operating in memory-only mode", {
    currentSize,
    maxSize: this.config.maxStorageBytes,
    bytesNeeded: bytesToWrite,
    freedBytes,
    reason: "No evictable CoValues (all are in-memory or unsynced)",
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
- **Eviction only targets safe candidates**: CoValues not in memory (won't break incremental writes) and synced (can be re-fetched)
- **Memory-only fallback**: When no candidates available, data stays in memory and syncs normally
- **Self-healing**: As CoValues are unmounted from memory or synced, they become evictable
- **No user-facing errors**: Application continues to work seamlessly
- **Data integrity preserved**: Never evict data that could be lost

---

## Testing Strategy

### Unit Tests

1. **StorageEvictionManager**
   - Eviction score calculation with various age/size combinations
   - Candidate filtering (excludes in-memory and unsynced)
   - Batch eviction respects batch size limits
   - Graceful degradation when no candidates available

2. **Storage Size Tracking**
   - SQLite PRAGMA returns correct size
   - Size updates on store
   - Size calculation for existing CoValues

3. **Timestamp Handling**
   - `lastAccessedEpoch` uses `Date.now()` correctly
   - Timestamps persist correctly across app restarts
   - Age calculation works correctly with persisted timestamps

### Integration Tests

1. **End-to-end Eviction Flow**
   - Fill storage to limit
   - Verify eviction triggers
   - Verify correct CoValues are evicted (by score)
   - Verify in-memory CoValues are NOT evicted
   - Verify unsynced CoValues are NOT evicted

2. **Limit Enforcement**
   - Write that would exceed limit triggers eviction
   - Memory-only mode activates when no candidates available
   - Storage resumes when candidates become available

3. **Recovery After Eviction**
   - Evicted CoValue can be re-fetched from server
   - Re-fetched CoValue has fresh `lastAccessedEpoch`

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
| `packages/cojson/src/storage/StorageEvictionManager.ts` | NEW | Core eviction logic |
| `packages/cojson/src/storage/types.ts` | MODIFY | Add eviction methods to StorageAPI |
| `packages/cojson/src/storage/storageSync.ts` | MODIFY | Add limit checks, metadata tracking, delete methods |
| `packages/cojson/src/storage/storageAsync.ts` | MODIFY | Add limit checks, metadata tracking, delete methods |
| `packages/cojson/src/storage/knownState.ts` | MODIFY | Add `clearKnownState` and `clearKnownStates` methods |
| `packages/cojson/src/storage/sqlite/client.ts` | MODIFY | Implement eviction methods |
| `packages/cojson/src/storage/sqlite/sqliteMigrations.ts` | MODIFY | Add migration v5 |
| `packages/cojson/src/storage/sqliteAsync/client.ts` | MODIFY | Implement eviction methods |
| `packages/cojson-storage-indexeddb/src/idbClient.ts` | MODIFY | Implement eviction methods |
| `packages/cojson-storage-indexeddb/src/idbNode.ts` | MODIFY | Add schema migration |
| `packages/cojson/src/localNode.ts` | MODIFY | Initialize eviction manager |
| `packages/cojson/src/config.ts` | MODIFY | Add eviction config |

**Note:** `GarbageCollector.ts` does NOT need modification. The storage eviction system uses a separate `lastAccessedEpoch` field (with `Date.now()`) that is updated on store/load operations, independent of the GC's `performance.now()`-based tracking.
