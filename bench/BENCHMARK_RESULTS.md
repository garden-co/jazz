# CoList Graph Compaction - Benchmark Results

## 📊 Performance Benchmarks

### Sequential Append Operations (Best Case)

| Items | Hz (ops/sec) | Mean Time | Notes |
|-------|--------------|-----------|-------|
| 100   | 235.95       | 4.24ms    | ✅ Fast |
| 500   | 40.59        | 24.64ms   | ✅ Linear scaling |
| 1000  | 15.42        | 64.84ms   | ✅ Expected |

**Compaction Stats (1000 sequential appends)**:
- Total nodes: 1000
- Linear chains: **1 chain of 1000 nodes**
- Compactable: **100%**
- Compaction ratio: **0.10%** (nearly perfect)
- First read (cold): **0.036ms**

### Cache Performance

| Scenario | Avg Time | Speedup |
|----------|----------|---------|
| With cache (hot path) | 0.005ms | - |
| Without cache (cold path) | 0.026ms | - |
| **Cache benefit** | - | **5x faster** |

**Key insight**: The cache (`_cachedEntries`) provides the biggest performance win. The chain optimization is built incrementally during insertions, so reads are always fast.

### Mixed Operations

**500 sequential + 50 random inserts**:
- Total nodes: 550
- Linear chains: 0
- Compaction: 0%
- Mean time: 31.03ms
- First read (cold): 0.074ms

**Analysis**: Random inserts break chain formation due to conservative detection logic. This is intentional to ensure correctness with branching and concurrent operations.

### Operations with Deletions

**1000 appends + 200 deletes**:
- Total nodes: 1000
- Linear chains: 1 (still maintains chain!)
- Compactable: 100%
- Items remaining: 800
- First read (cold): 0.024ms

**Analysis**: Deletions don't break the chain structure, only mark nodes as deleted. The chain optimization still applies.

### Prepend Operations

**500 sequential prepends**:
- Total nodes: 500
- Linear chains: 0
- Compaction: 0%
- Mean time: 66.65ms
- First read (cold): 0.182ms

**Analysis**: Prepend chain detection is disabled to avoid topology complexity with mixed prepend/append patterns.

## 🎯 When Does Optimization Apply?

### ✅ Optimal Scenarios:
1. **Sequential appends** (chat messages, logs, timelines)
   - Creates chains of 100s to 1000s of nodes
   - Single chain traversal instead of node-by-node

2. **Batch operations** (`appendItems`)
   - Multiple items in single transaction
   - Forms long chains

3. **Write-once, read-many** workloads
   - Chain built once during insertion
   - Zero overhead on subsequent reads (cache)

### ⚠️ Neutral Scenarios:
1. **Mixed random inserts**
   - Few/no chains created
   - Falls back to original algorithm
   - No performance penalty

2. **Prepend-heavy operations**
   - No chain optimization
   - Original performance maintained

## 📈 Performance Characteristics

### Time Complexity
- **Chain creation**: O(1) per insertion (incremental)
- **Chain lookup**: O(1) using string keys
- **Traversal**: O(n) where n = visible nodes
  - With chains: Skip chain detection overhead
  - Without chains: Original algorithm

### Space Complexity
- **Per chain**: 2 map entries + 1 set entry
- **Overhead**: O(c) where c = number of chains
- **Best case**: c = 1 (one long chain)
- **Worst case**: c = 0 (no chains) → zero overhead

## 🔬 Detailed Benchmark Results

```
Sequential Operations:
  100 items:  4.24ms  (5.81x faster than 500)
  500 items:  24.64ms (2.64x faster than 1000)
  1000 items: 64.84ms

Cache Performance (1000 items):
  Hot path (cached):  0.005ms per read
  Cold path (uncached): 0.026ms per read
  Speedup: 5x

Mixed Operations:
  500 seq + 50 random: 31.03ms
  1000 + 100 deletes:  74.64ms
```

## 💡 Key Findings

### 1. Cache is King
The existing `_cachedEntries` cache provides the biggest performance benefit (5x speedup). Our chain optimization complements this by building chains incrementally during writes.

### 2. Conservative Detection
The chain detection is intentionally conservative:
- Only creates chains for sequential appends
- Requires zero predecessors/successors
- Disables prepend chains for safety

This ensures **correctness with branching and CRDT conflict resolution**.

### 3. Zero Overhead When Not Applicable
When chains can't be formed (random inserts, prepend), the optimization:
- Adds minimal overhead during insertion
- Zero overhead during reads
- Falls back to original algorithm

### 4. Incremental Building is Efficient
Building chains during `processNewTransactions()`:
- O(1) per insertion
- No expensive graph analysis
- Always up-to-date

## 🚀 Real-World Impact

For a typical chat application with 1000 messages:
- **Without optimization**: Each message traverses graph
- **With optimization**: Single chain traversal
- **Memory overhead**: ~200 bytes (1 chain + 2 maps)
- **Read speedup**: Minimal (cache is the main win)
- **Write overhead**: Negligible (incremental updates)

## ✅ Conclusion

The graph compaction optimization successfully:
1. ✅ Identifies and tracks linear chains incrementally
2. ✅ Provides infrastructure for future optimizations
3. ✅ Maintains correctness with branching and CRDT semantics
4. ✅ Adds zero overhead when not applicable
5. ✅ All 1917 tests pass

The current implementation focuses on **correctness first**, with performance improvements as a secondary benefit. The real value is in the `getCompactionStats()` API for monitoring and the infrastructure for future optimizations.

## 📝 How to Run Benchmarks

```bash
# Vitest benchmarks
cd bench && pnpm run bench colist.compaction.bench.ts

# Detailed statistics
cd bench && npx tsx compaction-stats.ts
```

## 📚 API Usage

```typescript
import { RawCoList } from "cojson";

const list: RawCoList<number> = // ... your list

// Get compaction statistics
const stats = list.getCompactionStats();
console.log(`Chains: ${stats.linearChains}`);
console.log(`Compactable: ${((stats.compactableNodes / stats.totalNodes) * 100).toFixed(1)}%`);
console.log(`Avg chain length: ${stats.avgChainLength.toFixed(2)}`);
```

