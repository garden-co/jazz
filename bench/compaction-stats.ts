import { WasmCrypto } from "../packages/cojson/src/crypto/WasmCrypto.js";
import { LocalNode } from "../packages/cojson/src/localNode.js";
import { RawCoList } from "../packages/cojson/src/coValues/coList.js";

async function showCompactionStats() {
  console.log("📊 CoList Graph Compaction - Statistics Analysis\n");
  console.log("=".repeat(70) + "\n");

  const crypto = await WasmCrypto.create();

  // Test 1: Sequential appends (best case)
  console.log("🔹 Test 1: Sequential Appends (1000 items)");
  console.log("-".repeat(70));

  const account1 = LocalNode.internalCreateAccount({ crypto });
  const node1 = account1.core.node;
  const group1 = node1.createGroup();
  const list1 = group1.createList() as RawCoList<number>;

  const startInsert1 = performance.now();
  for (let i = 0; i < 1000; i++) {
    list1.append(i);
  }
  const endInsert1 = performance.now();

  const stats1 = list1.getCompactionStats();

  // Measure read performance
  list1._cachedEntries = undefined;
  const startRead1 = performance.now();
  const items1 = list1.asArray();
  const endRead1 = performance.now();

  console.log(`  Insertions: ${(endInsert1 - startInsert1).toFixed(2)}ms`);
  console.log(`  First read (cold): ${(endRead1 - startRead1).toFixed(3)}ms`);
  console.log(`  Total nodes: ${stats1.totalNodes}`);
  console.log(`  Linear chains: ${stats1.linearChains}`);
  console.log(
    `  Compactable nodes: ${stats1.compactableNodes} (${((stats1.compactableNodes / stats1.totalNodes) * 100).toFixed(1)}%)`,
  );
  console.log(`  Avg chain length: ${stats1.avgChainLength.toFixed(2)}`);
  console.log(`  Max chain length: ${stats1.maxChainLength}`);
  console.log(
    `  Compaction ratio: ${(stats1.compactionRatio * 100).toFixed(2)}%`,
  );
  console.log(`  Items in list: ${items1.length}\n`);

  // Test 2: Mixed operations
  console.log("🔹 Test 2: Mixed Operations (500 sequential + 50 random)");
  console.log("-".repeat(70));

  const account2 = LocalNode.internalCreateAccount({ crypto });
  const node2 = account2.core.node;
  const group2 = node2.createGroup();
  const list2 = group2.createList() as RawCoList<number>;

  const startInsert2 = performance.now();
  for (let i = 0; i < 500; i++) {
    list2.append(i);
  }
  for (let i = 0; i < 50; i++) {
    const idx = Math.floor(Math.random() * list2.asArray().length);
    list2.append(1000 + i, idx);
  }
  const endInsert2 = performance.now();

  const stats2 = list2.getCompactionStats();

  list2._cachedEntries = undefined;
  const startRead2 = performance.now();
  const items2 = list2.asArray();
  const endRead2 = performance.now();

  console.log(`  Insertions: ${(endInsert2 - startInsert2).toFixed(2)}ms`);
  console.log(`  First read (cold): ${(endRead2 - startRead2).toFixed(3)}ms`);
  console.log(`  Total nodes: ${stats2.totalNodes}`);
  console.log(`  Linear chains: ${stats2.linearChains}`);
  console.log(
    `  Compactable nodes: ${stats2.compactableNodes} (${((stats2.compactableNodes / stats2.totalNodes) * 100).toFixed(1)}%)`,
  );
  console.log(`  Avg chain length: ${stats2.avgChainLength.toFixed(2)}`);
  console.log(`  Max chain length: ${stats2.maxChainLength}`);
  console.log(
    `  Compaction ratio: ${(stats2.compactionRatio * 100).toFixed(2)}%`,
  );
  console.log(`  Items in list: ${items2.length}\n`);

  // Test 3: Prepend operations
  console.log("🔹 Test 3: Prepend Operations (500 items)");
  console.log("-".repeat(70));

  const account3 = LocalNode.internalCreateAccount({ crypto });
  const node3 = account3.core.node;
  const group3 = node3.createGroup();
  const list3 = group3.createList() as RawCoList<number>;

  const startInsert3 = performance.now();
  for (let i = 0; i < 500; i++) {
    list3.prepend(i, 0);
  }
  const endInsert3 = performance.now();

  const stats3 = list3.getCompactionStats();

  list3._cachedEntries = undefined;
  const startRead3 = performance.now();
  const items3 = list3.asArray();
  const endRead3 = performance.now();

  console.log(`  Insertions: ${(endInsert3 - startInsert3).toFixed(2)}ms`);
  console.log(`  First read (cold): ${(endRead3 - startRead3).toFixed(3)}ms`);
  console.log(`  Total nodes: ${stats3.totalNodes}`);
  console.log(`  Linear chains: ${stats3.linearChains}`);
  console.log(
    `  Compactable nodes: ${stats3.compactableNodes} (${stats3.totalNodes > 0 ? ((stats3.compactableNodes / stats3.totalNodes) * 100).toFixed(1) : "0.0"}%)`,
  );
  console.log(`  Avg chain length: ${stats3.avgChainLength.toFixed(2)}`);
  console.log(`  Max chain length: ${stats3.maxChainLength}`);
  console.log(
    `  Compaction ratio: ${(stats3.compactionRatio * 100).toFixed(2)}%`,
  );
  console.log(`  Items in list: ${items3.length}\n`);

  // Test 4: With deletions
  console.log("🔹 Test 4: With Deletions (1000 inserts + 200 deletes)");
  console.log("-".repeat(70));

  const account4 = LocalNode.internalCreateAccount({ crypto });
  const node4 = account4.core.node;
  const group4 = node4.createGroup();
  const list4 = group4.createList() as RawCoList<number>;

  const startInsert4 = performance.now();
  for (let i = 0; i < 1000; i++) {
    list4.append(i);
  }
  for (let i = 0; i < 200; i++) {
    const items = list4.asArray();
    if (items.length > 0) {
      const idx = Math.floor(Math.random() * items.length);
      list4.delete(idx);
    }
  }
  const endInsert4 = performance.now();

  const stats4 = list4.getCompactionStats();

  list4._cachedEntries = undefined;
  const startRead4 = performance.now();
  const items4 = list4.asArray();
  const endRead4 = performance.now();

  console.log(`  Operations: ${(endInsert4 - startInsert4).toFixed(2)}ms`);
  console.log(`  First read (cold): ${(endRead4 - startRead4).toFixed(3)}ms`);
  console.log(`  Total nodes: ${stats4.totalNodes}`);
  console.log(`  Linear chains: ${stats4.linearChains}`);
  console.log(
    `  Compactable nodes: ${stats4.compactableNodes} (${((stats4.compactableNodes / stats4.totalNodes) * 100).toFixed(1)}%)`,
  );
  console.log(`  Avg chain length: ${stats4.avgChainLength.toFixed(2)}`);
  console.log(`  Max chain length: ${stats4.maxChainLength}`);
  console.log(
    `  Compaction ratio: ${(stats4.compactionRatio * 100).toFixed(2)}%`,
  );
  console.log(`  Items in list: ${items4.length}\n`);

  // Test 5: Cache performance comparison
  console.log("🔹 Test 5: Cache Performance (1000 items, 1000 reads)");
  console.log("-".repeat(70));

  const account5 = LocalNode.internalCreateAccount({ crypto });
  const node5 = account5.core.node;
  const group5 = node5.createGroup();
  const list5 = group5.createList() as RawCoList<number>;

  for (let i = 0; i < 1000; i++) {
    list5.append(i);
  }

  // Warm up cache
  list5.asArray();

  const iterations = 1000;
  const startCached = performance.now();
  for (let i = 0; i < iterations; i++) {
    list5.asArray();
  }
  const endCached = performance.now();

  const avgCached = (endCached - startCached) / iterations;

  // Measure without cache
  let totalUncached = 0;
  for (let i = 0; i < 100; i++) {
    list5._cachedEntries = undefined;
    const start = performance.now();
    list5.asArray();
    const end = performance.now();
    totalUncached += end - start;
  }
  const avgUncached = totalUncached / 100;

  console.log(`  With cache (hot path): ${avgCached.toFixed(6)}ms per read`);
  console.log(
    `  Without cache (cold path): ${avgUncached.toFixed(3)}ms per read`,
  );
  console.log(`  Cache speedup: ${(avgUncached / avgCached).toFixed(0)}x\n`);

  // Summary
  console.log("=".repeat(70));
  console.log("📈 Summary");
  console.log("=".repeat(70));
  console.log(
    `✅ Sequential operations create long chains (up to ${stats1.maxChainLength} nodes)`,
  );
  console.log(
    `✅ Mixed operations still achieve ${((stats2.compactableNodes / stats2.totalNodes) * 100).toFixed(0)}% compaction`,
  );
  console.log(
    `✅ Cache provides ${(avgUncached / avgCached).toFixed(0)}x speedup on repeated reads`,
  );
  console.log(`✅ Optimization is transparent - no API changes required\n`);
}

showCompactionStats().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
