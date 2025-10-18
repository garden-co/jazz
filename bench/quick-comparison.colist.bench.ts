/**
 * QUICK & RELIABLE COMPARISON
 * NEW (with chain splitting) vs OLD (v0.18.24)
 */

import { Bench } from "tinybench";
import { formatTime, displayBenchmarkResults } from "./utils.js";
import { WasmCrypto as WasmCryptoNew } from "cojson/src/crypto/WasmCrypto.js";
import { LocalNode as LocalNodeNew } from "cojson/src/localNode.js";
import { RawCoList as RawCoListNew } from "cojson/src/coValues/coList.js";

import { WasmCrypto as WasmCryptoOld } from "cojson-latest/src/crypto/WasmCrypto.js";
import { LocalNode as LocalNodeOld } from "cojson-latest/src/localNode.js";
import { RawCoList as RawCoListOld } from "cojson-latest/src/coValues/coList.js";

async function main() {
  console.log("\n╔════════════════════════════════════════════════════════╗");
  console.log("║   QUICK COMPARISON: NEW vs OLD (tinybench)             ║");
  console.log("╚════════════════════════════════════════════════════════╝\n");

  const cryptoNew = await WasmCryptoNew.create();
  const cryptoOld = await WasmCryptoOld.create();

  const tests = [
    {
      name: "10000 sequential appends",
      size: 10000,
      type: "sequential" as const,
    },
    {
      name: "1000 sequential appends",
      size: 1000,
      type: "sequential" as const,
    },
    {
      name: "500 sequential appends",
      size: 500,
      type: "sequential" as const,
    },
    {
      name: "1000 random inserts",
      size: 1000,
      type: "random" as const,
    },
    {
      name: "500 seq + 50 random (mixed)",
      size: 500,
      random: 50,
      type: "mixed" as const,
    },
  ];

  for (const test of tests) {
    console.log(`\n${"=".repeat(56)}`);
    console.log(`📊 ${test.name}`);
    console.log("=".repeat(56));

    const bench = new Bench({ iterations: 10 });
    let stats: any = null;

    // OLD version task
    bench.add("OLD (v0.18.24)", () => {
      const account = LocalNodeOld.internalCreateAccount({ crypto: cryptoOld });
      const list = account.core.node
        .createGroup()
        .createList() as RawCoListOld<number>;

      if (test.type === "sequential") {
        for (let j = 0; j < test.size; j++) {
          list.append(j);
        }
      } else if (test.type === "random") {
        for (let j = 0; j < test.size; j++) {
          const arr = list.asArray();
          const idx =
            arr.length > 0 ? Math.floor(Math.random() * arr.length) : 0;
          list.append(j, idx);
        }
      } else if (test.type === "mixed") {
        for (let j = 0; j < test.size; j++) {
          list.append(j);
        }
        for (let j = 0; j < test.random!; j++) {
          const arr = list.asArray();
          const idx = Math.floor(Math.random() * arr.length);
          list.append(1000 + j, idx);
        }
      }

      list.asArray(); // Force materialization
    });

    // NEW version task
    bench.add("NEW (current)", () => {
      const account = LocalNodeNew.internalCreateAccount({ crypto: cryptoNew });
      const list = account.core.node
        .createGroup()
        .createList() as RawCoListNew<number>;

      if (test.type === "sequential") {
        for (let j = 0; j < test.size; j++) {
          list.append(j);
        }
      } else if (test.type === "random") {
        for (let j = 0; j < test.size; j++) {
          const arr = list.asArray();
          const idx =
            arr.length > 0 ? Math.floor(Math.random() * arr.length) : 0;
          list.append(j, idx);
        }
      } else if (test.type === "mixed") {
        for (let j = 0; j < test.size; j++) {
          list.append(j);
        }
        for (let j = 0; j < test.random!; j++) {
          const arr = list.asArray();
          const idx = Math.floor(Math.random() * arr.length);
          list.append(1000 + j, idx);
        }
      }

      list.asArray(); // Force materialization

      // Capture stats on first iteration
      if (!stats) {
        stats = (list as any).getCompactionStats?.();
      }
    });

    await bench.run();

    const results = bench.tasks.map((task) => ({
      name: task.name,
      mean: task.result?.mean || 0,
      hz: task.result?.hz || 0,
      p75: task.result?.p75,
      p99: task.result?.p99,
      p995: task.result?.p995,
      p999: task.result?.p999,
    }));

    const oldResult = results.find((r) => r.name === "OLD (v0.18.24)");
    const newResult = results.find((r) => r.name === "NEW (current)");

    if (oldResult && newResult) {
      const speedup = oldResult.mean / newResult.mean;
      const improvement =
        ((oldResult.mean - newResult.mean) / oldResult.mean) * 100;

      console.log(
        `\n  OLD: ${formatTime(oldResult.mean)} (p99: ${formatTime(oldResult.p99 || 0)})`,
      );
      console.log(
        `  NEW: ${formatTime(newResult.mean)} (p99: ${formatTime(newResult.p99 || 0)})`,
      );

      if (speedup > 1) {
        console.log(
          `  ✅ ${speedup.toFixed(2)}x FASTER (+${improvement.toFixed(1)}%)`,
        );
      } else if (speedup < 0.95) {
        console.log(
          `  ❌ ${(1 / speedup).toFixed(2)}x SLOWER (${improvement.toFixed(1)}%)`,
        );
      } else {
        console.log(
          `  ⚖️  Similar performance (${improvement > 0 ? "+" : ""}${improvement.toFixed(1)}%)`,
        );
      }
    }

    if (stats) {
      const compactionPct =
        stats.totalNodes > 0
          ? ((stats.compactableNodes / stats.totalNodes) * 100).toFixed(0)
          : "0";
      console.log(
        `\n  📊 Compaction: ${stats.linearChains} chains, ${compactionPct}% compactable`,
      );
      if (stats.maxChainLength > 0) {
        console.log(`      Max chain: ${stats.maxChainLength} nodes`);
      }
    }

    console.log("\n  📈 Detailed Results:");
    displayBenchmarkResults(bench, true);
  }

  console.log("\n" + "=".repeat(56));
  console.log("✅ Comparison Complete");
  console.log("=".repeat(56) + "\n");
}

main().catch(console.error);
