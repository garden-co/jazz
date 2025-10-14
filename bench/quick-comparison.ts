/**
 * QUICK & RELIABLE COMPARISON
 * NEW (with chain splitting) vs OLD (v0.18.24)
 */

import { WasmCrypto as WasmCryptoNew } from "cojson/src/crypto/WasmCrypto.js";
import { LocalNode as LocalNodeNew } from "cojson/src/localNode.js";
import { RawCoList as RawCoListNew } from "cojson/src/coValues/coList.js";

import { WasmCrypto as WasmCryptoOld } from "cojson-latest/src/crypto/WasmCrypto.js";
import { LocalNode as LocalNodeOld } from "cojson-latest/src/localNode.js";
import { RawCoList as RawCoListOld } from "cojson-latest/src/coValues/coList.js";

function mean(arr: number[]): number {
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function formatTime(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(2)}µs`;
  if (ms < 1000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

async function main() {
  console.log("\n╔════════════════════════════════════════════════════════╗");
  console.log("║   QUICK COMPARISON: NEW vs OLD                         ║");
  console.log("╚════════════════════════════════════════════════════════╝\n");

  const cryptoNew = await WasmCryptoNew.create();
  const cryptoOld = await WasmCryptoOld.create();

  const tests = [
    {
      name: "10000 sequential appends",
      size: 10000,
      type: "sequential",
    },
    {
      name: "1000 sequential appends",
      size: 1000,
      type: "sequential",
    },
    {
      name: "500 sequential appends",
      size: 500,
      type: "sequential",
    },
    {
      name: "100 random inserts",
      size: 100,
      type: "random",
    },
    {
      name: "500 seq + 50 random (mixed)",
      size: 500,
      random: 50,
      type: "mixed",
    },
  ];

  for (const test of tests) {
    console.log(`\n${"=".repeat(56)}`);
    console.log(`📊 ${test.name}`);
    console.log("=".repeat(56));

    const iterations = 10;

    // OLD version
    const oldTimes: number[] = [];
    for (let i = 0; i < iterations; i++) {
      const account = LocalNodeOld.internalCreateAccount({ crypto: cryptoOld });
      const list = account.core.node
        .createGroup()
        .createList() as RawCoListOld<number>;

      const start = performance.now();

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

      const items = list.asArray();
      const end = performance.now();
      oldTimes.push(end - start);
    }

    // NEW version
    const newTimes: number[] = [];
    let stats: any = null;
    for (let i = 0; i < iterations; i++) {
      const account = LocalNodeNew.internalCreateAccount({ crypto: cryptoNew });
      const list = account.core.node
        .createGroup()
        .createList() as RawCoListNew<number>;

      const start = performance.now();

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

      const items = list.asArray();
      const end = performance.now();
      newTimes.push(end - start);

      if (i === 0) {
        stats = (list as any).getCompactionStats?.();
      }
    }

    const oldAvg = mean(oldTimes);
    const newAvg = mean(newTimes);
    const speedup = oldAvg / newAvg;
    const improvement = ((oldAvg - newAvg) / oldAvg) * 100;

    console.log(`\n  OLD: ${formatTime(oldAvg)}`);
    console.log(`  NEW: ${formatTime(newAvg)}`);

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
  }

  console.log("\n" + "=".repeat(56));
  console.log("✅ Comparison Complete");
  console.log("=".repeat(56) + "\n");
}

main().catch(console.error);
