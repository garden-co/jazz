#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { performance } from "node:perf_hooks";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../../..");
const wasmModule = await import(pathToFileURL(join(repoRoot, "crates/jazz-wasm/pkg/jazz_wasm.js")));
const wasmBytes = await readFile(join(repoRoot, "crates/jazz-wasm/pkg/jazz_wasm_bg.wasm"));
await wasmModule.default(wasmBytes);

const probes = [
  ["arithmetic_hash", 200_000_000, 0, wasmModule.benchProbeArithmeticHash],
  ["dyn_dispatch", 100_000_000, 0, wasmModule.benchProbeDynDispatch],
  ["refcell_borrow", 100_000_000, 0, wasmModule.benchProbeRefCellBorrow],
  ["alloc_churn", 5_000_000, 0, wasmModule.benchProbeAllocChurn],
  ["random_access_memory", 50_000_000, 4_194_304, wasmModule.benchProbeRandomAccessMemory],
];

const results = [];
for (const [shape, iterations, entries, run] of probes) {
  if (typeof run !== "function") {
    throw new Error(`${shape} probe export missing; rebuild jazz-wasm with --features bench-probes`);
  }
  const started = performance.now();
  const checksum = entries > 0 ? run(iterations, entries) : run(iterations);
  results.push({
    shape,
    iterations,
    entries,
    elapsedMs: Math.round((performance.now() - started) * 1000) / 1000,
    checksum: checksum.toString(),
  });
}
console.log(JSON.stringify(results, null, 2));
