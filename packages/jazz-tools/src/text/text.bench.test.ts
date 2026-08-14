/**
 * Opt-in durable receipt for the production ordinary-row text API.
 *
 * JAZZ_TEXT_BENCH=1 JAZZ_TEXT_BENCH_INITIAL_BYTES=102400 \
 * JAZZ_TEXT_BENCH_EDITS=300 pnpm --dir packages/jazz-tools exec vitest run \
 * src/text/text.bench.test.ts --reporter=verbose
 */
import { mkdtempSync, readdirSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { schema as s } from "../index.js";
import {
  createTextStore,
  textTableDefinitions,
  textTablesFromApp,
  type TextSnapshot,
} from "./index.js";

const app = s.defineApp({
  ...textTableDefinitions,
  text_benchmark_whole: s.table({ value: s.string() }),
});
const enabled = process.env.JAZZ_TEXT_BENCH === "1";
const editCount = Number(process.env.JAZZ_TEXT_BENCH_EDITS ?? "300");
const initialBytes = Number(process.env.JAZZ_TEXT_BENCH_INITIAL_BYTES ?? "102400");

function percentile(values: readonly number[], quantile: number): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * quantile))] ?? 0;
}

function directorySize(path: string): number {
  const stat = statSync(path);
  return stat.isDirectory()
    ? readdirSync(path).reduce((total, child) => total + directorySize(join(path, child)), 0)
    : stat.size;
}

function insertionOffset(length: number, edit: number, workload: "end" | "middle"): number {
  return workload === "end" ? length : (edit * 48271 + 17) % (length + 1);
}

async function wholeStringReceipt(initial: string, workload: "end" | "middle") {
  const path = mkdtempSync(join(tmpdir(), "jazz-text-whole-"));
  const context = createJazzContext({
    appId: `text-whole-${crypto.randomUUID()}`,
    app,
    permissions: {},
    driver: { type: "persistent", dataPath: path },
    adminSecret: "ordinary-text-benchmark",
    tier: "local",
  });
  const db = context.asBackend();
  let value = initial;
  let logicalBytes = value.length;
  const latencies: number[] = [];
  try {
    const document = db.insert(app.text_benchmark_whole, { value });
    await document.wait({ tier: "local" });
    for (let edit = 0; edit < editCount; edit += 1) {
      const at = insertionOffset(value.length, edit, workload);
      value = value.slice(0, at) + "x" + value.slice(at);
      const started = performance.now();
      const write = db.update(app.text_benchmark_whole, document.value.id, { value });
      await write.wait({ tier: "local" });
      latencies.push(performance.now() - started);
      logicalBytes += value.length;
    }
    const readStarted = performance.now();
    expect(
      (await db.one(app.text_benchmark_whole.where({ id: document.value.id }), { tier: "local" }))
        ?.value,
    ).toBe(value);
    const materializationMs = performance.now() - readStarted;
    await context.shutdown();
    return {
      label: `whole-${workload}`,
      editCount,
      p50Ms: percentile(latencies, 0.5),
      p95Ms: percentile(latencies, 0.95),
      ordinaryRowsPerEdit: 1,
      retainedVersionRows: 0,
      logicalWriteBytes: logicalBytes,
      diskBytes: directorySize(path),
      currentReadMs: materializationMs,
    };
  } finally {
    await context.shutdown().catch(() => undefined);
    rmSync(path, { recursive: true, force: true });
  }
}

async function textReceipt(initial: string, workload: "end" | "middle") {
  const path = mkdtempSync(join(tmpdir(), "jazz-text-frontier-"));
  const context = createJazzContext({
    appId: `text-frontier-${crypto.randomUUID()}`,
    app,
    permissions: {},
    driver: { type: "persistent", dataPath: path },
    adminSecret: "ordinary-text-benchmark",
    tier: "local",
  });
  const db = context.asBackend();
  const store = createTextStore(db, textTablesFromApp(app), {
    maxPatches: 32,
    maxPatchBytes: 4096,
    leafBytes: 4096,
  });
  const latencies: number[] = [];
  const consolidationLatencies: number[] = [];
  const samples: TextSnapshot[] = [];
  let logicalFrontierBytes = 0;
  try {
    let snapshot = await store.create(initial);
    const initialNodeCount = (await db.all(app.jazz_text_nodes, { tier: "local" })).length;
    samples.push(snapshot);
    for (let edit = 0; edit < editCount; edit += 1) {
      const before = snapshot;
      const at = insertionOffset(snapshot.length, edit, workload);
      const started = performance.now();
      snapshot = await store.insert(snapshot, at, "x");
      const elapsed = performance.now() - started;
      latencies.push(elapsed);
      logicalFrontierBytes += snapshot.patchBytes + 16 * 3 + 1;
      if (snapshot.patchCount === 0 && before.patchCount > 0) {
        consolidationLatencies.push(elapsed);
      }
      if (edit === Math.floor(editCount / 2) || edit === editCount - 1) samples.push(snapshot);
    }
    const historicalReads: number[] = [];
    for (const sample of samples) {
      const started = performance.now();
      const loaded = await store.readVersion(sample.versionId);
      historicalReads.push(performance.now() - started);
      expect(loaded.text).toBe(sample.text);
    }
    const [nodes, versions] = await Promise.all([
      db.all(app.jazz_text_nodes, { tier: "local" }),
      db.all(app.jazz_text_versions, { tier: "local" }),
    ]);
    const nodePayloadBytes = nodes.reduce(
      (total, node) =>
        total + (node.text?.length ?? 0) + (node.left?.length ?? 0) + (node.right?.length ?? 0),
      0,
    );
    await context.shutdown();
    return {
      label: `frontier-${workload}`,
      editCount,
      p50Ms: percentile(latencies, 0.5),
      p95Ms: percentile(latencies, 0.95),
      ordinaryRowsPerEdit: 2 + (nodes.length - initialNodeCount) / editCount,
      retainedVersionRows: versions.length,
      consolidationCount: consolidationLatencies.length,
      consolidationP50Ms: percentile(consolidationLatencies, 0.5),
      consolidationP95Ms: percentile(consolidationLatencies, 0.95),
      logicalWriteBytesLowerBound: logicalFrontierBytes + nodePayloadBytes,
      diskBytes: directorySize(path),
      historicalMaterializationP50Ms: percentile(historicalReads, 0.5),
      finalPatchCount: snapshot.patchCount,
    };
  } finally {
    await context.shutdown().catch(() => undefined);
    rmSync(path, { recursive: true, force: true });
  }
}

describe.skipIf(!enabled)("ordinary-row text durable receipt", () => {
  it("compares whole strings with the bounded patch frontier", async () => {
    const initial = "a".repeat(initialBytes);
    const receipts = [];
    for (const workload of ["end", "middle"] as const) {
      receipts.push(await wholeStringReceipt(initial, workload));
      receipts.push(await textReceipt(initial, workload));
    }
    for (const receipt of receipts) console.info(`[ordinary-text] ${JSON.stringify(receipt)}`);
    expect(receipts).toHaveLength(4);
  }, 600_000);
});
