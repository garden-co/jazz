/**
 * Opt-in end-to-end benchmark for the production ordinary-row stream helper.
 *
 * JAZZ_STREAM_BENCH=1 JAZZ_STREAM_BENCH_APPENDS=100 \
 *   pnpm --dir packages/jazz-tools exec vitest run --config vitest.config.ts \
 *   src/runtime/stream-storage.benchmark.test.ts --reporter=verbose
 */
import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createDb } from "./db.js";
import { createConventionalStreamStorage } from "./stream-storage.js";

const app = s.defineApp({
  streams: s.table({ rootId: s.string(), prefixBytes: s.int(), inlineTail: s.bytes() }),
  stream_nodes: s.table({
    childIds: s.array(s.string()),
    childLengths: s.array(s.int()),
    height: s.int(),
  }),
  stream_parts: s.table({ data: s.bytes() }),
});

const enabled = process.env.JAZZ_STREAM_BENCH === "1";
const appends = Number(process.env.JAZZ_STREAM_BENCH_APPENDS ?? "100");
const appendBytes = Number(process.env.JAZZ_STREAM_BENCH_APPEND_BYTES ?? "32");
const tailBytes = Number(process.env.JAZZ_STREAM_BENCH_TAIL_BYTES ?? `${64 * 1024}`);
const fanout = Number(process.env.JAZZ_STREAM_BENCH_FANOUT ?? "32");

function percentile(values: number[], quantile: number) {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * quantile))] ?? 0;
}

describe.skipIf(!enabled)("ordinary-row stream production benchmark", () => {
  it("measures authority-accepted appends, consolidation, reads, and stored rows", async () => {
    const appId = randomUUID();
    const adminSecret = "stream-storage-benchmark-admin";
    const server = await startLocalJazzServer({ appId, adminSecret });
    const latencies: number[] = [];
    const consolidationLatencies: number[] = [];
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        schema: app.wasmSchema,
        permissions: {},
      });
      const db = await createDb({
        appId,
        adminSecret,
        serverUrl: server.url,
        driver: { type: "memory" },
      });
      const storage = createConventionalStreamStorage(db, app, {
        inlineTailBytes: tailBytes,
        fanout,
      });
      try {
        const stream = await storage.create({ tier: "edge" });
        const snapshots = [];
        let prefixBytes = 0;
        for (let ordinal = 0; ordinal < appends; ordinal += 1) {
          const chunk = new Uint8Array(appendBytes);
          chunk.fill(ordinal & 255);
          const started = performance.now();
          const snapshot = await storage.append(stream.id, chunk);
          const elapsed = performance.now() - started;
          latencies.push(elapsed);
          if (snapshot.prefixBytes !== prefixBytes) consolidationLatencies.push(elapsed);
          prefixBytes = snapshot.prefixBytes;
          snapshots.push(snapshot);
        }

        const readStarted = performance.now();
        const current = await storage.read(stream.id);
        const fullReadMs = performance.now() - readStarted;
        expect(current).toHaveLength(appends * appendBytes);
        for (const index of new Set([0, Math.floor(appends / 2), appends - 1])) {
          expect(await storage.read(snapshots[index]!)).toHaveLength((index + 1) * appendBytes);
        }
        const [nodes, parts] = await Promise.all([
          db.all(app.stream_nodes.where({}), { tier: "local" }),
          db.all(app.stream_parts.where({}), { tier: "local" }),
        ]);
        console.info(
          `[ordinary-stream-production] ${JSON.stringify({
            appends,
            appendBytes,
            tailBytes,
            fanout,
            durableP50Ms: percentile(latencies, 0.5),
            durableP95Ms: percentile(latencies, 0.95),
            durableMaxMs: Math.max(...latencies),
            consolidationCount: consolidationLatencies.length,
            consolidationP50Ms: percentile(consolidationLatencies, 0.5),
            consolidationP95Ms: percentile(consolidationLatencies, 0.95),
            nodeRows: nodes.length,
            partRows: parts.length,
            fullReadMs,
            clientStorage: "Node createDb uses in-memory WASM; disk delta not measured here",
          })}`,
        );
      } finally {
        await db.shutdown();
      }
    } finally {
      await server.stop();
    }
  }, 180_000);
});
