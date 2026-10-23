/**
 * Cost probes for the lite design: write latency, wire bytes and proxy memory.
 * Prints numbers; asserts nothing about them. Run with JAZZ_LITE_COST=1.
 */
import { afterAll, beforeAll, describe, it } from "vitest";
import { app, type Todo } from "../app/schema.js";
import type { LiteDb } from "../src/client.js";
import { rssKiB, startHarness, type Harness } from "./harness.js";

const enabled = process.env.JAZZ_LITE_COST === "1";
let harness: Harness;

function percentile(values: number[], p: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))]!;
}
const fmt = (values: number[]) =>
  `p50 ${percentile(values, 50).toFixed(1)} ms, p95 ${percentile(values, 95).toFixed(1)} ms`;

function watch(db: LiteDb, options?: Parameters<LiteDb["subscribe"]>[2]) {
  let latest: Todo[] = [];
  let wake = () => {};
  const stop = db.subscribe(
    app.todos,
    (rows) => {
      latest = rows;
      wake();
    },
    options,
  );
  const until = (predicate: (rows: Todo[]) => boolean) =>
    new Promise<void>((resolve) => {
      wake = () => predicate(latest) && resolve();
      wake();
    });
  return { until, stop, rows: () => latest };
}

describe.skipIf(!enabled)("Jazz Lite cost probes", () => {
  beforeAll(async () => {
    harness = await startHarness({ upstreamInChild: true });
  }, 120_000);
  afterAll(async () => {
    await harness?.stop();
  });

  it("write latency on localhost", async () => {
    const db = harness.lite(await harness.identity("cost-latency"));
    await db.ready();
    const owner = db.accountId!;
    for (const readTier of ["remote", "local-first"] as const) {
      const sub = watch(db, { tier: readTier });
      await sub.until(() => true);
      for (const tier of ["global", "local"] as const) {
        const ack: number[] = [];
        const visible: number[] = [];
        for (let i = 0; i < 40; i++) {
          const title = `${readTier}-${tier}-${i}`;
          const start = performance.now();
          const shown = sub.until((rows) => rows.some((t) => t.title === title));
          await db.insert(app.todos, { title, done: false, owner_id: owner }, { tier });
          ack.push(performance.now() - start);
          await shown;
          visible.push(performance.now() - start);
        }
        console.log(
          `[cost] subscription tier=${readTier}, insert wait=${tier}: ack ${fmt(ack)}; visible in subscription ${fmt(visible)}`,
        );
      }
      sub.stop();
    }
  });

  it("wire bytes: initial result vs one-row update", async () => {
    const db = harness.lite(await harness.identity("cost-wire"));
    await db.ready();
    const owner = db.accountId!;
    for (let i = 0; i < 100; i++) {
      await db.insert(
        app.todos,
        { title: `wire-${i}`, done: false, owner_id: owner },
        { tier: "local" },
      );
    }
    const before = harness.proxy.stats.bytesOut;
    const sub = watch(db);
    await sub.until((rows) => rows.length === 100);
    const initial = harness.proxy.stats.bytesOut - before;
    const mid = harness.proxy.stats.bytesOut;
    await db.update(app.todos, sub.rows()[42]!.id, { done: true });
    await sub.until((rows) => rows.some((t) => t.done));
    console.log(
      `[cost] 100-row subscription: initial ${initial} B; one update ${harness.proxy.stats.bytesOut - mid} B (incl. write ack)`,
    );
    sub.stop();
  });

  it("proxy memory per connected user", async () => {
    const users = 100;
    const tokens = await Promise.all(
      Array.from({ length: users }, (_, i) => harness.identity(`cost-mem-${i}`)),
    );
    const snap = () => {
      global.gc?.();
      return {
        proxy: rssKiB(),
        heap: process.memoryUsage().heapUsed / 1024,
        server: rssKiB(harness.upstream.pid),
      };
    };
    const report = (label: string, from: ReturnType<typeof snap>, to: ReturnType<typeof snap>) =>
      console.log(
        `[cost] ${label}, per user: proxy RSS +${((to.proxy - from.proxy) / users).toFixed(0)} KiB ` +
          `(JS heap +${((to.heap - from.heap) / users).toFixed(0)} KiB), ` +
          `upstream server RSS +${((to.server - from.server) / users).toFixed(0)} KiB`,
      );
    const dbs: LiteDb[] = [];
    const s0 = snap();
    for (const token of tokens) {
      const db = harness.lite(token);
      await db.ready();
      dbs.push(db);
    }
    const s1 = snap();
    for (const db of dbs) {
      for (let i = 0; i < 10; i++) {
        await db.insert(
          app.todos,
          { title: `m-${i}`, done: false, owner_id: db.accountId! },
          { tier: "local" },
        );
      }
    }
    const s2 = snap();
    const subs: Array<ReturnType<typeof watch>> = [];
    for (const db of dbs) {
      const sub = watch(db);
      await sub.until((rows) => rows.length === 10);
      subs.push(sub);
    }
    const s3 = snap();
    report(`${users} users connected (no subscriptions)`, s0, s1);
    report(`10 writes each`, s1, s2);
    report(`1 live subscription each (10 rows)`, s2, s3);
    for (const sub of subs) sub.stop();
  }, 300_000);
});
