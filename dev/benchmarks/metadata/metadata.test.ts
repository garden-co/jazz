import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { benchmarkMetadata, getBenchmarkMetadata, throughput } from "./index.ts";

const root = new URL("../../../", import.meta.url);
// No fixed total: parallel example lanes add suites independently. Coverage of
// every Divan function is asserted below; names must stay unique.
test("catalogue documents every current wallclock case once", () => {
  assert.ok(benchmarkMetadata.length > 0);
  assert.equal(new Set(benchmarkMetadata.map((m) => m.name)).size, benchmarkMetadata.length);
  for (const m of benchmarkMetadata) {
    for (const key of ["name", "title", "description", "fixture", "storage", "source"] as const)
      assert.ok(m[key].length > 0, `${m.name}: ${key}`);
    assert.ok(m.includes.length > 0 && m.excludes.length > 0, m.name);
    assert.ok(m.work.count > 0 && Number.isFinite(m.work.count), m.name);
    assert.ok(m.work.unit.endsWith("/s") && m.work.explanation.length > 0, m.name);
    assert.ok(existsSync(new URL(m.source, root)), `${m.name}: source exists`);
  }
});

test("every current Divan function in the owned suites has a metadata entry", () => {
  const paths = [...new Set(benchmarkMetadata.map((m) => m.source))];
  for (const path of paths) {
    const source = readFileSync(new URL(path, root), "utf8");
    const functions = [...source.matchAll(/#\[divan::bench\(([\s\S]*?)\)\]\s*fn\s+(\w+)/g)];
    assert.ok(functions.length > 0, path);
    for (const [, attributes, name] of functions) {
      assert.ok(
        benchmarkMetadata.some((m) => m.name === name || m.name.startsWith(`${name}[`)),
        `${path}: ${name}`,
      );
      const args = attributes.match(/args\s*=\s*\[([^\]]+)\]/)?.[1];
      if (!args) continue;
      const tokens = args.match(/\([^)]*\)|[^,\s]+/g) ?? [];
      for (const token of tokens) {
        let suffix: string;
        if (token.startsWith("("))
          suffix = `(${(token.match(/\d[\d_]*/g) ?? []).map((n) => Number(n.replaceAll("_", ""))).join(", ")})`;
        else if (/^\d[\d_]*$/.test(token)) suffix = String(Number(token.replaceAll("_", "")));
        else {
          assert.match(token, /^\w+$/);
          const value = source.match(new RegExp(`const ${token}: usize = ([\\d_]+);`))?.[1];
          assert.ok(value, `${path}: resolve benchmark argument ${token}`);
          suffix = String(Number(value.replaceAll("_", "")));
        }
        assert.ok(
          getBenchmarkMetadata(`${name}[${suffix}]`),
          `${path}: missing variant ${name}[${suffix}]`,
        );
      }
    }
  }
});

test("denominators distinguish transaction count, batch rows, queries and load context", () => {
  assert.equal(getBenchmarkMetadata("stage_plan_add_task_1350")?.work.count, 1350);
  assert.equal(getBenchmarkMetadata("stage_plan_bulk_complete_1350")?.work.unit, "rows updated/s");
  assert.equal(getBenchmarkMetadata("first_sync_27518_rocksdb")?.work.count, 27518);
  assert.equal(getBenchmarkMetadata("first_sync_local_relay_27518_rocksdb")?.work.count, 27518);
  assert.match(
    getBenchmarkMetadata("first_sync_27518_rocksdb")!.description,
    /Member, not anonymous/,
  );
  assert.equal(getBenchmarkMetadata("stage_plan_open_task_detail")?.work.count, 1);
  assert.equal(getBenchmarkMetadata("band_chat_new_message_rooms_open[100]")?.work.count, 1);
  assert.equal(getBenchmarkMetadata("wequencer_open_pattern_views[100]")?.work.count, 100);
  assert.equal(getBenchmarkMetadata("big_label_releases_live_view_100k")?.work.count, 1);
  assert.equal(getBenchmarkMetadata("ingest_walltime_100k")?.work.count, 100000);
});

test("unknown names and unsupported parameters never get guessed metadata", () => {
  assert.equal(getBenchmarkMetadata("mystery_1000000"), null);
  assert.equal(getBenchmarkMetadata("ingest_walltime_1M"), null);
  assert.equal(getBenchmarkMetadata("stage_plan_crew_dashboard[(3, 12)]"), null);
});

test("throughput uses work per second and rejects invalid timings", () => {
  const work = getBenchmarkMetadata("stage_plan_add_task_1350")!.work;
  assert.equal(throughput(6, work), 225);
  for (const seconds of [0, -1, NaN, Infinity]) assert.equal(throughput(seconds, work), null);
  assert.equal(throughput(1, { ...work, count: 0 }), null);
});

test("former names are retired and point at catalogued successors", async () => {
  const { formerBenchmarkNames, getBenchmarkMetadata } = await import("./index.ts");
  for (const [former, successor] of formerBenchmarkNames) {
    assert.equal(getBenchmarkMetadata(former), null, `${former} is still catalogued`);
    if (successor) assert.ok(getBenchmarkMetadata(successor), `${successor} is not catalogued`);
  }
});
