import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { benchmarkMetadata, getBenchmarkMetadata, throughput } from "./index.ts";

const root = new URL("../../../", import.meta.url);
test("catalogue documents all 71 known current and retired wallclock cases", () => {
  assert.equal(benchmarkMetadata.length, 71);
  assert.equal(new Set(benchmarkMetadata.map((m) => m.name)).size, 71);
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
  assert.equal(getBenchmarkMetadata("sequential_insert_1350_rocksdb")?.work.count, 1350);
  assert.equal(getBenchmarkMetadata("batch_update_1350_rocksdb")?.work.unit, "rows updated/s");
  assert.equal(getBenchmarkMetadata("first_sync_27518_rocksdb")?.work.count, 27518);
  assert.match(
    getBenchmarkMetadata("first_sync_27518_rocksdb")!.description,
    /Member, not anonymous/,
  );
  assert.equal(getBenchmarkMetadata("query_task_detail_profile_s_memory")?.work.count, 1);
  assert.equal(getBenchmarkMetadata("matching_write_fanout[100]")?.work.count, 1);
  assert.equal(getBenchmarkMetadata("attach_route_bindings[100]")?.work.count, 100);
  assert.equal(getBenchmarkMetadata("maintained_subscription_hydration_100k")?.work.count, 1);
  assert.equal(getBenchmarkMetadata("ingest_walltime_100k")?.work.count, 100000);
});

test("unknown names and unsupported parameters never get guessed metadata", () => {
  assert.equal(getBenchmarkMetadata("mystery_1000000"), null);
  assert.equal(getBenchmarkMetadata("ingest_walltime_1M"), null);
  assert.equal(getBenchmarkMetadata("query_comments_scaling_memory[(3, 12, 9)]"), null);
});

test("throughput uses work per second and rejects invalid timings", () => {
  const work = getBenchmarkMetadata("sequential_insert_1350_rocksdb")!.work;
  assert.equal(throughput(6, work), 225);
  for (const seconds of [0, -1, NaN, Infinity]) assert.equal(throughput(seconds, work), null);
  assert.equal(throughput(1, { ...work, count: 0 }), null);
});
