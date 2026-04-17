import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { ACTIVE_SKIP_MIN_OBSERVATIONS, NATIVE_BENCHMARKS, skipIds } from "./ci_benchmarks.mjs";
import {
  buildNativeCriterionCommand,
  buildNativeExampleBaseCommand,
  NATIVE_CRITERION_FEATURES_BY_ENGINE,
  NATIVE_EXAMPLE_FEATURES_BY_ENGINE,
} from "./run_ci_benchmarks.mjs";

test("native benchmark catalog defines RocksDB and SQLite variants for each native scenario", () => {
  const ids = new Set(NATIVE_BENCHMARKS.map((entry) => entry.id));

  assert.ok(ids.has("native:rocksdb:w1_interactive"));
  assert.ok(ids.has("native:sqlite:w1_interactive"));
  assert.ok(ids.has("native:rocksdb:w4_cold_start"));
  assert.ok(ids.has("native:sqlite:w4_cold_start"));

  assert.ok(ids.has("native-criterion:rocksdb:r1_crud_sustained"));
  assert.ok(ids.has("native-criterion:sqlite:r1_crud_sustained"));
  assert.ok(ids.has("native-criterion:rocksdb:r2_reads_sustained"));
  assert.ok(ids.has("native-criterion:sqlite:r2_reads_sustained"));
  assert.ok(ids.has("native-criterion:rocksdb:r3_cold_load"));
  assert.ok(ids.has("native-criterion:sqlite:r3_cold_load"));
  assert.ok(ids.has("native-criterion:rocksdb:r4_fanout_updates"));
  assert.ok(ids.has("native-criterion:sqlite:r4_fanout_updates"));
  assert.ok(ids.has("native-criterion:rocksdb:r5_permission_recursive"));
  assert.ok(ids.has("native-criterion:sqlite:r5_permission_recursive"));
  assert.ok(ids.has("native-criterion:rocksdb:r6_permission_write_heavy"));
  assert.ok(ids.has("native-criterion:sqlite:r6_permission_write_heavy"));
  assert.ok(ids.has("native-criterion:rocksdb:r7_hotspot_history"));
  assert.ok(ids.has("native-criterion:sqlite:r7_hotspot_history"));
  assert.ok(ids.has("native-criterion:rocksdb:r8_many_branches"));
  assert.ok(ids.has("native-criterion:sqlite:r8_many_branches"));
  assert.ok(ids.has("native-criterion:rocksdb:r9_subscribed_write_path"));
  assert.ok(ids.has("native-criterion:sqlite:r9_subscribed_write_path"));
});

test("native benchmark catalog targets storage-backed engine-specific Criterion groups", () => {
  const rocksdbCrud = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:rocksdb:r1_crud_sustained",
  );
  const sqliteCrud = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:sqlite:r1_crud_sustained",
  );
  const rocksdbColdLoad = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:rocksdb:r3_cold_load",
  );
  const sqliteColdLoad = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:sqlite:r3_cold_load",
  );

  assert.ok(rocksdbCrud, "expected RocksDB CRUD benchmark entry");
  assert.equal(rocksdbCrud.storage_engine, "rocksdb");
  assert.equal(rocksdbCrud.criterion_filter, "realistic_phase1/crud_sustained_rocksdb");

  assert.ok(sqliteCrud, "expected SQLite CRUD benchmark entry");
  assert.equal(sqliteCrud.storage_engine, "sqlite");
  assert.equal(sqliteCrud.criterion_filter, "realistic_phase1/crud_sustained_sqlite");

  assert.ok(rocksdbColdLoad, "expected RocksDB cold-load benchmark entry");
  assert.equal(rocksdbColdLoad.criterion_filter, "realistic_phase1/cold_load_rocksdb");

  assert.ok(sqliteColdLoad, "expected SQLite cold-load benchmark entry");
  assert.equal(sqliteColdLoad.criterion_filter, "realistic_phase1/cold_load_sqlite");
});

test("native example command opts into the RocksDB storage backend", () => {
  const benchmark = NATIVE_BENCHMARKS.find((entry) => entry.id === "native:rocksdb:w1_interactive");
  assert.ok(benchmark, "expected the RocksDB W1 native example benchmark");

  const command = buildNativeExampleBaseCommand(benchmark, { profile: "s" });
  assert.equal(NATIVE_EXAMPLE_FEATURES_BY_ENGINE.rocksdb, "client,rocksdb");
  assert.deepEqual(command.slice(0, 8), [
    "cargo",
    "run",
    "--release",
    "-p",
    "jazz-tools",
    "--features",
    "client,rocksdb",
    "--example",
  ]);
});

test("native example command opts into the SQLite storage backend", () => {
  const benchmark = NATIVE_BENCHMARKS.find((entry) => entry.id === "native:sqlite:w1_interactive");
  assert.ok(benchmark, "expected the SQLite W1 native example benchmark");

  const command = buildNativeExampleBaseCommand(benchmark, { profile: "s" });
  assert.equal(NATIVE_EXAMPLE_FEATURES_BY_ENGINE.sqlite, "client,sqlite");
  assert.deepEqual(command.slice(0, 8), [
    "cargo",
    "run",
    "--release",
    "-p",
    "jazz-tools",
    "--features",
    "client,sqlite",
    "--example",
  ]);
});

test("native Criterion command opts into the RocksDB storage backend", () => {
  const benchmark = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:rocksdb:r3_cold_load",
  );
  assert.ok(benchmark, "expected the RocksDB R3 native Criterion benchmark");

  const command = buildNativeCriterionCommand(benchmark);
  assert.equal(NATIVE_CRITERION_FEATURES_BY_ENGINE.rocksdb, "rocksdb");
  assert.deepEqual(command, [
    "cargo",
    "bench",
    "-p",
    "jazz-tools",
    "--features",
    "rocksdb",
    "--bench",
    "realistic_phase1",
    "--",
    "realistic_phase1/cold_load_rocksdb",
  ]);
});

test("native Criterion command opts into the SQLite storage backend", () => {
  const benchmark = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:sqlite:r3_cold_load",
  );
  assert.ok(benchmark, "expected the SQLite R3 native Criterion benchmark");

  const command = buildNativeCriterionCommand(benchmark);
  assert.equal(NATIVE_CRITERION_FEATURES_BY_ENGINE.sqlite, "sqlite");
  assert.deepEqual(command, [
    "cargo",
    "bench",
    "-p",
    "jazz-tools",
    "--features",
    "sqlite",
    "--bench",
    "realistic_phase1",
    "--",
    "realistic_phase1/cold_load_sqlite",
  ]);
});

test("benchmark workflow prebuilds the RocksDB-backed and SQLite-backed native binaries", () => {
  const workflow = readFileSync(
    new URL("../../.github/workflows/benchmarks.yml", import.meta.url),
    "utf8",
  );

  assert.match(
    workflow,
    /cargo build --release -p jazz-tools --features client,rocksdb --example realistic_bench/,
  );
  assert.match(
    workflow,
    /cargo build --release -p jazz-tools --features client,sqlite --example realistic_bench/,
  );
  assert.match(
    workflow,
    /cargo bench -p jazz-tools --features rocksdb --bench realistic_phase1 --no-run/,
  );
  assert.match(
    workflow,
    /cargo bench -p jazz-tools --features sqlite --bench realistic_phase1 --no-run/,
  );
});

test("benchmark workflow builds jazz-napi before browser benchmarks", () => {
  const workflow = readFileSync(
    new URL("../../.github/workflows/benchmarks.yml", import.meta.url),
    "utf8",
  );

  assert.match(workflow, /pnpm --filter jazz-napi build/);
});

test("configured skips only activate after repeated timeout observations", () => {
  const skipSet = {
    entries: [
      { id: "browser:b6", observations: ACTIVE_SKIP_MIN_OBSERVATIONS - 1 },
      { id: "native:rocksdb:w1_interactive", observations: ACTIVE_SKIP_MIN_OBSERVATIONS },
      { id: "native:sqlite:w1_interactive" },
    ],
  };

  assert.deepEqual([...skipIds(skipSet)].sort(), ["native:rocksdb:w1_interactive"]);
});

test("trimmed CI scenarios keep their non-trivial topology", () => {
  const w1Ci = JSON.parse(
    readFileSync(new URL("./ci/scenarios/w1_interactive.json", import.meta.url), "utf8"),
  );
  const w4Ci = JSON.parse(
    readFileSync(new URL("./ci/scenarios/w4_cold_start.json", import.meta.url), "utf8"),
  );
  const r4Ci = JSON.parse(
    readFileSync(new URL("./ci/scenarios/r4_fanout_updates.json", import.meta.url), "utf8"),
  );
  const r5Ci = JSON.parse(
    readFileSync(new URL("./ci/scenarios/r5_permission_recursive.json", import.meta.url), "utf8"),
  );
  const r6Ci = JSON.parse(
    readFileSync(new URL("./ci/scenarios/r6_permission_write_heavy.json", import.meta.url), "utf8"),
  );
  const r7Ci = JSON.parse(
    readFileSync(new URL("./ci/scenarios/r7_hotspot_history.json", import.meta.url), "utf8"),
  );
  const r8Ci = JSON.parse(
    readFileSync(new URL("./ci/scenarios/r8_many_branches.json", import.meta.url), "utf8"),
  );
  const r9Ci = JSON.parse(
    readFileSync(new URL("./ci/scenarios/r9_subscribed_write_path.json", import.meta.url), "utf8"),
  );
  const browserHarness = readFileSync(
    new URL("../../packages/jazz-tools/tests/browser/realistic-bench.test.ts", import.meta.url),
    "utf8",
  );

  assert.equal(w1Ci.operation_count, 10000);
  assert.equal(w4Ci.reopen_cycles, 50);
  assert.equal(r4Ci.operation_count, 4);
  assert.deepEqual(r4Ci.fanout_clients, [10, 20]);
  assert.equal(r5Ci.docs_per_folder, 16);
  assert.equal(r5Ci.denied_docs, 64);
  assert.equal(r5Ci.shared_chain_depth, 4);
  assert.deepEqual(r5Ci.recursive_depths, [1, 3, 6]);
  assert.equal(r6Ci.docs_per_folder, 16);
  assert.equal(r6Ci.denied_docs, 64);
  assert.equal(r6Ci.shared_chain_depth, 4);
  assert.deepEqual(r6Ci.recursive_depths, [1, 3, 6]);
  assert.equal(r7Ci.operation_count, 512);
  assert.equal(r7Ci.hot_task_count, 10);
  assert.equal(r8Ci.branch_count, 1000);
  assert.equal(r8Ci.commits_per_branch, 4);
  assert.equal(r8Ci.merge_fanin, 8);
  assert.equal(r9Ci.scale, 128);
  assert.match(browserHarness, /b6UpdateCount:\s*6000\b/);
});
