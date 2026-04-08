import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { NATIVE_BENCHMARKS } from "./ci_benchmarks.mjs";
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
  const benchmark = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native:rocksdb:w1_interactive",
  );
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
  const benchmark = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native:sqlite:w1_interactive",
  );
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
