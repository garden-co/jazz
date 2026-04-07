import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { NATIVE_BENCHMARKS } from "./ci_benchmarks.mjs";
import {
  buildNativeCriterionCommand,
  buildNativeExampleBaseCommand,
  NATIVE_CRITERION_FEATURES,
  NATIVE_EXAMPLE_FEATURES,
} from "./run_ci_benchmarks.mjs";

test("native benchmark catalog targets RocksDB for the cold-load Criterion run", () => {
  const benchmark = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:r3_cold_load_rocksdb",
  );

  assert.ok(benchmark, "expected a RocksDB cold-load benchmark entry");
  assert.equal(benchmark.label, "Criterion R3 cold-load RocksDB");
  assert.equal(benchmark.log_path, "logs/criterion_r3_cold_load_rocksdb.log");
  assert.equal(benchmark.criterion_filter, "realistic_phase1/cold_load_rocksdb");
});

test("native example command opts into the RocksDB storage backend", () => {
  const benchmark = NATIVE_BENCHMARKS.find((entry) => entry.id === "native:w1_interactive");
  assert.ok(benchmark, "expected the W1 native example benchmark");

  const command = buildNativeExampleBaseCommand(benchmark, { profile: "s" });
  assert.equal(NATIVE_EXAMPLE_FEATURES, "client,rocksdb");
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

test("native Criterion command opts into the RocksDB storage backend", () => {
  const benchmark = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:r3_cold_load_rocksdb",
  );
  assert.ok(benchmark, "expected the R3 native Criterion benchmark");

  const command = buildNativeCriterionCommand(benchmark);
  assert.equal(NATIVE_CRITERION_FEATURES, "rocksdb");
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

test("benchmark workflow prebuilds the RocksDB-backed native binaries", () => {
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
    /cargo bench -p jazz-tools --features rocksdb --bench realistic_phase1 --no-run/,
  );
});
