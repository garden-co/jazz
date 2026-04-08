import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

test("update_history ingests engine-specific native and browser manifests from artifact roots", () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-bench-history-"));
  const historyPath = path.join(tempRoot, "history.json");
  const nativeRoot = path.join(tempRoot, "native");
  const browserRoot = path.join(tempRoot, "browser");

  const nativeRocksdbDir = path.join(nativeRoot, "rocksdb");
  writeJson(path.join(nativeRocksdbDir, "metadata.json"), {
    repository: "garden-co/jazz2",
    run_id: "100",
    run_attempt: "1",
    sha: "abc123",
    ref: "refs/heads/main",
    branch: "main",
    profile: "s",
    storage_engine: "rocksdb",
  });
  writeJson(path.join(nativeRocksdbDir, "manifest.json"), {
    kind: "realistic-bench-native",
    generated_at: "2026-04-08T10:00:00Z",
    storage_engine: "rocksdb",
  });
  writeJson(path.join(nativeRocksdbDir, "suite_status.json"), {
    benchmarks: [
      { id: "native:rocksdb:w4_cold_start", status: "passed" },
      { id: "native-criterion:rocksdb:r1_crud_sustained", status: "passed" },
    ],
  });
  writeJson(path.join(nativeRocksdbDir, "w4_cold_start.json"), {
    scenario_id: "W4",
    scenario_name: "cold_start_reopen",
    topology: "local_only",
    total_operations: 150,
    wall_time_ms: 1000,
    throughput_ops_per_sec: 150,
    operation_summaries: {},
    extra: {},
  });
  writeJson(path.join(nativeRocksdbDir, "criterion_realistic_phase1.json"), {
    generated_at: "2026-04-08T10:00:00Z",
    benchmarks: [
      {
        full_id: "realistic_phase1/crud_sustained_rocksdb/r1_s_rocksdb",
        group_id: "realistic_phase1/crud_sustained_rocksdb",
        benchmark_id: "r1_s_rocksdb",
        throughput_elements: 100,
        scenario_id: "R1",
        scenario_name: "crud",
        metrics: {
          mean_ms: 12,
          mean_ci_low_ms: 11,
          mean_ci_high_ms: 13,
          elems_per_sec: 1000,
          elems_per_sec_ci_low: 900,
          elems_per_sec_ci_high: 1100,
        },
      },
    ],
  });

  const nativeSqliteDir = path.join(nativeRoot, "sqlite");
  writeJson(path.join(nativeSqliteDir, "metadata.json"), {
    repository: "garden-co/jazz2",
    run_id: "100",
    run_attempt: "1",
    sha: "abc123",
    ref: "refs/heads/main",
    branch: "main",
    profile: "s",
    storage_engine: "sqlite",
  });
  writeJson(path.join(nativeSqliteDir, "manifest.json"), {
    kind: "realistic-bench-native",
    generated_at: "2026-04-08T10:00:00Z",
    storage_engine: "sqlite",
  });
  writeJson(path.join(nativeSqliteDir, "suite_status.json"), {
    benchmarks: [
      { id: "native:sqlite:w4_cold_start", status: "passed" },
      { id: "native-criterion:sqlite:r1_crud_sustained", status: "passed" },
    ],
  });
  writeJson(path.join(nativeSqliteDir, "w4_cold_start.json"), {
    scenario_id: "W4",
    scenario_name: "cold_start_reopen",
    topology: "local_only",
    total_operations: 150,
    wall_time_ms: 1100,
    throughput_ops_per_sec: 136,
    operation_summaries: {},
    extra: {},
  });
  writeJson(path.join(nativeSqliteDir, "criterion_realistic_phase1.json"), {
    generated_at: "2026-04-08T10:00:00Z",
    benchmarks: [
      {
        full_id: "realistic_phase1/crud_sustained_sqlite/r1_s_sqlite",
        group_id: "realistic_phase1/crud_sustained_sqlite",
        benchmark_id: "r1_s_sqlite",
        throughput_elements: 100,
        scenario_id: "R1",
        scenario_name: "crud",
        metrics: {
          mean_ms: 15,
          mean_ci_low_ms: 14,
          mean_ci_high_ms: 16,
          elems_per_sec: 800,
          elems_per_sec_ci_low: 760,
          elems_per_sec_ci_high: 840,
        },
      },
    ],
  });

  writeJson(path.join(browserRoot, "metadata.json"), {
    repository: "garden-co/jazz2",
    run_id: "100",
    run_attempt: "1",
    sha: "abc123",
    ref: "refs/heads/main",
    branch: "main",
    profile: "s",
    storage_engine: "opfs-btree",
  });
  writeJson(path.join(browserRoot, "manifest.json"), {
    kind: "realistic-bench-browser",
    generated_at: "2026-04-08T10:00:00Z",
    storage_engine: "opfs-btree",
  });
  writeJson(path.join(browserRoot, "realistic.json"), {
    generated_at: "2026-04-08T10:00:00Z",
    profile: "s",
    storage_engine: "opfs-btree",
    scenarios: [
      {
        scenario_id: "W4",
        scenario_name: "cold_start_reopen",
        topology: "local_only",
        total_operations: 25,
        wall_time_ms: 2000,
        throughput_ops_per_sec: 12,
        operation_summaries: {},
        extra: {},
      },
    ],
  });

  execFileSync(
    "node",
    [
      "benchmarks/realistic/update_history.mjs",
      "--history",
      historyPath,
      "--native",
      nativeRoot,
      "--browser",
      browserRoot,
    ],
    {
      cwd: "/Users/anselm/.codex/worktrees/25dc/jazz2",
      stdio: "pipe",
    },
  );

  const history = readJson(historyPath);
  const runKeys = history.runs.map((run) => [run.suite, run.storage_engine]);

  assert.deepEqual(runKeys, [
    ["native", "rocksdb"],
    ["native-criterion", "rocksdb"],
    ["native", "sqlite"],
    ["native-criterion", "sqlite"],
    ["browser", "opfs-btree"],
  ]);

  const nativeRocksdbRun = history.runs.find(
    (run) => run.suite === "native" && run.storage_engine === "rocksdb",
  );
  const nativeSqliteCriterionRun = history.runs.find(
    (run) => run.suite === "native-criterion" && run.storage_engine === "sqlite",
  );
  const browserRun = history.runs.find(
    (run) => run.suite === "browser" && run.storage_engine === "opfs-btree",
  );

  assert.ok(nativeRocksdbRun, "expected native RocksDB run");
  assert.equal(nativeRocksdbRun.id, "native:rocksdb:100:1:abc123:s");

  assert.ok(nativeSqliteCriterionRun, "expected native criterion SQLite run");
  assert.equal(nativeSqliteCriterionRun.id, "native-criterion:sqlite:100:1:abc123:s");
  assert.equal(
    nativeSqliteCriterionRun.scenarios[0].topology,
    "realistic_phase1/crud_sustained_sqlite",
  );

  assert.ok(browserRun, "expected browser OPFS-btree run");
  assert.equal(browserRun.id, "browser:opfs-btree:100:1:abc123:s");
});
