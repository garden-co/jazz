import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  ACTIVE_SKIP_MIN_OBSERVATIONS,
  BROWSER_BENCHMARKS,
  JAZZ_SIM_BENCHMARKS,
  NATIVE_BENCHMARKS,
  readSkipSet,
  skipIds,
} from "./ci_benchmarks.mjs";
import {
  benchmarkTimeoutSeconds,
  buildJazzSimCommand,
  buildNativeCriterionCommand,
  buildNativeExampleBaseCommand,
  NATIVE_CRITERION_FEATURES_BY_ENGINE,
  NATIVE_EXAMPLE_FEATURES_BY_ENGINE,
} from "./run_ci_benchmarks.mjs";

function cargoTargetNames(manifest, kind) {
  const text = readFileSync(new URL(manifest, import.meta.url), "utf8");
  const section = new RegExp(`\\[\\[${kind}\\]\\]([\\s\\S]*?)(?=\\n\\[\\[|$)`, "g");
  return new Set(
    [...text.matchAll(section)].flatMap((match) => {
      const name = match[1].match(/^name\s*=\s*"([^"]+)"/m)?.[1];
      return name ? [name] : [];
    }),
  );
}

function withoutLineBlockComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
}

function browserHarnessScenarioIds(
  text = readFileSync(
    new URL("../../../packages/jazz-tools/tests/browser/realistic-bench.test.ts", import.meta.url),
    "utf8",
  ),
) {
  const runners = text.match(/const runners = \[([\s\S]*?)\n\s*\];/)?.[1];
  assert.ok(runners, "expected the realistic browser harness runner list");
  const executableRunners = withoutLineBlockComments(runners);
  return new Set([...executableRunners.matchAll(/id: "([A-Z][0-9]+)"/g)].map((match) => match[1]));
}

function realisticCriterionGroups(
  text = readFileSync(
    new URL("../../../crates/jazz/benches/realistic_phase1.rs", import.meta.url),
    "utf8",
  ),
) {
  const executableSource = withoutLineBlockComments(text);
  return new Set(
    [...executableSource.matchAll(/benchmark_group\("([^"]+)"\)/g)].map((match) => match[1]),
  );
}

function nativeExampleScenario(path) {
  return JSON.parse(readFileSync(new URL(`../../../${path}`, import.meta.url), "utf8"));
}

function nativeExampleDispatchModes(
  text = readFileSync(
    new URL("../../../crates/jazz/examples/realistic_bench.rs", import.meta.url),
    "utf8",
  ),
) {
  const executableSource = withoutLineBlockComments(text);
  return new Set(
    [...executableSource.matchAll(/ScenarioMode::(\w+)\s*=>/g)].map((match) => match[1]),
  );
}

function assertScheduledNativeCriterionGroups(benchmarks, groups) {
  for (const benchmark of benchmarks.filter((entry) => entry.kind === "criterion")) {
    assert.ok(
      groups.has(benchmark.criterion_filter),
      `missing realistic Criterion group for ${benchmark.id}: ${benchmark.criterion_filter}`,
    );
  }
}

function assertScheduledNativeExampleInputs(benchmarks, dispatchModes) {
  for (const benchmark of benchmarks.filter((entry) => entry.kind === "native-example")) {
    const scenario = nativeExampleScenario(benchmark.scenario_path);
    const profile = nativeExampleScenario(benchmark.profile_path);
    assert.ok(
      dispatchModes.has(
        scenario.mode
          .split("_")
          .map((part) => part[0].toUpperCase() + part.slice(1))
          .join(""),
      ),
      `native example does not dispatch ${scenario.id} (${scenario.mode})`,
    );
    assert.equal(typeof scenario.id, "string", `missing scenario id for ${benchmark.id}`);
    assert.equal(typeof profile.id, "string", `missing profile id for ${benchmark.id}`);
  }
}

function assertScheduledBrowserHarnessScenarios(benchmarks, scenarios) {
  for (const benchmark of benchmarks) {
    assert.ok(
      scenarios.has(benchmark.scenario_id),
      `missing browser harness scenario for ${benchmark.id}: ${benchmark.scenario_id}`,
    );
  }
}

test("scheduled benchmark manifest targets exist in Cargo and the browser harness", () => {
  const jazzBenches = cargoTargetNames("../../../crates/jazz/Cargo.toml", "bench");
  const jazzExamples = cargoTargetNames("../../../crates/jazz/Cargo.toml", "example");
  const jazzSimBenches = cargoTargetNames("../../../crates/jazz-sim/Cargo.toml", "bench");
  const browserScenarios = browserHarnessScenarioIds();

  assert.ok(jazzBenches.has("realistic_phase1"));
  assert.ok(jazzExamples.has("realistic_bench"));
  for (const benchmark of NATIVE_BENCHMARKS) {
    assert.ok(
      benchmark.kind === "criterion"
        ? jazzBenches.has("realistic_phase1")
        : jazzExamples.has("realistic_bench"),
      `native manifest target is missing for ${benchmark.id}`,
    );
  }
  for (const benchmark of JAZZ_SIM_BENCHMARKS) {
    assert.ok(jazzSimBenches.has(benchmark.bench), `missing jazz-sim bench for ${benchmark.id}`);
  }
  assertScheduledBrowserHarnessScenarios(BROWSER_BENCHMARKS, browserScenarios);
});

test("scheduled native filters and example inputs resolve to maintained source", () => {
  assertScheduledNativeCriterionGroups(NATIVE_BENCHMARKS, realisticCriterionGroups());
  assertScheduledNativeExampleInputs(NATIVE_BENCHMARKS, nativeExampleDispatchModes());

  const scheduledScenarioIds = NATIVE_BENCHMARKS.filter((entry) => entry.kind === "native-example")
    .map((entry) => nativeExampleScenario(entry.scenario_path).id)
    .sort();
  assert.deepEqual(scheduledScenarioIds, ["W1", "W1", "W4", "W4"]);

  const nativeScenarioIds = [
    nativeExampleScenario("dev/benchmarks/realistic/scenarios/w1_interactive.json"),
    nativeExampleScenario("dev/benchmarks/realistic/scenarios/w3_offline_reconnect.json"),
    nativeExampleScenario("dev/benchmarks/realistic/scenarios/w4_cold_start.json"),
  ]
    .map((scenario) => scenario.id)
    .sort();
  assert.deepEqual(nativeScenarioIds, ["W1", "W3", "W4"]);
  assert.ok(nativeExampleDispatchModes().has("OfflineReconnect"));
});

test("native Criterion filter contract rejects a nonexistent group", () => {
  assert.throws(
    () =>
      assertScheduledNativeCriterionGroups(
        [
          {
            id: "native-criterion:rocksdb:planted",
            kind: "criterion",
            criterion_filter: "realistic_phase1/nonexistent_group",
          },
        ],
        realisticCriterionGroups(),
      ),
    /missing realistic Criterion group.*nonexistent_group/,
  );
});

test("Rust Criterion extractor ignores commented-out groups", () => {
  const commentedCriterion = `
    // c.benchmark_group("realistic_phase1/comment_only_group");
    /* c.benchmark_group("realistic_phase1/block_comment_group"); */`;
  assert.deepEqual([...realisticCriterionGroups(commentedCriterion)], []);
  assert.throws(
    () =>
      assertScheduledNativeCriterionGroups(
        [
          {
            id: "native-criterion:rocksdb:comment-only",
            kind: "criterion",
            criterion_filter: "realistic_phase1/comment_only_group",
          },
        ],
        realisticCriterionGroups(commentedCriterion),
      ),
    /missing realistic Criterion group.*comment_only_group/,
  );
});

test("Rust native example extractor ignores commented-out dispatch modes", () => {
  const commentedMode = `
    // ScenarioMode::Unsupported => unreachable!(),
    /* ScenarioMode::BlockCommentOnly => unreachable!(), */`;
  assert.deepEqual([...nativeExampleDispatchModes(commentedMode)], []);
  assert.ok(!nativeExampleDispatchModes(commentedMode).has("Unsupported"));
});

test("browser harness contract ignores retired scenario ids in comments", () => {
  const plantedHarness = `
    const runners = [
      { id: "B1", run: async () => {} },
      // retired { id: "B8", run: async () => {} }
    ];`;
  const executableIds = browserHarnessScenarioIds(plantedHarness);
  assert.deepEqual([...executableIds], ["B1"]);
  assert.throws(
    () =>
      assertScheduledBrowserHarnessScenarios(
        [{ id: "browser:b8", scenario_id: "B8" }],
        executableIds,
      ),
    /missing browser harness scenario.*B8/,
  );
});

test("every configured benchmark skip still names a scheduled benchmark", () => {
  const scheduledIds = new Set(
    [...NATIVE_BENCHMARKS, ...BROWSER_BENCHMARKS, ...JAZZ_SIM_BENCHMARKS].map(
      (benchmark) => benchmark.id,
    ),
  );
  const skipSet = readSkipSet(new URL("./ci_skip_set.json", import.meta.url));

  for (const entry of skipSet.entries) {
    assert.ok(scheduledIds.has(entry.id), `stale benchmark skip ${entry.id}`);
  }
});

test("native benchmark catalog defines RocksDB and SQLite variants for each native scenario", () => {
  const ids = new Set(NATIVE_BENCHMARKS.map((entry) => entry.id));

  assert.ok(ids.has("native:rocksdb:w1_interactive"));
  assert.ok(ids.has("native:sqlite:w1_interactive"));
  assert.ok(ids.has("native:rocksdb:w4_cold_start"));
  assert.ok(ids.has("native:sqlite:w4_cold_start"));

  assert.ok(ids.has("native-criterion:rocksdb:r1_crud"));
  assert.ok(ids.has("native-criterion:sqlite:r1_crud"));
  assert.ok(ids.has("native-criterion:rocksdb:r2_reads"));
  assert.ok(ids.has("native-criterion:sqlite:r2_reads"));
  assert.ok(ids.has("native-criterion:rocksdb:r3_rocksdb_cold_load"));
  assert.ok(!ids.has("native-criterion:sqlite:r3_rocksdb_cold_load"));
  assert.ok(ids.has("native-criterion:rocksdb:r4_hot_task_history"));
  assert.ok(ids.has("native-criterion:sqlite:r4_hot_task_history"));
  assert.ok(ids.has("native-criterion:rocksdb:r9_subscribed_write"));
  assert.ok(ids.has("native-criterion:sqlite:r9_subscribed_write"));
  assert.ok(ids.has("native-criterion:rocksdb:r10_sync_fanout"));
  assert.ok(ids.has("native-criterion:sqlite:r10_sync_fanout"));
  assert.ok(ids.has("native-criterion:rocksdb:r11_byte_wire_resume"));
  assert.ok(ids.has("native-criterion:sqlite:r11_byte_wire_resume"));
  assert.ok(ids.has("native-criterion:rocksdb:r12_recursive_permissions"));
  assert.ok(ids.has("native-criterion:sqlite:r12_recursive_permissions"));
  assert.ok(ids.has("native-criterion:rocksdb:r13_permission_filtered_resume"));
  assert.ok(ids.has("native-criterion:sqlite:r13_permission_filtered_resume"));
});

test("native benchmark catalog targets storage-backed engine-specific Criterion groups", () => {
  const rocksdbCrud = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:rocksdb:r1_crud",
  );
  const sqliteCrud = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:sqlite:r1_crud",
  );
  const rocksdbColdLoad = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:rocksdb:r3_rocksdb_cold_load",
  );

  assert.ok(rocksdbCrud, "expected RocksDB CRUD benchmark entry");
  assert.equal(rocksdbCrud.storage_engine, "rocksdb");
  assert.equal(rocksdbCrud.criterion_filter, "realistic_phase1/r1_crud");

  assert.ok(sqliteCrud, "expected SQLite CRUD benchmark entry");
  assert.equal(sqliteCrud.storage_engine, "sqlite");
  assert.equal(sqliteCrud.criterion_filter, "realistic_phase1/r1_crud");

  assert.ok(rocksdbColdLoad, "expected RocksDB cold-load benchmark entry");
  assert.equal(rocksdbColdLoad.criterion_filter, "realistic_phase1/r3_rocksdb_cold_load");
});

test("jazz-sim catalog defines fast scenarios and encoded wire canaries", () => {
  const ids = new Set(JAZZ_SIM_BENCHMARKS.map((entry) => entry.id));

  assert.ok(ids.has("jazz-sim:s1_saas"));
  assert.ok(ids.has("jazz-sim:s2_canvas"));
  assert.ok(ids.has("jazz-sim:s3_permissions"));
  assert.ok(ids.has("jazz-sim:s4_order_processing"));
  assert.ok(ids.has("jazz-sim:s5_durable_stream"));
  assert.ok(ids.has("jazz-sim:s7_migrations"));
  assert.ok(ids.has("jazz-sim:s9_durable_execution"));
  assert.ok(ids.has("jazz-sim:s2_canvas:wire_frames"));
  assert.ok(ids.has("jazz-sim:s1_saas:wire_frames"));
});

test("jazz-sim command runs the requested benchmark quietly", () => {
  const benchmark = JAZZ_SIM_BENCHMARKS.find((entry) => entry.id === "jazz-sim:s2_canvas");
  assert.ok(benchmark, "expected the S2 jazz-sim benchmark");

  assert.deepEqual(buildJazzSimCommand(benchmark), [
    "cargo",
    "bench",
    "--manifest-path",
    "Cargo.toml",
    "-p",
    "jazz-sim",
    "--bench",
    "s2_canvas",
    "--quiet",
  ]);
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
    "jazz",
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
    "jazz",
    "--features",
    "client,sqlite",
    "--example",
  ]);
});

test("native Criterion command opts into the RocksDB storage backend", () => {
  const benchmark = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:rocksdb:r3_rocksdb_cold_load",
  );
  assert.ok(benchmark, "expected the RocksDB R3 native Criterion benchmark");

  const command = buildNativeCriterionCommand(benchmark);
  assert.equal(NATIVE_CRITERION_FEATURES_BY_ENGINE.rocksdb, "rocksdb");
  assert.deepEqual(command, [
    "cargo",
    "bench",
    "-p",
    "jazz",
    "--features",
    "rocksdb",
    "--bench",
    "realistic_phase1",
    "--",
    "realistic_phase1/r3_rocksdb_cold_load",
  ]);
});

test("native Criterion command opts into the SQLite storage backend", () => {
  const benchmark = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:sqlite:r2_reads",
  );
  assert.ok(benchmark, "expected a SQLite native Criterion benchmark");
  assert.equal(benchmarkTimeoutSeconds(benchmark, 60), 60);

  const command = buildNativeCriterionCommand(benchmark);
  assert.equal(NATIVE_CRITERION_FEATURES_BY_ENGINE.sqlite, "sqlite");
  assert.deepEqual(command, [
    "cargo",
    "bench",
    "-p",
    "jazz",
    "--features",
    "sqlite",
    "--bench",
    "realistic_phase1",
    "--",
    "realistic_phase1/r2_reads",
  ]);
});

test("native Criterion command runs the R13 assertion-bearing benchmark", () => {
  const benchmark = NATIVE_BENCHMARKS.find(
    (entry) => entry.id === "native-criterion:sqlite:r13_permission_filtered_resume",
  );
  assert.ok(benchmark, "expected the SQLite R13 native Criterion benchmark");
  assert.equal(benchmark.timeout_seconds, 90);
  assert.equal(benchmarkTimeoutSeconds(benchmark, 60), 90);

  assert.deepEqual(buildNativeCriterionCommand(benchmark), [
    "cargo",
    "bench",
    "-p",
    "jazz",
    "--features",
    "sqlite",
    "--bench",
    "realistic_phase1",
    "--",
    "realistic_phase1/r13_permission_filtered_resume",
  ]);
});

test("benchmark workflow prebuilds the RocksDB-backed and SQLite-backed native binaries", () => {
  const workflow = readFileSync(
    new URL("../../../.github/workflows/benchmarks.yml", import.meta.url),
    "utf8",
  );

  assert.match(
    workflow,
    /cargo build --release -p jazz --features client,rocksdb,transport-compression-zstd --example realistic_bench/,
  );
  assert.match(
    workflow,
    /cargo build --release -p jazz --features client,sqlite,transport-compression-zstd --example realistic_bench/,
  );
  assert.match(
    workflow,
    /cargo bench -p jazz --features rocksdb,transport-compression-zstd --bench realistic_phase1 --no-run/,
  );
  assert.match(
    workflow,
    /cargo bench -p jazz --features sqlite,transport-compression-zstd --bench realistic_phase1 --no-run/,
  );
  assert.doesNotMatch(workflow, /-p jazz-tools\b/);
});

test("benchmark workflow runs the jazz-sim benchmark suite", () => {
  const workflow = readFileSync(
    new URL("../../../.github/workflows/benchmarks.yml", import.meta.url),
    "utf8",
  );

  assert.match(workflow, /cargo bench -p jazz-sim --bench s2_canvas --no-run/);
  assert.match(workflow, /--suite jazz-sim/);
  assert.match(workflow, /bench-out\/native\/jazz-sim\/metadata\.json/);
  assert.match(workflow, /kind: "realistic-bench-jazz-sim"/);
  assert.match(workflow, /JAZZ_SIM_BENCHMARKS\.flatMap/);
  assert.match(workflow, /path\.join\(dir, "manifest\.json"\)/);
});

test("benchmark workflow builds jazz-napi before browser benchmarks", () => {
  const workflow = readFileSync(
    new URL("../../../.github/workflows/benchmarks.yml", import.meta.url),
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
    new URL("../../../packages/jazz-tools/tests/browser/realistic-bench.test.ts", import.meta.url),
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
