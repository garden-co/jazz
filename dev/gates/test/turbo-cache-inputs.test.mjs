#!/usr/bin/env node
/**
 * Turbo's dry graph exposes both the resolved input set and its task hash. Keep
 * this test deliberately build-free: it proves the cache key changes for direct
 * and transitive Rust dependencies without invalidating unrelated artifacts.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { workspaceDependencyInputs } from "../../artifacts/provenance.mjs";

const root = resolve(import.meta.dirname, "../../..");
const taskDefinitions = [
  {
    task: "@jazz/rust#build:crates",
    config: "build:crates",
    rootManifest: "crates/jazz-cli/Cargo.toml",
    localRoot: false,
  },
  {
    task: "jazz-wasm#build",
    config: "jazz-wasm#build",
    rootManifest: "crates/jazz-wasm/Cargo.toml",
    localRoot: true,
  },
  {
    task: "jazz-wasm#build:fast",
    config: "jazz-wasm#build:fast",
    rootManifest: "crates/jazz-wasm/Cargo.toml",
    localRoot: true,
  },
  {
    task: "jazz-napi#build",
    config: "jazz-napi#build",
    rootManifest: "crates/jazz-napi/Cargo.toml",
    localRoot: true,
  },
];
const tasks = taskDefinitions.map(({ task }) => task);
const uncachedCorrectnessArtifactTasks = tasks.filter((task) => task !== "@jazz/rust#build:crates");
const turbo = JSON.parse(readFileSync(resolve(root, "turbo.json"), "utf8"));
const cargoInputs = new Map();

function jazzToolsDryGraph() {
  const output = execFileSync(
    "pnpm",
    ["exec", "turbo", "run", "build", "--filter=jazz-tools", "--dry=json"],
    { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] },
  );
  const task = JSON.parse(output).tasks.find(
    (candidate) => candidate.taskId === "jazz-tools#build",
  );
  assert.ok(task, "dry graph omitted jazz-tools#build");
  return task;
}

function assertUncachedCorrectnessArtifactTask(task) {
  // A native generation contains its own Cargo target and can be tens of
  // gigabytes. It is deliberately published through the content-addressed
  // correctness-artifact store, not Turbo's tarball cache. Turbo's outer
  // `cache.local`/`cache.remote` reports this invocation's cache eligibility,
  // which CI/environment flags can disable even if the task itself says
  // `cache: true`. Check Turbo's resolved task definition instead.
  assert.equal(
    task.resolvedTaskDefinition.cache,
    false,
    `${task.taskId} could archive native correctness output through Turbo`,
  );
}

function dryGraph() {
  const output = execFileSync(
    "pnpm",
    [
      "exec",
      "turbo",
      "run",
      "build:crates",
      "build",
      "build:fast",
      "--filter=@jazz/rust",
      "--filter=jazz-wasm",
      "--filter=jazz-napi",
      "--dry=json",
    ],
    { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] },
  );
  const graph = JSON.parse(output);
  const selected = new Map(
    graph.tasks.filter((task) => tasks.includes(task.taskId)).map((task) => [task.taskId, task]),
  );
  assert.equal(selected.size, tasks.length, "dry graph omitted an artifact task");
  return selected;
}

function hashes(graph) {
  return Object.fromEntries(tasks.map((task) => [task, graph.get(task).hash]));
}

function containsInput(graph, task, suffix) {
  return Object.keys(graph.get(task).inputs).some((path) => path.endsWith(suffix));
}

function dependencyInputs(definition) {
  const { rootManifest } = definition;
  if (!cargoInputs.has(rootManifest)) {
    cargoInputs.set(rootManifest, workspaceDependencyInputs(root, rootManifest));
  }
  return cargoInputs.get(rootManifest);
}

function configuredCargoInputs(definition) {
  const { config, rootManifest, localRoot } = definition;
  const rootCrate = rootManifest.slice(0, -"/Cargo.toml".length);
  const expected = dependencyInputs(definition)
    .filter((path) => !localRoot || path !== rootCrate)
    .map((path) => `$TURBO_ROOT$/${path}/**`)
    .sort();
  const actual = turbo.tasks[config].inputs
    .filter((input) => /^\$TURBO_ROOT\$\/crates\/[^/]+\/\*\*$/.test(input))
    .sort();
  assert.deepEqual(actual, expected, `${config} diverges from its Cargo dependency closure`);
}

function affectedTasks(crate) {
  const dependency = `crates/${crate}`;
  return new Set(
    taskDefinitions
      .filter((definition) => dependencyInputs(definition).includes(dependency))
      .map(({ task }) => task),
  );
}

const closures = ["jazz", "groove", "jazz-native-transport", "benchmark-guard"].map((name) => ({
  name,
  file: resolve(root, `crates/${name}/src/lib.rs`),
  affected: affectedTasks(name),
}));
assert.deepEqual(
  [...affectedTasks("jazz-native-transport")].sort(),
  ["@jazz/rust#build:crates", "jazz-napi#build"],
  "native transport must affect CLI and NAPI but not WASM",
);
const unrelated = resolve(root, "crates/jazz-sim/src/lib.rs");
const originals = new Map(closures.map(({ file }) => [file, readFileSync(file, "utf8")]));
const unrelatedOriginal = readFileSync(unrelated, "utf8");

try {
  // JAZZ_CORRECTNESS_WASM_PACKAGE deliberately remains pass-through: it is a
  // sealed path, not a stable Turbo hash input. The bundle that copies it must
  // remain explicitly uncached even if the outer invocation has caching off.
  const jazzTools = jazzToolsDryGraph();
  assert.equal(
    jazzTools.resolvedTaskDefinition.cache,
    false,
    "jazz-tools could cache a stale sealed WASM bundle",
  );

  const baseline = dryGraph();
  for (const task of uncachedCorrectnessArtifactTasks)
    assertUncachedCorrectnessArtifactTask(baseline.get(task));
  for (const definition of taskDefinitions) configuredCargoInputs(definition);
  for (const { name, affected } of closures)
    for (const task of tasks)
      assert.equal(
        containsInput(baseline, task, `${name}/src/lib.rs`),
        affected.has(task),
        `${task} has the wrong ${name} dependency coverage`,
      );
  for (const task of tasks)
    assert(
      !containsInput(baseline, task, "jazz-sim/src/lib.rs"),
      `${task} includes unrelated jazz-sim`,
    );

  const baselineHashes = hashes(baseline);
  for (const { name, file, affected } of closures) {
    const original = originals.get(file);
    writeFileSync(file, `${original}\n// turbo cache-input ${name} edit probe\n`);
    const afterEdit = hashes(dryGraph());
    for (const task of tasks)
      assert[affected.has(task) ? "notEqual" : "equal"](
        afterEdit[task],
        baselineHashes[task],
        `${task} has the wrong response to a ${name} edit`,
      );

    writeFileSync(file, original);
    rmSync(file);
    const afterRemoval = hashes(dryGraph());
    for (const task of tasks)
      assert[affected.has(task) ? "notEqual" : "equal"](
        afterRemoval[task],
        baselineHashes[task],
        `${task} has the wrong response to a ${name} removal`,
      );
    writeFileSync(file, original);
  }

  writeFileSync(unrelated, `${unrelatedOriginal}\n// turbo cache-input isolation probe\n`);
  const afterUnrelated = hashes(dryGraph());
  for (const task of tasks)
    assert.equal(afterUnrelated[task], baselineHashes[task], `${task} hashes unrelated jazz-sim`);
} finally {
  for (const { file } of closures) writeFileSync(file, originals.get(file));
  writeFileSync(unrelated, unrelatedOriginal);
}

console.log(
  "Turbo artifact cache inputs are sensitive to closure edits only; sealed Jazz Tools bundles cannot restore.",
);
