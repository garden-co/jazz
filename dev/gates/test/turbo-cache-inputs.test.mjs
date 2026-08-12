#!/usr/bin/env node
/**
 * Turbo's dry graph exposes both the resolved input set and its task hash. Keep
 * this test deliberately build-free: it proves the cache key changes for a
 * shared Rust dependency, but not for a crate outside these artifact closures.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "../../..");
const tasks = ["@jazz/rust#build:crates", "jazz-wasm#build", "jazz-napi#build"];

function dryGraph() {
  const output = execFileSync(
    "pnpm",
    [
      "exec",
      "turbo",
      "run",
      "build:crates",
      "build",
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

const relevant = resolve(root, "crates/jazz/src/lib.rs");
const unrelated = resolve(root, "crates/jazz-sim/src/lib.rs");
const relevantOriginal = readFileSync(relevant, "utf8");
const unrelatedOriginal = readFileSync(unrelated, "utf8");

try {
  const baseline = dryGraph();
  assert(containsInput(baseline, "@jazz/rust#build:crates", "jazz/src/lib.rs"));
  assert(containsInput(baseline, "jazz-wasm#build", "jazz/src/lib.rs"));
  assert(containsInput(baseline, "jazz-napi#build", "jazz/src/lib.rs"));
  for (const task of tasks)
    assert(
      !containsInput(baseline, task, "jazz-sim/src/lib.rs"),
      `${task} includes unrelated jazz-sim`,
    );

  const beforeRelevant = hashes(baseline);
  writeFileSync(relevant, `${relevantOriginal}\n// turbo cache-input sensitivity probe\n`);
  const afterRelevant = hashes(dryGraph());
  for (const task of tasks)
    assert.notEqual(afterRelevant[task], beforeRelevant[task], `${task} missed a shared Rust edit`);

  writeFileSync(relevant, relevantOriginal);
  const beforeUnrelated = hashes(dryGraph());
  writeFileSync(unrelated, `${unrelatedOriginal}\n// turbo cache-input isolation probe\n`);
  const afterUnrelated = hashes(dryGraph());
  for (const task of tasks)
    assert.equal(afterUnrelated[task], beforeUnrelated[task], `${task} hashes unrelated jazz-sim`);
} finally {
  writeFileSync(relevant, relevantOriginal);
  writeFileSync(unrelated, unrelatedOriginal);
}

console.log("Turbo artifact cache inputs are sensitive to closure edits only.");
