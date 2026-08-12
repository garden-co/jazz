import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");
const workflow = fs.readFileSync(path.join(root, ".github/workflows/starters-e2e.yml"), "utf8");
const starters = fs.readFileSync(
  path.join(root, "packages/create-jazz-e2e/src/starters.ts"),
  "utf8",
);

function job(name, nextName) {
  const start = workflow.indexOf(`  ${name}:`);
  const end = nextName ? workflow.indexOf(`  ${nextName}:`, start + 1) : workflow.length;
  assert.notEqual(start, -1, `missing ${name} job`);
  assert.notEqual(end, -1, `missing boundary after ${name} job`);
  return workflow.slice(start, end);
}

function listedStarters(source) {
  return [...source.matchAll(/^\s+- ([-a-z]+)$/gm)].map((match) => match[1]);
}

test("release starter gate covers the canonical scaffold catalogue and no ordinary PR", () => {
  const canonical = starters.match(/export const KNOWN_STARTERS = \[([\s\S]*?)\] as const;/)?.[1];
  assert.ok(canonical, "could not find the canonical starter catalogue");
  const expected = [...canonical.matchAll(/"([^"]+)"/g)].map((match) => match[1]);
  assert.ok(expected.length > 0, "canonical starter catalogue is empty");

  assert.match(workflow, /^  workflow_dispatch:/m);
  assert.match(workflow, /^  pull_request:\n    branches: \[main\]$/m);
  assert.doesNotMatch(workflow, /^  push:/m);

  const prepare = job("prepare", "e2e");
  const e2e = job("e2e");
  const releaseOnly =
    /github\.event_name == 'workflow_dispatch' \|\| startsWith\(github\.head_ref, 'changeset-release\/'\)/;
  assert.match(prepare, releaseOnly);
  assert.match(e2e, releaseOnly);

  const matrix = e2e.match(/matrix:\n        starter:\n([\s\S]*?)\n    steps:/)?.[1];
  assert.ok(matrix, "could not find the starter E2E matrix");
  assert.deepEqual(listedStarters(matrix), expected);
});

test("release starter gate exercises packaged artifacts through create-jazz-e2e", () => {
  const prepare = job("prepare", "e2e");
  const e2e = job("e2e");
  assert.match(prepare, /pnpm run build:core/);
  assert.match(prepare, /for pkg in jazz-tools jazz-napi jazz-wasm;/);
  assert.match(prepare, /name: starters-e2e-build-state/);
  assert.match(e2e, /name: starters-e2e-build-state/);
  assert.match(e2e, /--tarball-dir "\$GITHUB_WORKSPACE\/_e2e-state\/tarballs"/);
  assert.match(e2e, /--verbose --keep/);
});
