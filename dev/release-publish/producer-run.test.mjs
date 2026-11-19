// Contract: release-publish.yml imports artifacts only from a successful
// release-artifacts.yml run of this repository that built exactly the commit
// being published, with a manifest naming that commit, tree and version.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { expectedProblems, manifestProblems, producerRunProblems } from "./producer-run.mjs";

const repo = "garden-co/jazz";
const sha = "a".repeat(40);
const tree = "f".repeat(40);
const version = "2.0.0-alpha.69";
const run = (overrides = {}) => ({
  id: 1,
  path: ".github/workflows/release-artifacts.yml",
  event: "push",
  status: "completed",
  conclusion: "success",
  head_sha: sha,
  head_repository: { full_name: repo },
  repository: { full_name: repo },
  ...overrides,
});
const manifest = (overrides = {}) => ({ sha, tree, version, ...overrides });

test("the run that built the published commit is accepted", () => {
  assert.deepEqual(producerRunProblems(run(), { sha, repo }), []);
  assert.deepEqual(producerRunProblems(run({ event: "workflow_dispatch" }), { sha, repo }), []);
  assert.deepEqual(manifestProblems(manifest(), { sha, tree, version }), []);
  assert.deepEqual(manifestProblems(manifest({ source_sha: sha }), { sha, tree, version }), []);
});

for (const [name, changes] of [
  ["a run that built another commit (even with the same tree)", { head_sha: "b".repeat(40) }],
  ["another workflow", { path: ".github/workflows/preview-build.yml" }],
  ["a fork's run", { head_repository: { full_name: "someone/jazz" } }],
  ["a pull_request run", { event: "pull_request" }],
  ["a failed run", { conclusion: "failure" }],
  ["an unfinished run", { status: "in_progress", conclusion: null }],
]) {
  test(`refuses ${name}`, () => {
    assert.notDeepEqual(producerRunProblems(run(changes), { sha, repo }), []);
  });
}

for (const [name, changes] of [
  ["another commit", { sha: "b".repeat(40) }],
  ["another source commit", { source_sha: "b".repeat(40) }],
  ["another tree", { tree: "e".repeat(40) }],
  ["another version", { version: "2.0.0-alpha.68" }],
  ["no sha", { sha: undefined }],
]) {
  test(`refuses a manifest naming ${name}`, () => {
    assert.notDeepEqual(manifestProblems(manifest(changes), { sha, tree, version }), []);
  });
}

test("the CLI resolve-artifacts.sh runs exits 1 with the reason", () => {
  const dir = mkdtempSync(join(tmpdir(), "producer-"));
  writeFileSync(join(dir, "run.json"), JSON.stringify(run({ head_sha: "c".repeat(40) })));
  writeFileSync(join(dir, "manifest.json"), JSON.stringify(manifest()));
  const script = fileURLToPath(new URL("./producer-run.mjs", import.meta.url));
  const env = { ...process.env, SHA: sha, TREE: tree, VERSION: version, GH_REPO: repo };
  assert.throws(
    () =>
      execFileSync("node", [script, join(dir, "run.json"), join(dir, "manifest.json")], {
        env,
        encoding: "utf8",
      }),
    (error) => error.status === 1 && /built c{40}/.test(error.stdout),
  );
  writeFileSync(join(dir, "run.json"), JSON.stringify(run()));
  execFileSync("node", [script, join(dir, "run.json"), join(dir, "manifest.json")], { env });
});

test("resolve-artifacts.sh holds every input to the rule", () => {
  const script = readFileSync(
    fileURLToPath(new URL("./resolve-artifacts.sh", import.meta.url)),
    "utf8",
  );
  // Every release-artifacts candidate (searched or named) goes through producer_ok.
  assert.match(
    script,
    /for run in "\$\{candidates\[@\]\}"; do\n  if producer_ok "\$\{run\}"; then/,
  );
  assert.match(script, /candidates=\("\$\{ARTIFACTS_RUN_ID\}"\)/);
  assert.match(script, /runs\?head_sha=\$\{SHA\}/);
  // The inspector comes from that same run.
  assert.match(script, /echo "inspector_run_id=\$\{run\}"/);
  // Legacy runs can't meet it: refused when publishing.
  assert.match(
    script,
    /if \[ -n "\$\{LEGACY_RUN_IDS:-\}" \] && \[ "\$\{MODE\}" != dry-run \]; then[^\n]*\n[^\n]*\n  exit 1/,
  );
  // MODE is required: no mode, no default.
  assert.match(script, /: "\$\{SHA:\?\}" "\$\{VERSION:\?\}" "\$\{GH_REPO:\?\}" "\$\{MODE:\?\}"/);
});

test("unusable expected values fail closed", () => {
  assert.deepEqual(expectedProblems({ sha, tree, version, repo }), []);
  for (const bad of [
    { sha: "" },
    { sha: "abc" },
    { tree: undefined },
    { version: "" },
    { repo: "" },
    { repo: "jazz" },
  ])
    assert.notDeepEqual(
      expectedProblems({ sha, tree, version, repo, ...bad }),
      [],
      JSON.stringify(bad),
    );
  const dir = mkdtempSync(join(tmpdir(), "producer-"));
  writeFileSync(join(dir, "run.json"), JSON.stringify(run()));
  writeFileSync(join(dir, "manifest.json"), JSON.stringify(manifest()));
  const script = fileURLToPath(new URL("./producer-run.mjs", import.meta.url));
  assert.throws(
    () =>
      execFileSync("node", [script, join(dir, "run.json"), join(dir, "manifest.json")], {
        env: { ...process.env, SHA: "", TREE: tree, VERSION: version, GH_REPO: repo },
        encoding: "utf8",
      }),
    (error) => error.status === 1 && /refusing/.test(error.stdout),
  );
});
