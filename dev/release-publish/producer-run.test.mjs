// Contract: release-publish.yml imports artifacts only from a successful
// release-artifacts.yml run of this repository, on release or
// changeset-release/*, that built a commit with the published tree, with a
// manifest naming that commit, the tree and the version.
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
const vpHead = "b".repeat(40);
const tree = "f".repeat(40);
const version = "2.0.0-alpha.69";
const run = (overrides = {}) => ({
  id: 1,
  path: ".github/workflows/release-artifacts.yml",
  event: "push",
  head_branch: "release",
  status: "completed",
  conclusion: "success",
  head_sha: sha,
  head_repository: { full_name: repo },
  repository: { full_name: repo },
  ...overrides,
});
const manifest = (overrides = {}) => ({ sha, tree, version, ...overrides });
const ok = (r, headTree = tree) => producerRunProblems(r, { tree, repo, headTree });

test("the published commit's own run on release is accepted", () => {
  assert.deepEqual(ok(run()), []);
  assert.deepEqual(manifestProblems(manifest(), { headSha: sha, tree, version }), []);
  assert.deepEqual(
    manifestProblems(manifest({ source_sha: sha }), { headSha: sha, tree, version }),
    [],
  );
});

test("a tree-equal run on changeset-release is accepted", () => {
  const vp = run({
    event: "workflow_dispatch",
    head_branch: "changeset-release/release",
    head_sha: vpHead,
  });
  assert.deepEqual(ok(vp), []);
  assert.deepEqual(
    manifestProblems(manifest({ sha: vpHead }), { headSha: vpHead, tree, version }),
    [],
  );
});

for (const [name, changes, headTree] of [
  ["a tree-equal run on main", { head_branch: "main", head_sha: vpHead }],
  [
    "a tree-equal pull_request run",
    { event: "pull_request", head_branch: "changeset-release/release", head_sha: vpHead },
  ],
  [
    "a tree-equal fork run",
    {
      head_repository: { full_name: "someone/jazz" },
      head_branch: "changeset-release/release",
      head_sha: vpHead,
    },
  ],
  [
    "a run that built a different tree",
    { head_branch: "changeset-release/release", head_sha: vpHead },
    "e".repeat(40),
  ],
  [
    "a run whose commit can't be read",
    { head_branch: "changeset-release/release", head_sha: vpHead },
    null,
  ],
  ["another workflow", { path: ".github/workflows/preview-build.yml" }],
  ["a failed run", { conclusion: "failure" }],
  ["an unfinished run", { status: "in_progress", conclusion: null }],
]) {
  test(`refuses ${name}`, () => {
    assert.notDeepEqual(ok(run(changes), headTree === undefined ? tree : headTree), []);
  });
}

for (const [name, changes] of [
  ["a sha other than the run's head_sha", { sha: vpHead }],
  ["a source_sha other than the run's head_sha", { source_sha: vpHead }],
  ["another tree", { tree: "e".repeat(40) }],
  ["another version", { version: "2.0.0-alpha.68" }],
  ["no sha", { sha: undefined }],
]) {
  test(`refuses a manifest naming ${name}`, () => {
    assert.notDeepEqual(manifestProblems(manifest(changes), { headSha: sha, tree, version }), []);
  });
}

test("the CLI resolve-artifacts.sh runs exits 1 with the reason", () => {
  const dir = mkdtempSync(join(tmpdir(), "producer-"));
  const script = fileURLToPath(new URL("./producer-run.mjs", import.meta.url));
  const env = {
    ...process.env,
    SHA: sha,
    TREE: tree,
    VERSION: version,
    GH_REPO: repo,
    HEAD_TREE: tree,
  };
  const cli = (r, m, extra = {}) => {
    writeFileSync(join(dir, "run.json"), JSON.stringify(r));
    writeFileSync(join(dir, "manifest.json"), JSON.stringify(m));
    return execFileSync("node", [script, join(dir, "run.json"), join(dir, "manifest.json")], {
      env: { ...env, ...extra },
      encoding: "utf8",
    });
  };
  cli(run(), manifest());
  cli(
    run({ head_branch: "changeset-release/release", head_sha: vpHead }),
    manifest({ sha: vpHead }),
  );
  assert.throws(
    () => cli(run({ head_branch: "changeset-release/release", head_sha: vpHead }), manifest()),
    (error) => error.status === 1 && /manifest sha/.test(error.stdout),
  );
  assert.throws(
    () => cli(run({ head_sha: vpHead }), manifest({ sha: vpHead }), { HEAD_TREE: "e".repeat(40) }),
    (error) => error.status === 1 && /built tree/.test(error.stdout),
  );
});

test("resolve-artifacts.sh holds every input to the rule", () => {
  const script = readFileSync(
    fileURLToPath(new URL("./resolve-artifacts.sh", import.meta.url)),
    "utf8",
  );
  // Every release-artifacts candidate (searched or named) goes through producer_ok.
  assert.match(
    script,
    /for run in "\$\{candidates\[@\]\}"; do\n(?:  [^\n]*\n){2}  if source_sha="\$\(producer_ok "\$\{run\}"\)"; then/,
  );
  assert.match(script, /candidates=\("\$\{ARTIFACTS_RUN_ID\}"\)/);
  // The published commit's own runs are tried first.
  assert.match(script, /for query in "head_sha=\$\{SHA\}&" "branch=release&"/);
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
