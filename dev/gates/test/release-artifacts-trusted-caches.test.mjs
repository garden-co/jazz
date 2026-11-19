// #supply-chain (2026-11-19): a release build on an untrusted ref restores
// nothing from a cache an untrusted ref could have written. This walks
// release-artifacts.yml, every reusable workflow it calls and every composite
// action those use, and fails on any cache-capable step that doesn't sit behind
// the trusted-ref condition, on any action it doesn't know, and on any
// cross-run download. Commit-gated sticky disks and the S3 sccache tier (read
// role on untrusted refs) are allowed.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { parse } from "yaml";

const root = path.resolve(import.meta.dirname, "../../..");
const ENTRY = ".github/workflows/release-artifacts.yml";
const TRUST =
  "${{ (github.ref == 'refs/heads/main' || github.ref == 'refs/heads/release' || startsWith(github.ref, 'refs/heads/merge-group/')) && 'true' || 'false' }}";
const FLAG = "${{ env.JAZZ_TRUSTED_CACHE }}";
const COND = "env.JAZZ_TRUSTED_CACHE == 'true'";

// Actions that can restore a cache, and how a step proves it doesn't on an
// untrusted ref. `where` is "workflow" (guarded by the workflow's flag) or
// "composite" (guarded by the composite's own input).
const CACHE_CAPABLE = {
  "actions/cache": { guard: "if" },
  "actions/cache/restore": { guard: "if" },
  "Swatinem/rust-cache": { guard: "if" },
  "actions/setup-node": { guard: "cache-input" },
  "mlugg/setup-zig": { guard: "use-cache" },
  "./.github/actions/setup-build": { guard: "input", input: "restore-caches" },
  "./.github/actions/install-rust-tool": { guard: "input", input: "restore-cache" },
  "./.github/actions/apple-sccache": { guard: "input", input: "restore-cache" },
};

// Checked to restore no cache. stickydisk: commit-gated (sccache-commit);
// actions/cache/save only writes, into the running ref's own scope, which a
// trusted ref never reads; configure-aws-credentials: the S3 tier's role.
const CACHE_FREE = new Set([
  "actions/checkout",
  "actions/upload-artifact",
  "actions/download-artifact",
  "actions/attest-build-provenance",
  "actions/create-github-app-token",
  "actions/cache/save",
  "aws-actions/configure-aws-credentials",
  "dtolnay/rust-toolchain",
  "android-actions/setup-android",
  "pnpm/action-setup",
  "useblacksmith/stickydisk",
  "./.github/actions/stop-sccache",
]);

const read = (file) => parse(fs.readFileSync(path.join(root, file), "utf8"));
const actionFile = (dir) =>
  ["action.yml", "action.yaml"]
    .map((f) => path.join(dir, f))
    .find((f) => fs.existsSync(path.join(root, f)));
const name = (uses) => uses.split("@")[0];

function walk() {
  const workflows = new Map();
  const composites = new Map();
  const visitAction = (uses) => {
    const dir = uses.replace(/^\.\//, "");
    if (composites.has(dir)) return;
    const doc = read(actionFile(dir));
    composites.set(dir, doc);
    if (doc.runs?.using === "composite")
      for (const step of doc.runs.steps ?? [])
        if (step.uses?.startsWith("./.github/actions/")) visitAction(name(step.uses));
  };
  const visitWorkflow = (file) => {
    if (workflows.has(file)) return;
    const doc = read(file);
    workflows.set(file, doc);
    for (const job of Object.values(doc.jobs)) {
      if (job.uses) {
        assert.ok(
          job.uses.startsWith("./.github/workflows/"),
          `${file}: calls a non-local workflow ${job.uses}`,
        );
        visitWorkflow(name(job.uses).replace(/^\.\//, ""));
        continue;
      }
      for (const step of job.steps ?? [])
        if (step.uses?.startsWith("./.github/actions/")) visitAction(name(step.uses));
    }
  };
  visitWorkflow(ENTRY);
  return { workflows, composites };
}

const { workflows, composites } = walk();
const workflowSteps = [...workflows].flatMap(([file, doc]) =>
  Object.entries(doc.jobs).flatMap(([job, j]) =>
    (j.steps ?? []).map((step) => ({ where: `${file}:${job}`, step })),
  ),
);
const compositeSteps = [...composites].flatMap(([dir, doc]) =>
  (doc.runs?.steps ?? []).map((step) => ({
    where: dir,
    step,
    inputs: Object.keys(doc.inputs ?? {}),
  })),
);

test("the walk reaches release-artifacts.yml's whole tree", () => {
  assert.deepEqual([...workflows.keys()].sort(), [
    ".github/workflows/attach-language-artifacts.yml",
    ".github/workflows/build-jazz-packages.yml",
    ".github/workflows/release-artifacts.yml",
    ".github/workflows/swift-mirror.yml",
  ]);
  assert.ok(composites.has(".github/actions/setup-build"));
});

test("every workflow in the tree derives the trust flag from the ref, and only from the ref", () => {
  for (const [file, doc] of workflows) assert.equal(doc.env?.JAZZ_TRUSTED_CACHE, TRUST, file);
});

test("every action the tree uses is classified", () => {
  for (const { where, step } of [...workflowSteps, ...compositeSteps]) {
    if (!step.uses) continue;
    const n = name(step.uses);
    assert.ok(n in CACHE_CAPABLE || CACHE_FREE.has(n), `${where}: unclassified action ${n}`);
  }
});

function guardProblem(step, { flag, cond }) {
  const n = name(step.uses);
  const rule = CACHE_CAPABLE[n];
  if (!rule) return undefined;
  const w = step.with ?? {};
  switch (rule.guard) {
    case "if":
      return String(step.if ?? "").includes(cond) ? undefined : `${n} not behind ${cond}`;
    case "cache-input":
      return !("cache" in w) || String(w.cache).includes(cond)
        ? undefined
        : `${n} cache: ${w.cache} not behind ${cond}`;
    case "use-cache":
      return String(w["use-cache"] ?? "").includes(cond)
        ? undefined
        : `${n} use-cache not behind ${cond}`;
    case "input":
      return w[rule.input] === flag
        ? undefined
        : `${n} ${rule.input}: ${w[rule.input]} is not ${flag}`;
  }
  return `${n}: unknown guard`;
}

test("every cache-capable step in the tree's workflows sits behind the trusted-ref flag", () => {
  for (const { where, step } of workflowSteps) {
    if (!step.uses) continue;
    const problem = guardProblem(step, { flag: FLAG, cond: COND });
    assert.equal(problem, undefined, `${where}: ${problem}`);
  }
});

test("inside the composites, every restore sits behind their restore input", () => {
  for (const { where, step, inputs } of compositeSteps) {
    if (!step.uses || !(name(step.uses) in CACHE_CAPABLE)) continue;
    const input = inputs.find((i) => i === "restore-caches" || i === "restore-cache");
    assert.ok(input, `${where}: has a cache-capable step but no restore input`);
    const problem = guardProblem(step, {
      flag: `\${{ inputs.${input} }}`,
      cond: `inputs.${input} == 'true'`,
    });
    assert.equal(problem, undefined, `${where}: ${problem}`);
  }
});

test("no cross-run artifact download, and no Turbo remote cache, anywhere in the tree", () => {
  for (const { where, step } of [...workflowSteps, ...compositeSteps]) {
    if (step.uses && name(step.uses) === "actions/download-artifact")
      assert.ok(!("run-id" in (step.with ?? {})), `${where}: download-artifact with run-id`);
    assert.doesNotMatch(
      String(step.run ?? ""),
      /gh run download|gh api [^\n]*\/artifacts/,
      `${where}: downloads another run's artifacts`,
    );
    for (const key of Object.keys(step.env ?? {}))
      assert.doesNotMatch(key, /^TURBO_(TOKEN|API|TEAM)$/, `${where}: ${key}`);
  }
  for (const [file, doc] of workflows)
    for (const [job, j] of Object.entries(doc.jobs))
      for (const key of Object.keys({ ...(doc.env ?? {}), ...(j.env ?? {}) }))
        assert.doesNotMatch(key, /^TURBO_(TOKEN|API|TEAM)$/, `${file}:${job}: ${key}`);
});
