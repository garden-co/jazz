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
  // A disk an untrusted ref commits is one main later mounts: the commit, not
  // the mount, is what has to be gated.
  "useblacksmith/stickydisk": { guard: "commit" },
};

// Checked to restore no cache. actions/cache/save only writes, into the
// running ref's own scope, which a trusted ref never reads;
// configure-aws-credentials: the S3 tier's role.
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

// GitHub expressions, just enough to read guards structurally: split at
// depth 0 (outside parentheses and quotes).
const unwrap = (expr) => {
  let e = String(expr ?? "").trim();
  if (e.startsWith("${{") && e.endsWith("}}")) e = e.slice(3, -2).trim();
  for (;;) {
    if (!(e.startsWith("(") && e.endsWith(")"))) return e;
    let depth = 0;
    let wraps = true;
    for (let i = 0; i < e.length; i++) {
      if (e[i] === "'") i = e.indexOf("'", i + 1);
      else if (e[i] === "(") depth++;
      else if (e[i] === ")" && --depth === 0 && i < e.length - 1) wraps = false;
    }
    if (!wraps) return e;
    e = e.slice(1, -1).trim();
  }
};
export function splitTop(expr, op) {
  const e = unwrap(expr);
  const parts = [];
  let depth = 0;
  let last = 0;
  for (let i = 0; i < e.length; i++) {
    if (e[i] === "'") i = e.indexOf("'", i + 1);
    else if (e[i] === "(") depth++;
    else if (e[i] === ")") depth--;
    else if (depth === 0 && e.startsWith(op, i)) {
      parts.push(unwrap(e.slice(last, i)));
      last = i + op.length;
      i += op.length - 1;
    }
  }
  parts.push(unwrap(e.slice(last)));
  return parts;
}
/** `cond`, or a conjunction with `cond` as a top-level conjunct (no top-level ||). */
export function requiresAll(expr, conds) {
  if (splitTop(expr, "||").length !== 1) return false;
  return conjunctsRequire(splitTop(expr, "&&"), conds);
}
const conjunctsRequire = (conjuncts, conds) =>
  conds.every((cond) =>
    typeof cond === "function" ? conjuncts.some(cond) : conjuncts.includes(cond),
  );
/** `guard && value || falsy`, where guard requires every cond; or just `guard`. */
export function guardedValue(expr, conds) {
  const ors = splitTop(expr, "||");
  if (ors.length === 1) return requiresAll(expr, conds);
  if (ors.length !== 2 || !["''", "'false'", "false"].includes(ors[1])) return false;
  const ands = splitTop(ors[0], "&&");
  if (ands.length < 2) return false;
  return conjunctsRequire(ands.slice(0, -1), conds);
}
const TRUSTED_WRITE_REFS = [
  "github.ref == 'refs/heads/main'",
  "github.ref == 'refs/heads/release'",
];
const trustedWriteRef = (conjunct) =>
  splitTop(conjunct, "||").every((alternative) => TRUSTED_WRITE_REFS.includes(alternative));
/** A sticky disk commits only on a push to main or release. */
export const commitGated = (value) =>
  unwrap(value) === "'false'" ||
  String(value) === "false" ||
  guardedValue(value, ["github.event_name == 'push'", trustedWriteRef]);

function guardProblem(step, { flag, cond, commitInput }) {
  const n = name(step.uses);
  const rule = CACHE_CAPABLE[n];
  if (!rule) return undefined;
  const w = step.with ?? {};
  switch (rule.guard) {
    case "if":
      return requiresAll(step.if, [cond])
        ? undefined
        : `${n}: if: ${step.if} doesn't require ${cond}`;
    case "cache-input":
      if ("cache" in w && !guardedValue(w.cache, [cond]))
        return `${n}: cache: ${w.cache} not behind ${cond}`;
      return guardedValue(w["package-manager-cache"], [cond])
        ? undefined
        : `${n}: package-manager-cache: ${w["package-manager-cache"]} not behind ${cond}`;
    case "use-cache":
      return guardedValue(w["use-cache"], [cond])
        ? undefined
        : `${n}: use-cache: ${w["use-cache"]} not behind ${cond}`;
    case "input":
      return w[rule.input] === flag
        ? undefined
        : `${n}: ${rule.input}: ${w[rule.input]} is not ${flag}`;
    case "commit":
      if (commitInput)
        return w.commit === `\${{ inputs.${commitInput} }}`
          ? undefined
          : `${n}: commit: ${w.commit} is not passed through from inputs.${commitInput}`;
      return commitGated(w.commit)
        ? undefined
        : `${n}: commit: ${w.commit} commits outside a push to main or release`;
  }
  return `${n}: unknown guard`;
}

test("the guard reader rejects planted bypasses", () => {
  const cond = COND;
  for (const ok of [
    cond,
    `${cond} && (startsWith(matrix.platform, 'linux-'))`,
    `(x == 'y') && ${cond}`,
  ])
    assert.ok(requiresAll(ok, [cond]), ok);
  for (const bad of [
    `${cond} || always()`,
    `always() || ${cond}`,
    `(${cond} || x)`,
    `x`,
    `!${cond}`,
  ])
    assert.ok(!requiresAll(bad, [cond]), bad);
  assert.ok(guardedValue(`\${{ ${cond} && 'pnpm' || '' }}`, [cond]));
  assert.ok(!guardedValue(`\${{ ${cond} && 'pnpm' || 'pnpm' }}`, [cond]));
  assert.ok(!guardedValue(`\${{ ${cond} || always() && 'pnpm' || '' }}`, [cond]));
  assert.ok(!guardedValue("pnpm", [cond]));
  for (const ok of [
    "false",
    "${{ github.event_name == 'push' && github.ref == 'refs/heads/main' && 'true' || 'false' }}",
    "${{ github.event_name == 'push' && (github.ref == 'refs/heads/main' || github.ref == 'refs/heads/release') && 'true' || 'false' }}",
  ])
    assert.ok(commitGated(ok), ok);
  for (const bad of [
    "true",
    "${{ github.ref == 'refs/heads/main' && 'true' || 'false' }}",
    "${{ github.event_name == 'push' && (github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/heads/changeset-release/')) && 'true' || 'false' }}",
    "${{ github.event_name == 'push' || github.ref == 'refs/heads/main' && 'true' || 'false' }}",
  ])
    assert.ok(!commitGated(bad), bad);
});

test("every cache-capable step in the tree's workflows sits behind the trusted-ref flag", () => {
  for (const { where, step } of workflowSteps) {
    if (!step.uses) continue;
    const problem = guardProblem(step, { flag: FLAG, cond: COND });
    assert.equal(problem, undefined, `${where}: ${problem}`);
  }
});

// Which input of a composite feeds its sticky disk's commit, and so must be
// commit-gated at every caller in the tree.
const commitInputs = { ".github/actions/setup-build": "sccache-commit" };

test("every caller that mounts setup-build's sccache disk passes a commit-gated sccache-commit", () => {
  for (const { where, step } of workflowSteps) {
    if (!step.uses || name(step.uses) !== "./.github/actions/setup-build") continue;
    const w = step.with ?? {};
    if (!("sccache" in w) || String(w.sccache) === "false") continue;
    assert.ok(commitGated(w["sccache-commit"]), `${where}: sccache-commit: ${w["sccache-commit"]}`);
  }
});

test("inside the composites, every restore sits behind their restore input", () => {
  for (const { where, step, inputs } of compositeSteps) {
    if (!step.uses || !(name(step.uses) in CACHE_CAPABLE)) continue;
    const input = inputs.find((i) => i === "restore-caches" || i === "restore-cache");
    assert.ok(input, `${where}: has a cache-capable step but no restore input`);
    const commitInput =
      name(step.uses) === "useblacksmith/stickydisk" ? commitInputs[where] : undefined;
    const problem = guardProblem(step, {
      flag: `\${{ inputs.${input} }}`,
      cond: `inputs.${input} == 'true'`,
      commitInput,
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
