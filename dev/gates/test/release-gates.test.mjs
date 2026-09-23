import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
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

const expectedReleaseCondition =
  "github.event_name == 'workflow_dispatch' || github.head_ref == 'changeset-release/release'";
const unmarkedConventionalProvenance =
  /^[ \t]*(createdAt|createdBy|updatedAt|updatedBy):(?![ \t]*s\.allowExternalProvenanceName\()/m;

function releaseCondition(jobSource) {
  return jobSource.match(/^    if: \$\{\{ (.*) \}\}$/m)?.[1];
}

test("release starter gate covers the canonical scaffold catalogue and no ordinary PR", () => {
  const canonical = starters.match(/export const KNOWN_STARTERS = \[([\s\S]*?)\] as const;/)?.[1];
  assert.ok(canonical, "could not find the canonical starter catalogue");
  const expected = [...canonical.matchAll(/"([^"]+)"/g)].map((match) => match[1]);
  assert.ok(expected.length > 0, "canonical starter catalogue is empty");

  assert.match(workflow, /^  workflow_dispatch:/m);
  assert.match(workflow, /^  pull_request:\n    branches: \[release\]$/m);
  assert.doesNotMatch(workflow, /^  push:/m);

  const prepare = job("prepare", "e2e");
  const e2e = job("e2e");
  assert.equal(releaseCondition(prepare), expectedReleaseCondition);
  assert.equal(releaseCondition(e2e), expectedReleaseCondition);

  const matrix = e2e.match(/matrix:\n        starter:\n([\s\S]*?)\n    steps:/)?.[1];
  assert.ok(matrix, "could not find the starter E2E matrix");
  assert.deepEqual(listedStarters(matrix), expected);
});
test("manual starter filters reject unknown dispatch values before preparation", () => {
  const prepare = job("prepare", "e2e");
  const validation = prepare.match(
    /- name: Validate workflow_dispatch starter[\s\S]*?(?=\n      - name:|\n  e2e:)/,
  )?.[0];
  assert.ok(validation, "missing manual starter validation step");
  assert.match(validation, /if: \$\{\{ github\.event_name == 'workflow_dispatch' \}\}/);
  assert.match(validation, /STARTER_FILTER: \$\{\{ github\.event\.inputs\.starter \}\}/);
  assert.match(validation, /\*\)[\s\S]*?exit 1/);

  const shell = validation.match(/^\s{8}run: \|\n(?<body>(?: {10}.*\n?)+)/m)?.groups?.body;
  assert.ok(shell, "missing manual starter validation shell body");
  const shellBody = shell.replace(/^ {10}/gm, "");
  const runValidation = (filter) =>
    spawnSync("bash", ["-c", shellBody], {
      cwd: root,
      encoding: "utf8",
      env: { ...process.env, STARTER_FILTER: filter },
    });

  const unknown = runValidation("not-a-canonical-starter");
  assert.notEqual(unknown.status, 0, unknown.stderr);
  assert.equal(runValidation("").status, 0);

  const accepted = validation.match(/^\s+""\|([^)\n]+)\)$/m)?.[1]?.split("|");
  const canonical = starters.match(/export const KNOWN_STARTERS = \[([\s\S]*?)\] as const;/)?.[1];
  assert.ok(canonical, "could not find the canonical starter catalogue");
  const expected = [...canonical.matchAll(/"([^"]+)"/g)].map((match) => match[1]);
  assert.ok(expected.length > 0, "canonical starter catalogue is empty");
  assert.deepEqual(accepted, expected);
  assert.equal(runValidation(expected[0]).status, 0);
  assert.ok(
    prepare.indexOf("- name: Validate workflow_dispatch starter") <
      prepare.indexOf("pnpm install --frozen-lockfile"),
    "validate the dispatch filter before installing dependencies",
  );
  assert.ok(
    prepare.indexOf("- name: Validate workflow_dispatch starter") <
      prepare.indexOf("pnpm run build:core"),
    "validate the dispatch filter before building the workspace",
  );
});

test("release starter gate rejects prefix and unconditional trigger broadening", () => {
  for (const broadened of [
    "github.event_name == 'workflow_dispatch' || startsWith(github.head_ref, 'changeset-release/')",
    "github.event_name == 'workflow_dispatch' || true",
  ]) {
    assert.notEqual(broadened, expectedReleaseCondition);
  }
  assert.doesNotMatch(workflow, /startsWith\(github\.head_ref, 'changeset-release\/'/);
  assert.doesNotMatch(workflow, /github\.event_name == 'workflow_dispatch' \|\| true/);
});

test("release starter gate exercises packaged artifacts through create-jazz-e2e", () => {
  const prepare = job("prepare", "e2e");
  const e2e = job("e2e");
  assert.match(prepare, /pnpm run build:core/);
  assert.match(prepare, /node dev\/artifacts\/verify-starter-e2e-artifacts\.mjs/);
  assert.match(prepare, /for pkg in jazz-tools jazz-napi jazz-wasm;/);
  assert.match(prepare, /name: starters-e2e-build-state/);
  // The clean matrix checkout keeps tracked bootstrap files, but the NAPI
  // loader, its fingerprint receipt, native binary, and manifest are build
  // outputs restored from the prepare artifact.
  for (const runtimeArtifact of [
    "crates/jazz-napi/*.node",
    "crates/jazz-napi/*.manifest.json",
    "crates/jazz-napi/native-loader.cjs",
    "crates/jazz-napi/native-artifact-fingerprint.cjs",
  ]) {
    assert.ok(
      prepare.includes(runtimeArtifact),
      `missing NAPI runtime artifact ${runtimeArtifact}`,
    );
  }
  assert.match(e2e, /name: starters-e2e-build-state/);
  assert.match(
    e2e,
    /name: Restore prebuilt artifacts[\s\S]*?name: Verify restored NAPI harness runtime[\s\S]*?require\("\.\/crates\/jazz-napi"\)/,
  );
  assert.match(e2e, /--tarball-dir "\$GITHUB_WORKSPACE\/_e2e-state\/tarballs"/);
  assert.match(e2e, /--verbose --keep/);
});

test("release starter gate reuses its pnpm store across the prepare and matrix jobs", () => {
  const expectedCache =
    /name: Cache pnpm store[\s\S]*path: \$\{\{ steps\.pnpm-store\.outputs\.path \}\}[\s\S]*key: starters-e2e-pnpm-\$\{\{ runner\.os \}\}-\$\{\{ hashFiles\('pnpm-lock\.yaml'\) \}\}/;
  for (const source of [job("prepare", "e2e"), job("e2e")]) {
    assert.match(source, /name: Get pnpm store directory/);
    assert.match(source, expectedCache);
    assert.ok(
      source.indexOf("name: Cache pnpm store") < source.indexOf("pnpm install --frozen-lockfile"),
      "restore the pnpm store before installing dependencies",
    );
  }
});

test("provenance gate targets only unmarked Jazz provenance aliases", () => {
  assert.equal(unmarkedConventionalProvenance.test("createdAt: s.timestamp(),"), true);
  assert.equal(
    unmarkedConventionalProvenance.test("createdAt: s.allowExternalProvenanceName(s.timestamp()),"),
    false,
  );
  for (const domainEventTime of ["createdOn", "creationTime", "occurredAt"]) {
    assert.equal(unmarkedConventionalProvenance.test(`${domainEventTime}: s.timestamp(),`), false);
  }
});

test("official examples do not duplicate Jazz provenance without an explicit external marker", () => {
  const offenders = [];

  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(file);
      } else if (
        entry.isFile() &&
        entry.name === "schema.ts" &&
        unmarkedConventionalProvenance.test(fs.readFileSync(file, "utf8"))
      ) {
        offenders.push(path.relative(root, file));
      }
    }
  }

  visit(path.join(root, "examples"));
  visit(path.join(root, "starters"));
  assert.deepEqual(
    offenders,
    [],
    "Examples must use Jazz $ provenance or s.allowExternalProvenanceName(...)",
  );
});
