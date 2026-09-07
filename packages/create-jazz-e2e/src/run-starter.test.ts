import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import test from "node:test";

import {
  assertNapiBindingMatchesHarness,
  assertInstalledJazzNapiMatchesHarness,
  loadedHarnessNapiFingerprint,
  patchInstalledJazzNapi,
  runStarter,
  loadStarterSchema,
} from "./run-starter.js";

import { getStarterConfig } from "./starters.js";

function missingTarballDir(): string {
  return path.join(os.tmpdir(), `create-jazz-e2e-missing-${randomUUID()}`);
}

function writeNapiFingerprint(packageDir: string, fingerprint: string): void {
  fs.mkdirSync(packageDir, { recursive: true });
  fs.writeFileSync(
    path.join(packageDir, "native-artifact-fingerprint.cjs"),
    `module.exports = { expectedNativeArtifactFingerprint: "${fingerprint}" };\n`,
  );
}

function linkPnpmJazzNapi(root: string, packageDir: string): void {
  const link = path.join(root, "app", "node_modules", "jazz-napi");
  fs.mkdirSync(path.dirname(link), { recursive: true });
  fs.symlinkSync(packageDir, link, "dir");
}

test("rejects a candidate whose matching workspace metadata disagrees with the loaded harness", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-napi-mismatch-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  writeNapiFingerprint(path.join(root, "crates", "jazz-napi"), "a".repeat(64));
  writeNapiFingerprint(path.join(root, "app", "node_modules", "jazz-napi"), "b".repeat(64));

  assert.throws(
    () => assertInstalledJazzNapiMatchesHarness(path.join(root, "app"), "a".repeat(64)),
    {
      message:
        /Packed Jazz NAPI fingerprint b{64} does not match this harness's native binding a{64}/,
    },
  );
});

test("rejects a candidate even when stale workspace metadata matches it but the loaded binding differs", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-napi-stale-metadata-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const candidateFingerprint = "c".repeat(64);
  writeNapiFingerprint(path.join(root, "crates", "jazz-napi"), candidateFingerprint);
  writeNapiFingerprint(path.join(root, "app", "node_modules", "jazz-napi"), candidateFingerprint);

  assert.throws(
    () => assertInstalledJazzNapiMatchesHarness(path.join(root, "app"), "d".repeat(64)),
    /Packed Jazz NAPI fingerprint c{64} does not match this harness's native binding d{64}/,
  );
});

test("accepts a packed NAPI candidate with the loaded harness fingerprint", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-napi-match-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const fingerprint = loadedHarnessNapiFingerprint();
  writeNapiFingerprint(path.join(root, "crates", "jazz-napi"), fingerprint);
  writeNapiFingerprint(path.join(root, "app", "node_modules", "jazz-napi"), fingerprint);

  assert.doesNotThrow(() => assertInstalledJazzNapiMatchesHarness(path.join(root, "app")));
});

test("rejects a mismatched NAPI candidate reached through pnpm's package symlink", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-napi-symlink-mismatch-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const packageDir = path.join(root, "store", "jazz-napi");
  writeNapiFingerprint(packageDir, "b".repeat(64));
  linkPnpmJazzNapi(root, packageDir);

  assert.throws(
    () => assertInstalledJazzNapiMatchesHarness(path.join(root, "app"), "a".repeat(64)),
    /Packed Jazz NAPI fingerprint b{64} does not match this harness's native binding a{64}/,
  );
});

test("rejects a stale repair binary even when candidate metadata matches the harness", () => {
  const fingerprint = "e".repeat(64);
  assert.throws(
    () =>
      assertNapiBindingMatchesHarness(
        "f".repeat(64),
        fingerprint,
        fingerprint,
        "/workspace/crates/jazz-napi/jazz-napi.linux-x64-gnu.node",
      ),
    /binding .* fingerprint f{64} does not match candidate e{64} and harness e{64}/,
  );
});

test("accepts a repair binary whose actual binding matches candidate and harness", () => {
  const fingerprint = "e".repeat(64);
  assert.doesNotThrow(() =>
    assertNapiBindingMatchesHarness(
      fingerprint,
      fingerprint,
      fingerprint,
      "/workspace/crates/jazz-napi/jazz-napi.linux-x64-gnu.node",
    ),
  );
});

test("refuses to copy a stale workspace NAPI binary despite matching candidate metadata", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-napi-repair-stale-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const fingerprint = "e".repeat(64);
  const staleFingerprint = "f".repeat(64);
  const source = path.join(root, "crates", "jazz-napi", "jazz-napi.fake.node");
  const candidateDir = path.join(root, "app", "node_modules", "jazz-napi");
  writeNapiFingerprint(candidateDir, fingerprint);
  fs.mkdirSync(path.dirname(source), { recursive: true });
  fs.writeFileSync(source, "fixture");

  assert.throws(
    () =>
      patchInstalledJazzNapi(path.join(root, "app"), root, fingerprint, (bindingPath) =>
        bindingPath === source ? staleFingerprint : fingerprint,
      ),
    /binding .* fingerprint f{64} does not match candidate e{64} and harness e{64}/,
  );
  assert.equal(fs.existsSync(path.join(candidateDir, path.basename(source))), false);
});

test("copies a workspace NAPI binary only after its actual binding matches", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-napi-repair-match-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const fingerprint = "e".repeat(64);
  const source = path.join(root, "crates", "jazz-napi", "jazz-napi.fake.node");
  const candidateDir = path.join(root, "app", "node_modules", "jazz-napi");
  writeNapiFingerprint(candidateDir, fingerprint);
  fs.mkdirSync(path.dirname(source), { recursive: true });
  fs.writeFileSync(source, "fixture");

  patchInstalledJazzNapi(path.join(root, "app"), root, fingerprint, () => fingerprint);
  assert.equal(fs.existsSync(path.join(candidateDir, path.basename(source))), true);
});

test("leaves a valid packed NAPI candidate alone when the workspace has no binary", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-napi-packed-only-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const fingerprint = "e".repeat(64);
  const candidateDir = path.join(root, "app", "node_modules", "jazz-napi");
  writeNapiFingerprint(candidateDir, fingerprint);
  fs.writeFileSync(path.join(candidateDir, "jazz-napi.fake.node"), "fixture");

  assert.doesNotThrow(() =>
    patchInstalledJazzNapi(path.join(root, "app"), root, fingerprint, () => fingerprint),
  );
});

test("rejects a candidate needing repair when no workspace NAPI binary exists", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-napi-no-repair-source-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const fingerprint = "e".repeat(64);
  writeNapiFingerprint(path.join(root, "app", "node_modules", "jazz-napi"), fingerprint);

  assert.throws(
    () => patchInstalledJazzNapi(path.join(root, "app"), root, fingerprint, () => fingerprint),
    /No Jazz NAPI binary is available to repair the packed starter candidate/,
  );
});

test("repairs a NAPI candidate reached through pnpm's package symlink", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-napi-symlink-repair-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const fingerprint = "e".repeat(64);
  const source = path.join(root, "crates", "jazz-napi", "jazz-napi.fake.node");
  const packageDir = path.join(root, "store", "jazz-napi");
  writeNapiFingerprint(packageDir, fingerprint);
  linkPnpmJazzNapi(root, packageDir);
  fs.mkdirSync(path.dirname(source), { recursive: true });
  fs.writeFileSync(source, "fixture");

  patchInstalledJazzNapi(path.join(root, "app"), root, fingerprint, () => fingerprint);
  assert.equal(fs.existsSync(path.join(packageDir, path.basename(source))), true);
});

test("cleanup preserves a caller-provided work directory", async (t) => {
  const testDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-caller-test-"));
  const workDir = path.join(testDir, "caller-owned");
  const sentinel = path.join(workDir, "sentinel.txt");
  fs.mkdirSync(workDir);
  fs.writeFileSync(sentinel, "caller data");
  t.after(() => fs.rmSync(testDir, { recursive: true, force: true }));

  const result = await runStarter({
    starter: "react-localfirst",
    repoRoot: process.cwd(),
    workDir,
    tarballDir: missingTarballDir(),
  });

  assert.equal(result.success, false);
  assert.match(result.errorMessage ?? "", /does not exist/);
  assert.equal(fs.existsSync(workDir), true);
  assert.equal(fs.readFileSync(sentinel, "utf8"), "caller data");
});

test("cleanup removes a harness-created temporary work directory", async (t) => {
  const result = await runStarter({
    starter: "react-localfirst",
    repoRoot: process.cwd(),
    tarballDir: missingTarballDir(),
  });
  const workDir = path.dirname(result.appDir);
  t.after(() => fs.rmSync(workDir, { recursive: true, force: true }));

  assert.equal(result.success, false);
  assert.match(result.errorMessage ?? "", /does not exist/);
  assert.equal(fs.existsSync(workDir), false);
});

test("loads separate starter permissions and retains account ownership predicates", async () => {
  const repoRoot = path.resolve(import.meta.dirname, "../../..");
  for (const starter of ["ts-hybrid", "sveltekit-hybrid"] as const) {
    const loaded = await loadStarterSchema(
      path.join(repoRoot, "starters", starter),
      getStarterConfig(starter),
    );
    assert.ok(loaded && !(loaded instanceof Uint8Array));
    const policies = loaded.todos?.policies;
    assert.ok(policies, `${starter} must publish its separate permissions module`);
    for (const operation of ["select", "update", "delete"] as const) {
      assert.match(JSON.stringify(policies[operation]), /\$createdBy.account/);
      assert.match(JSON.stringify(policies[operation]), /"user","account"/);
    }
  }
});
