import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const scriptPath = join(dirname(fileURLToPath(import.meta.url)), "stage-inspector-overlay.mjs");

function makeFixture(t) {
  const root = mkdtempSync(join(tmpdir(), "stage-inspector-overlay-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const jazzToolsScripts = join(root, "packages/jazz-tools/scripts");
  const source = join(root, "packages/inspector/dist-embedded");
  const destination = join(root, "packages/jazz-tools/dist/dev/inspector-overlay/embedded");
  const fixtureScript = join(jazzToolsScripts, "stage-inspector-overlay.mjs");

  mkdirSync(jazzToolsScripts, { recursive: true });
  copyFileSync(scriptPath, fixtureScript);
  mkdirSync(destination, { recursive: true });
  writeFileSync(join(destination, "embedded.html"), "old embedded content");

  return { source, destination, fixtureScript };
}

function runStage(fixtureScript) {
  return spawnSync(process.execPath, [fixtureScript], { encoding: "utf8" });
}

test("staging replaces an existing embedded overlay from a valid source", (t) => {
  const { source, destination, fixtureScript } = makeFixture(t);
  mkdirSync(source, { recursive: true });
  writeFileSync(join(source, "embedded.html"), "new embedded content");

  const result = runStage(fixtureScript);

  assert.equal(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(destination, "embedded.html"), "utf8"), "new embedded content");
});

test("a missing source fails without destroying the existing embedded overlay", (t) => {
  const { destination, fixtureScript } = makeFixture(t);

  const result = runStage(fixtureScript);

  assert.notEqual(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(destination, "embedded.html"), "utf8"), "old embedded content");
});

test("a source without embedded.html fails validation without destroying the existing overlay", (t) => {
  const { source, destination, fixtureScript } = makeFixture(t);
  mkdirSync(source, { recursive: true });
  writeFileSync(join(source, "other-asset.txt"), "not the embedded page");

  const result = runStage(fixtureScript);

  assert.notEqual(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(destination, "embedded.html"), "utf8"), "old embedded content");
});
