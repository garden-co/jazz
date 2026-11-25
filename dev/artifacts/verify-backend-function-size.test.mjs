import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { checkNoBundledBinaries } from "../release-publish/npm-publish.mjs";
import { MAX_TRACED_BYTES, judgeTrace } from "./verify-backend-function-size.mjs";
import { verifyPackedNapi } from "./verify-packed-napi.mjs";

const linux = "node_modules/@garden-co/jazz-napi-linux-x64-gnu/jazz-napi.linux-x64-gnu.node";

test("a trace with only the linux-x64 binary, under budget, passes", () => {
  const receipt = judgeTrace([
    [linux, 63_000_000],
    ["node_modules/jazz-tools/dist/backend/index.js", 44_000_000],
  ]);
  assert.deepEqual(receipt.problems, []);
  assert.equal(receipt.totalBytes, 107_000_000);
});

test("beta.3's shape fails: over budget, and every platform's binary bundled", () => {
  const receipt = judgeTrace([
    [linux, 63_000_000],
    ["node_modules/jazz-napi/jazz-napi.darwin-arm64.node", 51_000_000],
    ["node_modules/jazz-napi/jazz-napi.linux-x64-gnu.node", 63_000_000],
  ]);
  assert.equal(receipt.problems.length, 2);
  assert.match(receipt.problems[0], /over the 150 MB budget/);
  assert.match(receipt.problems[1], /jazz-napi\/jazz-napi\.darwin-arm64\.node/);
});

test("over budget alone fails", () => {
  const receipt = judgeTrace([
    [linux, 63_000_000],
    ["node_modules/jazz-tools/dist/big.js", MAX_TRACED_BYTES],
  ]);
  assert.deepEqual(receipt.problems.length, 1);
});

test("a trace without the linux-x64 platform package fails", () => {
  const receipt = judgeTrace([["node_modules/jazz-tools/dist/backend/index.js", 1]]);
  assert.match(receipt.problems[0], /lacks @garden-co\/jazz-napi-linux-x64-gnu/);
});

test("a jazz-napi tarball with a .node file is refused by publish and the packed check", (t) => {
  const dir = mkdtempSync(join(tmpdir(), "jazz-napi-tgz-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  mkdirSync(join(dir, "package"));
  writeFileSync(join(dir, "package", "package.json"), '{"name":"jazz-napi"}');
  writeFileSync(join(dir, "package", "index.cjs"), "");
  execFileSync("tar", ["-czf", join(dir, "clean.tgz"), "-C", dir, "package"]);
  checkNoBundledBinaries(join(dir, "clean.tgz"));
  writeFileSync(join(dir, "package", "jazz-napi.linux-x64-gnu.node"), "bytes");
  execFileSync("tar", ["-czf", join(dir, "bundled.tgz"), "-C", dir, "package"]);
  assert.throws(
    () => checkNoBundledBinaries(join(dir, "bundled.tgz")),
    /bundles native binaries \(package\/jazz-napi\.linux-x64-gnu\.node\)/,
  );
  assert.throws(
    () => verifyPackedNapi(join(dir, "bundled.tgz")),
    /must not contain native binaries/,
  );
});
