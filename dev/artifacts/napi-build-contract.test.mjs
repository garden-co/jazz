import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";

const build = readFileSync(new URL("./build.mjs", import.meta.url), "utf8");
const wrapper = readFileSync(
  new URL("../../crates/jazz-napi/scripts/build.js", import.meta.url),
  "utf8",
);

test("all NAPI entrypoints use one target-aware fail-closed build path", () => {
  assert.match(wrapper, /dev\/artifacts\/build\.mjs/);
  assert.match(build, /const expectedNapiBinding/);
  assert.match(build, /NAPI build produced no \$\{expectedNapiBinding\}/);
  assert.match(build, /NAPI build produced an unloadable host binding/);
  assert.match(build, /writeManifest\(root, kind, profile, target\)/);
});

test("a direct cross-target build stages only its expected binding", () => {
  // The plant is the unrelated target name: selecting this would discard a
  // valid foreign artifact when a direct --target build succeeds.
  const unrelated = "jazz-napi.darwin-arm64.node";
  assert.equal(
    build.includes(`join(root, "crates/jazz-napi", ${JSON.stringify(unrelated)})`),
    false,
  );
  assert.match(build, /const napiPath = expectedNapiBinding/);
  assert.match(build, /if \(napiPath && existsSync\(napiPath\)\) renameSync/);
});
