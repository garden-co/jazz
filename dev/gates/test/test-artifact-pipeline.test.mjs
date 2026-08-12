import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { buildTestArtifacts } from "../build-test-artifacts.mjs";

test("test artifact pipeline overlaps independent bindings and repairs NAPI only after a failed load", async () => {
  const calls = [];
  let releaseLoadAttempts = 0;
  await buildTestArtifacts(async (command, args, label, env) => {
    calls.push({ command, args, label, env });
    if (label === "load release NAPI" && ++releaseLoadAttempts === 1)
      throw new Error("simulated corrupt binding");
  });

  const labels = calls.map((call) => call.label);
  assert.deepEqual(labels.slice(0, 3), ["CLI", "fast WASM", "release NAPI"]);
  assert.equal(labels.filter((label) => label === "release NAPI").length, 1);
  assert.equal(labels.filter((label) => label === "repair release NAPI").length, 1);
  assert.ok(labels.indexOf("jazz-tools") > labels.indexOf("fast WASM"));
  assert.ok(labels.indexOf("verify fast WASM provenance") < labels.indexOf("load release NAPI"));
  assert.ok(labels.indexOf("verify release NAPI provenance") < labels.indexOf("load release NAPI"));
  assert.ok(labels.indexOf("load repaired release NAPI") > labels.indexOf("repair release NAPI"));
  assert.equal(calls[1].env.CARGO_TARGET_DIR, "target/test-artifacts-wasm");
  assert.equal(calls[2].env.CARGO_TARGET_DIR, "target/test-artifacts-napi");
  assert.equal(calls[1].env.CARGO_BUILD_JOBS, "2");
  assert.equal(calls[2].env.CARGO_BUILD_JOBS, "2");
});

test("CI uses the correctness artifact path while package builds keep release WASM", () => {
  const workflow = readFileSync(
    new URL("../../../.github/workflows/ci.yml", import.meta.url),
    "utf8",
  );
  const packageJson = readFileSync(new URL("../../../package.json", import.meta.url), "utf8");
  assert.match(workflow, /pnpm build:test-artifacts/);
  assert.match(packageJson, /"build:test-artifacts": "node dev\/gates\/build-test-artifacts\.mjs"/);
  assert.match(
    packageJson,
    /"build:ci": "turbo run build:crates.*jazz-wasm.*jazz-napi.*jazz-tools/,
  );
});

test("Turbo invalidates each native artifact only for its Cargo closure", () => {
  const turbo = readFileSync(new URL("../../../turbo.json", import.meta.url), "utf8");
  assert.match(turbo, /"jazz-wasm#build"[\s\S]*?"\$TURBO_ROOT\$\/crates\/jazz-wasm\/\*\*"/);
  assert.match(turbo, /"jazz-wasm#build"[\s\S]*?"\$TURBO_ROOT\$\/crates\/opfs-btree\/\*\*"/);
  assert.match(turbo, /"jazz-napi#build"[\s\S]*?"\$TURBO_ROOT\$\/crates\/jazz-napi\/\*\*"/);
  const napi = turbo.slice(turbo.indexOf('"jazz-napi#build"'), turbo.indexOf('"jazz-wasm#build"'));
  const wasm = turbo.slice(turbo.indexOf('"jazz-wasm#build"'), turbo.indexOf('"build:crates"'));
  assert.doesNotMatch(napi, /dependsOn/);
  assert.doesNotMatch(wasm, /dependsOn/);
  assert.doesNotMatch(napi, /crates\/\*\*\/\.rs/);
  assert.doesNotMatch(wasm, /crates\/\*\*\/\.rs/);
  assert.doesNotMatch(napi, /crates\/opfs-btree/);
});
