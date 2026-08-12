import assert from "node:assert/strict";
import test from "node:test";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { buildTestArtifacts, command, testArtifactTargets } from "../build-test-artifacts.mjs";

test("test artifact pipeline overlaps independent bindings and repairs NAPI only after a failed load", async () => {
  const calls = [];
  let releaseLoadAttempts = 0;
  await buildTestArtifacts(async (command, args, label, options) => {
    calls.push({ command, args, label, options });
    if (label === "load release NAPI" && ++releaseLoadAttempts === 1)
      throw new Error("simulated corrupt binding");
  });

  const labels = calls.map((call) => call.label);
  assert.deepEqual(labels.slice(0, 3), ["CLI", "fast WASM", "release NAPI"]);
  assert.equal(labels.filter((label) => label === "release NAPI").length, 1);
  assert.equal(labels.filter((label) => label === "repair release NAPI").length, 1);
  assert.ok(labels.indexOf("jazz-tools") > labels.indexOf("fast WASM"));
  assert.ok(labels.indexOf("verify fast WASM provenance") < labels.indexOf("load release NAPI"));
  assert.ok(labels.indexOf("load repaired release NAPI") > labels.indexOf("repair release NAPI"));
  assert.ok(
    labels.indexOf("verify release NAPI provenance") > labels.indexOf("load repaired release NAPI"),
  );
  assert.equal(calls[1].options.env.CARGO_TARGET_DIR, testArtifactTargets.wasm);
  assert.equal(calls[2].options.env.CARGO_TARGET_DIR, testArtifactTargets.napi);
  assert.ok(isAbsolute(calls[1].options.env.CARGO_TARGET_DIR));
  assert.ok(isAbsolute(calls[2].options.env.CARGO_TARGET_DIR));
});

test("a failed build aborts its still-running sibling commands", async () => {
  const aborted = [];
  await assert.rejects(
    buildTestArtifacts((unusedCommand, unusedArgs, label, { signal }) => {
      if (label === "CLI") return Promise.reject(new Error("simulated CLI failure"));
      return new Promise((resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            aborted.push(label);
            reject(new Error(`${label} aborted`));
          },
          { once: true },
        );
      });
    }),
    /simulated CLI failure/,
  );
  assert.deepEqual(aborted.sort(), ["fast WASM", "release NAPI"]);
});

test("real subprocess receives an absolute target directory", async () => {
  await command(
    process.execPath,
    [
      "-e",
      "const {isAbsolute}=require('node:path'); if(!isAbsolute(process.env.CARGO_TARGET_DIR)) process.exit(42)",
    ],
    "absolute target smoke",
    { env: { CARGO_TARGET_DIR: testArtifactTargets.wasm } },
  );
});

test("aborting a real subprocess terminates its spawned child", async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-cancel-"));
  const marker = join(fixture, "orphaned-child");
  const controller = new AbortController();
  const parentScript = [
    "const {spawn}=require('node:child_process')",
    `spawn(process.execPath,['-e',${JSON.stringify(`setTimeout(()=>require('node:fs').writeFileSync(${JSON.stringify(marker)},'orphan'),400)`)}],{stdio:'ignore'})`,
    "setInterval(()=>{},1000)",
  ].join(";");
  const running = command(process.execPath, ["-e", parentScript], "cancellation smoke", {
    signal: controller.signal,
  });
  setTimeout(() => controller.abort(), 100);
  await assert.rejects(running, /cancellation smoke failed/);
  await new Promise((resolve) => setTimeout(resolve, 500));
  assert.equal(existsSync(marker), false);
  rmSync(fixture, { recursive: true, force: true });
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
  const turbo = JSON.parse(readFileSync(new URL("../../../turbo.json", import.meta.url), "utf8"));
  const napi = turbo.tasks["jazz-napi#build"];
  const wasm = turbo.tasks["jazz-wasm#build"];
  const cli = turbo.tasks["build:crates"];
  assert.equal(napi.dependsOn, undefined);
  assert.equal(wasm.dependsOn, undefined);
  for (const [task, closure] of [
    [napi, ["jazz-napi", "jazz", "groove", "opfs-btree"]],
    [wasm, ["jazz-wasm", "jazz", "groove", "opfs-btree"]],
    [cli, ["jazz", "groove", "opfs-btree"]],
  ]) {
    for (const crate of closure)
      assert.ok(task.inputs.includes(`$TURBO_ROOT$/crates/${crate}/**`), crate);
    assert.equal(task.inputs.includes("$TURBO_ROOT$/crates/**/*.rs"), false);
  }
});
