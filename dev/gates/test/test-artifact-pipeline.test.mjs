import assert from "node:assert/strict";
import test from "node:test";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildTestArtifacts, command } from "../build-test-artifacts.mjs";

test("test artifact pipeline overlaps independent bindings and repairs NAPI only after a failed load", async () => {
  const calls = [];
  let releaseLoadAttempts = 0;
  await buildTestArtifacts(async (command, args, label, options) => {
    calls.push({ command, args, label, options });
    if (label === "load release NAPI" && ++releaseLoadAttempts === 1)
      throw new Error("simulated corrupt binding");
  });

  const labels = calls.map((call) => call.label);
  assert.deepEqual(labels.slice(0, 4), ["release NAPI", "CLI", "fast WASM", "jazz-tools"]);
  assert.equal(labels.filter((label) => label === "release NAPI").length, 1);
  assert.equal(labels.filter((label) => label === "repair release NAPI").length, 1);
  assert.ok(labels.indexOf("jazz-tools") > labels.indexOf("fast WASM"));
  assert.ok(labels.indexOf("verify fast WASM provenance") < labels.indexOf("load release NAPI"));
  assert.ok(labels.indexOf("load repaired release NAPI") > labels.indexOf("repair release NAPI"));
  assert.ok(
    labels.indexOf("verify release NAPI provenance") > labels.indexOf("load repaired release NAPI"),
  );
  for (const call of calls.filter(({ label }) =>
    ["CLI", "fast WASM", "release NAPI", "repair release NAPI"].includes(label),
  ))
    assert.equal(call.options.env?.CARGO_TARGET_DIR, undefined, call.label);
});

test("a failed build aborts its still-running sibling commands", async () => {
  const aborted = [];
  let resolveCli;
  let resolveWasm;
  let resolveTools;
  const running = buildTestArtifacts((unusedCommand, unusedArgs, label, { signal } = {}) => {
    if (label === "release NAPI") return Promise.resolve();
    if (label === "CLI" || label === "fast WASM" || label === "jazz-tools")
      return new Promise((resolve, reject) => {
        if (label === "CLI") resolveCli = resolve;
        else if (label === "fast WASM") resolveWasm = resolve;
        else resolveTools = resolve;
        signal.addEventListener(
          "abort",
          () => {
            aborted.push(label);
            reject(new Error(`${label} aborted`));
          },
          { once: true },
        );
      });
    return Promise.resolve();
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(typeof resolveCli, "function");
  assert.equal(typeof resolveWasm, "function");
  assert.equal(resolveTools, undefined);
  resolveCli();
  resolveWasm();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(typeof resolveTools, "function");
  resolveTools();
  await running;

  await assert.rejects(
    buildTestArtifacts((unusedCommand, unusedArgs, label, { signal } = {}) => {
      if (label === "release NAPI") return Promise.resolve();
      if (label === "CLI")
        return new Promise((unusedResolve, reject) =>
          setImmediate(() => reject(new Error("simulated CLI failure"))),
        );
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

  await assert.rejects(
    buildTestArtifacts((unusedCommand, unusedArgs, label, { signal } = {}) => {
      if (label === "release NAPI") return Promise.reject(new Error("simulated NAPI failure"));
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
    /simulated NAPI failure/,
  );
  assert.deepEqual(aborted, ["fast WASM"]);
});

test("real subprocess inherits the caller's cache-compatible Cargo target", async () => {
  await command(
    process.execPath,
    [
      "-e",
      `if(process.env.CARGO_TARGET_DIR!==${JSON.stringify(process.env.CARGO_TARGET_DIR)}) process.exit(42)`,
    ],
    "default target smoke",
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
  const pipeline = readFileSync(new URL("../build-test-artifacts.mjs", import.meta.url), "utf8");
  assert.match(workflow, /pnpm build:test-artifacts/);
  assert.match(packageJson, /"build:test-artifacts": "node dev\/gates\/build-test-artifacts\.mjs"/);
  assert.match(
    packageJson,
    /"build:ci": "turbo run build:crates.*jazz-wasm.*jazz-napi.*jazz-tools/,
  );
  assert.doesNotMatch(workflow, /CARGO_TARGET_DIR/);
  assert.doesNotMatch(pipeline, /target\/test-artifacts-(?:wasm|napi)/);
});

test("Turbo invalidates each native artifact only for its Cargo closure", () => {
  const turbo = JSON.parse(readFileSync(new URL("../../../turbo.json", import.meta.url), "utf8"));
  const napi = turbo.tasks["jazz-napi#build"];
  const wasm = turbo.tasks["jazz-wasm#build"];
  const cli = turbo.tasks["build:crates"];
  assert.equal(napi.dependsOn, undefined);
  assert.equal(wasm.dependsOn, undefined);
  for (const [task, inputs] of [
    [
      napi,
      ["jazz-napi", "jazz", "groove", "opfs-btree"].map(
        (crate) => `$TURBO_ROOT$/crates/${crate}/**`,
      ),
    ],
    [
      wasm,
      ["jazz-wasm", "jazz", "groove", "opfs-btree"].map(
        (crate) => `$TURBO_ROOT$/crates/${crate}/**`,
      ),
    ],
    [
      cli,
      ["jazz", "groove", "opfs-btree"].flatMap((crate) => [
        `$TURBO_ROOT$/crates/${crate}/Cargo.toml`,
        `$TURBO_ROOT$/crates/${crate}/src/**/*.rs`,
      ]),
    ],
  ]) {
    for (const input of inputs) assert.ok(task.inputs.includes(input), input);
    assert.equal(task.inputs.includes("$TURBO_ROOT$/crates/**/*.rs"), false);
  }
});
