import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";

const root = resolve(new URL("../../..", import.meta.url).pathname);
const read = (path) => readFileSync(resolve(root, path), "utf8");

test("every direct Node/browser correctness entrypoint uses the one sealed consumer runner", () => {
  const packages = [
    "packages/jazz-tools/package.json",
    "packages/inspector/package.json",
    "examples/band-chat/apps/nextjs-betterauth/package.json",
    "examples/record-player/apps/next-betterauth/package.json",
  ];
  for (const path of packages) {
    const pkg = JSON.parse(read(path));
    assert.match(
      pkg.scripts["test:browser"],
      /run-correctness-consumer\.mjs --/,
      `${path} bypasses the producer-manifest admission boundary`,
    );
    assert.doesNotMatch(
      pkg.scripts["test:browser"],
      /verify-correctness-test-artifacts\.mjs\s*&&|^\s*(?:vitest|pnpm\s+test:topology)/,
      `${path} retains a direct mutable-artifact browser command`,
    );
  }

  const focused = read("packages/jazz-tools/scripts/test-browser-focused.mjs");
  assert.match(focused, /run-correctness-consumer\.mjs/);
  assert.doesNotMatch(focused, /verify-correctness-test-artifacts\.mjs/);

  const aggregate = read("dev/gates/run-ts-consumers.mjs");
  assert.match(aggregate, /correctnessConsumerEnvironment\(root\)/);
  assert.doesNotMatch(aggregate, /env:\s*process\.env/);
});

test("sealed consumers select immutable artifact paths rather than worktree pointers", () => {
  const runner = read("dev/gates/run-correctness-consumer.mjs");
  const producer = read("dev/artifacts/correctness-artifact-producer.mjs");
  for (const variable of [
    "JAZZ_CORRECTNESS_WASM_PACKAGE",
    "JAZZ_CORRECTNESS_NAPI_BINDING",
    "JAZZ_CORRECTNESS_CLI",
  ])
    assert.match(producer, new RegExp(variable));
  assert.match(runner, /correctnessArtifactConsumerEnvironment/);

  const binding = read("crates/jazz-napi/native-binding.cjs");
  assert.match(binding, /JAZZ_CORRECTNESS_ARTIFACT_RUN/);
  assert.ok(
    binding.indexOf('if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1")') <
      binding.indexOf("else if (existsSync(correctnessPointer))"),
    "sealed binding selection must precede every mutable pointer fallback",
  );

  const worker = read("packages/jazz-tools/scripts/bundle-broker-worker.mjs");
  assert.match(worker, /JAZZ_CORRECTNESS_WASM_PACKAGE/);
  assert.ok(
    worker.indexOf("const sealedWasmPackage") < worker.indexOf("const snapshot = sealedWasmPackage"),
    "sealed worker bundling must not consult a mutable WASM pointer",
  );

  for (const config of [
    "packages/jazz-tools/vitest.config.ts",
    "packages/jazz-tools/vitest.config.browser.ts",
  ]) {
    const source = read(config);
    assert.match(source, /JAZZ_CORRECTNESS_WASM_PACKAGE/);
    assert.match(source, /"jazz-wasm"/);
  }
  const testRuntime = read("packages/jazz-tools/src/runtime/testing/wasm-runtime-test-utils.ts");
  assert.match(testRuntime, /JAZZ_CORRECTNESS_WASM_PACKAGE/);
});
