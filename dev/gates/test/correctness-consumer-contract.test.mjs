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

  const recordPlayer = JSON.parse(
    read("examples/record-player/apps/next-betterauth/package.json"),
  );
  assert.match(recordPlayer.scripts["test:topology"], /run-correctness-consumer\.mjs --/);
  assert.equal(recordPlayer.scripts["test:browser"], "pnpm test:topology");

  const focused = read("packages/jazz-tools/scripts/test-browser-focused.mjs");
  assert.match(focused, /run-correctness-consumer\.mjs/);
  assert.doesNotMatch(focused, /verify-correctness-test-artifacts\.mjs/);

  const aggregate = read("dev/gates/run-ts-consumers.mjs");
  assert.match(aggregate, /runCorrectnessConsumer\(/);
  assert.match(aggregate, /rootDir: root/);
  assert.doesNotMatch(aggregate, /env:\s*process\.env/);
});

test("sealed consumers select content-addressed artifact paths rather than worktree pointers", () => {
  const runner = read("dev/gates/run-correctness-consumer.mjs");
  const producer = read("dev/artifacts/correctness-artifact-producer.mjs");
  for (const variable of [
    "JAZZ_CORRECTNESS_WASM_PACKAGE",
    "JAZZ_CORRECTNESS_NAPI_BINDING",
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
    "packages/jazz-tools/vitest.config.react.ts",
    "packages/jazz-tools/vitest.config.solid.ts",
    "packages/jazz-tools/vitest.config.svelte.ts",
    "examples/band-chat/apps/nextjs-betterauth/vitest.config.browser.ts",
    "examples/record-player/apps/next-betterauth/vitest.config.browser.ts",
  ]) {
    const source = read(config);
    assert.match(source, /JAZZ_CORRECTNESS_WASM_PACKAGE/);
    assert.match(source, /"jazz-wasm"/);
  }
  const testRuntime = read("packages/jazz-tools/src/runtime/testing/wasm-runtime-test-utils.ts");
  assert.match(testRuntime, /JAZZ_CORRECTNESS_WASM_PACKAGE/);
  for (const config of ["packages/inspector/vitest.config.ts", "packages/inspector/vite.config.ts"])
    assert.match(read(config), /JAZZ_CORRECTNESS_WASM_PACKAGE/);
});

test("every direct Jazz Tools Vitest consumer is sealed before it runs", () => {
  const pkg = JSON.parse(read("packages/jazz-tools/package.json"));
  for (const name of [
    "test:node",
    "test:topology-fixture",
    "test:react",
    "test:solid",
    "test:svelte",
    "test:browser",
  ])
    assert.match(
      pkg.scripts[name],
      /run-correctness-consumer\.mjs --/,
      `${name} bypasses the producer-manifest admission boundary`,
    );
});

test("performance benchmarks remain on their explicit release-artifact boundary", () => {
  const pkg = JSON.parse(read("packages/jazz-tools/package.json"));
  for (const name of [
    "bench:abstract:node",
    "bench:abstract:browser",
    "bench:realistic:browser",
  ]) {
    assert.doesNotMatch(pkg.scripts[name], /run-correctness-consumer\.mjs/);
    assert.match(pkg.scripts[name], /vitest run/);
  }
  const workflow = read(".github/workflows/benchmarks.yml");
  assert.match(workflow, /name: Build jazz-tools server binary[\s\S]*cargo build -p jazz-cli --bin jazz-tools/);
  assert.match(workflow, /name: Build jazz-napi package[\s\S]*pnpm --dir crates\/jazz-napi run build/);
  assert.match(workflow, /name: Build jazz-wasm package[\s\S]*pnpm --dir crates\/jazz-wasm run build/);
  assert.match(workflow, /name: Run browser benchmark suite/);
  assert.ok(
    workflow.indexOf("name: Build jazz-wasm package") <
      workflow.indexOf("name: Run browser benchmark suite"),
    "browser benchmarks must run after their explicit release WASM build",
  );
  const browserConfig = read("packages/jazz-tools/vitest.config.browser.ts");
  assert.match(browserConfig, /performanceArtifactRun/);
  assert.match(browserConfig, /sealedWasmPackage \|\| performanceArtifactRun/);
});

test("every correctness snapshot artifact has an actual runtime consumer", () => {
  const producer = read("dev/artifacts/correctness-artifact-producer.mjs");
  assert.match(producer, /JAZZ_CORRECTNESS_WASM_PACKAGE/);
  assert.match(producer, /JAZZ_CORRECTNESS_NAPI_BINDING/);
  assert.doesNotMatch(producer, /JAZZ_CORRECTNESS_CLI|cliArtifact|cliFingerprint/);
  assert.match(read("packages/jazz-tools/vitest.config.ts"), /JAZZ_CORRECTNESS_WASM_PACKAGE/);
  assert.match(read("crates/jazz-napi/native-binding.cjs"), /JAZZ_CORRECTNESS_NAPI_BINDING/);
});
