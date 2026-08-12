import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");
const workflow = fs.readFileSync(path.join(root, ".github/workflows/ci.yml"), "utf8");
const packageJson = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
const job = (name, nextName) => {
  const start = workflow.indexOf(`  ${name}:`);
  const end = nextName ? workflow.indexOf(`  ${nextName}:`, start + 1) : workflow.length;
  assert.notEqual(start, -1, `missing ${name} job`);
  assert.notEqual(end, -1, `missing boundary after ${name} job`);
  return workflow.slice(start, end);
};

test("Rust CI uses pinned prebuilt tools without charging Rust-only jobs for wasm-pack", () => {
  const buildIntegration = job("build-integration", "lint");
  const lint = job("lint", "test-rust");
  const rust = job("test-rust", "test-ts");
  const typescript = job("test-ts");

  assert.doesNotMatch(workflow, /cargo install cargo-nextest/);
  assert.match(rust, /tool: cargo-nextest@0\.9\.143/);
  assert.doesNotMatch(rust, /ensure:rust-toolchain|wasm-pack/);
  assert.doesNotMatch(rust, /rust-components:/);
  assert.doesNotMatch(lint, /ensure:rust-toolchain|wasm-pack/);
  assert.doesNotMatch(buildIntegration, /ensure:rust-toolchain|wasm-pack/);
  assert.match(typescript, /tool: wasm-pack@0\.13\.1/);
});

test("lint keeps its one workspace Clippy invocation inside pnpm lint", () => {
  const lint = job("lint", "test-rust");
  assert.match(lint, /run: pnpm lint/);
  assert.doesNotMatch(lint, /^\s*- run: cargo clippy/m);
});

test("CI runs the workflow contract test through its package script", () => {
  const lint = job("lint", "test-rust");
  assert.equal(
    packageJson.scripts["test:ci-workflow"],
    "node --test dev/gates/test/ci-rust-throughput.test.mjs dev/gates/test/test-artifact-pipeline.test.mjs",
  );
  assert.match(lint, /run: pnpm test:ci-workflow/);
});

test("TypeScript CI overlaps independent Node and browser suites after one artifact build", () => {
  const typescript = job("test-ts");
  assert.match(typescript, /name: Build correctness-test artifacts\s+run: pnpm build:test-artifacts/);
  assert.match(typescript, /name: Run Node and browser test suites in parallel/);
  assert.match(typescript, /pnpm test .* &\s+node_tests_pid=\$!/);
  assert.match(typescript, /pnpm --filter jazz-tools test:browser &\s+browser_tests_pid=\$!/);
  assert.match(typescript, /wait "\$\{node_tests_pid\}"/);
  assert.match(typescript, /wait "\$\{browser_tests_pid\}"/);
  assert.doesNotMatch(typescript, /rust-components: clippy,rustfmt/);
});
