import assert from "node:assert/strict";
import test from "node:test";
import packageJson from "../../package.json" with { type: "json" };

test("browser receipts rebuild the embedded inspector through sealed artifact admission", () => {
  assert.equal(
    packageJson.scripts["test:browser"],
    "node ../../dev/gates/run-correctness-consumer.mjs -- bash -lc 'pnpm run build:embedded && pnpm exec playwright test --config playwright.config.ts'",
  );
});

test("the normal Inspector test target keeps the browser command contract covered", () => {
  assert.match(packageJson.scripts.test, /test:browser-contract/);
});
