import assert from "node:assert/strict";
import test from "node:test";
import packageJson from "../../package.json" with { type: "json" };

test("browser receipts rebuild the embedded inspector before Playwright", () => {
  assert.equal(
    packageJson.scripts["test:browser"],
    "pnpm run build:embedded && playwright test --config playwright.config.ts",
  );
});
