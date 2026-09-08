import assert from "node:assert/strict";
import { test } from "node:test";
import { dirname, join } from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
// Playwright is the declared direct dependency; playwright-core is nested in
// pnpm's isolated dependency directory.
const playwrightCoreRoot = join(
  dirname(require.resolve("playwright/package.json")),
  "..",
  "playwright-core",
);
const { CRExecutionContext } = require(
  join(playwrightCoreRoot, "lib/server/chromium/crExecutionContext.js"),
);

async function evaluateWithProtocolError(message) {
  const context = new CRExecutionContext(
    { send: async () => Promise.reject(new Error(message)) },
    { id: 1 },
  );
  return context.evaluateWithArguments("() => undefined", true, { _objectId: "utility" }, [], []);
}

test("Playwright preserves a collected Promise diagnosis", async () => {
  await assert.rejects(
    evaluateWithProtocolError("Promise was collected"),
    /Resulting promise was garbage collected\./,
  );
});

test("Playwright still reports other protocol errors as context loss", async () => {
  await assert.rejects(
    evaluateWithProtocolError("Cannot find context with specified id"),
    /Execution context was destroyed, most likely because of a navigation\./,
  );
});
