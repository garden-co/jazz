import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";
import packageJson from "../package.json" with { type: "json" };
import { providerReceipt, topologyReceipt } from "../vitest-receipts.mjs";

const cwd = fileURLToPath(new URL("..", import.meta.url));
const browserConfig = readFileSync(path.join(cwd, "vitest.config.browser.ts"), "utf8");
const selectionSource = readFileSync(new URL(import.meta.url), "utf8");

test("browser and provider gates select disjoint receipts", () => {
  assert.equal(topologyReceipt, "tests/browser/topology.e2e.test.ts");
  assert.equal(providerReceipt, "tests/browser/provider.e2e.test.tsx");
  assert.notEqual(topologyReceipt, providerReceipt);
  assert.match(browserConfig, /include:\s*\[topologyReceipt\]/);
  assert.match(
    readFileSync(path.join(cwd, "vitest.config.provider.ts"), "utf8"),
    /include:\s*\[providerReceipt\]/,
  );
});

test("the Node package gate excludes the browser-only topology receipt", () => {
  assert.match(packageJson.scripts.test, /test:unit/);
  assert.match(packageJson.scripts.test, /test:provider/);
  assert.match(packageJson.scripts.test, /test:selection/);
  assert.doesNotMatch(packageJson.scripts.test, /test:topology/);
  assert.equal(
    packageJson.scripts["test:topology"],
    "node ../../../../dev/gates/run-correctness-consumer.mjs -- pnpm exec vitest run --config vitest.config.browser.ts",
  );
  assert.equal(packageJson.scripts["test:browser"], "pnpm test:topology");
});

test("the topology browser project provides the complete Jazz server command contract", () => {
  for (const command of [
    "jazzServerInfo",
    "jazzServerStop",
    "jazzServerBlockNetwork",
    "jazzServerUnblockNetwork",
    "jazzServerJwtForUser",
  ]) {
    assert.match(browserConfig, new RegExp(`\\b${command}\\b`));
  }
});

test("topology's browser server can retry an occupied preferred port", () => {
  assert.match(browserConfig, /api:\s*\{\s*port:\s*63318,\s*strictPort:\s*false\s*\}/);
});

test("selection checks remain data-only and never boot a Vitest browser server", () => {
  assert.doesNotMatch(selectionSource, new RegExp(["node", "child_process"].join(":")));
  assert.doesNotMatch(selectionSource, new RegExp(["vitest", "list"].join("\\s+")));
});
