import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import packageJson from "../package.json" with { type: "json" };

const cwd = fileURLToPath(new URL("..", import.meta.url));
const browserConfig = readFileSync(path.join(cwd, "vitest.config.browser.ts"), "utf8");

test("the Node package gate excludes browser-only topology receipts", () => {
  assert.match(packageJson.scripts.test, /test:permissions/);
  assert.match(packageJson.scripts.test, /test:selection/);
  assert.doesNotMatch(packageJson.scripts.test, /test:browser/);
  assert.equal(packageJson.scripts["test:selection"], "node --test tests/test-selection.test.mjs");
});

test("the topology browser project provides the complete Jazz server command contract", () => {
  for (const command of [
    "jazzServerInfo",
    "jazzServerStop",
    "jazzServerBlockNetwork",
    "jazzServerUnblockNetwork",
    "jazzServerJwtForUser",
  ]) {
    assert.match(browserConfig, new RegExp(`\\b${command}\\s*:`));
  }
});
