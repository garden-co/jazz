import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";
import packageJson from "../package.json" with { type: "json" };

const cwd = fileURLToPath(new URL("..", import.meta.url));
const browserConfig = readFileSync(path.join(cwd, "vitest.config.browser.ts"), "utf8");

function listedTests(config) {
  return execFileSync("pnpm", ["exec", "vitest", "list", "--config", config], {
    cwd,
    encoding: "utf8",
  });
}

test("browser and provider gates select disjoint receipts", () => {
  const topology = listedTests("vitest.config.browser.ts");
  const provider = listedTests("vitest.config.provider.ts");

  assert.match(topology, /tests\/browser\/topology\.e2e\.test\.ts/);
  assert.doesNotMatch(topology, /tests\/browser\/provider\.e2e\.test\.tsx/);
  assert.match(provider, /tests\/browser\/provider\.e2e\.test\.tsx/);
  assert.doesNotMatch(provider, /tests\/browser\/topology\.e2e\.test\.ts/);
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
