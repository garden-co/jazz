import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";
import packageJson from "../package.json" with { type: "json" };

const cwd = fileURLToPath(new URL("..", import.meta.url));

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

test("the maintained package gate includes every non-topology receipt", () => {
  assert.match(packageJson.scripts.test, /test:unit/);
  assert.match(packageJson.scripts.test, /test:provider/);
  assert.match(packageJson.scripts.test, /test:selection/);
  assert.equal(
    packageJson.scripts["test:topology"],
    "vitest run --config vitest.config.browser.ts",
  );
});
