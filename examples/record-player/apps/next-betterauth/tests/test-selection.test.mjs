import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";
import packageJson from "../package.json" with { type: "json" };
import { providerReceipt, topologyReceipt } from "../vitest-receipts.mjs";

const cwd = fileURLToPath(new URL("..", import.meta.url));
const browserConfig = readFileSync(path.join(cwd, "vitest.config.browser.ts"), "utf8");
const execFileAsync = promisify(execFile);

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

test("topology's browser server uses an OS-assigned port", () => {
  assert.match(browserConfig, /api:\s*\{\s*port:\s*0,\s*strictPort:\s*true\s*\}/);
  assert.doesNotMatch(browserConfig, /port:\s*633\d+/);
});

if (process.env.JAZZ_RECORD_PLAYER_SELECTION_CONCURRENCY_CHILD !== "1") {
  test("selection checks do not contend with a concurrently booted topology browser server", async () => {
    // `vitest list` boots the same browser server used by the topology suite,
    // without executing its long-running scenarios. This is the CI shape that
    // previously collided: package `test` ran its selection receipt while the
    // workspace's parallel browser lane already owned the topology server.
    await Promise.all([
      execFileAsync("pnpm", ["exec", "vitest", "list", "--config", "vitest.config.browser.ts"], {
        cwd,
      }),
      execFileAsync(process.execPath, ["--test", "tests/test-selection.test.mjs"], {
        cwd,
        env: { ...process.env, JAZZ_RECORD_PLAYER_SELECTION_CONCURRENCY_CHILD: "1" },
      }),
    ]);
  });
}
