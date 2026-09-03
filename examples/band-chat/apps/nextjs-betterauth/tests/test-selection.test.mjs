import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import packageJson from "../package.json" with { type: "json" };
import { run } from "../scripts/test-browser-focused.mjs";

const cwd = fileURLToPath(new URL("..", import.meta.url));
const browserConfig = readFileSync(path.join(cwd, "vitest.config.browser.ts"), "utf8");
const focusedBrowserRunner = readFileSync(
  path.join(cwd, "scripts/test-browser-focused.mjs"),
  "utf8",
);

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

test("the focused browser receipt builds and tests in one admitted consumer process tree", () => {
  const calls = [];
  const file = path.join(cwd, "tests/browser/topology.e2e.test.tsx");
  assert.equal(
    run([file], (command, args) => {
      calls.push({ command, args });
      return { status: 0 };
    }),
    0,
  );
  assert.deepEqual(calls, [
    {
      command: "node",
      args: [
        "../../../../dev/gates/run-correctness-consumer.mjs",
        "--",
        "bash",
        "-c",
        'pnpm --filter jazz-tools build && pnpm exec vitest run --config vitest.config.browser.ts "$1"',
        "--",
        file,
      ],
    },
  ]);
  assert.match(focusedBrowserRunner, /run-correctness-consumer\.mjs/);
});
