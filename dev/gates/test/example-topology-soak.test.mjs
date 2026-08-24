import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";

const root = resolve(import.meta.dirname, "../../..");
const runner = resolve(root, "dev/gates/run-example-topology-soak.mjs");

test("records pass, failure, timeout, and reproducible commands", () => {
  const temporary = mkdtempSync(resolve(tmpdir(), "jazz-topology-soak-"));
  const registry = resolve(temporary, "registry.json");
  const output = resolve(temporary, "output");
  writeFileSync(
    registry,
    JSON.stringify({
      schemaVersion: 1,
      scenarios: [
        { id: "planted.pass", topology: ["fixture"], cwd: ".", argv: ["node", "-e", ""] },
        {
          id: "planted.failure",
          topology: ["fixture"],
          cwd: "packages/jazz-tools",
          argv: ["node", "-e", "process.exit(7)"],
        },
        {
          id: "planted.timeout",
          topology: ["fixture"],
          cwd: ".",
          argv: ["node", "-e", "setTimeout(() => {}, 5000)"],
        },
      ],
    }),
  );

  const result = spawnSync(
    "node",
    [runner, "--seed-count", "1", "--watchdog-seconds", "1", "--output", output],
    {
      cwd: root,
      env: { ...process.env, JAZZ_EXAMPLE_TOPOLOGY_REGISTRY: registry },
      encoding: "utf8",
    },
  );
  assert.equal(result.status, 1, result.stderr);
  const summary = JSON.parse(readFileSync(resolve(output, "summary.json"), "utf8"));
  assert.deepEqual(
    summary.cases.map(({ status }) => status),
    ["passed", "failed", "timeout"],
  );
  assert.equal(summary.cases[1].exitCode, 7);
  assert.match(
    summary.cases[1].replay,
    /^cd packages\/jazz-tools && JAZZ_EXAMPLE_TOPOLOGY_SEED=11 node -e /,
  );
  assert.equal(summary.failures.length, 2);
});

test("rejects duplicate scenario ids", () => {
  const temporary = mkdtempSync(resolve(tmpdir(), "jazz-topology-soak-"));
  const registry = resolve(temporary, "registry.json");
  writeFileSync(
    registry,
    JSON.stringify({
      schemaVersion: 1,
      scenarios: [
        { id: "duplicate", topology: ["fixture"], cwd: ".", argv: ["node", "-e", ""] },
        { id: "duplicate", topology: ["fixture"], cwd: ".", argv: ["node", "-e", ""] },
      ],
    }),
  );
  const result = spawnSync("node", [runner, "--list"], {
    cwd: root,
    env: { ...process.env, JAZZ_EXAMPLE_TOPOLOGY_REGISTRY: registry },
    encoding: "utf8",
  });
  assert.equal(result.status, 2);
  assert.match(result.stderr, /duplicate scenario id: duplicate/);
});
