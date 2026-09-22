import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";

const root = resolve(import.meta.dirname, "../../..");
const runner = resolve(root, "dev/gates/run-example-topology-soak.mjs");

test("records pass, failure, timeout, and reproducible commands", () => {
  const temporary = mkdtempSync(resolve(root, "target/jazz-topology-soak-"));
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
  rmSync(temporary, { recursive: true });
});

test("rejects duplicate scenario ids", () => {
  const temporary = mkdtempSync(resolve(root, "target/jazz-topology-soak-"));
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
  rmSync(temporary, { recursive: true });
});

test("timeout terminates descendants", async () => {
  const temporary = mkdtempSync(resolve(root, "target/jazz-topology-soak-"));
  const registry = resolve(temporary, "registry.json");
  const childPid = resolve(temporary, "child.pid");
  const program = `
    const { spawn } = require("node:child_process");
    const { writeFileSync } = require("node:fs");
    const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { detached: true, stdio: "ignore" });
    writeFileSync(${JSON.stringify(childPid)}, String(child.pid));
    setInterval(() => {}, 1000);
  `;
  writeFileSync(
    registry,
    JSON.stringify({
      schemaVersion: 1,
      scenarios: [
        {
          id: "planted.descendant-timeout",
          topology: ["fixture"],
          cwd: ".",
          argv: ["node", "-e", program],
        },
      ],
    }),
  );
  const result = spawnSync(
    "node",
    [runner, "--seed-count", "1", "--watchdog-seconds", "1", "--output", temporary],
    {
      cwd: root,
      env: { ...process.env, JAZZ_EXAMPLE_TOPOLOGY_REGISTRY: registry },
      encoding: "utf8",
    },
  );
  assert.equal(result.status, 1, result.stderr);
  const pid = Number(readFileSync(childPid, "utf8"));
  await assertProcessGone(pid);
  rmSync(temporary, { recursive: true });
});

test("rejects an output path that escapes through a symlink", () => {
  const temporary = mkdtempSync(resolve(root, "target/jazz-topology-soak-"));
  const outside = mkdtempSync(resolve(tmpdir(), "jazz-topology-outside-"));
  const registry = resolve(temporary, "registry.json");
  const link = resolve(temporary, "outside-link");
  symlinkSync(outside, link, "dir");
  writeFileSync(
    registry,
    JSON.stringify({
      schemaVersion: 1,
      scenarios: [
        { id: "planted.pass", topology: ["fixture"], cwd: ".", argv: ["node", "-e", ""] },
      ],
    }),
  );
  const escapedOutput = resolve(link, "should-not-exist");
  const result = spawnSync("node", [runner, "--seed-count", "1", "--output", escapedOutput], {
    cwd: root,
    env: { ...process.env, JAZZ_EXAMPLE_TOPOLOGY_REGISTRY: registry },
    encoding: "utf8",
  });
  assert.equal(result.status, 2);
  assert.match(result.stderr, /output must remain inside the repository/);
  assert.equal(existsSync(resolve(outside, "should-not-exist")), false);
  rmSync(temporary, { recursive: true });
  rmSync(outside, { recursive: true });
});

async function assertProcessGone(pid) {
  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      process.kill(pid, 0);
      await new Promise((resolveWait) => setTimeout(resolveWait, 50));
    } catch (error) {
      if (error.code === "ESRCH") return;
      throw error;
    }
  }
  assert.fail(`descendant process ${pid} survived timeout`);
}
