import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";

const root = resolve(import.meta.dirname, "../../..");
const producerTasks = ["jazz-napi#build", "jazz-wasm#build", "jazz-wasm#build:fast"];

// Run the real strict-mode Turbo child boundary, replacing only the fixture's
// commands with a JSON reporter. This never runs Cargo or writes a target.
test("Rust producers retain the owner's target, jobs and offline boundary in strict mode", () => {
  const config = JSON.parse(readFileSync(join(root, "turbo.json"), "utf8"));
  const fixture = mkdtempSync(join(tmpdir(), "jazz-turbo-producer-env-"));
  const writeJson = (path, value) => writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
  let passed = false;
  try {
    writeJson(join(fixture, "package.json"), {
      name: "producer-environment-fixture",
      private: true,
      packageManager: "pnpm@10.14.0",
    });
    writeFileSync(join(fixture, "pnpm-workspace.yaml"), "packages:\n  - packages/*\n");
    writeFileSync(
      join(fixture, "pnpm-lock.yaml"),
      "lockfileVersion: '9.0'\nimporters:\n  .: {}\n  packages/napi: {}\n  packages/wasm: {}\n",
    );
    const tasks = { build: { cache: false }, "build:fast": { cache: false } };
    for (const task of producerTasks) {
      tasks[task] = { cache: false, passThroughEnv: config.tasks[task].passThroughEnv };
    }
    writeJson(join(fixture, "turbo.json"), { tasks });
    for (const [directory, name, names] of [
      ["napi", "jazz-napi", ["build"]],
      ["wasm", "jazz-wasm", ["build", "build:fast"]],
    ]) {
      const path = join(fixture, "packages", directory);
      mkdirSync(path, { recursive: true });
      writeJson(join(path, "package.json"), {
        name,
        version: "0.0.0",
        scripts: Object.fromEntries(
          names.map((task) => [task, `node ../../probe.cjs ${name}#${task}`]),
        ),
      });
    }
    writeFileSync(
      join(fixture, "probe.cjs"),
      `console.log('JAZZ_RUST_ENV_PROBE=' + JSON.stringify({
        task: process.argv[2],
        target: process.env.CARGO_TARGET_DIR ?? null,
        jobs: process.env.CARGO_BUILD_JOBS ?? null,
        offline: process.env.CARGO_NET_OFFLINE ?? null,
        unlisted: process.env.JAZZ_UNLISTED_PRODUCER_ENV_PROBE ?? null,
      }));\n`,
    );
    const expectedTarget = process.env.CARGO_TARGET_DIR ?? join(fixture, "uncreated-target");
    const output = execFileSync(
      join(root, "node_modules/.bin/turbo"),
      ["run", "build", "build:fast", "--env-mode=strict", "--cache=local:rw"],
      {
        cwd: fixture,
        encoding: "utf8",
        timeout: 30_000,
        env: {
          ...process.env,
          CARGO_TARGET_DIR: expectedTarget,
          CARGO_BUILD_JOBS: "2",
          CARGO_NET_OFFLINE: "true",
          JAZZ_UNLISTED_PRODUCER_ENV_PROBE: "must-not-cross-the-boundary",
        },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    const observed = output
      .split("\n")
      .filter((line) => line.includes("JAZZ_RUST_ENV_PROBE="))
      .map((line) => JSON.parse(line.split("JAZZ_RUST_ENV_PROBE=")[1]));
    console.log(JSON.stringify({ observedProducerEnvironment: observed }));
    assert.equal(observed.length, producerTasks.length, output);
    for (const task of producerTasks) {
      assert.deepEqual(
        observed.find((entry) => entry.task === task),
        {
          task,
          target: expectedTarget,
          jobs: "2",
          offline: "true",
          unlisted: null,
        },
      );
    }
    passed = true;
  } finally {
    if (passed) rmSync(fixture, { recursive: true, force: true });
    else console.error(`Retained failing environment fixture: ${fixture}`);
  }
});
