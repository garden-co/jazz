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
test("Rust producers retain the owner's target, jobs, offline boundary and sccache client in strict mode", () => {
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
        rustc: process.env.RUSTC ?? null,
        wrapper: process.env.RUSTC_WRAPPER ?? null,
        sccacheSocket: process.env.SCCACHE_SERVER_UDS ?? null,
        awsSecret: process.env.AWS_SECRET_ACCESS_KEY ?? null,
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
          // CI's setup-blacksmith starts one sccache server (holding the S3
          // tier's configuration and credentials) and points cargo at it with
          // these; setup-build's provisioned runners add an absolute RUSTC for
          // their host-owned daemon. Strict mode dropped them, so every
          // producer compile in CI bypassed sccache (merge group 743: 0
          // compile requests).
          RUSTC: "/opt/probe/bin/rustc",
          RUSTC_WRAPPER: "sccache",
          SCCACHE_SERVER_UDS: "/tmp/sccache-probe.sock",
          AWS_SECRET_ACCESS_KEY: "must-not-cross-the-boundary",
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
          rustc: "/opt/probe/bin/rustc",
          wrapper: "sccache",
          sccacheSocket: "/tmp/sccache-probe.sock",
          // The client only needs the wrapper and the server's socket; cloud
          // credentials stay with the server and never reach build scripts.
          awsSecret: null,
        },
      );
    }
    passed = true;
  } finally {
    if (passed) rmSync(fixture, { recursive: true, force: true });
    else console.error(`Retained failing environment fixture: ${fixture}`);
  }
});
