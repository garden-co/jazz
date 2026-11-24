import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";

const root = resolve(import.meta.dirname, "../../..");
const manifest = (path) => JSON.parse(readFileSync(join(root, path), "utf8"));
const quote = (value) => `'${value.replaceAll("'", "'\\''")}'`;

for (const [path, task] of [
  ["packages/inspector/package.json", "test:browser"],
  ["packages/jazz-tools/package.json", "test:topology-fixture"],
]) {
  test(`${path} ${task} preserves the caller's selected package manager`, () => {
    const directory = mkdtempSync(join(realpathSync(tmpdir()), "jazz-test-toolchain-"));
    try {
      const selected = join(directory, "selected");
      const login = join(directory, "login");
      const home = join(directory, "home");
      const receipt = join(directory, "receipt");
      for (const path of [selected, login, home]) mkdirSync(path);
      symlinkSync(process.execPath, join(selected, "node"));
      for (const [path, label] of [
        [selected, "selected"],
        [login, "login"],
      ]) {
        writeFileSync(
          join(path, "pnpm"),
          `#!/bin/sh\nprintf '${label}:%s\\n' "$*" >> "$RECEIPT"\n`,
          { mode: 0o755 },
        );
      }
      writeFileSync(
        join(home, ".bash_profile"),
        `export PATH=${quote(`${login}:/usr/bin:/bin`)}\n`,
      );
      const script = manifest(path).scripts[task];
      const boundary = script.indexOf(" -- ");
      assert.notEqual(boundary, -1, "task must retain correctness-artifact admission");
      const child = spawnSync("/bin/bash", ["-c", script.slice(boundary + 4)], {
        cwd: join(root, dirname(path)),
        env: {
          ...process.env,
          HOME: home,
          BASH_ENV: "/dev/null",
          PATH: `${selected}:/usr/bin:/bin`,
          RECEIPT: receipt,
        },
        encoding: "utf8",
        timeout: 10000,
      });
      assert.equal(child.status, 0, child.stderr);
      const invocations = readFileSync(receipt, "utf8").trim().split("\n");
      assert.equal(invocations.length, 2);
      assert.ok(
        invocations.every((line) => line.startsWith("selected:")),
        invocations.join("\n"),
      );
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
}

test("workspace tests and CI lanes receive only the allowed parent environment", () => {
  const directory = mkdtempSync(join(realpathSync(tmpdir()), "jazz-test-tmpdir-"));
  try {
    const rootManifest = manifest("package.json");
    const runner = rootManifest.scripts.test.split(" ");
    symlinkSync(join(root, "dev"), join(directory, "dev"), "dir");
    writeFileSync(
      join(directory, "package.json"),
      JSON.stringify({
        name: "root",
        private: true,
        packageManager: rootManifest.packageManager,
        scripts: { test: rootManifest.scripts.test },
      }),
    );
    writeFileSync(join(directory, "pnpm-workspace.yaml"), "packages: [probe]\n");
    mkdirSync(join(directory, "probe"));
    writeFileSync(
      join(directory, "probe/package.json"),
      JSON.stringify({
        name: "probe",
        scripts: { test: "node probe.cjs" },
      }),
    );
    const env = {
      TMPDIR: directory,
      JAZZ_TEST_SEALED_INSPECTOR_DIST: "1",
      RUSTC: "probe-rustc",
      RUSTC_WRAPPER: "probe-wrapper",
      SCCACHE_SERVER_UDS: "probe-socket",
      CI: "1",
      JAZZ_TEST_SEALED_TOOLS_DIST: "1",
      JAZZ_CORRECTNESS_ARTIFACT_RUN: "1",
      JAZZ_RN_TEST_BRIDGE: "1",
      JAZZ_CORRECTNESS_WASM_PACKAGE: "probe-wasm",
      JAZZ_CORRECTNESS_NAPI_BINDING: "probe-napi",
      JAZZ_CORRECTNESS_NAPI_FINGERPRINT: "probe-fingerprint",
      JAZZ_CORRECTNESS_CONSUMER_CAPABILITY: "probe-capability",
      JAZZ_CORRECTNESS_CONSUMER_TOKEN: "probe-token",
    };
    const blocked = [
      "UNLISTED_PROBE",
      "JAZZ_UNLISTED_PROBE",
      "AWS_ACCESS_KEY_ID",
      "AWS_SECRET_ACCESS_KEY",
      "AWS_SESSION_TOKEN",
      "TURBO_TOKEN",
      "TURBO_REMOTE_CACHE_SIGNATURE_KEY",
    ];
    writeFileSync(
      join(directory, "probe/probe.cjs"),
      `const assert = require("node:assert/strict");
      for (const [name, value] of Object.entries(${JSON.stringify(env)})) assert.equal(process.env[name], value);
      for (const name of ${JSON.stringify(blocked)}) assert.ok(!(name in process.env), name);
      console.log("workspace probe passed");`,
    );
    const childEnv = {
      ...process.env,
      ...env,
      ...Object.fromEntries(blocked.map((name) => [name, "probe"])),
      PATH: join(root, "node_modules/.bin") + ":" + process.env.PATH,
    };
    for (const [command, args, overrides] of [
      [runner[0], runner.slice(1), {}],
      [
        "bash",
        [join(root, "dev/gates/run-ts-tests.sh")],
        {
          JAZZ_SKIP_JAZZ_TOOLS_BUILD: "1",
          JAZZ_NODE_TEST_COMMAND: "node probe/probe.cjs",
          JAZZ_BROWSER_TEST_COMMAND: "node probe/probe.cjs",
        },
      ],
    ]) {
      const child = spawnSync(command, args, {
        cwd: directory,
        env: { ...childEnv, ...overrides },
        encoding: "utf8",
        timeout: 30000,
      });
      assert.equal(child.status, 0, `${child.stdout}\n${child.stderr}`);
      assert.match(child.stdout + child.stderr, /workspace probe passed/);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
