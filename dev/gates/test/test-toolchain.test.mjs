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

test("Turbo test tasks preserve the runner's canonical temporary directory", () => {
  const directory = mkdtempSync(join(realpathSync(tmpdir()), "jazz-test-tmpdir-"));
  try {
    const task = { ...manifest("turbo.json").tasks.test, dependsOn: [] };
    writeFileSync(join(directory, "turbo.json"), JSON.stringify({ tasks: { test: task } }));
    writeFileSync(
      join(directory, "package.json"),
      JSON.stringify({
        name: "jazz-env-probe",
        private: true,
        packageManager: manifest("package.json").packageManager,
        scripts: { test: "node probe.mjs" },
      }),
    );
    writeFileSync(
      join(directory, "pnpm-lock.yaml"),
      "lockfileVersion: '9.0'\nimporters:\n  .: {}\n",
    );
    writeFileSync(join(directory, "pnpm-workspace.yaml"), "packages: []\n");
    writeFileSync(
      join(directory, "probe.mjs"),
      `import assert from 'node:assert/strict'; assert.equal(process.env.TMPDIR, ${JSON.stringify(directory)});\n`,
    );
    const child = spawnSync(join(root, "node_modules/.bin/turbo"), ["run", "test", "--no-daemon"], {
      cwd: directory,
      env: { ...process.env, TMPDIR: directory, TURBO_TELEMETRY_DISABLED: "1" },
      encoding: "utf8",
      timeout: 30000,
    });
    assert.equal(child.status, 0, `${child.stdout}\n${child.stderr}`);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
