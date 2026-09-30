import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

const SCRIPT = path.resolve(new URL("./bootstrap_runner.sh", import.meta.url).pathname);

test("bootstrap treats RUNNER_USER as data when resolving the runner home", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-bootstrap-injection-"));
  const bin = path.join(root, "bin");
  const marker = path.join(root, "evaluated");
  fs.mkdirSync(bin);

  const stub = (name, body) => {
    const file = path.join(bin, name);
    fs.writeFileSync(file, `#!/bin/sh\n${body}\n`);
    fs.chmodSync(file, 0o755);
  };
  stub("id", "exit 1");
  stub("useradd", "exit 0");
  stub("apt-get", "exit 1");

  try {
    const result = spawnSync("/bin/bash", [SCRIPT], {
      cwd: root,
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${bin}:/usr/bin:/bin`,
        RUNNER_USER: `$(touch ${marker})`,
        RUNNER_TOKEN: "test-token",
        INSTALL_SSM_AGENT: "0",
        SKIP_HARDENING: "1",
      },
      timeout: 5000,
    });

    assert.equal(result.error, undefined, result.error?.message);
    assert.notEqual(result.status, 0, "stubbed apt-get should stop bootstrap");
    assert.equal(fs.existsSync(marker), false, "RUNNER_USER command substitution must not execute");
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
