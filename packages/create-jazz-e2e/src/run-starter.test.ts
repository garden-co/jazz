import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import test from "node:test";

import { runStarter } from "./run-starter.js";

function missingTarballDir(): string {
  return path.join(os.tmpdir(), `create-jazz-e2e-missing-${randomUUID()}`);
}

test("cleanup preserves a caller-provided work directory", async (t) => {
  const testDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-e2e-caller-test-"));
  const workDir = path.join(testDir, "caller-owned");
  const sentinel = path.join(workDir, "sentinel.txt");
  fs.mkdirSync(workDir);
  fs.writeFileSync(sentinel, "caller data");
  t.after(() => fs.rmSync(testDir, { recursive: true, force: true }));

  const result = await runStarter({
    starter: "react-localfirst",
    repoRoot: process.cwd(),
    workDir,
    tarballDir: missingTarballDir(),
  });

  assert.equal(result.success, false);
  assert.match(result.errorMessage ?? "", /does not exist/);
  assert.equal(fs.existsSync(workDir), true);
  assert.equal(fs.readFileSync(sentinel, "utf8"), "caller data");
});

test("cleanup removes a harness-created temporary work directory", async (t) => {
  const result = await runStarter({
    starter: "react-localfirst",
    repoRoot: process.cwd(),
    tarballDir: missingTarballDir(),
  });
  const workDir = path.dirname(result.appDir);
  t.after(() => fs.rmSync(workDir, { recursive: true, force: true }));

  assert.equal(result.success, false);
  assert.match(result.errorMessage ?? "", /does not exist/);
  assert.equal(fs.existsSync(workDir), false);
});
