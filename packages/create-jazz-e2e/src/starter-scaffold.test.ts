import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { scaffoldStarter } from "./starter-scaffold.js";

const repoRoot = path.resolve(import.meta.dirname, "..", "..", "..");

function tempTarget(t: test.TestContext): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "cje2e-scaffold-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  return path.join(root, "app");
}

test("scaffolds a starter with concrete versions and a Better Auth secret", async (t) => {
  const targetDir = tempTarget(t);
  await scaffoldStarter({ repoRoot, starter: "ts-betterauth", appName: "my-app", targetDir });

  const manifest = JSON.parse(fs.readFileSync(path.join(targetDir, "package.json"), "utf8"));
  assert.equal(manifest.name, "my-app");
  const ranges = { ...manifest.dependencies, ...manifest.devDependencies };
  for (const [name, range] of Object.entries(ranges)) {
    assert.doesNotMatch(String(range), /^(workspace|catalog):/, `${name} kept ${range}`);
  }
  const jazzTools = JSON.parse(
    fs.readFileSync(path.join(repoRoot, "packages/jazz-tools/package.json"), "utf8"),
  );
  assert.equal(manifest.dependencies["jazz-tools"], `^${jazzTools.version}`);
  const env = fs.readFileSync(path.join(targetDir, ".env"), "utf8");
  assert.match(env, /^BETTER_AUTH_SECRET=\S+\n$/);
  assert.equal(fs.existsSync(path.join(targetDir, "node_modules")), false);
});

test("writes no .env for local-first starters", async (t) => {
  const targetDir = tempTarget(t);
  await scaffoldStarter({ repoRoot, starter: "ts-localfirst", appName: "my-app", targetDir });
  assert.equal(fs.existsSync(path.join(targetDir, ".env")), false);
});

test("rejects unknown starters and existing target dirs", async (t) => {
  const targetDir = tempTarget(t);
  await assert.rejects(
    scaffoldStarter({ repoRoot, starter: "nope", appName: "my-app", targetDir }),
    /Unknown starter "nope"/,
  );
  fs.mkdirSync(targetDir);
  await assert.rejects(
    scaffoldStarter({ repoRoot, starter: "ts-localfirst", appName: "my-app", targetDir }),
    /already exists/,
  );
});
