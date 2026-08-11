import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");
const runner = path.join(root, "dev/gates/run-canonical.sh");

test("canonical gates do not advertise the removed jazz-server package gate", () => {
  const source = fs.readFileSync(runner, "utf8");
  assert.doesNotMatch(source, /cargo-test-jazz-server/);
  assert.doesNotMatch(source, /cargo test -p\s+jazz-server/);
});

test("the removed jazz-server gate is rejected as an unknown selector", () => {
  const result = spawnSync("bash", [runner, "--only", "cargo-test-jazz-server"], {
    cwd: root,
    encoding: "utf8",
  });
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}\n${result.stderr}`, /unknown gate id/);
});
