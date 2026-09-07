import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const packageDir = fileURLToPath(new URL("..", import.meta.url));

function runConsumer(source) {
  const result = spawnSync(process.execPath, ["--input-type=module", "--eval", source], {
    cwd: packageDir,
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.error?.message ?? result.stderr);
}

test("Classic co and z named imports lead to migration guidance, not a linking error", () => {
  runConsumer(`
    import assert from "node:assert/strict";
    import { co, z } from "jazz-tools";

    assert.throws(() => co.map({ title: z.string() }), error => {
      assert.equal(error.code, "JAZZ_CLASSIC_API_REMOVED");
      assert.match(error.message, /co.map/);
      assert.match(error.message, /Jazz Classic/);
      assert.match(error.message, /Jazz 2/);
      assert.ok(error.message.includes("node_modules/jazz-tools/README.md"));
      assert.ok(error.message.includes("https://jazz.tools/llms-full.txt"));
      return true;
    });
  `);
});
