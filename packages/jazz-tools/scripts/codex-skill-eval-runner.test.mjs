import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import test from "node:test";

const runnerPath = fileURLToPath(new URL("./codex-skill-eval-runner.mjs", import.meta.url));

test("disables multi-agent execution for blind evaluation calls", () => {
  const tempDir = mkdtempSync(join(tmpdir(), "jazz-skill-eval-runner-test-"));
  const argsPath = join(tempDir, "args.json");
  const fakeCodexPath = join(tempDir, "fake-codex.mjs");

  try {
    writeFileSync(
      fakeCodexPath,
      `#!/usr/bin/env node
import { writeFileSync } from "node:fs";

const args = process.argv.slice(2);
if (args[0] === "--version") {
  process.stdout.write("codex-cli test");
  process.exit(0);
}

writeFileSync(process.env.FAKE_CODEX_ARGS_PATH, JSON.stringify(args));
const outputIndex = args.indexOf("--output-last-message");
writeFileSync(args[outputIndex + 1], JSON.stringify({ answer: "ok" }));
`,
    );
    chmodSync(fakeCodexPath, 0o755);

    const result = spawnSync(process.execPath, [runnerPath], {
      encoding: "utf8",
      env: {
        ...process.env,
        CODEX_BIN: fakeCodexPath,
        FAKE_CODEX_ARGS_PATH: argsPath,
      },
      input: JSON.stringify({
        prompt: "Evaluate one case.",
        schema: {
          type: "object",
          properties: { answer: { type: "string" } },
          required: ["answer"],
          additionalProperties: false,
        },
      }),
    });

    assert.equal(result.status, 0, result.stderr);
    const args = JSON.parse(readFileSync(argsPath, "utf8"));
    const disableIndex = args.indexOf("--disable");
    assert.notEqual(disableIndex, -1, `missing --disable in ${JSON.stringify(args)}`);
    assert.equal(args[disableIndex + 1], "multi_agent");
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
});
