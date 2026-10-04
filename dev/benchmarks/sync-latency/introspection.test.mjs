import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

test("CodSpeed introspection exits before loading the SDK or starting native server threads", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-codspeed-introspection-"));
  const metadata = join(dir, "flags.json");
  try {
    const result = spawnSync(
      process.execPath,
      [
        fileURLToPath(new URL("./probe.mjs", import.meta.url)),
        "--codspeed",
        "--sdk-root",
        join(dir, "deliberately-missing-sdk"),
      ],
      {
        encoding: "utf8",
        timeout: 10_000,
        env: {
          ...process.env,
          CODSPEED_ENV: "local",
          CODSPEED_RUNNER_MODE: "walltime",
          __CODSPEED_NODE_CORE_INTROSPECTION_PATH__: metadata,
        },
      },
    );
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout, "");
    const { flags } = JSON.parse(await readFile(metadata, "utf8"));
    assert(flags.includes("--perf-prof"));
    assert(flags.includes("--interpreted-frames-native-stack"));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
