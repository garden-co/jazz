import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { seal, verify } from "./artifact.mjs";

test("Node benchmark handoff rejects different sources, runs, runtimes and changed bytes", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-node-bench-artifact-"));
  const archive = join(dir, "runtime.tgz");
  const identity = {
    source: "head",
    run: "123",
    node: "v24.13.0",
    arch: "arm64",
    nativeFeatures: "default,mimalloc-safe/no_opt_arch",
  };
  try {
    await writeFile(archive, "compiled SDK and native runtime");
    await seal(archive, identity);
    await verify(archive, identity);
    for (const field of Object.keys(identity)) {
      await assert.rejects(
        verify(archive, { ...identity, [field]: "different" }),
        /identity differs/,
      );
    }
    await writeFile(archive, "stale or damaged native runtime");
    await assert.rejects(verify(archive, identity), /archive changed/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
