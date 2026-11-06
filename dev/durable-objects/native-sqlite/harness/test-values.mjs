// Actual Rust -> JS -> DO SQLite -> JS -> Rust typed values.
// Build first: FAST=1 ../build.sh --features scope-tests
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createHarness } from "./runtime.mjs";
const mf = createHarness({ port: 0 });
try {
  const response = await mf.dispatchFetch("http://localhost/value-checks");
  assert.equal(response.status, 200, await response.clone().text());
  assert.equal(await response.text(), "value checks passed");
  console.log("Durable Object Rust value bridge checks passed.");
  if (process.argv.includes("--sync")) {
    const persist = await mkdtemp(join(tmpdir(), "jazz-do-values-"));
    try {
      for (const verify of [false, true]) {
        const synced = createHarness({ port: 0, durableObjectsPersist: persist });
        try {
          const url = new URL(await synced.ready);
          url.protocol = "ws:";
          url.pathname = "/sync"; // Separate authority from the value fixtures.
          const env = { ...process.env, VALUE_TEST: "1" };
          delete env.NATIVE;
          delete env.SCOPE_TEST;
          delete env.VERIFY_ONLY;
          if (verify) env.VERIFY_ONLY = "1";
          await new Promise((resolve, reject) => {
            const child = spawn(
              fileURLToPath(
                new URL("../../../../target/debug/doc-shelf-do-ceiling-ws", import.meta.url),
              ),
              [url.href.replace(/\/$/, "")],
              { env, stdio: "inherit", timeout: 120_000 },
            );
            child.once("error", reject);
            child.once("exit", (code, signal) => {
              if (code === 0) resolve();
              else reject(new Error(`numeric sync failed: ${signal ?? code}`));
            });
          });
        } finally {
          await synced.dispose();
        }
      }
    } finally {
      await rm(persist, { recursive: true, force: true });
    }
    console.log("Numeric sync, rejection, and persisted restart assertions passed.");
  }
} finally {
  await mf.dispose();
}
