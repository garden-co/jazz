// Actual Rust -> JS -> transactionSync conformance, sync rejection and restart.
// Build with: node ../../scripts/prepare-durable-object-tests.mjs
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createHarness } from "./runtime.mjs";

const executable = fileURLToPath(
  new URL("../../../../target/debug/doc-shelf-do-ceiling-ws", import.meta.url),
);
const persist = await mkdtemp(join(tmpdir(), "jazz-do-scopes-"));
let mf;
async function run(verify) {
  mf = createHarness({ port: 0, durableObjectsPersist: persist });
  const url = new URL(await mf.ready);
  url.protocol = "ws:";
  const env = {
    ...process.env,
    SMOKE: "1",
    SCOPE_TEST: "1",
    STORE: "20",
    PROJECT: "20",
    SESSIONS: "2",
    GROUP: "2",
    ROWS: "2",
    WRITES: "2",
  };
  delete env.NATIVE;
  delete env.VERIFY_ONLY;
  delete env.REOPEN_CATALOGUE;
  if (verify) {
    env.VERIFY_ONLY = "1";
    if (process.argv.includes("--catalogue")) env.REOPEN_CATALOGUE = "1";
  }
  // Both objects live in the same workerd process and share the WASM module.
  // Identical fixture row IDs must remain private to each storage owner.
  for (const object of ["", "/isolated"]) {
    await new Promise((resolve, reject) => {
      const child = spawn(executable, [`${url.href.replace(/\/$/, "")}${object}`], {
        env,
        stdio: "inherit",
        timeout: 120_000,
      });
      child.once("error", reject);
      child.once("exit", (code, signal) => {
        try {
          assert.equal(code, 0, `scope client failed (${signal ?? code})`);
          resolve();
        } catch (error) {
          reject(error);
        }
      });
    });
  }
  await mf.dispose();
  mf = null;
}
try {
  await run(false);
  // A new workerd process/isolate and reconstructed object, with only SQLite
  // retained. Fresh replicas must see the successful writes, not rejected ones.
  await run(true);
  console.log("DO scope conformance, rejected sync batch, and persisted restart passed.");
} finally {
  await mf?.dispose();
  await rm(persist, { recursive: true, force: true });
}
