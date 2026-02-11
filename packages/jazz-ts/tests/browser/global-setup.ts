/**
 * Global setup for browser tests — spawns a real jazz-cli server.
 *
 * Pattern mirrors crates/jazz-cli/tests/test_server.rs:
 * - Spawn `jazz server` with known secrets on a fixed port
 * - Poll /health until ready
 * - Tear down on completion
 */

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID };

let serverProcess: ChildProcess | null = null;
let dataDir: string | null = null;

/** Poll /health until server responds (up to 10 seconds). */
async function waitForHealth(port: number): Promise<void> {
  const url = `http://127.0.0.1:${port}/health`;
  for (let i = 0; i < 100; i++) {
    try {
      const resp = await fetch(url);
      if (resp.ok) return;
    } catch {
      // Server not ready yet
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`Server failed to become ready on port ${port} within 10 seconds`);
}

export async function setup(): Promise<void> {
  dataDir = mkdtempSync(join(tmpdir(), "jazz-browser-test-"));

  // Path to the jazz binary (relative to this file → ../../../../target/debug/jazz)
  const jazzBinary = join(import.meta.dirname ?? __dirname, "../../../../target/debug/jazz");

  serverProcess = spawn(
    jazzBinary,
    ["server", APP_ID, "--port", TEST_PORT.toString(), "--data-dir", dataDir],
    {
      env: {
        ...process.env,
        JAZZ_JWT_SECRET: JWT_SECRET,
        JAZZ_ADMIN_SECRET: ADMIN_SECRET,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  serverProcess.stdout?.on("data", (data: Buffer) => {
    process.stdout.write(`[jazz-server] ${data}`);
  });
  serverProcess.stderr?.on("data", (data: Buffer) => {
    process.stderr.write(`[jazz-server] ${data}`);
  });

  await waitForHealth(TEST_PORT);
}

export async function teardown(): Promise<void> {
  if (serverProcess) {
    serverProcess.kill("SIGTERM");
    await new Promise<void>((resolve) => {
      serverProcess?.on("exit", () => resolve());
      setTimeout(resolve, 2000);
    });
    serverProcess = null;
  }

  if (dataDir) {
    try {
      rmSync(dataDir, { recursive: true, force: true });
    } catch {
      // Best effort cleanup
    }
    dataDir = null;
  }
}
