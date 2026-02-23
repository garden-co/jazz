/**
 * Global setup for browser tests — spawns a real jazz-tools server.
 *
 * Pattern mirrors crates/jazz-tools/tests/test_server.rs:
 * - Spawn `jazz-tools server` with known secrets on a fixed port
 * - Poll /health until ready
 * - Tear down on completion
 */

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID };

const HEALTH_POLL_INTERVAL_MS = 100;
const HEALTH_TIMEOUT_MS = 30_000;

let serverProcess: ChildProcess | null = null;
let dataDir: string | null = null;

async function isHealthy(port: number): Promise<boolean> {
  const url = `http://127.0.0.1:${port}/health`;
  try {
    const resp = await fetch(url);
    return resp.ok;
  } catch {
    return false;
  }
}

/** Poll /health until server responds. */
async function waitForHealth(port: number, timeoutMs = HEALTH_TIMEOUT_MS): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (await isHealthy(port)) {
      return;
    }
    await new Promise((r) => setTimeout(r, HEALTH_POLL_INTERVAL_MS));
  }

  throw new Error(
    `Server failed to become ready on port ${port} within ${Math.ceil(timeoutMs / 1000)} seconds`,
  );
}

export async function setup(): Promise<void> {
  // Vitest browser runs can invoke global setup multiple times.
  // Reuse an existing healthy process instead of racing to bind the same port.
  if (await isHealthy(TEST_PORT)) {
    return;
  }

  dataDir = mkdtempSync(join(tmpdir(), "jazz-browser-test-"));

  // Path to the jazz binary (relative to this file → ../../../../target/debug/jazz-tools)
  const jazzBinary = join(import.meta.dirname ?? __dirname, "../../../../target/debug/jazz-tools");

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

  try {
    await waitForHealth(TEST_PORT);
  } catch (error) {
    // Another concurrent setup may have started the server first.
    if (await isHealthy(TEST_PORT)) {
      return;
    }
    throw error;
  }
}

export async function teardown(): Promise<void> {
  if (serverProcess) {
    try {
      serverProcess.kill("SIGTERM");
    } catch {
      // Process may already be gone.
    }

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
