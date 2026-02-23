/**
 * Global setup for browser tests — reuses or spawns a real jazz-tools server.
 *
 * Pattern mirrors crates/jazz-tools/tests/test_server.rs:
 * - Reuse an already-healthy server on a fixed port when available
 * - Otherwise spawn `jazz-tools server` with known secrets
 * - Poll /health until ready
 * - Tear down on completion
 */

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID };

const HEALTH_TIMEOUT_MS = 10_000;
const HEALTH_POLL_INTERVAL_MS = 100;
const HEALTH_REQUEST_TIMEOUT_MS = 400;
const TEARDOWN_TIMEOUT_MS = 2_000;
const STDERR_TAIL_MAX_CHARS = 8_000;
const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;
const HEALTH_URL = `${SERVER_URL}/health`;
const BINARY_PATH = join(
  fileURLToPath(new URL(".", import.meta.url)),
  "../../../../target/debug/jazz-tools",
);

let ownedServerProcess: ChildProcess | null = null;
let ownedDataDir: string | null = null;
let setupInFlight: Promise<void> | null = null;
let teardownInFlight: Promise<void> | null = null;

interface ExitInfo {
  code: number | null;
  signal: NodeJS.Signals | null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function appendTail(existing: string, chunk: Buffer | string): string {
  const next = existing + chunk.toString();
  if (next.length <= STDERR_TAIL_MAX_CHARS) {
    return next;
  }

  return next.slice(next.length - STDERR_TAIL_MAX_CHARS);
}

function cleanupDataDir(dir: string): void {
  try {
    rmSync(dir, { recursive: true, force: true });
  } catch {
    // Best effort cleanup
  }
}

async function probeHealth(): Promise<boolean> {
  try {
    const response = await fetch(HEALTH_URL, {
      signal: AbortSignal.timeout(HEALTH_REQUEST_TIMEOUT_MS),
    });
    return response.ok;
  } catch {
    return false;
  }
}

async function waitForHealth(timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (await probeHealth()) {
      return true;
    }
    await sleep(HEALTH_POLL_INTERVAL_MS);
  }

  return false;
}

async function stopProcess(processToStop: ChildProcess): Promise<void> {
  if (processToStop.exitCode !== null || processToStop.signalCode !== null) {
    return;
  }

  if (!processToStop.pid) {
    return;
  }

  try {
    processToStop.kill("SIGTERM");
  } catch {
    return;
  }

  await new Promise<void>((resolve) => {
    const onExit = () => {
      clearTimeout(timeout);
      resolve();
    };
    const timeout = setTimeout(() => {
      processToStop.off("exit", onExit);
      resolve();
    }, TEARDOWN_TIMEOUT_MS);
    processToStop.once("exit", onExit);
  });
}

async function setupInternal(): Promise<void> {
  // Short-circuit if another setup/process already has a healthy server.
  if (await probeHealth()) {
    return;
  }

  const candidateDataDir = mkdtempSync(join(tmpdir(), "jazz-browser-test-"));
  let stderrTail = "";
  let exited: ExitInfo | null = null;
  let spawnError: Error | null = null;

  const candidateProcess = spawn(
    BINARY_PATH,
    ["server", APP_ID, "--port", TEST_PORT.toString(), "--data-dir", candidateDataDir],
    {
      env: {
        ...process.env,
        JAZZ_JWT_SECRET: JWT_SECRET,
        JAZZ_ADMIN_SECRET: ADMIN_SECRET,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  candidateProcess.stdout?.on("data", (data: Buffer | string) => {
    process.stdout.write(`[jazz-server] ${data}`);
  });
  candidateProcess.stderr?.on("data", (data: Buffer | string) => {
    stderrTail = appendTail(stderrTail, data);
    process.stderr.write(`[jazz-server] ${data}`);
  });
  candidateProcess.once("exit", (code, signal) => {
    exited = { code, signal };
  });
  candidateProcess.once("error", (error) => {
    spawnError = error;
  });

  const deadline = Date.now() + HEALTH_TIMEOUT_MS;

  while (Date.now() < deadline) {
    if (await probeHealth()) {
      // If this process exited, another competitor won the race for the port.
      if (spawnError || exited) {
        cleanupDataDir(candidateDataDir);
        return;
      }

      ownedServerProcess = candidateProcess;
      ownedDataDir = candidateDataDir;
      return;
    }

    if (spawnError || exited) {
      break;
    }

    await sleep(HEALTH_POLL_INTERVAL_MS);
  }

  // Port-race path: our process exited, but another process might still be booting.
  const remainingMs = Math.max(0, deadline - Date.now());
  if ((spawnError || exited) && remainingMs > 0 && (await waitForHealth(remainingMs))) {
    cleanupDataDir(candidateDataDir);
    return;
  }

  await stopProcess(candidateProcess);
  cleanupDataDir(candidateDataDir);

  const exitDetails = exited
    ? `Process exited before health check passed (code=${exited.code ?? "null"}, signal=${exited.signal ?? "null"}).`
    : "Process did not report healthy before timeout.";
  const errorDetails = spawnError ? `Spawn error: ${spawnError.message}` : undefined;
  const stderrDetails = stderrTail.trim()
    ? `stderr (tail):\n${stderrTail.trimEnd()}`
    : "stderr (tail): <empty>";

  throw new Error(
    [
      `Failed to start browser test server on ${SERVER_URL} within ${HEALTH_TIMEOUT_MS}ms.`,
      `Checked endpoint: ${HEALTH_URL}`,
      `Binary path: ${BINARY_PATH}`,
      exitDetails,
      errorDetails,
      stderrDetails,
      "Action: verify the binary exists, inspect logs above, and check for non-test processes holding the test port.",
    ]
      .filter((line): line is string => Boolean(line))
      .join("\n"),
  );
}

async function teardownInternal(): Promise<void> {
  const processToStop = ownedServerProcess;
  const dirToCleanup = ownedDataDir;

  ownedServerProcess = null;
  ownedDataDir = null;

  if (processToStop) {
    await stopProcess(processToStop);
  }

  if (dirToCleanup) {
    cleanupDataDir(dirToCleanup);
  }
}

export async function setup(): Promise<void> {
  if (!setupInFlight) {
    setupInFlight = setupInternal();
  }

  try {
    await setupInFlight;
  } finally {
    setupInFlight = null;
  }
}

export async function teardown(): Promise<void> {
  if (setupInFlight) {
    try {
      await setupInFlight;
    } catch {
      // setup() already surfaced the error and handled cleanup for failed attempts.
    }
  }

  if (!teardownInFlight) {
    teardownInFlight = teardownInternal();
  }

  try {
    await teardownInFlight;
  } finally {
    teardownInFlight = null;
  }
}
