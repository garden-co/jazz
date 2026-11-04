import { execFileSync, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createConnection } from "node:net";
import { resolve } from "node:path";
import { isDeviceRunNonce, persistedTitleForRun } from "../src/run-marker.ts";

export function assertCoreObservation(observation, runNonce) {
  if (
    !isDeviceRunNonce(runNonce) ||
    observation?.source !== "core" ||
    observation.runNonce !== runNonce ||
    observation.title !== persistedTitleForRun(runNonce) ||
    typeof observation.rowId !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(observation.rowId)
  ) {
    throw new Error("invalid run-bound Core observation");
  }
  return observation;
}

const harnessRoot = resolve(import.meta.dirname, "../../..");
const harnessCargoArgs = ["-p", "jazz-native-relay", "--example", "rn_server_session_harness"];
const deviceBackendScript = resolve(import.meta.dirname, "device-backend.mjs");

export function boundedHarnessOutput(output) {
  // Keep readiness/control lines bounded and private to the driver. Retain
  // token redaction defensively for any authentication diagnostics upstream.
  return output
    .replace(/^JAZZ_RN_SERVER_SESSION .+$/gm, "JAZZ_RN_SERVER_SESSION [redacted]")
    .replace(/\b[0-9a-f]{64}\b/g, "[redacted-secret]")
    .replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g, "[redacted-token]")
    .slice(-4_096);
}

function retainHarnessOutput(output, chunk) {
  // Keep enough for a split readiness line and diagnostic context without
  // allowing a wedged child to grow the device driver indefinitely.
  return `${output}${chunk}`.slice(-8_192);
}

function harnessDiagnostic({ child, stdout, stderr, device }) {
  const exit = child.exitCode === null ? "running" : `code=${child.exitCode}`;
  const signal = child.signalCode ? ` signal=${child.signalCode}` : "";
  return [
    `pid=${child.pid ?? "unavailable"}`,
    `exit=${exit}${signal}`,
    `device=${device}`,
    `stdout=${JSON.stringify(boundedHarnessOutput(stdout))}`,
    `stderr=${JSON.stringify(boundedHarnessOutput(stderr))}`,
  ].join("; ");
}

/** Start the host-only multi-thread Core fixture for the seed phase; stop
 * it before offline verification. The fixture emits fresh JWTs at runtime; this
 * driver passes them directly to the native fixture and never writes
 * them to source, Gradle config, logs, or a receipt. */
export async function startLocalServerSessionHarness({
  device,
  runNonce,
  host,
  backendScript = deviceBackendScript,
}) {
  if (!isDeviceRunNonce(runNonce)) throw new Error("invalid device run nonce");
  // `cargo run` includes compilation. On a cold hosted runner that can take
  // longer than the service-readiness allowance and, with --quiet, produces no
  // marker at all. Make build failure explicit and reserve the timer for the
  // already-built process reaching a listening Core session.
  execFileSync("cargo", ["build", "--quiet", ...harnessCargoArgs], {
    cwd: harnessRoot,
    stdio: "inherit",
  });
  // Core and the app's backend share this run's admin secret; neither the
  // device nor any receipt ever sees it.
  const adminSecret = randomBytes(32).toString("hex");
  const child = spawn("cargo", ["run", "--quiet", ...harnessCargoArgs], {
    cwd: harnessRoot,
    stdio: ["pipe", "pipe", "pipe"],
    detached: process.platform !== "win32",
    env: { ...process.env, JAZZ_DEVICE_RUN_NONCE: runNonce, JAZZ_DEVICE_ADMIN_SECRET: adminSecret },
  });
  let observation;
  let stdout = "";
  let stderr = "";
  const waitForLine = (prefix, timeoutMs = 15_000) =>
    new Promise((resolve, reject) => {
      let settled = false;
      const finish = (run) => {
        if (!settled) {
          settled = true;
          clearTimeout(deadline);
          run();
        }
      };
      const deadline = setTimeout(
        () => finish(() => reject(new Error(`harness did not emit ${prefix}`))),
        timeoutMs,
      );
      const poll = () => {
        if (settled) return;
        const line = stdout.split(/\r?\n/).find((item) => item.startsWith(prefix));
        if (line) finish(() => resolve(line));
        else if (child.exitCode !== null || child.signalCode)
          finish(() => reject(new Error(`harness exited before ${prefix}`)));
        else setTimeout(poll, 20);
      };
      poll();
    });
  const session = await new Promise((resolveSession, rejectSession) => {
    let settled = false;
    const fail = (reason) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      void terminateHarness(child).catch(() => {});
      rejectSession(
        new Error(`${reason}; ${harnessDiagnostic({ child, stdout, stderr, device })}`),
      );
    };
    const succeed = (value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      resolveSession(value);
    };
    const timeout = setTimeout(
      () => fail("local Core harness timed out waiting for readiness"),
      60_000,
    );
    child.stdout.on("data", (chunk) => {
      stdout = retainHarnessOutput(stdout, chunk);
      const observedLine = stdout
        .split(/\r?\n/)
        .slice(0, -1)
        .find((item) => item.startsWith("JAZZ_RN_CORE_OBSERVATION "));
      if (observedLine) {
        try {
          observation = JSON.parse(observedLine.slice("JAZZ_RN_CORE_OBSERVATION ".length));
        } catch {
          /* A partial line is retried on the next stdout chunk. */
        }
      }
      const line = stdout
        .split(/\r?\n/)
        .slice(0, -1)
        .find((item) => item.startsWith("JAZZ_RN_SERVER_SESSION "));
      if (!line) return;
      try {
        succeed(JSON.parse(line.slice("JAZZ_RN_SERVER_SESSION ".length)));
      } catch {
        fail("local Core harness emitted invalid readiness JSON");
      }
    });
    child.stderr.on("data", (chunk) => {
      stderr = retainHarnessOutput(stderr, chunk);
    });
    child.once("error", (error) => {
      fail(`could not spawn local Core harness (${error.message})`);
    });
    child.once("exit", (code, signal) => {
      fail(
        `local Core harness exited before readiness (code=${code ?? "none"}, signal=${signal ?? "none"})`,
      );
    });
  });
  if (
    Object.keys(session).some((key) => key !== "server_port" && key !== "app_id") ||
    !isPort(session.server_port) ||
    typeof session.app_id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(session.app_id)
  ) {
    await terminateHarness(child);
    throw new Error("local Core harness emitted malformed session material");
  }
  let backend;
  try {
    backend = await startDeviceBackend({
      coreUrl: `http://127.0.0.1:${session.server_port}`,
      appId: session.app_id,
      adminSecret,
      device,
      script: backendScript,
    });
  } catch (error) {
    await terminateHarness(child);
    throw error;
  }
  const terminateAll = async () => {
    await terminateHarness(backend.child);
    await terminateHarness(child);
  };
  return {
    child,
    backend: backend.child,
    async waitForCoreObservation(timeoutMs = 60_000) {
      const deadline = Date.now() + timeoutMs;
      while (!observation) {
        if (child.exitCode !== null || child.signalCode || Date.now() >= deadline) {
          throw new Error(
            `missing run-bound Core observation; ${harnessDiagnostic({ child, stdout, stderr, device })}`,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      return assertCoreObservation(observation, runNonce);
    },
    // Offline means no upstream at all: the backend stops with Core.
    async stopForOfflineRestart() {
      await stopForOfflineRestart(backend.child, backend.port);
      await stopForOfflineRestart(child, session.server_port);
    },
    terminate: terminateAll,
    async interruptAndRecover() {
      child.stdin.write("interrupt-server\n");
      await waitForLine("JAZZ_RN_SERVER_INTERRUPTED ");
      await assertEndpointRefused(session.server_port);
      child.stdin.write("recover-server\n");
      await waitForLine("JAZZ_RN_SERVER_RECOVERED ");
    },
    // The device reaches Jazz through the app's backend, never Core.
    endpoint: `http://${host}:${backend.port}${backend.basePath}`,
  };
}

function isPort(value) {
  return Number.isInteger(value) && value >= 1 && value <= 65_535;
}

/** Start the app's backend in front of Core and wait for its listening port. */
export async function startDeviceBackend({
  coreUrl,
  appId,
  adminSecret,
  device,
  script = deviceBackendScript,
}) {
  const child = spawn(process.execPath, ["--experimental-strip-types", script], {
    cwd: resolve(import.meta.dirname, ".."),
    stdio: ["pipe", "pipe", "pipe"],
    detached: process.platform !== "win32",
    env: {
      ...process.env,
      JAZZ_DEVICE_CORE_URL: coreUrl,
      JAZZ_DEVICE_APP_ID: appId,
      JAZZ_DEVICE_ADMIN_SECRET: adminSecret,
    },
  });
  let stdout = "";
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr = retainHarnessOutput(stderr, chunk);
  });
  const ready = await new Promise((resolveReady, rejectReady) => {
    let settled = false;
    const fail = (reason) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      void terminateHarness(child).catch(() => {});
      rejectReady(new Error(`${reason}; ${harnessDiagnostic({ child, stdout, stderr, device })}`));
    };
    const timeout = setTimeout(
      () => fail("device backend timed out waiting for readiness"),
      60_000,
    );
    child.stdout.on("data", (chunk) => {
      stdout = retainHarnessOutput(stdout, chunk);
      const line = stdout
        .split(/\r?\n/)
        .slice(0, -1)
        .find((item) => item.startsWith("JAZZ_RN_DEVICE_BACKEND "));
      if (!line || settled) return;
      try {
        const value = JSON.parse(line.slice("JAZZ_RN_DEVICE_BACKEND ".length));
        if (
          !isPort(value.port) ||
          typeof value.basePath !== "string" ||
          !value.basePath.startsWith("/")
        )
          return fail("device backend emitted malformed readiness");
        settled = true;
        clearTimeout(timeout);
        resolveReady(value);
      } catch {
        fail("device backend emitted invalid readiness JSON");
      }
    });
    child.once("error", (error) => fail(`could not spawn device backend (${error.message})`));
    child.once("exit", (code, signal) =>
      fail(
        `device backend exited before readiness (code=${code ?? "none"}, signal=${signal ?? "none"})`,
      ),
    );
  });
  return { child, port: ready.port, basePath: ready.basePath };
}

/** Cargo can be a parent of the actual fixture. Close its control reader and
 * terminate the whole Unix process group, escalating if it does not exit. */
export async function terminateHarness(child, timeoutMs = 5_000, processInfo = process) {
  const groupAlive = () => {
    if (processInfo.platform === "win32" || !child.pid)
      return child.exitCode === null && !child.signalCode;
    try {
      processInfo.kill(-child.pid, 0);
      return true;
    } catch (error) {
      // Permission still proves a process group exists; only ESRCH proves it
      // disappeared. Do not confuse Cargo's parent exit with group cleanup.
      if (error?.code === "ESRCH") return false;
      if (error?.code === "EPERM") return true;
      throw error;
    }
  };
  if (!groupAlive()) return;
  child.stdin?.end();
  const signal = (name) => {
    try {
      if (processInfo.platform !== "win32" && child.pid) processInfo.kill(-child.pid, name);
      else child.kill(name);
    } catch (error) {
      if (error?.code !== "ESRCH") throw error;
    }
  };
  const waitForGroupExit = async (ms) => {
    const deadline = Date.now() + ms;
    while (groupAlive()) {
      if (Date.now() >= deadline) return false;
      await new Promise((resolve) => setTimeout(resolve, Math.min(25, deadline - Date.now())));
    }
    return true;
  };
  signal("SIGTERM");
  if (!(await waitForGroupExit(timeoutMs))) {
    signal("SIGKILL");
    if (!(await waitForGroupExit(timeoutMs))) {
      throw new Error("local Core harness process group survived SIGKILL");
    }
  }
}

function assertEndpointRefused(port) {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      reject(new Error("interrupted server remained reachable"));
    });
    socket.once("error", (error) => {
      socket.destroy();
      error.code === "ECONNREFUSED" ? resolve() : reject(error);
    });
    socket.setTimeout(1_000, () => {
      socket.destroy();
      reject(new Error("interrupted server refusal timed out"));
    });
  });
}

/** Fail closed: a stopped process alone is insufficient if a descendant still
 * serves Core. Preserve the original endpoint for the native SQLite scope. */
export async function stopForOfflineRestart(child, port, terminate = terminateHarness) {
  await terminate(child);
  await new Promise((resolve, reject) => {
    const socket = createConnection({ host: "127.0.0.1", port });
    socket.setTimeout(1_000);
    socket.once("connect", () => {
      socket.destroy();
      reject(new Error("offline restart refused: upstream remains reachable"));
    });
    socket.once("error", (error) => {
      socket.destroy();
      if (error.code === "ECONNREFUSED") resolve();
      else reject(new Error("offline restart endpoint refusal was not established"));
    });
    socket.once("timeout", () => {
      socket.destroy();
      reject(new Error("offline restart endpoint probe timed out"));
    });
  });
}
