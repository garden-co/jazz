import { execFileSync, spawn } from "node:child_process";
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
const harnessCargoArgs = ["-p", "jazz-native-relay", "--example", "rn_edge_session_harness"];

export function boundedHarnessOutput(output) {
  // The one machine-readable line deliberately contains ephemeral bearer
  // material. It must remain readable by this process but never appear in a
  // failure diagnostic.
  return output
    .replace(/^JAZZ_RN_EDGE_SESSION .+$/gm, "JAZZ_RN_EDGE_SESSION [redacted]")
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

/** Start the host-only multi-thread Edge/Core fixture and retain it across the
 * seed/verify process boundary. The fixture emits fresh JWTs at runtime; this
 * driver passes them directly to the native fixture and never writes
 * them to source, Gradle config, logs, or a receipt. */
export async function startLocalEdgeSessionHarness({ device, runNonce, host }) {
  if (!isDeviceRunNonce(runNonce)) throw new Error("invalid device run nonce");
  // `cargo run` includes compilation. On a cold hosted runner that can take
  // longer than the service-readiness allowance and, with --quiet, produces no
  // marker at all. Make build failure explicit and reserve the timer for the
  // already-built process reaching a listening Edge/Core session.
  execFileSync("cargo", ["build", "--quiet", ...harnessCargoArgs], {
    cwd: harnessRoot,
    stdio: "inherit",
  });
  const child = spawn("cargo", ["run", "--quiet", ...harnessCargoArgs], {
    cwd: harnessRoot,
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, JAZZ_DEVICE_RUN_NONCE: runNonce },
  });
  let observation;
  let stdout = "";
  let stderr = "";
  const session = await new Promise((resolveSession, rejectSession) => {
    let settled = false;
    const fail = (reason) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      if (!child.killed) child.kill("SIGTERM");
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
      () => fail("local Edge/Core harness timed out waiting for readiness"),
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
        .find((item) => item.startsWith("JAZZ_RN_EDGE_SESSION "));
      if (!line) return;
      try {
        succeed(JSON.parse(line.slice("JAZZ_RN_EDGE_SESSION ".length)));
      } catch {
        fail("local Edge/Core harness emitted invalid readiness JSON");
      }
    });
    child.stderr.on("data", (chunk) => {
      stderr = retainHarnessOutput(stderr, chunk);
    });
    child.once("error", (error) => {
      fail(`could not spawn local Edge/Core harness (${error.message})`);
    });
    child.once("exit", (code, signal) => {
      fail(
        `local Edge/Core harness exited before readiness (code=${code ?? "none"}, signal=${signal ?? "none"})`,
      );
    });
  });
  if (
    !Number.isInteger(session.edge_port) ||
    session.edge_port < 1 ||
    session.edge_port > 65_535 ||
    typeof session.bearer_a !== "string" ||
    typeof session.bearer_b !== "string"
  ) {
    child.kill("SIGTERM");
    throw new Error("local Edge/Core harness emitted malformed session material");
  }
  return {
    child,
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
    endpoint: `http://${host}:${session.edge_port}`,
    bearerA: session.bearer_a,
    bearerB: session.bearer_b,
  };
}
