import { execFileSync, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { assertDeviceReceipt } from "./device-driver.mjs";
import { androidAcceptanceFailure } from "./android-diagnostics.mjs";
import { verifyAndroidRelayStage } from "./android-relay-stage.mjs";
import { scenariosForAcceptancePhase } from "../src/scenarios.ts";

const serial = process.env.ANDROID_SERIAL;
const apk = process.env.JAZZ_DEVICE_APK;
if (!apk) throw new Error("JAZZ_DEVICE_APK must point to the assembled development-build APK");
const relayRoot =
  process.env.JAZZ_DEVICE_RELAY_ROOT ?? resolve(import.meta.dirname, "../../../crates/jazz-rn");
verifyAndroidRelayStage({
  packageRoot: relayRoot,
  sourceRevision: process.env.JAZZ_DEVICE_RELAY_SOURCE_REVISION,
});
const adb = (args) =>
  execFileSync("adb", serial ? ["-s", serial, ...args] : args, { encoding: "utf8" });

const harnessRoot = resolve(import.meta.dirname, "../../..");
const harnessCargoArgs = ["-p", "jazz-native-relay", "--example", "rn_edge_session_harness"];

function boundedHarnessOutput(output) {
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

function harnessDiagnostic({ child, stdout, stderr, emulator }) {
  const exit = child.exitCode === null ? "running" : `code=${child.exitCode}`;
  const signal = child.signalCode ? ` signal=${child.signalCode}` : "";
  return [
    `pid=${child.pid ?? "unavailable"}`,
    `exit=${exit}${signal}`,
    `emulator=${emulator}`,
    `stdout=${JSON.stringify(boundedHarnessOutput(stdout))}`,
    `stderr=${JSON.stringify(boundedHarnessOutput(stderr))}`,
  ].join("; ");
}

/** Start the host-only multi-thread Edge/Core fixture and retain it across the
 * seed/verify process boundary. The fixture emits fresh JWTs at runtime; this
 * driver passes them directly to the native Android activity and never writes
 * them to source, Gradle config, logs, or a receipt. */
async function startLocalEdgeSessionHarness(emulator) {
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
  });
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
        new Error(`${reason}; ${harnessDiagnostic({ child, stdout, stderr, emulator })}`),
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
      const line = stdout.split(/\r?\n/).find((item) => item.startsWith("JAZZ_RN_EDGE_SESSION "));
      if (!line) return;
      try {
        succeed(JSON.parse(line.slice("JAZZ_RN_EDGE_SESSION ".length)));
      } catch (error) {
        fail(`local Edge/Core harness emitted invalid readiness JSON (${error.message})`);
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
    endpoint: `http://10.0.2.2:${session.edge_port}`,
    bearerA: session.bearer_a,
    bearerB: session.bearer_b,
  };
}
// The unfiltered emulator buffer is noisy enough to make `adb logcat -d`
// unreliable on hosted runners. Receipts come from React Native's console
// bridge; pre-receipt diagnostics use one native tag containing allowlisted
// codes only.
const acceptanceLogcat = () =>
  adb(["logcat", "-d", "-v", "threadtime", "ReactNativeJS:I", "JazzDeviceAcceptance:E", "*:S"]);
const startedAt = Date.now();
const runNonce = process.env.JAZZ_DEVICE_RUN_NONCE ?? randomUUID();
// Android IDs are scoped per app/signing identity, so use the immutable system
// build fingerprint that both trusted fixture code and adb observe identically.
const adbState = adb(["get-state"]).trim();
if (adbState !== "device")
  throw new Error(`Android emulator is not ready (adb state: ${adbState || "empty"})`);
const deviceIdentifier = adb(["shell", "getprop", "ro.build.fingerprint"]).trim();
const localSession = await startLocalEdgeSessionHarness(
  `serial=${serial ?? "default"}, adb-state=${adbState}, fingerprint=${deviceIdentifier}`,
);
process.once("exit", () => localSession.child.kill("SIGTERM"));
adb(["install", "-r", apk]);
const packagePath = adb(["shell", "pm", "path", "dev.jazz.rndeviceacceptance"])
  .trim()
  .replace(/^package:/, "");
if (!packagePath.startsWith("/"))
  throw new Error("Android package manager did not report an installed APK path");
const buildFingerprint = adb(["shell", "sha256sum", packagePath]).trim().split(/\s+/)[0];
if (!/^[0-9a-f]{64}$/i.test(buildFingerprint ?? ""))
  throw new Error("could not hash the installed Android package artifact");
async function launchAndAssert(phase) {
  const phaseStartedAt = Date.now();
  adb(["logcat", "-c"]);
  adb([
    "shell",
    "am",
    "start",
    "-n",
    "dev.jazz.rndeviceacceptance/.MainActivity",
    "--es",
    "jazzDeviceRunNonce",
    runNonce,
    "--es",
    "jazzDeviceAcceptancePhase",
    phase,
    "--es",
    "jazzDeviceEdgeEndpoint",
    localSession.endpoint,
    "--es",
    "jazzDeviceBearerA",
    localSession.bearerA,
    "--es",
    "jazzDeviceBearerB",
    localSession.bearerB,
  ]);
  const expected = {
    platform: "android",
    deviceIdentifier,
    buildFingerprint,
    runNonce,
    startedAt: phaseStartedAt,
    scenarios: scenariosForAcceptancePhase(phase)
      .filter((item) => item.state === "passed")
      .map((item) => item.scenario),
  };
  const deadline = Date.now() + 2 * 60 * 1000;
  let output = "";
  for (;;) {
    output = acceptanceLogcat();
    if (output.includes("JAZZ_DEVICE_RESULT ")) {
      try {
        return assertDeviceReceipt(output, expected);
      } catch {
        throw new Error(androidAcceptanceFailure("invalid-receipt", phase, output));
      }
    }
    if (Date.now() >= deadline) throw new Error(androidAcceptanceFailure("timeout", phase, output));
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
}

await launchAndAssert("seed");
// This must be a process boundary: no JSI alias or relay process can survive.
adb(["shell", "am", "force-stop", "dev.jazz.rndeviceacceptance"]);
await launchAndAssert("verify");
localSession.child.kill("SIGTERM");
