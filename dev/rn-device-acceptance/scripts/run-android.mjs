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

/** Start the host-only multi-thread Edge/Core fixture and retain it across the
 * seed/verify process boundary. The fixture emits fresh JWTs at runtime; this
 * driver passes them directly to the native Android activity and never writes
 * them to source, Gradle config, logs, or a receipt. */
async function startLocalEdgeSessionHarness() {
  const child = spawn(
    "cargo",
    ["run", "--quiet", "-p", "jazz-native-relay", "--example", "rn_edge_session_harness"],
    {
      cwd: resolve(import.meta.dirname, "../../.."),
      stdio: ["ignore", "pipe", "inherit"],
    },
  );
  let output = "";
  const session = await new Promise((resolveSession, rejectSession) => {
    const timeout = setTimeout(
      () => rejectSession(new Error("local Edge/Core harness timed out")),
      60_000,
    );
    child.stdout.on("data", (chunk) => {
      output += chunk;
      const line = output.split(/\r?\n/).find((item) => item.startsWith("JAZZ_RN_EDGE_SESSION "));
      if (!line) return;
      clearTimeout(timeout);
      try {
        resolveSession(JSON.parse(line.slice("JAZZ_RN_EDGE_SESSION ".length)));
      } catch (error) {
        rejectSession(error);
      }
    });
    child.once("error", (error) => {
      clearTimeout(timeout);
      rejectSession(error);
    });
    child.once("exit", (code) => {
      clearTimeout(timeout);
      rejectSession(
        new Error(`local Edge/Core harness exited before readiness (${code ?? "signal"})`),
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
const deviceIdentifier = adb(["shell", "getprop", "ro.build.fingerprint"]).trim();
const localSession = await startLocalEdgeSessionHarness();
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
