import { verifyAndroidReleaseNetworkPolicy } from "./android-network-policy.mjs";
import { startCoreObservationControl } from "./core-observation-control.mjs";
import { startLocalEdgeSessionHarness } from "./edge-session-harness.mjs";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { assertDeviceReceipt } from "./device-driver.mjs";
import { androidAcceptanceFailure } from "./android-diagnostics.mjs";
import { adb } from "./android-adb.mjs";
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
verifyAndroidReleaseNetworkPolicy(resolve(import.meta.dirname, "../android"));
const androidAdb = (args) => adb(args, { serial });

// The unfiltered emulator buffer is noisy enough to make `adb logcat -d`
// unreliable on hosted runners. Receipts come from React Native's console
// bridge; pre-receipt diagnostics use one native tag containing allowlisted
// codes only.
const acceptanceLogcat = () =>
  androidAdb([
    "logcat",
    "-d",
    "-v",
    "threadtime",
    "ReactNativeJS:I",
    "JazzDeviceAcceptance:E",
    "*:S",
  ]);
const startedAt = Date.now();
const runNonce = process.env.JAZZ_DEVICE_RUN_NONCE ?? randomUUID();
// Android IDs are scoped per app/signing identity, so use the immutable system
// build fingerprint that both trusted fixture code and adb observe identically.
const adbState = androidAdb(["get-state"]).trim();
if (adbState !== "device")
  throw new Error(`Android emulator is not ready (adb state: ${adbState || "empty"})`);
const deviceIdentifier = androidAdb(["shell", "getprop", "ro.build.fingerprint"]).trim();
const localSession = await startLocalEdgeSessionHarness({
  device: `serial=${serial ?? "default"}, adb-state=${adbState}, fingerprint=${deviceIdentifier}`,
  runNonce,
  host: "10.0.2.2",
});
process.once("exit", () => localSession.child.kill("SIGTERM"));
let control;
try {
  androidAdb(["install", "-r", apk]);
  const packagePath = androidAdb(["shell", "pm", "path", "dev.jazz.rndeviceacceptance"])
    .trim()
    .replace(/^package:/, "");
  if (!packagePath.startsWith("/"))
    throw new Error("Android package manager did not report an installed APK path");
  const buildFingerprint = androidAdb(["shell", "sha256sum", packagePath]).trim().split(/\s+/)[0];
  if (!/^[0-9a-f]{64}$/i.test(buildFingerprint ?? ""))
    throw new Error("could not hash the installed Android package artifact");
  control = await startCoreObservationControl({
    session: localSession,
    expected: { platform: "android", deviceIdentifier, buildFingerprint, runNonce },
    host: "10.0.2.2",
  });
  async function launchAndAssert(phase) {
    const phaseStartedAt = Date.now();
    androidAdb(["logcat", "-c"]);
    androidAdb([
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
      "jazzDeviceCoreObservationEndpoint",
      control.endpoint,
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
      if (Date.now() >= deadline)
        throw new Error(
          `${androidAcceptanceFailure("timeout", phase, output)}; Core observation control: ${control.diagnostic()}`,
        );
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
  }

  await launchAndAssert("seed");
  const coreObservation = await localSession.waitForCoreObservation();
  console.log(
    "JAZZ_DEVICE_CORE_RESULT " +
      JSON.stringify({
        platform: "android",
        deviceIdentifier: deviceIdentifier,
        buildFingerprint,
        ...coreObservation,
        observedAt: new Date().toISOString(),
      }),
  );
  // This must be a process boundary: no JSI alias or relay process can survive.
  androidAdb(["shell", "am", "force-stop", "dev.jazz.rndeviceacceptance"]);
  await localSession.stopForOfflineRestart();
  console.log(
    "JAZZ_DEVICE_REOPEN_PROVENANCE " +
      JSON.stringify({
        platform: "android",
        runNonce,
        upstream: "stopped-and-endpoint-refused",
        scopeEndpoint: "unchanged",
      }),
  );
  await launchAndAssert("verify");
} finally {
  localSession.child.kill("SIGTERM");
  await control?.close();
}
