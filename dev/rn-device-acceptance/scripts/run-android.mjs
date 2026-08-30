import { execFileSync } from "node:child_process";
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
// The unfiltered emulator buffer is noisy enough to make `adb logcat -d`
// unreliable on hosted runners. Receipts come from React Native's console
// bridge; pre-receipt diagnostics use one native tag containing allowlisted
// codes only.
const acceptanceLogcat = () =>
  adb([
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
const deviceIdentifier = adb(["shell", "getprop", "ro.build.fingerprint"]).trim();
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
      throw new Error(androidAcceptanceFailure("timeout", phase, output));
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
}

await launchAndAssert("seed");
// This must be a process boundary: no JSI alias or relay process can survive.
adb(["shell", "am", "force-stop", "dev.jazz.rndeviceacceptance"]);
await launchAndAssert("verify");
