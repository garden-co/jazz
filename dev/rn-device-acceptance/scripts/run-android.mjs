import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { assertDeviceReceipt } from "./device-driver.mjs";
import { verifyAndroidRelayStage } from "./android-relay-stage.mjs";
import { scenarioPlan } from "../src/scenarios.ts";

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
]);
const expected = {
  platform: "android",
  deviceIdentifier,
  buildFingerprint,
  runNonce,
  startedAt,
  scenarios: scenarioPlan.filter((item) => item.state === "passed").map((item) => item.scenario),
};
const deadline = Date.now() + 2 * 60 * 1000;
for (;;) {
  const output = adb(["logcat", "-d"]);
  if (output.includes("JAZZ_DEVICE_RESULT ")) {
    assertDeviceReceipt(output, expected);
    break;
  }
  if (Date.now() >= deadline)
    throw new Error("Timed out waiting for a JAZZ_DEVICE_RESULT from the launched Android app");
  await new Promise((resolve) => setTimeout(resolve, 1_000));
}
