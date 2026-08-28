import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { assertDeviceReceipt } from "./device-driver.mjs";
import { scenarioPlan } from "../src/scenarios.ts";

const serial = process.env.ANDROID_SERIAL;
const apk = process.env.JAZZ_DEVICE_APK;
if (!apk) throw new Error("JAZZ_DEVICE_APK must point to the assembled development-build APK");
const adb = (args) =>
  execFileSync("adb", serial ? ["-s", serial, ...args] : args, { encoding: "utf8" });
const startedAt = Date.now();
const runNonce = process.env.JAZZ_DEVICE_RUN_NONCE ?? randomUUID();
const buildFingerprint = createHash("sha256").update(readFileSync(apk)).digest("hex");
// Android secure ID is readable by the trusted fixture, unlike adb's transport
// serial. Bind both sides to that same stable, non-display identifier.
const deviceIdentifier = adb(["shell", "settings", "get", "secure", "android_id"]).trim();
adb(["install", "-r", apk]);
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
  "jazzDeviceBuildFingerprint",
  buildFingerprint,
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
