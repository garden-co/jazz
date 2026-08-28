import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { assertDeviceReceipt } from "./device-driver.mjs";
import { scenarioPlan } from "../src/scenarios.ts";

const serial = process.env.ANDROID_SERIAL;
const apk = process.env.JAZZ_DEVICE_APK;
if (!apk) throw new Error("JAZZ_DEVICE_APK must point to the assembled development-build APK");
const adb = (args) => execFileSync("adb", serial ? ["-s", serial, ...args] : args, { encoding: "utf8" });
const startedAt = Date.now();
const runNonce = process.env.JAZZ_DEVICE_RUN_NONCE ?? randomUUID();
const buildFingerprint = createHash("sha256").update(readFileSync(apk)).digest("hex");
const deviceIdentifier = (serial ?? adb(["get-serialno"])).trim();
adb(["install", "-r", apk]);
adb(["logcat", "-c"]);
adb(["shell", "am", "start", "-n", "dev.jazz.rndeviceacceptance/.MainActivity", "--es", "jazzDeviceRunNonce", runNonce, "--es", "jazzDeviceBuildFingerprint", buildFingerprint]);
const output = adb(["logcat", "-d"]);
assertDeviceReceipt(output, { platform: "android", deviceIdentifier, buildFingerprint, runNonce, startedAt, scenarios: scenarioPlan.map((item) => item.scenario) });
