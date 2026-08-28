import { execFileSync } from "node:child_process";
import { assertDeviceReceipt } from "./device-driver.mjs";

const serial = process.env.ANDROID_SERIAL;
const apk = process.env.JAZZ_DEVICE_APK;
if (!apk) throw new Error("JAZZ_DEVICE_APK must point to the assembled development-build APK");
const adb = (args) => execFileSync("adb", serial ? ["-s", serial, ...args] : args, { encoding: "utf8" });
adb(["install", "-r", apk]);
adb(["logcat", "-c"]);
adb(["shell", "monkey", "-p", "dev.jazz.rndeviceacceptance", "1"]);
const output = adb(["logcat", "-d"]);
assertDeviceReceipt(output, "android");
