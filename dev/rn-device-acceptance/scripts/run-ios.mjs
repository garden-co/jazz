import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { assertDeviceReceipt } from "./device-driver.mjs";
import { scenarioPlan } from "../src/scenarios.ts";

const udid = process.env.IOS_SIMULATOR_UDID;
const app = process.env.JAZZ_DEVICE_APP;
const buildFingerprint = process.env.JAZZ_DEVICE_BUILD_FINGERPRINT;
if (!udid || !app || !buildFingerprint) throw new Error("IOS_SIMULATOR_UDID, JAZZ_DEVICE_APP, and JAZZ_DEVICE_BUILD_FINGERPRINT are required");
const simctl = (args) => execFileSync("xcrun", ["simctl", ...args], { encoding: "utf8" });
const startedAt = Date.now();
const runNonce = process.env.JAZZ_DEVICE_RUN_NONCE ?? randomUUID();
simctl(["bootstatus", udid, "-b"]);
simctl(["install", udid, app]);
simctl(["launch", udid, "dev.jazz.rndeviceacceptance", "-JazzDeviceRunNonce", runNonce, "-JazzDeviceBuildFingerprint", buildFingerprint]);
const output = simctl(["spawn", udid, "log", "show", "--last", "2m", "--style", "compact", "--predicate", "eventMessage CONTAINS 'JAZZ_DEVICE_RESULT'"]);
assertDeviceReceipt(output, { platform: "ios", deviceIdentifier: udid, buildFingerprint, runNonce, startedAt, scenarios: scenarioPlan.map((item) => item.scenario) });
