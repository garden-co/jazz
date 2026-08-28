import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { assertDeviceReceipt } from "./device-driver.mjs";
import { boundedDiagnostic, relevantAppLogs, sanitizedCommandFailure } from "./ios-diagnostics.mjs";
import { scenarioPlan } from "../src/scenarios.ts";

const udid = process.env.IOS_SIMULATOR_UDID;
const app = process.env.JAZZ_DEVICE_APP;
if (!udid || !app) throw new Error("IOS_SIMULATOR_UDID and JAZZ_DEVICE_APP are required");
const buildFingerprint =
  process.env.JAZZ_DEVICE_BUILD_FINGERPRINT ??
  createHash("sha256")
    .update(readFileSync(`${app}/JazzRNdeviceacceptance`))
    .digest("hex");
const simctl = (args) => execFileSync("xcrun", ["simctl", ...args], { encoding: "utf8" });
const trySimctl = (args) => {
  try {
    return boundedDiagnostic(simctl(args).trim());
  } catch (error) {
    return sanitizedCommandFailure(error);
  }
};
const startedAt = Date.now();
const runNonce = process.env.JAZZ_DEVICE_RUN_NONCE ?? randomUUID();
simctl(["bootstatus", udid, "-b"]);
simctl(["install", udid, app]);
const launchResult = simctl([
  "launch",
  udid,
  "dev.jazz.rndeviceacceptance",
  "-JazzDeviceRunNonce",
  runNonce,
  "-JazzDeviceBuildFingerprint",
  buildFingerprint,
  "-JazzDeviceDeviceIdentifier",
  udid,
]).trim();
if (!/^\d+$/.test(launchResult))
  throw new Error(`simctl launch did not return an app process id: ${launchResult}`);
const receiptOutput = () =>
  simctl([
    "spawn",
    udid,
    "log",
    "show",
    "--last",
    "2m",
    "--style",
    "compact",
    "--predicate",
    "eventMessage CONTAINS 'JAZZ_DEVICE_RESULT'",
  ]);
const expected = {
  platform: "ios",
  deviceIdentifier: udid,
  buildFingerprint,
  runNonce,
  startedAt,
  scenarios: scenarioPlan.filter((item) => item.state === "passed").map((item) => item.scenario),
};
const diagnostics = () =>
  [
    `simctl launch PID: ${launchResult}`,
    `app data container:\n${trySimctl(["get_app_container", udid, "dev.jazz.rndeviceacceptance", "data"])}`,
    `launchd app state:\n${trySimctl(["spawn", udid, "launchctl", "print", "gui/501"])}`,
    `recent app logs (capped):\n${relevantAppLogs(
      trySimctl([
        "spawn",
        udid,
        "log",
        "show",
        "--last",
        "3m",
        "--style",
        "compact",
        "--predicate",
        'process == "JazzRNdeviceacceptance"',
      ]),
      "JazzRNdeviceacceptance",
    )}`,
  ].join("\n\n");
for (let attempt = 0; attempt < 30; attempt += 1) {
  try {
    assertDeviceReceipt(receiptOutput(), expected);
    break;
  } catch (error) {
    if (attempt === 29) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`${detail}\n\n${diagnostics()}`);
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1000);
  }
}
