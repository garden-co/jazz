import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { assertDeviceReceipt } from "./device-driver.mjs";
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
const startedAt = Date.now();
const runNonce = process.env.JAZZ_DEVICE_RUN_NONCE ?? randomUUID();
simctl(["bootstatus", udid, "-b"]);
simctl(["install", udid, app]);
simctl([
  "launch",
  udid,
  "dev.jazz.rndeviceacceptance",
  "-JazzDeviceRunNonce",
  runNonce,
  "-JazzDeviceBuildFingerprint",
  buildFingerprint,
]);
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
  scenarios: scenarioPlan.map((item) => item.scenario),
};
for (let attempt = 0; attempt < 30; attempt += 1) {
  try {
    assertDeviceReceipt(receiptOutput(), expected);
    break;
  } catch (error) {
    if (attempt === 29) throw error;
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1000);
  }
}
