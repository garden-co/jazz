import { execFileSync } from "node:child_process";
import { assertDeviceReceipt } from "./device-driver.mjs";

const udid = process.env.IOS_SIMULATOR_UDID;
const app = process.env.JAZZ_DEVICE_APP;
if (!udid || !app) throw new Error("IOS_SIMULATOR_UDID and JAZZ_DEVICE_APP are required");
const simctl = (args) => execFileSync("xcrun", ["simctl", ...args], { encoding: "utf8" });
simctl(["bootstatus", udid, "-b"]);
simctl(["install", udid, app]);
simctl(["launch", udid, "dev.jazz.rndeviceacceptance"]);
const output = simctl(["spawn", udid, "log", "show", "--last", "2m", "--style", "compact", "--predicate", "eventMessage CONTAINS 'JAZZ_DEVICE_RESULT'"]);
assertDeviceReceipt(output, "ios");
