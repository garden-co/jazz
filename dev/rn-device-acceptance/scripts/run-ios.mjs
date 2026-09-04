import { startCoreObservationControl } from "./core-observation-control.mjs";
import { boundedHarnessOutput, startLocalEdgeSessionHarness } from "./edge-session-harness.mjs";
import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { assertDeviceReceipt } from "./device-driver.mjs";
import {
  boundedDiagnostic,
  parseLaunchProcessId,
  relevantAppLogs,
  safeDeviceDiagnostic,
  sanitizedCommandFailure,
} from "./ios-diagnostics.mjs";
import { scenariosForAcceptancePhase } from "../src/scenarios.ts";

const udid = process.env.IOS_SIMULATOR_UDID;
const app = process.env.JAZZ_DEVICE_APP;
if (!udid || !app) throw new Error("IOS_SIMULATOR_UDID and JAZZ_DEVICE_APP are required");
const buildFingerprint =
  process.env.JAZZ_DEVICE_BUILD_FINGERPRINT ??
  createHash("sha256")
    .update(readFileSync(`${app}/JazzRNdeviceacceptance`))
    .digest("hex");
const simctl = (args) => {
  try {
    return execFileSync("xcrun", ["simctl", ...args], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error) {
    // Launch arguments contain ephemeral bearers. Never expose child_process's
    // default exception, which includes the entire command and captured logs.
    throw new Error(sanitizedCommandFailure(error));
  }
};
const trySimctl = (args) => {
  try {
    return boundedDiagnostic(boundedHarnessOutput(simctl(args).trim()));
  } catch (error) {
    return sanitizedCommandFailure(error);
  }
};
const runNonce = process.env.JAZZ_DEVICE_RUN_NONCE ?? randomUUID();
const localSession = await startLocalEdgeSessionHarness({
  device: udid,
  runNonce,
  host: "127.0.0.1",
});
process.once("exit", () => localSession.child.kill("SIGTERM"));
let control;
try {
  simctl(["bootstatus", udid, "-b"]);
  simctl(["install", udid, app]);
  const appDataContainer = () =>
    simctl(["get_app_container", udid, "dev.jazz.rndeviceacceptance", "data"]).trim();
  const receiptFilePath = () =>
    join(appDataContainer(), "Library", "Caches", "jazz-device-receipt.ndjson");
  const diagnosticFilePath = () =>
    join(appDataContainer(), "Library", "Caches", "jazz-device-diagnostic.txt");
  // Reinstalling preserves the simulator data container. A former launch's valid
  // receipt must not become a confusing stale candidate for this fresh nonce.
  const receiptFile = () => {
    const file = receiptFilePath();
    return existsSync(file) ? readFileSync(file, "utf8") : "";
  };
  const diagnosticFile = () => {
    const file = diagnosticFilePath();
    return existsSync(file) ? readFileSync(file, "utf8") : "";
  };
  const diagnostics = (launchPid) =>
    [
      `simctl launch PID: ${launchPid}`,
      `app data container:\n${trySimctl(["get_app_container", udid, "dev.jazz.rndeviceacceptance", "data"])}`,
      `app receipt file:\n${boundedDiagnostic(receiptFile())}`,
      `app JavaScript/native diagnostic:\n${safeDeviceDiagnostic(diagnosticFile())}`,
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
  control = await startCoreObservationControl({
    session: localSession,
    expected: { platform: "ios", deviceIdentifier: udid, buildFingerprint, runNonce },
    host: "localhost",
  });
  async function launchAndAssert(phase) {
    rmSync(receiptFilePath(), { force: true });
    rmSync(diagnosticFilePath(), { force: true });
    const phaseStartedAt = Date.now();
    const launchPid = parseLaunchProcessId(
      simctl([
        "launch",
        udid,
        "dev.jazz.rndeviceacceptance",
        "-JazzDeviceRunNonce",
        runNonce,
        "-JazzDeviceDeviceIdentifier",
        udid,
        "-JazzDeviceAcceptancePhase",
        phase,
        "-JazzDeviceCoreObservationEndpoint",
        control.endpoint,
        "-JazzDeviceEdgeEndpoint",
        localSession.endpoint,
        "-JazzDeviceBearerA",
        localSession.bearerA,
        "-JazzDeviceBearerB",
        localSession.bearerB,
      ]),
    );
    const expected = {
      platform: "ios",
      deviceIdentifier: udid,
      buildFingerprint,
      runNonce,
      startedAt: phaseStartedAt,
      scenarios: scenariosForAcceptancePhase(phase)
        .filter((item) => item.state === "passed")
        .map((item) => item.scenario),
    };
    for (let attempt = 0; attempt < 90; attempt += 1) {
      try {
        return assertDeviceReceipt(receiptFile(), expected);
      } catch (error) {
        if (attempt === 89) {
          const detail = error instanceof Error ? error.message : String(error);
          throw new Error(`${detail}\n\n${diagnostics(launchPid)}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 1_000));
      }
    }
  }

  await launchAndAssert("seed");
  await localSession.waitForCoreObservation();
  // A full process termination is required: backgrounding could retain the old
  // JSI bridge and relay owner and would not establish restart durability.
  simctl(["terminate", udid, "dev.jazz.rndeviceacceptance"]);
  await launchAndAssert("verify");
} finally {
  localSession.child.kill("SIGTERM");
  await control?.close();
}
