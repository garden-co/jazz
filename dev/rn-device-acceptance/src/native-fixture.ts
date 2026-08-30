import { NativeModules } from "react-native";
import { executeNativeRelayCommand } from "jazz-rn";
import { decodeBase64 } from "./base64.ts";
import type { DeviceDiagnosticCode } from "./device-diagnostics.ts";
import type { Platform } from "./protocol";
import type { AdmittedRelay } from "./relay-admission";

export type DeviceReceiptContext = {
  platform: Platform;
  deviceIdentifier: string;
  buildFingerprint: string;
  runNonce: string;
};

type FixtureModule = {
  admittedCapability(): Promise<string>;
  logout(): Promise<void>;
  switchAuthScope(): Promise<string>;
  receiptContext(): Promise<DeviceReceiptContext>;
  recordReceipt(receipt: string): Promise<void>;
  recordDiagnostic(code: DeviceDiagnosticCode): Promise<void>;
  clearDiagnostic(): Promise<void>;
  acceptancePhase(): Promise<"seed" | "verify">;
};

/**
 * The native fixture is an adapter for the public command function, not part of
 * jazz-tools' internal relay-frame API. Keep its boundary structural so it
 * cannot make those low-level implementation types public by accident.
 */
function fixtureModule(): FixtureModule {
  const fixture = NativeModules.JazzDeviceFixture as FixtureModule | undefined;
  if (!fixture)
    throw new Error(
      "JazzDeviceFixture is absent; regenerate a native development build, not Expo Go",
    );
  return fixture;
}

/**
 * The host selects only the bounded acceptance phase. It is not relay scope
 * input: the compiled fixture still chooses every app/storage/auth identity
 * and the SQLite location. Splitting the process-restart receipt this way
 * lets the host kill the whole app between a durable write and the later
 * observation without creating a JS reset/path-selection escape hatch.
 */
export async function nativeAcceptancePhase(): Promise<"seed" | "verify"> {
  const phase = await fixtureModule().acceptancePhase();
  if (phase !== "seed" && phase !== "verify")
    throw new Error("JazzDeviceFixture returned an invalid acceptance phase");
  return phase;
}

function decodeCapability(value: string): Uint8Array {
  return decodeBase64(value);
}

/** The only fixture material that crosses to JS is the opaque 32-byte lease. */
export async function admittedNativeRelay(): Promise<AdmittedRelay> {
  const fixture = fixtureModule();
  const capability = decodeCapability(await fixture.admittedCapability());
  if (capability.byteLength !== 32)
    throw new Error("JazzDeviceFixture returned a non-opaque admission capability");
  return {
    executor: { execute: executeNativeRelayCommand },
    capability,
  };
}

/** Trusted application logout revokes the currently admitted relay scope. */
export async function logoutNativeRelay(): Promise<void> {
  await fixtureModule().logout();
}

/** The fixture's scope B is derived in native code and replaces scope A there. */
export async function switchNativeRelayAuthScope(): Promise<AdmittedRelay> {
  const capability = decodeCapability(await fixtureModule().switchAuthScope());
  if (capability.byteLength !== 32)
    throw new Error("JazzDeviceFixture returned a non-opaque switched admission capability");
  return { executor: { execute: executeNativeRelayCommand }, capability };
}

/** Trusted package/launch identity used solely to bind observed device receipts. */
export async function deviceReceiptContext(): Promise<DeviceReceiptContext> {
  const context = await fixtureModule().receiptContext();
  if (
    !(["android", "ios"] as const).includes(context.platform) ||
    !context.deviceIdentifier ||
    !/^[0-9a-f]{64}$/.test(context.buildFingerprint) ||
    !context.runNonce
  ) {
    throw new Error("JazzDeviceFixture returned an invalid trusted receipt context");
  }
  return context;
}

/**
 * Release iOS builds do not provide a reliable unified-log sink for React
 * Native's `console.log`. The test-only native fixture persists the exact
 * protocol line only after JavaScript has completed its proof; the host reads
 * that app-sandbox file and still validates it independently.
 */
export async function recordDeviceReceipt(receipt: string): Promise<void> {
  await fixtureModule().recordReceipt(receipt);
}

/** Persist only an allowlisted, non-secret pre-receipt failure for the host driver. */
export async function recordDeviceDiagnostic(code: DeviceDiagnosticCode): Promise<void> {
  await fixtureModule().recordDiagnostic(code);
}

/** Clear the pending stage only after the complete native lifecycle succeeds. */
export async function clearDeviceDiagnostic(): Promise<void> {
  await fixtureModule().clearDiagnostic();
}
