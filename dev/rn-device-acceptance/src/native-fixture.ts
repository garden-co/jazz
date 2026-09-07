import { NativeModules, Platform as NativePlatform } from "react-native";
import { executeNativeRelayCommand } from "jazz-rn";
import { createAccountManager } from "jazz-tools/expo";
import { createNativeAccountTestSession } from "jazz-tools/_dev/native-account-session";
import { schemaSource } from "./scope-fixture";
import type { SeedBoundary } from "./seed-teardown";
import type { DeviceDiagnosticCode } from "./device-diagnostics.ts";
import type { Platform } from "./protocol";
import type { AdmittedRelay } from "./relay-admission";
import { isDeviceRunNonce } from "./run-marker";

export type DeviceReceiptContext = {
  platform: Platform;
  deviceIdentifier: string;
  buildFingerprint: string;
  runNonce: string;
};

type FixtureModule = {
  waitForCoreObservation(): Promise<void>;
  recordSeedBoundary(code: SeedBoundary): boolean;
  recordSameRuntimeWakeBoundary(): boolean;
  edgeEndpoint(): Promise<string>;
  receiptContext(): Promise<DeviceReceiptContext>;
  recordReceipt(receipt: string): Promise<void>;
  recordDiagnostic(code: DeviceDiagnosticCode): Promise<void>;
  recordScopeWriterReadDiagnostic?(detail: string): Promise<void>;
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

/** The host selects seed or offline process-restart verification. Accounts
 * and their storage roots are retained by the production account manager. */
export async function nativeAcceptancePhase(): Promise<"seed" | "verify"> {
  const phase = await fixtureModule().acceptancePhase();
  if (phase !== "seed" && phase !== "verify")
    throw new Error("JazzDeviceFixture returned an invalid acceptance phase");
  return phase;
}

type AccountLease = Awaited<ReturnType<typeof createNativeAccountTestSession>>;
let lease: AccountLease | undefined;
let prepare:
  | Promise<{
      a: Awaited<ReturnType<typeof createAccountManager>>;
      b: Awaited<ReturnType<typeof createAccountManager>>;
      endpoint: string;
      phase: "seed" | "verify";
    }>
  | undefined;

function retainedAccounts() {
  return (prepare ??= (async () => {
    const endpoint = await fixtureModule().edgeEndpoint();
    const url = new URL(endpoint);
    if (
      !["http:", "https:"].includes(url.protocol) ||
      !url.hostname ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    )
      throw new Error("JazzDeviceFixture returned an invalid Edge endpoint");
    const phase = await nativeAcceptancePhase();
    const options = { appId: "jazz-device-acceptance", serverUrl: endpoint };
    const a = await createAccountManager({ ...options, profile: "device-scope-a" });
    const b = await createAccountManager({ ...options, profile: "device-scope-b" });
    return { a, b, endpoint, phase };
  })());
}

async function admit(scope: "a" | "b") {
  const accounts = await retainedAccounts();
  const manager = accounts[scope];
  let account = manager.getLoggedIn();
  if (!account) {
    if (accounts.phase === "verify") throw new Error("Process restart lost its retained account");
    account = manager.createLocalFirst();
  }
  lease = await createNativeAccountTestSession(
    {
      appId: "jazz-device-acceptance",
      account,
      ...(accounts.phase === "seed" ? { serverUrl: accounts.endpoint } : {}),
    },
    schemaSource,
  );
  return lease;
}

/** Genuine retained accounts enter the same admission path as public clients. */
export async function admittedNativeRelay(): Promise<
  AdmittedRelay & { account: AccountLease["account"] }
> {
  const current = lease ?? (await admit("a"));
  return { executor: { execute: executeNativeRelayCommand }, ...current };
}

/** Close the fixture-owned lease; retain credentials for process-restart proof. */
export async function closeNativeRelay(): Promise<void> {
  lease?.close();
  lease = undefined;
}

/** Close A's lease before opening B; linking never participates in this path. */
export async function switchNativeRelayAuthScope(): Promise<AdmittedRelay> {
  await closeNativeRelay();
  const current = await admit("b");
  return { executor: { execute: executeNativeRelayCommand }, ...current };
}

/** Trusted package/launch identity used solely to bind observed device receipts. */
export async function deviceReceiptContext(): Promise<DeviceReceiptContext> {
  const context = await fixtureModule().receiptContext();
  if (
    !(["android", "ios"] as const).includes(context.platform) ||
    !context.deviceIdentifier ||
    !/^[0-9a-f]{64}$/.test(context.buildFingerprint) ||
    !isDeviceRunNonce(context.runNonce)
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

/** Android-only, bounded counters for the scope writer read timeout. */
export async function recordScopeWriterReadDiagnostic(detail: string): Promise<void> {
  if (NativePlatform.OS !== "android") return;
  if (
    !/^scope-isolation-writer-read-detail:last-(none|pending|subscription|rejected|closed|rows)-wakes-\d{1,6}-polls-\d{1,6}-row-responses-\d{1,6}-ready-(yes|no)$/.test(
      detail,
    )
  )
    throw new Error("invalid scope writer read diagnostic");
  const fixture = fixtureModule();
  if (!fixture.recordScopeWriterReadDiagnostic)
    throw new Error("JazzDeviceFixture cannot record scope writer read diagnostics");
  await fixture.recordScopeWriterReadDiagnostic(detail);
}

/** Clear the pending stage only after the complete native lifecycle succeeds. */
export async function clearDeviceDiagnostic(): Promise<void> {
  await fixtureModule().clearDiagnostic();
}

/** Test-only host acknowledgement, separate from the public write wait API. */
export async function waitForNativeCoreObservation(): Promise<void> {
  await fixtureModule().waitForCoreObservation();
}

/** Synchronous Android log capture survives a subsequent blocked JS/native call.
 * Diagnostic failure must never change the acceptance proof or teardown. */
export function recordNativeSeedBoundary(code: SeedBoundary): void {
  if (NativePlatform.OS !== "android") return;
  try {
    fixtureModule().recordSeedBoundary(code);
  } catch {
    // Preserve the actual acceptance outcome if the diagnostic sink fails.
  }
}

export function recordSameRuntimeWakeBoundary(): void {
  if (NativePlatform.OS !== "android") return;
  try {
    fixtureModule().recordSameRuntimeWakeBoundary();
  } catch {}
}
