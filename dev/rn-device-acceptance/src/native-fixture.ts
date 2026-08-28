import { NativeModules } from "react-native";
import { executeNativeRelayCommand } from "jazz-rn";
import type { Platform } from "./protocol";

export type DeviceReceiptContext = {
  platform: Platform;
  deviceIdentifier: string;
  buildFingerprint: string;
  runNonce: string;
};

type FixtureModule = {
  admittedCapability(): Promise<string>;
  logout(): Promise<void>;
  receiptContext(): Promise<DeviceReceiptContext>;
  recordReceipt(receipt: string): Promise<void>;
};

/**
 * The native fixture is an adapter for the public command function, not part of
 * jazz-tools' internal relay-frame API. Keep its boundary structural so it
 * cannot make those low-level implementation types public by accident.
 */
type AdmittedNativeRelay = {
  executor: { execute: typeof executeNativeRelayCommand };
  capability: Uint8Array;
};

function fixtureModule(): FixtureModule {
  const fixture = NativeModules.JazzDeviceFixture as FixtureModule | undefined;
  if (!fixture)
    throw new Error(
      "JazzDeviceFixture is absent; regenerate a native development build, not Expo Go",
    );
  return fixture;
}

function decodeCapability(value: string): Uint8Array {
  const bytes = globalThis.atob(value);
  return Uint8Array.from(bytes, (byte) => byte.charCodeAt(0));
}

/** The only fixture material that crosses to JS is the opaque 32-byte lease. */
export async function admittedNativeRelay(): Promise<AdmittedNativeRelay> {
  const fixture = fixtureModule();
  const capability = decodeCapability(await fixture.admittedCapability());
  if (capability.byteLength !== 32)
    throw new Error("JazzDeviceFixture returned a non-opaque admission capability");
  return {
    executor: { execute: executeNativeRelayCommand },
    capability,
  };
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
