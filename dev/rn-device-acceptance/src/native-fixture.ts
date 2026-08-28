import { NativeModules } from "react-native";
import { executeNativeRelayCommand } from "jazz-rn";

type NativeRelayCapability = Uint8Array;
type NativeRelayExecutor = { execute(commandBase64: string): Promise<string> };

type FixtureModule = { admittedCapability(): Promise<string>; logout(): Promise<void> };

function decodeCapability(value: string): Uint8Array {
  const bytes = globalThis.atob(value);
  return Uint8Array.from(bytes, (byte) => byte.charCodeAt(0));
}

/** The only fixture material that crosses to JS is the opaque 32-byte lease. */
export async function admittedNativeRelay(): Promise<{
  executor: NativeRelayExecutor;
  capability: NativeRelayCapability;
}> {
  const fixture = NativeModules.JazzDeviceFixture as FixtureModule | undefined;
  if (!fixture)
    throw new Error(
      "JazzDeviceFixture is absent; regenerate a native development build, not Expo Go",
    );
  const capability = decodeCapability(await fixture.admittedCapability());
  if (capability.byteLength !== 32)
    throw new Error("JazzDeviceFixture returned a non-opaque admission capability");
  return { executor: { execute: executeNativeRelayCommand }, capability };
}
