// Platform substitution only: database operations and command codecs remain
// the real RN implementation. No React Native/JSI behavior is claimed here.
import { createRequire } from "node:module";
import type { NativeForegroundRuntime } from "../../src/react-native/native-foreground-db.js";

type NativeHandle = object; // NAPI External with Rust Drop, never a pointer number.
interface TestBinding {
  __testRnDecodeForegroundCommand(command: Uint8Array): string;
  __testRnForegroundResponseCorpus(): string;
  nativeArtifactFingerprint(): string;
  __testRnHostNew(): NativeHandle;
  __testRnHostAbiVersion(host: NativeHandle): number;
  __testRnHostAdmit(host: NativeHandle, config: string): Uint8Array;
  __testRnHostOpenAttached(host: NativeHandle, capability: Uint8Array): NativeHandle;
  __testRnHostClose(host: NativeHandle): boolean;
  __testRnHostBeginPrivateSession(host: NativeHandle, config: string): Uint8Array;
  __testRnHostAttachCanonicalSchema(
    host: NativeHandle,
    capability: Uint8Array,
    schema: string,
  ): Uint8Array;
  __testRnHostRevoke(host: NativeHandle, capability: Uint8Array): void;
  __testRnForegroundExecute(foreground: NativeHandle, command: Uint8Array): Uint8Array;
  __testRnForegroundTick(foreground: NativeHandle): void;
  __testRnForegroundIsClosed(foreground: NativeHandle): boolean;
  __testRnForegroundSetTickScheduler(
    foreground: NativeHandle,
    callback: (urgency: string) => void,
  ): void;
  __testRnForegroundClose(foreground: NativeHandle): boolean;
}
const bindingPath = process.env.JAZZ_CORRECTNESS_NAPI_BINDING;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN !== "1" || !bindingPath)
  throw new Error("RN tests require the official sealed correctness consumer");
const binding = createRequire(import.meta.url)(bindingPath) as TestBinding;
if (typeof binding.__testRnHostNew !== "function")
  throw new Error("RN bridge missing: produce artifacts with JAZZ_RN_TEST_BRIDGE=1");
if (binding.nativeArtifactFingerprint() !== process.env.JAZZ_CORRECTNESS_NAPI_FINGERPRINT)
  throw new Error("RN test bridge does not match the admitted artifact fingerprint");
const probe = binding.__testRnHostNew();
const abiVersion = binding.__testRnHostAbiVersion(probe);
binding.__testRnHostClose(probe);

export function createPlatformHost() {
  const nativeHost = binding.__testRnHostNew();
  return {
    abiVersion,
    admit: (config: string) => binding.__testRnHostAdmit(nativeHost, config),
    beginPrivateSession: (config: string) =>
      binding.__testRnHostBeginPrivateSession(nativeHost, config),
    attachCanonicalSchema: (capability: Uint8Array, schema: string) =>
      binding.__testRnHostAttachCanonicalSchema(nativeHost, capability, schema),
    revoke: (capability: Uint8Array) => binding.__testRnHostRevoke(nativeHost, capability),
    close: () => binding.__testRnHostClose(nativeHost),
    openAttached(capability: Uint8Array): NativeForegroundRuntime {
      const foreground = binding.__testRnHostOpenAttached(nativeHost, capability);
      return {
        execute: (command) => binding.__testRnForegroundExecute(foreground, command),
        tick: () => binding.__testRnForegroundTick(foreground),
        isClosed: () => binding.__testRnForegroundIsClosed(foreground),
        setTickScheduler: (callback) =>
          binding.__testRnForegroundSetTickScheduler(foreground, callback),
        close: () => binding.__testRnForegroundClose(foreground),
      };
    },
  };
}
export function installPlatformHost(host: ReturnType<typeof createPlatformHost>) {
  Object.defineProperty(globalThis, "__jazzNativeForegroundRuntimeV1", {
    configurable: true,
    value: { abiVersion: host.abiVersion, openAttached: host.openAttached },
  });
}
export default { getAbiVersion: () => abiVersion };

// Cross-language codec probes use the same sealed bridge as the real host tests.
export function decodeCommandInRust(command: Uint8Array): unknown {
  return JSON.parse(binding.__testRnDecodeForegroundCommand(command));
}
export function rustResponseCorpus(): Uint8Array[] {
  return (JSON.parse(binding.__testRnForegroundResponseCorpus()) as number[][]).map((bytes) =>
    Uint8Array.from(bytes),
  );
}
