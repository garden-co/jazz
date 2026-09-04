// Platform substitution only: database operations and command codecs remain
// the real RN implementation. No React Native/JSI behavior is claimed here.
import { createRequire } from "node:module";
import type { NativeForegroundRuntime } from "../../src/react-native/native-foreground-db.js";

type NativeHandle = object; // NAPI External with Rust Drop, never a pointer number.
interface TestBinding {
  nativeArtifactFingerprint(): string;
  __testRnHostNew(): NativeHandle;
  __testRnHostAbiVersion(host: NativeHandle): number;
  __testRnHostAdmit(host: NativeHandle, config: string): Uint8Array;
  __testRnHostOpenAttached(host: NativeHandle, capability: Uint8Array): NativeHandle;
  __testRnHostClose(host: NativeHandle): boolean;
  __testRnForegroundExecute(foreground: NativeHandle, command: Uint8Array): Uint8Array;
  __testRnForegroundTick(foreground: NativeHandle): void;
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
const nativeHost = binding.__testRnHostNew();
export const host = {
  abiVersion: binding.__testRnHostAbiVersion(nativeHost),
  admit: (config: string) => binding.__testRnHostAdmit(nativeHost, config),
  close: () => binding.__testRnHostClose(nativeHost),
  openAttached(capability: Uint8Array): NativeForegroundRuntime {
    const foreground = binding.__testRnHostOpenAttached(nativeHost, capability);
    return {
      execute: (command) => binding.__testRnForegroundExecute(foreground, command),
      tick: () => binding.__testRnForegroundTick(foreground),
      setTickScheduler: (callback) =>
        binding.__testRnForegroundSetTickScheduler(foreground, callback),
      close: () => binding.__testRnForegroundClose(foreground),
    };
  },
};
Object.defineProperty(globalThis, "__jazzNativeForegroundRuntimeV1", {
  configurable: true,
  value: host,
});
export default { getAbiVersion: () => host.abiVersion };
