// Platform substitution only: database operations and command codecs remain
// the real RN implementation. No React Native/JSI behavior is claimed here.
import { createRequire } from "node:module";
const bindingPath = process.env.JAZZ_CORRECTNESS_NAPI_BINDING;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN !== "1" || !bindingPath)
  throw new Error("RN tests require the official sealed correctness consumer");
const binding = createRequire(import.meta.url)(bindingPath);
if (typeof binding.RnTestHost !== "function")
  throw new Error("RN bridge missing: produce artifacts with JAZZ_RN_TEST_BRIDGE=1");
if (binding.nativeArtifactFingerprint() !== process.env.JAZZ_CORRECTNESS_NAPI_FINGERPRINT)
  throw new Error("RN test bridge does not match the admitted artifact fingerprint");
export const host = new binding.RnTestHost();
Object.defineProperty(globalThis, "__jazzNativeForegroundRuntimeV1", {
  configurable: true,
  value: {
    abiVersion: host.abiVersion,
    openAttached: (capability: Uint8Array) => host.openAttached(capability),
  },
});
export default { getAbiVersion: () => host.abiVersion };
