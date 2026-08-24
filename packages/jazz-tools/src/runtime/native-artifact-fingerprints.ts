import { EXPECTED_NAPI_ARTIFACT_FINGERPRINT } from "./native-artifact-fingerprint-napi.js";
import { EXPECTED_WASM_ARTIFACT_FINGERPRINT } from "./native-artifact-fingerprint-wasm.js";

export const EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS = {
  napi: EXPECTED_NAPI_ARTIFACT_FINGERPRINT,
  wasm: EXPECTED_WASM_ARTIFACT_FINGERPRINT,
} as const;
