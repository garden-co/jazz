// Source-only checks do not build native artifacts. These declarations provide
// no runtime values; native producers supply the actual implementation modules.
// Keep contracts outside runtime: Svelte packaging emits implementation sibling
// declarations there before cleaning them up.
declare module "*native-artifact-fingerprint-napi.js" {
  export const EXPECTED_NAPI_ARTIFACT_FINGERPRINT: string;
}
declare module "*native-artifact-fingerprint-wasm.js" {
  export const EXPECTED_WASM_ARTIFACT_FINGERPRINT: string;
}
