/*
 * Deliberately dependency-free: shared session code (reachable from the React
 * Native entry) recognises this error, and must not import the generated
 * WASM/NAPI fingerprints that `native-artifact-compatibility.ts` checks
 * against. React Native builds don't emit those.
 */

/** `code` of a {@link NativeArtifactMismatchError}, which survives a realm hop. */
export const NATIVE_ARTIFACT_MISMATCH = "JAZZ_NATIVE_ARTIFACT_MISMATCH";

/**
 * A generated native artifact (the WASM or NAPI binding) is not the one this
 * jazz-tools build was made for: stale, from another version, or partial.
 */
export class NativeArtifactMismatchError extends Error {
  override readonly name = "NativeArtifactMismatchError";
  readonly code = NATIVE_ARTIFACT_MISMATCH;
  constructor(
    message: string,
    readonly kind: "NAPI" | "WASM",
    /** The fingerprint this jazz-tools build expects. */
    readonly expected: string,
    /** The artifact's own fingerprint; `undefined` when it has none. */
    readonly actual: string | undefined,
    /** An export a matching-fingerprint but partial artifact lacks. */
    readonly missingExport?: string,
  ) {
    super(message);
  }
}
