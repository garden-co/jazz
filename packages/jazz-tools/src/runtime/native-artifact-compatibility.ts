import { EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS } from "./native-artifact-fingerprints.js";

type NativeArtifactModule = {
  nativeArtifactFingerprint?: unknown;
};

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

/**
 * Fail before a generated native artifact can expose a partial runtime API.
 *
 * This is intentionally independent of the wire protocol: binding ABI can
 * change without a transport frame change.
 */
export function assertNativeArtifactCompatibility(
  artifact: NativeArtifactModule,
  kind: "NAPI" | "WASM",
  requiredExports: readonly string[] = [],
): void {
  const expected = EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS[kind.toLowerCase() as "napi" | "wasm"];
  if (typeof artifact.nativeArtifactFingerprint !== "function") {
    throw new NativeArtifactMismatchError(
      `Jazz ${kind} artifact is stale or incompatible: missing nativeArtifactFingerprint (expected ${expected}). ` +
        "In this monorepo rebuild generated bindings; for installed packages reinstall matching Jazz package versions.",
      kind,
      expected,
      undefined,
    );
  }
  const actual = artifact.nativeArtifactFingerprint();
  if (actual !== expected) {
    throw new NativeArtifactMismatchError(
      `Jazz ${kind} artifact ABI mismatch: expected ${expected}, got ${String(actual)}. ` +
        "In this monorepo rebuild generated bindings; for installed packages reinstall matching Jazz package versions.",
      kind,
      expected,
      String(actual),
    );
  }
  for (const name of requiredExports) {
    if (typeof (artifact as Record<string, unknown>)[name] !== "function")
      throw new NativeArtifactMismatchError(
        `Jazz ${kind} artifact is incomplete despite matching fingerprint: missing ${name}.`,
        kind,
        expected,
        expected,
        name,
      );
  }
}
