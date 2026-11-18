import { EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS } from "./native-artifact-fingerprints.js";
import { NativeArtifactMismatchError } from "./native-artifact-mismatch-error.js";

export {
  NATIVE_ARTIFACT_MISMATCH,
  NativeArtifactMismatchError,
} from "./native-artifact-mismatch-error.js";

type NativeArtifactModule = {
  nativeArtifactFingerprint?: unknown;
};

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
