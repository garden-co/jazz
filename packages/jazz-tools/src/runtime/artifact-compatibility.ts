export type NativeArtifactModule = {
  nativeArtifactFingerprint?: unknown;
};

/**
 * Fail before a generated native artifact can expose a partial runtime API.
 *
 * This is intentionally independent of the wire protocol: binding ABI can
 * change without a transport frame change.
 */
export function assertArtifactCompatibility(
  artifact: NativeArtifactModule,
  kind: "NAPI" | "WASM",
  expected: string,
  requiredExports: readonly string[] = [],
): void {
  if (typeof artifact.nativeArtifactFingerprint !== "function") {
    throw new Error(
      `Jazz ${kind} artifact is stale or incompatible: missing nativeArtifactFingerprint (expected ${expected}). ` +
        "In this monorepo rebuild generated bindings; for installed packages reinstall matching Jazz package versions.",
    );
  }
  const actual = artifact.nativeArtifactFingerprint();
  if (actual !== expected) {
    throw new Error(
      `Jazz ${kind} artifact ABI mismatch: expected ${expected}, got ${String(actual)}. ` +
        "In this monorepo rebuild generated bindings; for installed packages reinstall matching Jazz package versions.",
    );
  }
  for (const name of requiredExports) {
    if (typeof (artifact as Record<string, unknown>)[name] !== "function")
      throw new Error(
        `Jazz ${kind} artifact is incomplete despite matching fingerprint: missing ${name}.`,
      );
  }
}
