import { EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS } from "./native-artifact-fingerprints.js";
import {
  assertArtifactCompatibility,
  type NativeArtifactModule,
} from "./artifact-compatibility.js";

/** Retain the combined native entry point for consumers of both runtimes. */
export function assertNativeArtifactCompatibility(
  artifact: NativeArtifactModule,
  kind: "NAPI" | "WASM",
  requiredExports: readonly string[] = [],
): void {
  assertArtifactCompatibility(
    artifact,
    kind,
    EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS[kind.toLowerCase() as "napi" | "wasm"],
    requiredExports,
  );
}
