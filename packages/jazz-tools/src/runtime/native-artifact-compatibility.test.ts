import { describe, expect, it } from "vitest";
import { EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS } from "./native-artifact-fingerprints.js";
import { assertNativeArtifactCompatibility } from "./native-artifact-compatibility.js";

describe("native artifact ABI compatibility", () => {
  it("accepts the current generated artifact fingerprint", () => {
    expect(() =>
      assertNativeArtifactCompatibility(
        { nativeArtifactFingerprint: () => EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS.wasm },
        "WASM",
      ),
    ).not.toThrow();
  });

  it("rejects a stale generated artifact before runtime startup", () => {
    expect(() => assertNativeArtifactCompatibility({}, "NAPI")).toThrow(
      "missing nativeArtifactFingerprint",
    );
    expect(() =>
      assertNativeArtifactCompatibility(
        { nativeArtifactFingerprint: () => "old-artifact" },
        "WASM",
      ),
    ).toThrow(`expected ${EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS.wasm}, got old-artifact`);
  });

  it("rejects a same-fingerprint partial artifact", () => {
    expect(() =>
      assertNativeArtifactCompatibility(
        { nativeArtifactFingerprint: () => EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS.wasm },
        "WASM",
        ["WasmDb"],
      ),
    ).toThrow("missing WasmDb");
  });
});
