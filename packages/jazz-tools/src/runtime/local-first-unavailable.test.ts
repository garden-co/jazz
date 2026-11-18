import { describe, expect, it } from "vitest";
import {
  assertNativeArtifactCompatibility,
  NATIVE_ARTIFACT_MISMATCH,
} from "./native-artifact-compatibility.js";
import { EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS } from "./native-artifact-fingerprints.js";
import {
  describeLocalFirstFailure,
  localFirstUnavailableWarning,
} from "./local-first-unavailable.js";

function thrownBy(run: () => void): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
  throw new Error("did not throw");
}

describe("describeLocalFirstFailure", () => {
  it("names a WASM fingerprint mismatch, with both fingerprints", () => {
    const error = thrownBy(() =>
      assertNativeArtifactCompatibility({ nativeArtifactFingerprint: () => "9c1e" }, "WASM"),
    );
    expect(describeLocalFirstFailure(error)).toEqual({
      reason: "wasm-mismatch",
      message: "local-first unavailable: WASM fingerprint mismatch",
      expectedFingerprint: EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS.wasm,
      actualFingerprint: "9c1e",
      error,
    });
  });

  it("names a WASM without a fingerprint as a mismatch too", () => {
    const error = thrownBy(() => assertNativeArtifactCompatibility({}, "WASM"));
    const info = describeLocalFirstFailure(error);
    expect(info.reason).toBe("wasm-mismatch");
    expect(info.actualFingerprint).toBeUndefined();
  });

  it("recognises a mismatch that lost its class crossing a realm, or as a cause", () => {
    // A worker's error arrives as a plain Error with the same message.
    const fromWorker = new Error(
      "Jazz WASM artifact ABI mismatch: expected 1e30, got 9c1e. In this monorepo rebuild generated bindings.",
    );
    expect(describeLocalFirstFailure(fromWorker)).toMatchObject({
      reason: "wasm-mismatch",
      expectedFingerprint: "1e30",
      actualFingerprint: "9c1e",
    });
    const noFingerprintFromWorker = new Error(
      "Jazz WASM artifact is stale or incompatible: missing nativeArtifactFingerprint (expected 1e30). In this monorepo rebuild generated bindings.",
    );
    const noFingerprint = describeLocalFirstFailure(noFingerprintFromWorker);
    expect(noFingerprint.expectedFingerprint).toBe("1e30");
    expect(noFingerprint.actualFingerprint).toBeUndefined();
    const wrapped = new Error("could not open the local store", {
      cause: Object.assign(new Error("x"), { code: NATIVE_ARTIFACT_MISMATCH, kind: "WASM" }),
    });
    expect(describeLocalFirstFailure(wrapped).reason).toBe("wasm-mismatch");
  });

  it("calls anything else a load failure", () => {
    expect(describeLocalFirstFailure(new Error("offline")).reason).toBe("load-failed");
    expect(describeLocalFirstFailure("offline").error.message).toBe("offline");
    const napi = thrownBy(() => assertNativeArtifactCompatibility({}, "NAPI"));
    expect(describeLocalFirstFailure(napi).reason).toBe("load-failed");
  });

  it("names a partial build as incomplete, not as a fingerprint mismatch", () => {
    const error = thrownBy(() =>
      assertNativeArtifactCompatibility(
        { nativeArtifactFingerprint: () => EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS.wasm },
        "WASM",
        ["WasmDb"],
      ),
    );
    const info = describeLocalFirstFailure(error);
    expect(info).toMatchObject({
      reason: "wasm-mismatch",
      message: "local-first unavailable: incomplete WASM build",
      missingExport: "WasmDb",
    });
    expect(info.actualFingerprint).toBeUndefined();
    // The same, arriving from a worker as a plain Error.
    expect(describeLocalFirstFailure(new Error((error as Error).message))).toMatchObject({
      message: "local-first unavailable: incomplete WASM build",
      missingExport: "WasmDb",
    });
  });
});

describe("localFirstUnavailableWarning", () => {
  const warn = (error: unknown) =>
    localFirstUnavailableWarning(describeLocalFirstFailure(error), "generic", "staying thin");

  it("gives both fingerprints only when it knows both", () => {
    expect(
      warn(
        thrownBy(() =>
          assertNativeArtifactCompatibility({ nativeArtifactFingerprint: () => "9c1e" }, "WASM"),
        ),
      ),
    ).toMatch(
      `Jazz local-first unavailable: WASM fingerprint mismatch (expected ${EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS.wasm}, got 9c1e); staying thin,`,
    );
    expect(warn(thrownBy(() => assertNativeArtifactCompatibility({}, "WASM")))).toMatch(
      `(expected ${EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS.wasm}; the loaded WASM has no fingerprint); staying thin,`,
    );
    const bare = Object.assign(new Error("x"), { code: NATIVE_ARTIFACT_MISMATCH, kind: "WASM" });
    expect(warn(bare)).toMatch(
      /^Jazz local-first unavailable: WASM fingerprint mismatch; staying thin,/,
    );
    for (const line of [
      warn(bare),
      warn(new Error("Jazz WASM artifact ABI mismatch: something else")),
    ])
      expect(line).not.toMatch(/unknown|got none/);
  });

  it("says a partial build lacks an export, without contradicting itself", () => {
    const line = warn(
      thrownBy(() =>
        assertNativeArtifactCompatibility(
          { nativeArtifactFingerprint: () => EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS.wasm },
          "WASM",
          ["WasmDb"],
        ),
      ),
    );
    expect(line).toMatch(
      /^Jazz local-first unavailable: incomplete WASM build \(its fingerprint matches, but it lacks WasmDb\); staying thin,/,
    );
    expect(line).not.toMatch(/expected .*, got/);
  });

  it("keeps the generic line for other failures", () => {
    expect(warn(new Error("offline"))).toBe("generic");
  });
});
