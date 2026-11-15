import { describe, expect, it } from "vitest";
import {
  assertNativeArtifactCompatibility,
  NATIVE_ARTIFACT_MISMATCH,
} from "./native-artifact-compatibility.js";
import { EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS } from "./native-artifact-fingerprints.js";
import { describeLocalFirstFailure } from "./local-first-unavailable.js";

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
    expect(describeLocalFirstFailure(fromWorker).reason).toBe("wasm-mismatch");
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
});
