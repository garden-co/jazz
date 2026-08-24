import { describe, expect, it } from "vitest";
import { WIRE_PROTOCOL_VERSION } from "./native-runtime/websocket.js";
import { assertNativeArtifactProtocol } from "./native-artifact-compatibility.js";

describe("native artifact protocol compatibility", () => {
  it("accepts the current generated artifact marker", () => {
    expect(() =>
      assertNativeArtifactProtocol(
        { nativeArtifactProtocolVersion: () => WIRE_PROTOCOL_VERSION },
        "WASM",
      ),
    ).not.toThrow();
  });

  it("rejects a stale generated artifact before runtime startup", () => {
    expect(() => assertNativeArtifactProtocol({}, "NAPI")).toThrow(
      "missing the native protocol marker",
    );
    expect(() =>
      assertNativeArtifactProtocol({ nativeArtifactProtocolVersion: () => 11 }, "WASM"),
    ).toThrow(`expected ${WIRE_PROTOCOL_VERSION}, got 11`);
  });
});
