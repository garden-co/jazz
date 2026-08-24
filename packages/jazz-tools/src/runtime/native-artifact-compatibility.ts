import { WIRE_PROTOCOL_VERSION } from "./native-runtime/websocket.js";

type NativeArtifactModule = {
  nativeArtifactProtocolVersion?: unknown;
};

/**
 * Fail before a generated native artifact can expose a partial runtime API.
 *
 * The wire protocol is already the compatibility boundary shared by the JS,
 * NAPI, and WASM runtimes. A missing marker means current glue was paired with
 * an older generated artifact, not that the application made an invalid call.
 */
export function assertNativeArtifactProtocol(
  artifact: NativeArtifactModule,
  kind: "NAPI" | "WASM",
): void {
  if (typeof artifact.nativeArtifactProtocolVersion !== "function") {
    throw new Error(
      `Jazz ${kind} artifact is stale or incompatible: it is missing the native protocol marker. ` +
        "Rebuild generated bindings before starting the app or tests.",
    );
  }
  const actual = artifact.nativeArtifactProtocolVersion();
  if (actual !== WIRE_PROTOCOL_VERSION) {
    throw new Error(
      `Jazz ${kind} artifact protocol mismatch: expected ${WIRE_PROTOCOL_VERSION}, got ${String(actual)}. ` +
        "Rebuild generated bindings before starting the app or tests.",
    );
  }
}
