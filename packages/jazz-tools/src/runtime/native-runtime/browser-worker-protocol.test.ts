import { describe, expect, it } from "vitest";
import {
  BROWSER_RELAY_ERROR_MAX_CAUSE_DEPTH,
  BROWSER_RELAY_ERROR_MAX_CODE_CHARS,
  BROWSER_RELAY_ERROR_MAX_TOTAL_CHARS,
  deserializeBrowserRelayError,
  serializeBrowserRelayError,
  type BrowserRelayError,
} from "./browser-worker-protocol.js";

function codedError(message: string, code: string, cause?: unknown): Error {
  const error = new Error(message, cause === undefined ? undefined : { cause });
  Object.defineProperty(error, "code", { enumerable: true, value: code });
  return error;
}

function relayCauseDepth(error: Error): number {
  let depth = 0;
  let current: unknown = error;
  while (current instanceof Error && "cause" in current) {
    depth += 1;
    current = current.cause;
  }
  return depth;
}

describe("BrowserRelayError protocol", () => {
  it("round-trips the exact bounded causal codes at every error-chain level", () => {
    const error = codedError(
      "top-level failure",
      "top_level",
      codedError("storage failure", "storage_corrupt"),
    );

    const serialized = serializeBrowserRelayError(error);
    expect(serialized).toMatchObject({
      message: "top-level failure",
      code: "top_level",
      cause: { message: "storage failure", code: "storage_corrupt" },
    });

    const roundTripped = deserializeBrowserRelayError(serialized) as Error & { code?: string };
    expect(roundTripped.code).toBe("top_level");
    expect(roundTripped.cause).toMatchObject({
      message: "storage failure",
      code: "storage_corrupt",
    });
  });

  it("omits invalid outgoing codes and rejects invalid incoming codes", () => {
    const numericCode = new Error("numeric code") as Error & { code?: unknown };
    numericCode.code = 42;
    expect(serializeBrowserRelayError(numericCode)).not.toHaveProperty("code");

    const oversizedCode = codedError(
      "oversized code",
      "x".repeat(BROWSER_RELAY_ERROR_MAX_CODE_CHARS + 1),
    );
    expect(serializeBrowserRelayError(oversizedCode)).not.toHaveProperty("code");

    // A code is a stable machine-readable discriminator, never an optional
    // display string. An empty string must therefore be omitted on the way
    // out and rejected at the untrusted MessagePort boundary. This is a
    // planted sensitivity guard for `validBrowserRelayCode`'s non-empty
    // invariant: removing its `length > 0` check makes both assertions fail.
    const emptyCode = codedError("empty code", "");
    expect(serializeBrowserRelayError(emptyCode)).not.toHaveProperty("code");

    const rejected = deserializeBrowserRelayError({
      name: "Error",
      message: "untrusted",
      code: "x".repeat(BROWSER_RELAY_ERROR_MAX_CODE_CHARS + 1),
    });
    const emptyInboundCode = deserializeBrowserRelayError({
      name: "Error",
      message: "untrusted",
      code: "",
    });
    for (const error of [rejected, emptyInboundCode]) {
      expect(error).toMatchObject({
        name: "BrowserRelayErrorProtocolError",
        code: "browser_relay_error_protocol_violation",
      });
    }
  });

  it("bounds a 12k-deep causal chain without recursive serialization or deserialization", () => {
    let error: Error = codedError("leaf", "leaf");
    for (let index = 0; index < 12_000; index += 1) {
      error = codedError(`cause-${index}`, `cause_${index}`, error);
    }

    const serialized = serializeBrowserRelayError(error);
    expect(() => deserializeBrowserRelayError(serialized)).not.toThrow();
    expect(relayCauseDepth(deserializeBrowserRelayError(serialized))).toBeLessThanOrEqual(
      BROWSER_RELAY_ERROR_MAX_CAUSE_DEPTH,
    );
  });

  it("round-trips a max-field multi-node chain without emitting partial codes", () => {
    const code = "c".repeat(BROWSER_RELAY_ERROR_MAX_CODE_CHARS);
    let error: Error = codedError("m".repeat(8 * 1024), code);
    error.name = "n".repeat(256);
    error.stack = "s".repeat(16 * 1024);
    for (let index = 0; index < BROWSER_RELAY_ERROR_MAX_CAUSE_DEPTH; index += 1) {
      const parent = codedError("m".repeat(8 * 1024), code, error);
      parent.name = "n".repeat(256);
      parent.stack = "s".repeat(16 * 1024);
      error = parent;
    }

    const serialized = serializeBrowserRelayError(error);
    const codes: string[] = [];
    let current: unknown = serialized;
    while (current && typeof current === "object") {
      const relay = current as BrowserRelayError;
      if (relay.code !== undefined) codes.push(relay.code);
      current = relay.cause;
    }
    expect(codes.every((value) => value.length > 0)).toBe(true);
    expect(deserializeBrowserRelayError(serialized)).not.toMatchObject({
      name: "BrowserRelayErrorProtocolError",
    });
  });

  it("fails closed for malicious inbound depth and total-size violations", () => {
    const root: BrowserRelayError = { name: "Error", message: "root" };
    let current = root;
    for (let index = 0; index < 12_000; index += 1) {
      const nested: BrowserRelayError = { name: "Error", message: `nested-${index}` };
      current.cause = nested;
      current = nested;
    }

    for (const payload of [
      root,
      { name: "Error", message: "x".repeat(BROWSER_RELAY_ERROR_MAX_TOTAL_CHARS + 1) },
    ]) {
      expect(() => deserializeBrowserRelayError(payload)).not.toThrow();
      expect(deserializeBrowserRelayError(payload)).toMatchObject({
        name: "BrowserRelayErrorProtocolError",
        code: "browser_relay_error_protocol_violation",
      });
    }
  });
});
