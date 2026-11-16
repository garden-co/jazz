import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  decodeWireError,
  isReauthenticateWireError,
  isWireError,
  wireAuthFailureReason,
} from "./websocket.js";

type Case = {
  name: string;
  code: string;
  retry: string;
  message: string;
  reason: string;
  reasoned_hex: string;
  plain_hex: string;
};

const fixture = JSON.parse(
  readFileSync(
    new URL(
      "../../../../../crates/jazz/layers/protocol/fixtures/wire_reasoned_error_v1.json",
      import.meta.url,
    ),
    "utf8",
  ),
) as { encoding: string; cases: Case[] };

const bytes = (hex: string) => Uint8Array.from(Buffer.from(hex, "hex"));

// `wire-error-reason-v1`, pinned by the fixture Rust encodes: a client that
// offered FEATURE_TYPED_ERROR_REASON reads why a server refused it from the
// frame, not from its message.
describe("wire-error-reason-v1", () => {
  it.each(fixture.cases)("reads a $name refusal with its reason", (c) => {
    const reasoned = bytes(c.reasoned_hex);
    expect(isWireError(reasoned)).toBe(true);
    expect(decodeWireError(reasoned)).toEqual({
      code: c.code,
      retry: c.retry,
      message: c.message,
      reason: c.reason,
    });
    // The bytes every other client gets carry no reason.
    expect(decodeWireError(bytes(c.plain_hex))).toEqual({
      code: c.code,
      retry: c.retry,
      message: c.message,
    });
  });

  it("takes a named reason over the message", () => {
    const named = { code: "auth_failed", retry: "never", message: "token expired" };
    expect(wireAuthFailureReason({ ...named, reason: "revoked" })).toBe("revoked");
    expect(wireAuthFailureReason({ ...named, reason: "disabled" })).toBe("disabled");
    // An older server: the message decides, as before.
    expect(wireAuthFailureReason(named)).toBe("expired");
    // A reason from a newer server: the message decides.
    expect(wireAuthFailureReason({ ...named, reason: "unknown:9" })).toBe("expired");
  });

  it("knows a planned re-authentication by its reason, without the message prefix", () => {
    const error = { code: "auth_failed", retry: "after_resume", message: "session ends" };
    expect(isReauthenticateWireError({ ...error, reason: "reauthenticate" })).toBe(true);
    expect(isReauthenticateWireError(error)).toBe(false);
    expect(isReauthenticateWireError({ ...error, message: "reauthenticate: session ends" })).toBe(
      true,
    );
    // A named refusal is a refusal, whatever its message says.
    expect(
      isReauthenticateWireError({
        ...error,
        message: "reauthenticate: session ends",
        reason: "revoked",
      }),
    ).toBe(false);
  });
});
