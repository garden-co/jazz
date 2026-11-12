import { describe, it, expect } from "vitest";
import {
  authSecretStorageKey,
  generateAuthSecret,
  parseAuthSecret,
} from "./auth-secret-store.js";

describe("generateAuthSecret", () => {
  it("produces the versioned canonical 256-bit representation", () => {
    const secret = generateAuthSecret();
    expect(secret).toMatch(/^jazz-auth-v1:[A-Za-z0-9_-]{43}$/);
    expect(parseAuthSecret(secret)).toHaveLength(32);
  });

  it("produces different secrets each call", () => {
    const a = generateAuthSecret();
    const b = generateAuthSecret();
    expect(a).not.toBe(b);
  });

  it("rejects unversioned, padded, and noncanonical payloads", () => {
    expect(() => parseAuthSecret("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")).toThrow(
      /jazz-auth-v1/,
    );
    expect(() =>
      parseAuthSecret("jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
    ).toThrow(/43 unpadded/);
    // The final base64url sextet has unused bits for a 32-byte payload. This
    // different spelling decodes to the same bytes but must not be accepted.
    expect(() =>
      parseAuthSecret("jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB"),
    ).toThrow(/canonical/);
  });
});
