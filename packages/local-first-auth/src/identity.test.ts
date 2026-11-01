import { describe, expect, it } from "vitest";
import vectors from "../test-vectors.json" with { type: "json" };
import {
  generateSecret,
  identityOf,
  legacyJazzStorageKey,
  LocalFirstTokenError,
  parseSecret,
  signToken,
  verifyToken,
} from "./index.js";
import { base64urlEncode, utf8 } from "./encoding.js";

const { now, audience, ttlSeconds, cases } = vectors;

describe("shared vectors with Jazz's Rust core", () => {
  it.each(cases)("derives the same subject for $secret", (c) => {
    expect(identityOf(c.secret).subject).toBe(c.subject);
  });

  it.each(cases)("mints byte-identical tokens for $secret", (c) => {
    expect(signToken(c.secret, { audience, ttlSeconds, now })).toBe(c.token);
  });

  it.each(cases)("verifies Rust-minted tokens for $secret", (c) => {
    const who = verifyToken(c.token, { audience, now });
    expect(who.subject).toBe(c.subject);
    expect(who.issuer).toBe("urn:jazz:local-first");
  });
});

describe("verifyToken", () => {
  const secret = cases[2]!.secret;
  const token = signToken(secret, { audience, now });
  const [header, claims, signature] = token.split(".") as [string, string, string];
  const withClaims = (patch: Record<string, unknown>) => {
    const decoded = JSON.parse(atob(claims.replace(/-/g, "+").replace(/_/g, "/")));
    const body = base64urlEncode(utf8(JSON.stringify({ ...decoded, ...patch })));
    return `${header}.${body}.${signature}`;
  };

  it("rejects another audience", () => {
    expect(() => verifyToken(token, { audience: "other", now })).toThrow(/audience/);
  });
  it("rejects expired tokens and tokens from the future", () => {
    expect(() => verifyToken(token, { audience, now: now + 3600 })).toThrow(/expired/);
    expect(() => verifyToken(token, { audience, now: now - 61 })).toThrow(/future/);
    expect(verifyToken(token, { audience, now: now - 60 }).subject).toBe(cases[2]!.subject);
  });
  it("rejects lifetimes above the maximum", () => {
    const long = signToken(secret, { audience, now, ttlSeconds: 7200 });
    expect(() => verifyToken(long, { audience, now })).toThrow(/lifetime/);
    expect(verifyToken(long, { audience, now, maxTtlSeconds: 7200 }).subject).toBeDefined();
  });
  it("rejects tampered claims, unknown claims and foreign keys", () => {
    expect(() => verifyToken(withClaims({ sub: cases[0]!.subject }), { audience, now })).toThrow(
      LocalFirstTokenError,
    );
    expect(() => verifyToken(withClaims({ extra: 1 }), { audience, now })).toThrow(/unknown claim/);
    const other = signToken(cases[0]!.secret, { audience, now }).split(".")[2];
    expect(() => verifyToken(`${header}.${claims}.${other}`, { audience, now })).toThrow(
      /signature/,
    );
  });
  it("rejects non-canonical base64url in the signature", () => {
    const last = signature.at(-1)!;
    const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    const sibling = alphabet[alphabet.indexOf(last) ^ 1]!;
    expect(() =>
      verifyToken(`${header}.${claims}.${signature.slice(0, -1)}${sibling}`, { audience, now }),
    ).toThrow();
  });
  it("rejects malformed input", () => {
    for (const bad of ["", "a.b", "a.b.c", `${header}.${claims}`]) {
      expect(() => verifyToken(bad, { audience, now })).toThrow(LocalFirstTokenError);
    }
  });
});

describe("secrets", () => {
  it("rejects padded and non-canonical secrets", () => {
    expect(() => parseSecret("jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")).toThrow(
      /43 unpadded/,
    );
    // Same bytes as ...AAA, but with a non-zero unused trailing bit.
    expect(() => parseSecret("jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB")).toThrow(
      /canonical/,
    );
  });
  it("round-trips generated secrets", () => {
    const secret = generateSecret();
    expect(secret).toMatch(/^jazz-auth-v1:[A-Za-z0-9_-]{43}$/);
    expect(parseSecret(secret)).toHaveLength(32);
  });
  it("computes the storage keys jazz-tools used up to alpha.59", () => {
    // Values from jazz-tools' authSecretStorageKey({ appId, profile }).
    expect(legacyJazzStorageKey("chat-app")).toBe(
      "jazz-auth-store-v1-fwE2Hmmz8A0W6gji0yM1i7tD3GxrQW7lahy5g9s9m18",
    );
    expect(legacyJazzStorageKey("chat-app", "alice")).toBe(
      "jazz-auth-store-v1-ELxnntGic7Xl4bdw9edWdyBgwjC_G-fte6I7041VVXQ",
    );
  });
});
