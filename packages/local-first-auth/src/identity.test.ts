import { ed25519 } from "@noble/curves/ed25519.js";
import { sha512 } from "@noble/hashes/sha2.js";
import { describe, expect, it } from "vitest";
import vectors from "../test-vectors.json" with { type: "json" };
import {
  generateSecret,
  identityOf,
  secretStorageKey,
  LocalFirstTokenError,
  normalizeAudience,
  parseSecret,
  signToken,
  verifyToken,
} from "./index.js";
import { base64urlEncode, utf8 } from "./encoding.js";

const { now, audience, ttlSeconds, cases, audienceSecret, audiences } = vectors;

const json64 = (value: unknown) => base64urlEncode(utf8(JSON.stringify(value)));

/** A correctly signed token with any header and claims, signed by `secret`'s key. */
function forge(secret: string, header: unknown, claims: Record<string, unknown>): string {
  const seed = parseSecret(secret);
  const key = sha512(new Uint8Array([...utf8("jazz-auth-sign-v1"), ...seed])).slice(0, 32);
  const body = `${json64(header)}.${json64(claims)}`;
  return `${body}.${base64urlEncode(ed25519.sign(utf8(body), key))}`;
}

function claimsFor(secret: string, patch: Record<string, unknown> = {}) {
  const { subject, publicKey } = identityOf(secret);
  return {
    iss: "urn:jazz:local-first",
    sub: subject,
    aud: audience,
    jazz_pub_key: base64urlEncode(publicKey),
    iat: now,
    exp: now + 600,
    ...patch,
  };
}

const EDDSA = { alg: "EdDSA", typ: "JWT" };

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

describe("audience normalization shared with Rust", () => {
  it.each(audiences)("normalizes $audience like Rust", (c) => {
    expect(normalizeAudience(c.audience)).toBe(c.aud);
  });
  it.each(audiences)("mints Rust's token for $audience", (c) => {
    expect(signToken(audienceSecret, { audience: c.audience, ttlSeconds, now })).toBe(c.token);
  });
  it.each(audiences)("verifies Rust's token for $audience in either form", (c) => {
    expect(verifyToken(c.token, { audience: c.audience, now }).subject).toBeDefined();
    expect(verifyToken(c.token, { audience: c.aud, now }).subject).toBeDefined();
  });
  it("requires a non-empty audience on both sides", () => {
    const token = signToken(audienceSecret, { audience, now });
    for (const bad of [undefined, "", null, 7]) {
      expect(() => signToken(audienceSecret, { audience: bad as never, now })).toThrow(/audience/);
      expect(() => verifyToken(token, { audience: bad as never, now })).toThrow(/audience/);
    }
  });
});

describe("signToken", () => {
  it("rejects invalid lifetimes and clocks", () => {
    for (const ttl of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => signToken(audienceSecret, { audience, now, ttlSeconds: ttl })).toThrow(/ttl/);
    }
    expect(() => signToken(audienceSecret, { audience, now: 1.5 })).toThrow(/now/);
  });
});

describe("verifyToken attack cases", () => {
  const alice = cases[0]!.secret;
  const mallory = cases[1]!.secret;

  it("accepts a well-formed forged-by-owner token (the helper is sound)", () => {
    expect(verifyToken(forge(alice, EDDSA, claimsFor(alice)), { audience, now }).subject).toBe(
      cases[0]!.subject,
    );
  });
  it("rejects a correctly signed token by key B that claims A's subject", () => {
    const token = forge(mallory, EDDSA, claimsFor(mallory, { sub: cases[0]!.subject }));
    expect(() => verifyToken(token, { audience, now })).toThrow(/subject does not match/);
  });
  it("rejects A's key claimed in a token signed by B", () => {
    const token = forge(mallory, EDDSA, claimsFor(alice));
    expect(() => verifyToken(token, { audience, now })).toThrow(/signature/);
  });
  it("rejects alg none, HS256 and a missing alg", () => {
    for (const header of [
      { alg: "none", typ: "JWT" },
      { alg: "HS256", typ: "JWT" },
      { typ: "JWT" },
    ]) {
      expect(() => verifyToken(forge(alice, header, claimsFor(alice)), { audience, now })).toThrow(
        /alg/,
      );
    }
    const unsigned = `${json64({ alg: "none" })}.${json64(claimsFor(alice))}.`;
    expect(() => verifyToken(unsigned, { audience, now })).toThrow(LocalFirstTokenError);
  });
  it("rejects a small-order public key and a small-order R", () => {
    const identityPoint = new Uint8Array(32);
    identityPoint[0] = 1;
    const smallKey = forge(
      alice,
      EDDSA,
      claimsFor(alice, { jazz_pub_key: base64urlEncode(identityPoint) }),
    );
    expect(() => verifyToken(smallKey, { audience, now })).toThrow(/signature/);

    const [h, c] = forge(alice, EDDSA, claimsFor(alice)).split(".");
    const smallR = new Uint8Array(64);
    smallR.set(identityPoint);
    expect(() => verifyToken(`${h}.${c}.${base64urlEncode(smallR)}`, { audience, now })).toThrow(
      /signature/,
    );
  });
  it("rejects a missing aud, the anonymous issuer and a wrong issuer", () => {
    const { aud: _aud, ...noAud } = claimsFor(alice);
    expect(() => verifyToken(forge(alice, EDDSA, noAud), { audience, now })).toThrow(/audience/);
    for (const iss of ["urn:jazz:anonymous", "urn:other", undefined]) {
      const token = forge(alice, EDDSA, claimsFor(alice, { iss }));
      expect(() => verifyToken(token, { audience, now })).toThrow(/issuer/);
    }
  });
  it("rejects non-integer, missing and negative iat or exp", () => {
    for (const patch of [
      { iat: now + 0.5 },
      { exp: now + 600.5 },
      { iat: String(now) },
      { exp: undefined },
      { iat: -1 },
    ]) {
      const token = forge(alice, EDDSA, claimsFor(alice, patch));
      expect(() => verifyToken(token, { audience, now })).toThrow(LocalFirstTokenError);
    }
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
      verifyToken(`${header}.${claims}.${signature.slice(0, -1)}${sibling}`, {
        audience,
        now,
      }),
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
    expect(secretStorageKey("chat-app")).toBe(
      "jazz-auth-store-v1-fwE2Hmmz8A0W6gji0yM1i7tD3GxrQW7lahy5g9s9m18",
    );
    expect(secretStorageKey("chat-app", "alice")).toBe(
      "jazz-auth-store-v1-ELxnntGic7Xl4bdw9edWdyBgwjC_G-fte6I7041VVXQ",
    );
  });
});
