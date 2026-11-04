import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { fromJWT } from "./from-jwt.js";
import { canonicalAppId } from "./create-jazz.js";

const ISSUER = "https://api.workos.com/user_management/client_test";
const JWKS_URL = "https://api.workos.com/sso/jwks/client_test";

let privateKey: CryptoKey;
let otherKey: CryptoKey;
let jwk: Record<string, unknown>;

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  jwk = { ...(await exportJWK(pair.publicKey)), kid: "k1", alg: "RS256" };
  otherKey = (await generateKeyPair("RS256")).privateKey;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function sign(
  claims: Record<string, unknown>,
  { key = privateKey, issuer = ISSUER, expires = "5m" as string | number } = {},
) {
  return new SignJWT(claims)
    .setProtectedHeader({ alg: "RS256", kid: "k1" })
    .setIssuer(issuer)
    .setSubject("user_01")
    .setIssuedAt()
    .setExpirationTime(expires)
    .sign(key);
}

function request(token?: string) {
  return new Request("https://app.example/jazz", {
    headers: token ? { authorization: `Bearer ${token}` } : {},
  });
}

function serveJwks() {
  const fetch = vi.fn(async () => Response.json({ keys: [jwk] }));
  vi.stubGlobal("fetch", fetch);
  return fetch;
}

describe("fromJWT", () => {
  it("names the user by sub and passes the provider's claims", async () => {
    serveJwks();
    const authenticate = fromJWT({ jwksUrl: JWKS_URL, issuer: ISSUER });
    const token = await sign({ role: "admin", permissions: ["posts:write"], org_id: "org_1" });
    expect(await authenticate(request(token))).toEqual({
      userId: "user_01",
      claims: { role: "admin", permissions: ["posts:write"], org_id: "org_1" },
    });
  });

  it("verifies with a static public key and shapes claims on request", async () => {
    const authenticate = fromJWT({
      publicKey: jwk as never,
      issuer: ISSUER,
      claims: (payload) => ({ role: payload.role }),
    });
    const token = await sign({ role: "member", sid: "session_1" });
    expect(await authenticate(request(token))).toEqual({
      userId: "user_01",
      claims: { role: "member" },
    });
  });

  it("reads the token from where the app keeps it", async () => {
    serveJwks();
    const token = await sign({});
    const authenticate = fromJWT({
      jwksUrl: JWKS_URL,
      issuer: ISSUER,
      token: (req) => req.headers.get("x-access-token"),
    });
    const withHeader = new Request("https://app.example/jazz", {
      headers: { "x-access-token": token },
    });
    expect((await authenticate(withHeader))?.userId).toBe("user_01");
    expect(await authenticate(request(token))).toBeNull();
  });

  it("signs out requests without a token or with one that does not verify", async () => {
    serveJwks();
    const authenticate = fromJWT({ jwksUrl: JWKS_URL, issuer: ISSUER, audience: "my-app" });
    expect(await authenticate(request())).toBeNull();
    expect(await authenticate(request("not-a-jwt"))).toBeNull();
    // Wrong signer, wrong issuer, wrong audience, expired.
    expect(
      await authenticate(request(await sign({ aud: "my-app" }, { key: otherKey }))),
    ).toBeNull();
    expect(
      await authenticate(
        request(await sign({ aud: "my-app" }, { issuer: "https://evil.example" })),
      ),
    ).toBeNull();
    expect(await authenticate(request(await sign({ aud: "other" })))).toBeNull();
    expect(
      await authenticate(
        request(await sign({ aud: "my-app" }, { expires: Math.floor(Date.now() / 1000) - 10 })),
      ),
    ).toBeNull();
    expect((await authenticate(request(await sign({ aud: "my-app" }))))?.userId).toBe("user_01");
  });

  it("throws when the JWKS cannot be fetched, so clients retry", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("down", { status: 503 })),
    );
    const authenticate = fromJWT({
      jwksUrl: "https://unreachable.example/jwks",
      issuer: ISSUER,
    });
    await expect(authenticate(request(await sign({})))).rejects.toThrow(/Unable to fetch JWKS/);
  });

  it("refuses configurations it cannot verify safely", () => {
    expect(() => fromJWT({ issuer: ISSUER })).toThrow(/exactly one of jwksUrl and publicKey/);
    expect(() => fromJWT({ jwksUrl: JWKS_URL, issuer: "" })).toThrow(/issuer is required/);
    expect(() => fromJWT({ jwksUrl: JWKS_URL, issuer: "jazz:app:other" })).toThrow(/reserved/);
    expect(() => fromJWT({ jwksUrl: JWKS_URL, issuer: "urn:jazz:local-first" })).toThrow(
      /reserved/,
    );
  });
});

describe("canonicalAppId", () => {
  it("spells a UUID app id one way", () => {
    const canonical = "0f8fad5b-d9cb-469f-a165-70867728950e";
    expect(canonicalAppId(canonical)).toBe(canonical);
    expect(canonicalAppId(canonical.toUpperCase())).toBe(canonical);
    expect(canonicalAppId(canonical.replaceAll("-", ""))).toBe(canonical);
    expect(canonicalAppId("my-app")).toBe("my-app");
  });
});
