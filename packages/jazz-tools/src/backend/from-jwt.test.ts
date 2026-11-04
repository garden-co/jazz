import { createHmac } from "node:crypto";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { fromJWT } from "./from-jwt.js";

const ISSUER = "https://idp.example";
const AUDIENCE = "my-app";
let jwksCounter = 0;
/** A JWKS URL of its own per test: the verifier caches documents by URL. */
const jwksUrl = () => `https://idp.example/jwks/${++jwksCounter}`;

let privateKey: CryptoKey;
let otherKey: CryptoKey;
let rotatedKey: CryptoKey;
let jwk: Record<string, unknown>;
let rotatedJwk: Record<string, unknown>;

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  jwk = { ...(await exportJWK(pair.publicKey)), kid: "k1", alg: "RS256" };
  otherKey = (await generateKeyPair("RS256")).privateKey;
  const rotated = await generateKeyPair("RS256", { extractable: true });
  rotatedKey = rotated.privateKey;
  rotatedJwk = { ...(await exportJWK(rotated.publicKey)), kid: "k2", alg: "RS256" };
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

const now = () => Math.floor(Date.now() / 1000);

function sign(
  claims: Record<string, unknown>,
  {
    key = privateKey,
    kid = "k1",
    issuer = ISSUER,
    audience = AUDIENCE as string | null,
    expires = "5m" as string | number | null,
  } = {},
) {
  const jwt = new SignJWT(claims)
    .setProtectedHeader({ alg: "RS256", kid })
    .setIssuer(issuer)
    .setSubject("user_01")
    .setIssuedAt();
  if (audience !== null) jwt.setAudience(audience);
  if (expires !== null) jwt.setExpirationTime(expires);
  return jwt.sign(key);
}

function base64Url(value: string | Buffer) {
  return Buffer.from(value).toString("base64url");
}

function request(token?: string) {
  return new Request("https://app.example/jazz", {
    headers: token ? { authorization: `Bearer ${token}` } : {},
  });
}

function serveJwks(keys: () => unknown[] = () => [jwk]) {
  const fetch = vi.fn(async () => Response.json({ keys: keys() }));
  vi.stubGlobal("fetch", fetch);
  return fetch;
}

describe("fromJWT", () => {
  it("names the user by sub and passes the provider's claims", async () => {
    serveJwks();
    const authenticate = fromJWT({ jwksUrl: jwksUrl(), issuer: ISSUER, audience: AUDIENCE });
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
      audience: false,
      claims: (payload) => ({ role: payload.role }),
    });
    const token = await sign({ role: "member", sid: "session_1" }, { audience: null });
    expect(await authenticate(request(token))).toEqual({
      userId: "user_01",
      claims: { role: "member" },
    });
  });

  it("reads the token from where the app keeps it", async () => {
    serveJwks();
    const token = await sign({});
    const authenticate = fromJWT({
      jwksUrl: jwksUrl(),
      issuer: ISSUER,
      audience: AUDIENCE,
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
    const authenticate = fromJWT({ jwksUrl: jwksUrl(), issuer: ISSUER, audience: AUDIENCE });
    expect(await authenticate(request())).toBeNull();
    expect(await authenticate(request("not-a-jwt"))).toBeNull();
    // Wrong signer, wrong issuer, another app's audience, no audience.
    expect(await authenticate(request(await sign({}, { key: otherKey })))).toBeNull();
    expect(
      await authenticate(request(await sign({}, { issuer: "https://evil.example" }))),
    ).toBeNull();
    expect(await authenticate(request(await sign({}, { audience: "other-app" })))).toBeNull();
    expect(await authenticate(request(await sign({}, { audience: null })))).toBeNull();
    expect((await authenticate(request(await sign({}))))?.userId).toBe("user_01");
  });

  it("refuses unsigned and HMAC tokens against an asymmetric key", async () => {
    const authenticate = fromJWT({ publicKey: jwk as never, issuer: ISSUER, audience: AUDIENCE });
    const payload = base64Url(
      JSON.stringify({ iss: ISSUER, sub: "user_01", aud: AUDIENCE, exp: now() + 300 }),
    );
    const none = `${base64Url(JSON.stringify({ alg: "none", typ: "JWT" }))}.${payload}.`;
    expect(await authenticate(request(none))).toBeNull();
    // HS256 keyed with the public key's own bytes: the classic confusion.
    const header = base64Url(JSON.stringify({ alg: "HS256", typ: "JWT", kid: "k1" }));
    const mac = createHmac("sha256", JSON.stringify(jwk))
      .update(`${header}.${payload}`)
      .digest("base64url");
    expect(await authenticate(request(`${header}.${payload}.${mac}`))).toBeNull();
  });

  it("requires exp and allows a minute of clock skew on exp and nbf", async () => {
    const authenticate = fromJWT({ publicKey: jwk as never, issuer: ISSUER, audience: AUDIENCE });
    expect(await authenticate(request(await sign({}, { expires: null })))).toBeNull();
    // Expired 30s ago: within the tolerance. Two minutes ago: refused.
    expect((await authenticate(request(await sign({}, { expires: now() - 30 }))))?.userId).toBe(
      "user_01",
    );
    expect(await authenticate(request(await sign({}, { expires: now() - 120 })))).toBeNull();
    // Valid 30s from now (the server's clock is behind): accepted. In ten minutes: refused.
    expect((await authenticate(request(await sign({ nbf: now() + 30 }))))?.userId).toBe("user_01");
    expect(await authenticate(request(await sign({ nbf: now() + 600 })))).toBeNull();
    const strict = fromJWT({
      publicKey: jwk as never,
      issuer: ISSUER,
      audience: AUDIENCE,
      clockToleranceSeconds: 0,
    });
    expect(await strict(request(await sign({}, { expires: now() - 30 })))).toBeNull();
  });

  it("picks up a rotated key by refetching the JWKS", async () => {
    let keys = [jwk];
    const fetch = serveJwks(() => keys);
    const authenticate = fromJWT({ jwksUrl: jwksUrl(), issuer: ISSUER, audience: AUDIENCE });
    expect((await authenticate(request(await sign({}))))?.userId).toBe("user_01");
    // Past the refresh cooldown that follows every fetch.
    vi.useFakeTimers({ toFake: ["Date"], now: Date.now() + 31_000 });
    keys = [jwk, rotatedJwk];
    const rotatedToken = await sign({}, { key: rotatedKey, kid: "k2" });
    expect((await authenticate(request(rotatedToken)))?.userId).toBe("user_01");
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("throws when the JWKS cannot be fetched at all, so clients retry", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("down", { status: 503 })),
    );
    const authenticate = fromJWT({ jwksUrl: jwksUrl(), issuer: ISSUER, audience: AUDIENCE });
    await expect(authenticate(request(await sign({})))).rejects.toThrow(/Unable to fetch JWKS/);
  });

  it("keeps an unknown key retrying through an outage instead of signing it out", async () => {
    let up = true;
    const fetch = vi.fn(async () =>
      up ? Response.json({ keys: [jwk] }) : new Response("down", { status: 503 }),
    );
    vi.stubGlobal("fetch", fetch);
    const start = Date.now();
    const authenticate = fromJWT({ jwksUrl: jwksUrl(), issuer: ISSUER, audience: AUDIENCE });
    expect((await authenticate(request(await sign({}))))?.userId).toBe("user_01");
    vi.useFakeTimers({ toFake: ["Date"], now: start + 31_000 });
    up = false;
    const unknown = await sign({}, { key: otherKey, kid: "k9" });
    // The refresh fails: the request throws, so the client retries.
    await expect(authenticate(request(unknown))).rejects.toThrow(/JWKS/);
    // Within the cooldown no refetch, but the retry still fails rather than
    // being refused by keys that predate the outage.
    await expect(authenticate(request(unknown))).rejects.toThrow(/JWKS/);
    expect(fetch).toHaveBeenCalledTimes(2);
    // Known keys still work from the cache.
    expect((await authenticate(request(await sign({}))))?.userId).toBe("user_01");
    // Once the provider is back and the cooldown is over, a refresh that
    // succeeds is what refuses the unknown key.
    up = true;
    vi.setSystemTime(start + 62_000);
    expect(await authenticate(request(unknown))).toBeNull();
    expect(fetch).toHaveBeenCalledTimes(3);
  });

  it("refuses configurations it cannot verify safely", () => {
    const url = jwksUrl();
    expect(() => fromJWT({ issuer: ISSUER, audience: AUDIENCE })).toThrow(
      /exactly one of jwksUrl and publicKey/,
    );
    expect(() => fromJWT({ jwksUrl: url, issuer: "", audience: AUDIENCE })).toThrow(
      /issuer is required/,
    );
    expect(() => fromJWT({ jwksUrl: url, issuer: ISSUER } as never)).toThrow(
      /audience is required/,
    );
    expect(() => fromJWT({ jwksUrl: url, issuer: ISSUER, audience: "" })).toThrow(
      /audience is required/,
    );
    expect(() => fromJWT({ jwksUrl: url, issuer: "jazz:app:other", audience: AUDIENCE })).toThrow(
      /reserved/,
    );
    expect(() =>
      fromJWT({ jwksUrl: url, issuer: "urn:jazz:local-first", audience: AUDIENCE }),
    ).toThrow(/reserved/);
  });
});
