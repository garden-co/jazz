import { createHmac, createSign, generateKeyPairSync } from "node:crypto";
import { createServer, type Server } from "node:http";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  isRequestAuthenticationError,
  RequestAuthenticationError,
  resolveRequestSession,
  type BackendRequestAuthConfig,
} from "./request-auth.js";

// Which failures are the caller's (RequestAuthenticationError, answer 401) and
// which are the server's (a plain Error, answer 5xx).

const KID = "request-auth-errors-kid";
const SECRET = "request-auth-errors-secret";
const ISSUER = "https://issuer.example";

const mocks = vi.hoisted(() => ({ verifyLocalFirstIdentityProof: vi.fn() }));
vi.mock("jazz-napi", () => ({
  verifyLocalFirstIdentityProof: mocks.verifyLocalFirstIdentityProof,
}));

const b64 = (input: string | Buffer) =>
  (typeof input === "string" ? Buffer.from(input) : input)
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");

function jwt(payload: Record<string, unknown>, secret = SECRET) {
  const head = b64(JSON.stringify({ alg: "HS256", typ: "JWT", kid: KID }));
  const body = b64(JSON.stringify(payload));
  const signature = b64(createHmac("sha256", secret).update(`${head}.${body}`).digest());
  return `${head}.${body}.${signature}`;
}

const bearer = (token: string) => ({ headers: { authorization: `Bearer ${token}` } });
const staticKey = { kty: "oct", kid: KID, alg: "HS256", k: b64(SECRET) } as const;
const staticConfig: BackendRequestAuthConfig = { appId: "app", jwtPublicKey: staticKey };
const valid = () => jwt({ iss: ISSUER, sub: "reader" });

async function authFailure(promise: Promise<unknown>) {
  const error = await promise.then(
    () => {
      throw new Error("expected resolveRequestSession to reject");
    },
    (cause: unknown) => cause,
  );
  expect(error).toBeInstanceOf(RequestAuthenticationError);
  expect(isRequestAuthenticationError(error)).toBe(true);
  return error as RequestAuthenticationError;
}

async function serverFailure(promise: Promise<unknown>) {
  const error = await promise.then(
    () => {
      throw new Error("expected resolveRequestSession to reject");
    },
    (cause: unknown) => cause,
  );
  expect(error).toBeInstanceOf(Error);
  expect(isRequestAuthenticationError(error)).toBe(false);
  return error as Error;
}

let servers: Server[] = [];

/** Serves `handler` on a loopback port and returns its base URL. */
async function serve(handler: Parameters<typeof createServer>[1]) {
  const server = createServer(handler);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("no test port");
  return `http://127.0.0.1:${address.port}`;
}

/** A loopback URL nothing listens on. */
async function closedUrl() {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("no test port");
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return `http://127.0.0.1:${address.port}`;
}

beforeEach(() => mocks.verifyLocalFirstIdentityProof.mockReset());
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(
    servers.map((server) => new Promise<void>((resolve) => server.close(() => resolve()))),
  );
  servers = [];
});

describe("client-caused request auth failures are RequestAuthenticationErrors", () => {
  it("rejects a missing, non-bearer or empty Authorization header", async () => {
    await authFailure(resolveRequestSession({ headers: {} }, staticConfig));
    await authFailure(
      resolveRequestSession({ headers: { authorization: "Basic abc" } }, staticConfig),
    );
    await authFailure(resolveRequestSession(bearer(" "), staticConfig));
  });

  it("rejects a token that is not a JWT", async () => {
    await authFailure(resolveRequestSession(bearer("not-a-jwt"), staticConfig));
  });

  it("rejects a bad signature against a static key", async () => {
    const error = await authFailure(
      resolveRequestSession(bearer(jwt({ iss: ISSUER, sub: "reader" }, "wrong")), staticConfig),
    );
    expect(error.message).toMatch(/^Invalid JWT/);
  });

  it("rejects a bad signature against a JWKS", async () => {
    const url = await serve((_request, response) =>
      response
        .writeHead(200, { "Content-Type": "application/json" })
        .end(JSON.stringify({ keys: [{ kty: "oct", kid: KID, k: b64(SECRET) }] })),
    );
    await authFailure(
      resolveRequestSession(bearer(jwt({ iss: ISSUER, sub: "reader" }, "wrong")), {
        appId: "app-jwks-bad-signature",
        jwksUrl: `${url}/jwks`,
      }),
    );
  });

  it("rejects an expired token, another issuer, another audience and a reserved issuer", async () => {
    const past = Math.floor(Date.now() / 1000) - 60;
    await authFailure(
      resolveRequestSession(bearer(jwt({ iss: ISSUER, sub: "reader", exp: past })), staticConfig),
    );
    await authFailure(
      resolveRequestSession(bearer(valid()), { ...staticConfig, jwtIssuer: "https://other" }),
    );
    await authFailure(
      resolveRequestSession(bearer(jwt({ iss: ISSUER, sub: "reader", aud: "a" })), {
        ...staticConfig,
        jwtAudience: "b",
      }),
    );
    await authFailure(
      resolveRequestSession(bearer(jwt({ iss: "urn:jazz:system", sub: "reader" })), staticConfig),
    );
  });

  it("rejects an invalid local-first proof, and local-first tokens where they are disabled", async () => {
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: false });
    const local = jwt({ iss: "urn:jazz:local-first", sub: "local" });
    await authFailure(resolveRequestSession(bearer(local), staticConfig));
    await authFailure(
      resolveRequestSession(bearer(local), { ...staticConfig, allowLocalFirstAuth: false }),
    );
  });

  it.each([
    [401, "account_request_failed"],
    [403, "identity_not_authorized"],
    [404, "identity_not_assigned"],
    [409, "identity_already_assigned"],
    [400, "use_local_first_founding"],
    [400, "local_first_proof_required"],
  ])("rejects an identity the account registry refuses with %i %s", async (status, code) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(status === 401 ? "invalid account credential" : code, { status }),
    );
    const error = await authFailure(
      resolveRequestSession(bearer(valid()), {
        ...staticConfig,
        accountRegistry: "https://core.example/apps/app/accounts",
      }),
    );
    expect(error.code).toBe(code);
  });

  describe("with a static asymmetric key", () => {
    const rsa = generateKeyPairSync("rsa", { modulusLength: 2048 });
    const rsaPem = rsa.publicKey.export({ type: "spki", format: "pem" }).toString();
    const ec = generateKeyPairSync("ec", { namedCurve: "P-256" });
    const ecJwk = { ...ec.publicKey.export({ format: "jwk" }), kid: KID };
    const payload = b64(JSON.stringify({ iss: ISSUER, sub: "reader" }));
    const unsigned = (alg: string) => `${b64(JSON.stringify({ alg, typ: "JWT" }))}.${payload}`;
    const rs256 = () => {
      const input = unsigned("RS256");
      return `${input}.${b64(createSign("RSA-SHA256").update(input).sign(rsa.privateKey))}`;
    };
    const hmacSigned = (alg: string, secret: string) =>
      `${unsigned(alg)}.${b64(createHmac("sha256", secret).update(unsigned(alg)).digest())}`;

    it("accepts a token signed for the configured key", async () => {
      await expect(
        resolveRequestSession(bearer(rs256()), { appId: "app", jwtPublicKey: rsaPem }),
      ).resolves.toMatchObject({ user_id: "reader" });
    });

    it.each([
      ["HS256 with the public key as its secret", hmacSigned("HS256", rsaPem)],
      ["none", `${unsigned("none")}.`],
      ["ES256", `${unsigned("ES256")}.${b64("signature")}`],
      ["an unknown algorithm", `${unsigned("XX999")}.${b64("signature")}`],
    ])("rejects a token for an RSA PEM key that uses %s", async (_alg, token) => {
      const error = await authFailure(
        resolveRequestSession(bearer(token), { appId: "app", jwtPublicKey: rsaPem }),
      );
      expect(error.message).toMatch(/^Invalid JWT/);
    });

    it("rejects an HS256 token for an EC JWK", async () => {
      await authFailure(
        resolveRequestSession(bearer(hmacSigned("HS256", "anything")), {
          appId: "app",
          jwtPublicKey: ecJwk,
        }),
      );
    });
  });

  it("recognises the error from another copy of jazz-tools by name", () => {
    const foreign = Object.assign(new Error("Missing or invalid Authorization header"), {
      name: "RequestAuthenticationError",
    });
    expect(isRequestAuthenticationError(foreign)).toBe(true);
    expect(isRequestAuthenticationError(new Error("Unable to fetch JWKS: HTTP 503"))).toBe(false);
    expect(isRequestAuthenticationError("RequestAuthenticationError")).toBe(false);
  });
});

describe("server-side request auth failures stay plain errors", () => {
  it("fails on an unusable jwksUrl", async () => {
    await serverFailure(
      resolveRequestSession(bearer(valid()), {
        appId: "app-jwks-not-a-url",
        jwksUrl: "not a url",
      }),
    );
    await serverFailure(
      resolveRequestSession(bearer(valid()), {
        appId: "app-jwks-plaintext",
        jwksUrl: "http://jwks.example/keys",
      }),
    );
  });

  it("fails when the JWKS endpoint is unreachable", async () => {
    const error = await serverFailure(
      resolveRequestSession(bearer(valid()), {
        appId: "app-jwks-unreachable",
        jwksUrl: `${await closedUrl()}/jwks`,
      }),
    );
    expect(error.message).toMatch(/Unable to fetch JWKS/);
  });

  it("fails when the JWKS endpoint errors or answers garbage", async () => {
    const down = await serve((_request, response) => response.writeHead(503).end());
    await serverFailure(
      resolveRequestSession(bearer(valid()), { appId: "app-jwks-503", jwksUrl: `${down}/jwks` }),
    );
    const garbage = await serve((_request, response) =>
      response.writeHead(200, { "Content-Type": "application/json" }).end('{"keys":[]}'),
    );
    await serverFailure(
      resolveRequestSession(bearer(valid()), {
        appId: "app-jwks-empty",
        jwksUrl: `${garbage}/jwks`,
      }),
    );
  });

  it("fails when the JWKS refresh after an unknown key cannot be fetched", async () => {
    let now = 5_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    let up = true;
    const url = await serve((_request, response) => {
      if (!up) return void response.writeHead(503).end();
      response
        .writeHead(200, { "Content-Type": "application/json" })
        .end(JSON.stringify({ keys: [{ kty: "oct", kid: KID, k: b64(SECRET) }] }));
    });
    const config = { appId: "app-jwks-refresh-outage", jwksUrl: `${url}/jwks` };
    await expect(resolveRequestSession(bearer(valid()), config)).resolves.toMatchObject({
      user_id: "reader",
    });

    // Past the forced-refresh cooldown, a token signed with a key the cached
    // JWKS lacks makes core refetch, and the provider is down.
    now += 31_000;
    up = false;
    const error = await serverFailure(
      resolveRequestSession(bearer(jwt({ iss: ISSUER, sub: "reader" }, "rotated")), config),
    );
    expect(error.message).toMatch(/HTTP 503/);
  });

  it("fails on an unusable configured public key", async () => {
    await serverFailure(
      resolveRequestSession(bearer(valid()), { appId: "app", jwtPublicKey: "not a key" }),
    );
  });

  it("fails when no way to verify external tokens is configured", async () => {
    await serverFailure(resolveRequestSession(bearer(valid()), { appId: "app" }));
  });

  it("fails when the account registry is down or answers garbage", async () => {
    const config = { ...staticConfig, accountRegistry: "https://core.example/apps/app/accounts" };
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response("account_registry_unavailable", { status: 503 }));
    await serverFailure(resolveRequestSession(bearer(valid()), config));
    fetcher.mockRejectedValueOnce(new TypeError("fetch failed"));
    await serverFailure(resolveRequestSession(bearer(valid()), config));
    fetcher.mockResolvedValueOnce(new Response(JSON.stringify({ account: "nope" })));
    await serverFailure(resolveRequestSession(bearer(valid()), config));
  });

  it("fails when the registry answers 403 or 404 without a rejection code", async () => {
    // Core's app gate answers a wrong app id or registry path with a bare 404:
    // a misconfiguration must not sign every user out.
    const config = { ...staticConfig, accountRegistry: "https://core.example/apps/app/accounts" };
    const fetcher = vi.spyOn(globalThis, "fetch");
    for (const response of [
      new Response(null, { status: 404 }),
      new Response("not found", { status: 404 }),
      new Response(null, { status: 403 }),
      new Response("identity_not_authorized", { status: 404 }),
      new Response("identity_not_assigned", { status: 500 }),
    ]) {
      fetcher.mockResolvedValueOnce(response);
      await serverFailure(resolveRequestSession(bearer(valid()), config));
    }
  });
});
