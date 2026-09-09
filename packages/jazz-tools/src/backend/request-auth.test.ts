import { createHmac, randomUUID } from "node:crypto";
import { createServer as createHttpServer, type Server as HttpServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resolveRequestSession } from "./request-auth.js";

const JWT_KID = "backend-request-test-kid";
const JWT_SECRET = "backend-request-test-secret";

const mocks = vi.hoisted(() => ({
  verifyLocalFirstIdentityProof: vi.fn(),
}));

vi.mock("jazz-napi", () => ({
  verifyLocalFirstIdentityProof: mocks.verifyLocalFirstIdentityProof,
}));

function base64Url(input: Buffer | string): string {
  const encoded =
    typeof input === "string"
      ? Buffer.from(input, "utf8").toString("base64")
      : input.toString("base64");
  return encoded.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

function signHs256Jwt(payload: Record<string, unknown>, secret = JWT_SECRET): string {
  const header = {
    alg: "HS256",
    typ: "JWT",
    kid: JWT_KID,
  };
  const headerB64 = base64Url(JSON.stringify(header));
  const payloadB64 = base64Url(JSON.stringify(payload));
  const signature = createHmac("sha256", secret)
    .update(`${headerB64}.${payloadB64}`, "utf8")
    .digest("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
  return `${headerB64}.${payloadB64}.${signature}`;
}

function makeUnsignedJwt(payload: Record<string, unknown>): string {
  return `${base64Url(JSON.stringify({ alg: "none", typ: "JWT" }))}.${base64Url(JSON.stringify(payload))}.signature`;
}

async function getAvailablePort(): Promise<number> {
  const server = createNetServer();
  return await new Promise<number>((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("failed to allocate test port"));
        return;
      }
      const { port } = address;
      server.close((error) => {
        if (error) reject(error);
        else resolve(port);
      });
    });
  });
}

class JwksServer {
  private readonly server: HttpServer;
  readonly url: string;
  requests = 0;
  private secret: string;
  private failuresRemaining = 0;

  private constructor(server: HttpServer, url: string, secret: string) {
    this.server = server;
    this.url = url;
    this.secret = secret;
  }

  static async start(secret = JWT_SECRET, path = "/jwks"): Promise<JwksServer> {
    let instance: JwksServer;
    const server = createHttpServer((request, response) => {
      if (request.url !== path) {
        response.statusCode = 404;
        response.end("not found");
        return;
      }
      instance.requests += 1;
      if (instance.failuresRemaining > 0) {
        instance.failuresRemaining -= 1;
        response.statusCode = 503;
        response.end("temporarily unavailable");
        return;
      }

      response.statusCode = 200;
      response.setHeader("Content-Type", "application/json");
      response.end(
        JSON.stringify({
          keys: [
            {
              kty: "oct",
              kid: JWT_KID,
              k: base64Url(instance.secret),
            },
          ],
        }),
      );
    });

    const port = await getAvailablePort();
    await new Promise<void>((resolve, reject) => {
      server.listen(port, "127.0.0.1", (error?: unknown) => {
        if (error) reject(error);
        else resolve();
      });
    });

    instance = new JwksServer(server, `http://127.0.0.1:${port}${path}`, secret);
    return instance;
  }

  rotateKey(secret: string): void {
    this.secret = secret;
  }

  failNextFetch(): void {
    this.failuresRemaining += 1;
  }

  async stop(): Promise<void> {
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

describe("backend request auth", () => {
  const servers = new Set<JwksServer>();

  beforeEach(() => {
    mocks.verifyLocalFirstIdentityProof.mockReset();
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await Promise.all(
      Array.from(servers, async (server) => {
        servers.delete(server);
        await server.stop();
      }),
    );
  });

  it.each(["login", "login-or-register"] as const)(
    "uses the once-read verified bearer for %s",
    async (accountMode) => {
      const identity = { issuer: "https://issuer.example", subject: "reader" };
      const token = signHs256Jwt({
        iss: identity.issuer,
        sub: identity.subject,
        account: "untrusted-jwt-account",
      });
      const account = "00000000-0000-4000-8000-000000000012";
      const fetcher = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(new Response(JSON.stringify({ account, identity })));
      const header = vi
        .fn()
        .mockReturnValueOnce(`Bearer ${token}`)
        .mockImplementation(() => {
          throw new Error("request headers read twice");
        });
      const session = await resolveRequestSession(
        { header },
        {
          appId: "app",
          accountRegistry: "https://core.example/apps/app/accounts",
          jwtPublicKey: { kty: "oct", kid: JWT_KID, alg: "HS256", k: base64Url(JWT_SECRET) },
        },
        { account: accountMode },
      );
      expect(session.account_id).toBe(account);
      expect(header).toHaveBeenCalledTimes(1);
      expect(fetcher).toHaveBeenCalledWith(
        `https://core.example/apps/app/accounts/${accountMode}`,
        expect.objectContaining({
          credentials: "omit",
          redirect: "error",
          headers: expect.objectContaining({ Authorization: `Bearer ${token}` }),
        }),
      );
    },
  );

  it.each(["identity_unassigned", "identity_revoked"])(
    "rejects registry %s without registering the external identity",
    async (code) => {
      const token = signHs256Jwt({ iss: "https://issuer.example", sub: "reader" });
      const fetcher = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(new Response(code, { status: 403 }));
      await expect(
        resolveRequestSession(
          { headers: { authorization: `Bearer ${token}` } },
          {
            appId: "app",
            accountRegistry: "https://core.example/apps/app/accounts",
            jwtPublicKey: { kty: "oct", kid: JWT_KID, alg: "HS256", k: base64Url(JWT_SECRET) },
          },
        ),
      ).rejects.toMatchObject({ code });
      expect(fetcher).toHaveBeenCalledTimes(1);
      expect(fetcher.mock.calls[0]![0]).toBe("https://core.example/apps/app/accounts/login");
    },
  );

  it("rejects a registry response for a different authenticated identity", async () => {
    const token = signHs256Jwt({ iss: "https://issuer.example", sub: "reader" });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          account: "00000000-0000-4000-8000-000000000012",
          identity: { issuer: "https://issuer.example", subject: "other" },
        }),
      ),
    );
    await expect(
      resolveRequestSession(
        { headers: { authorization: `Bearer ${token}` } },
        {
          appId: "app",
          accountRegistry: "https://core.example/apps/app/accounts",
          jwtPublicKey: { kty: "oct", kid: JWT_KID, alg: "HS256", k: base64Url(JWT_SECRET) },
        },
      ),
    ).rejects.toMatchObject({ code: "invalid_account_response" });
  });

  it("does not fall back after explicit enrollment rejects a revoked identity", async () => {
    const token = signHs256Jwt({ iss: "https://issuer.example", sub: "revoked" });
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("identity_revoked", { status: 403 }));
    await expect(
      resolveRequestSession(
        { headers: { authorization: `Bearer ${token}` } },
        {
          appId: "app",
          accountRegistry: "https://core.example/apps/app/accounts",
          jwtPublicKey: { kty: "oct", kid: JWT_KID, alg: "HS256", k: base64Url(JWT_SECRET) },
        },
        { account: "login-or-register" },
      ),
    ).rejects.toMatchObject({ code: "identity_revoked" });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher.mock.calls[0]![0]).toBe(
      "https://core.example/apps/app/accounts/login-or-register",
    );
  });

  it.each(["login", "login-or-register"] as const)(
    "rejects invalid credentials before %s",
    async (accountMode) => {
      const fetcher = vi.spyOn(globalThis, "fetch");
      const config = {
        appId: "app",
        accountRegistry: "https://core.example/apps/app/accounts",
        jwtPublicKey: { kty: "oct", kid: JWT_KID, alg: "HS256", k: base64Url(JWT_SECRET) },
      };
      const badExternal = signHs256Jwt(
        { iss: "https://issuer.example", sub: "reader" },
        "wrong-key",
      );
      await expect(
        resolveRequestSession({ headers: { authorization: `Bearer ${badExternal}` } }, config, {
          account: accountMode,
        }),
      ).rejects.toThrow(/Invalid JWT/);
      mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: false });
      const badLocal = makeUnsignedJwt({ iss: "urn:jazz:local-first", sub: "local" });
      await expect(
        resolveRequestSession({ headers: { authorization: `Bearer ${badLocal}` } }, config, {
          account: accountMode,
        }),
      ).rejects.toThrow(/Invalid local-first/);
      expect(fetcher).not.toHaveBeenCalled();
    },
  );

  it("founds a verified local-first account using the exact original proof", async () => {
    const identity = {
      issuer: "urn:jazz:local-first",
      subject: "11111111-1111-1111-1111-111111111111",
    };
    const token = makeUnsignedJwt({ iss: identity.issuer, sub: identity.subject });
    const account = "00000000-0000-4000-8000-000000000012";
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: identity.subject });
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response(JSON.stringify({ account, identity })));
    const result = await resolveRequestSession(
      { headers: { authorization: `Bearer ${token}` } },
      {
        appId: "app",
        accountRegistry: "https://core.example/apps/app/accounts",
      },
    );
    expect(result.account_id).toBe(account);
    expect(fetcher).toHaveBeenCalledWith(
      "https://core.example/apps/app/accounts/found-local-first",
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: `Bearer ${token}` }),
      }),
    );
  });

  it("rejects malformed registry account UUIDs", async () => {
    const identity = { issuer: "https://issuer.example", subject: "reader" };
    const token = signHs256Jwt({ iss: identity.issuer, sub: identity.subject });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ account: "not-an-account", identity })),
    );
    await expect(
      resolveRequestSession(
        { headers: { authorization: `Bearer ${token}` } },
        {
          appId: "app",
          accountRegistry: "https://core.example/apps/app/accounts",
          jwtPublicKey: { kty: "oct", kid: JWT_KID, alg: "HS256", k: base64Url(JWT_SECRET) },
        },
      ),
    ).rejects.toMatchObject({ code: "invalid_account_response" });
  });

  it("rejects external JWTs when jwksUrl is not configured", async () => {
    const token = signHs256Jwt({
      sub: "user-subject",
      iss: "https://issuer.example",
      role: "editor",
    });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId: "app-without-jwks",
        },
      ),
    ).rejects.toThrow(/jwksUrl|jwtPublicKey/i);
  });

  it("accepts local-first JWTs without jwksUrl and uses the shared session mapping", async () => {
    const fetcher = vi.spyOn(globalThis, "fetch");
    const appId = "local-first-backend-app";
    const userId = "11111111-1111-1111-1111-111111111111";
    const token = makeUnsignedJwt({
      iss: "urn:jazz:local-first",
      sub: userId,
      auth_mode: "local-first",
    });
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: userId });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId,
        },
      ),
    ).resolves.toEqual({
      issuer: "urn:jazz:local-first",
      user_id: userId,
      // Supported custom JWT fields are policy inputs for reserved issuers
      // too; authMode is derived separately from verified issuer admission.
      claims: { auth_mode: "local-first" },
      authMode: "local-first",
    });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("rejects local-first JWTs when allowLocalFirstAuth is disabled", async () => {
    const appId = "local-first-disabled-app";
    const token = makeUnsignedJwt({
      iss: "urn:jazz:local-first",
      sub: "22222222-2222-2222-2222-222222222222",
      auth_mode: "local-first",
    });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId,
          allowLocalFirstAuth: false,
        },
      ),
    ).rejects.toThrow(/local-first/i);
  });

  it("verifies external JWTs via JWKS and returns a session keyed by sub", async () => {
    const jwks = await JwksServer.start();
    servers.add(jwks);
    const token = signHs256Jwt({
      sub: "user-subject",
      iss: "https://issuer.example",
      role: "editor",
    });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId: "app-with-jwks",
          jwksUrl: jwks.url,
        },
      ),
    ).resolves.toEqual({
      issuer: "https://issuer.example",
      user_id: "user-subject",
      claims: { role: "editor" },
      authMode: "external",
    });
  });

  it("rejects remote plaintext JWKS before fetching valid signing keys", async () => {
    const secret = randomUUID();
    const path = `/jwks/${randomUUID()}`;
    const token = signHs256Jwt({ iss: "https://issuer.example", sub: "transport-user" }, secret);
    const request = { headers: { authorization: `Bearer ${token}` } };
    const fetcher = vi.spyOn(globalThis, "fetch").mockImplementation(async () => {
      return new Response(
        JSON.stringify({
          keys: [{ kty: "oct", kid: JWT_KID, k: base64Url(secret) }],
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    });

    try {
      await expect(
        resolveRequestSession(request, {
          appId: "app-with-secure-jwks",
          jwksUrl: `https://keys.example${path}`,
        }),
      ).resolves.toMatchObject({
        issuer: "https://issuer.example",
        user_id: "transport-user",
        authMode: "external",
      });
      expect(fetcher).toHaveBeenCalledTimes(1);
      fetcher.mockClear();

      const [attempt] = await Promise.allSettled([
        resolveRequestSession(request, {
          appId: "app-with-plaintext-jwks",
          jwksUrl: `http://keys.example${path}`,
        }),
      ]);

      expect({
        status: attempt!.status,
        fetches: fetcher.mock.calls.length,
      }).toEqual({ status: "rejected", fetches: 0 });
    } finally {
      fetcher.mockRestore();
    }
  });

  it("rejects redirected JWKS without contacting the valid-key destination", async () => {
    const secret = randomUUID();
    const destination = await JwksServer.start(secret, `/jwks/${randomUUID()}`);
    const redirectPath = `/redirect/${randomUUID()}`;
    let redirectRequests = 0;
    const redirectServer = createHttpServer((request, response) => {
      if (request.url !== redirectPath) {
        response.writeHead(404).end();
        return;
      }
      redirectRequests += 1;
      response.writeHead(302, { Location: destination.url }).end();
    });

    try {
      await new Promise<void>((resolve, reject) => {
        redirectServer.once("error", reject);
        redirectServer.listen(0, "127.0.0.1", () => resolve());
      });
      const address = redirectServer.address();
      if (!address || typeof address === "string") {
        throw new Error("failed to allocate redirect test port");
      }
      const token = signHs256Jwt(
        { iss: "https://issuer.example", sub: "redirect-key-user" },
        secret,
      );
      const request = { headers: { authorization: `Bearer ${token}` } };

      await expect(
        resolveRequestSession(request, {
          appId: "app-with-direct-jwks",
          jwksUrl: destination.url,
        }),
      ).resolves.toMatchObject({
        issuer: "https://issuer.example",
        user_id: "redirect-key-user",
        authMode: "external",
      });
      expect(destination.requests).toBe(1);
      // Count only destination traffic caused by the redirected authentication attempt.
      destination.requests = 0;

      const [attempt] = await Promise.allSettled([
        resolveRequestSession(request, {
          appId: "app-with-redirected-jwks",
          jwksUrl: `http://127.0.0.1:${address.port}${redirectPath}`,
        }),
      ]);

      expect(redirectRequests).toBe(1);
      expect({
        status: attempt!.status,
        destinationRequests: destination.requests,
      }).toEqual({ status: "rejected", destinationRequests: 0 });
    } finally {
      await Promise.all([
        destination.stop(),
        new Promise<void>((resolve) => redirectServer.close(() => resolve())),
      ]);
    }
  });

  it.each([
    "http://localhost.",
    "http://localhost.example",
    "http://dev.localhost",
    "http://127.attacker.example",
    "http://126.255.255.255",
    "http://128.0.0.1",
    "http://0.0.0.0",
    "http://192.168.1.1",
    "http://[::ffff:127.0.0.1]",
    "http://[::2]",
    "ftp://localhost",
    "file://localhost",
    "data:application/json,",
  ])("rejects JWKS at %s before fetching signing keys", async (origin) => {
    const secret = randomUUID();
    const token = signHs256Jwt({ iss: "https://issuer.example", sub: "url-policy-user" }, secret);
    const fetcher = vi.spyOn(globalThis, "fetch").mockImplementation(async () => {
      return new Response(
        JSON.stringify({
          keys: [{ kty: "oct", kid: JWT_KID, k: base64Url(secret) }],
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    });

    const [attempt] = await Promise.allSettled([
      resolveRequestSession(
        { headers: { authorization: `Bearer ${token}` } },
        { appId: "app-with-url-policy", jwksUrl: `${origin}/jwks/${randomUUID()}` },
      ),
    ]);

    expect({
      status: attempt!.status,
      fetches: fetcher.mock.calls.length,
    }).toEqual({ status: "rejected", fetches: 0 });
  });

  it.each([
    // Bind both loopback families: localhost resolution differs across hosts.
    ["LOCALHOST", "::"],
    ["[0:0:0:0:0:0:0:1]", "::1"],
    ["127.1", "127.0.0.1"],
    ["2130706433", "127.0.0.1"],
    ["127.255.255.254", "127.255.255.254"],
  ])("verifies signing keys over HTTP at canonical loopback %s", async (hostname, listenHost) => {
    const secret = randomUUID();
    const path = `/jwks/${randomUUID()}`;
    let requests = 0;
    const server = createHttpServer((request, response) => {
      if (request.url !== path) {
        response.writeHead(404).end();
        return;
      }
      requests += 1;
      response.writeHead(200, { "Content-Type": "application/json" }).end(
        JSON.stringify({
          keys: [{ kty: "oct", kid: JWT_KID, k: base64Url(secret) }],
        }),
      );
    });

    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, listenHost, () => resolve());
      });
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("failed to allocate loopback test port");
      }
      const token = signHs256Jwt({ iss: "https://issuer.example", sub: "loopback-user" }, secret);

      await expect(
        resolveRequestSession(
          { headers: { authorization: `Bearer ${token}` } },
          {
            appId: "app-with-loopback-jwks",
            jwksUrl: `http://${hostname}:${address.port}${path}`,
          },
        ),
      ).resolves.toMatchObject({
        issuer: "https://issuer.example",
        user_id: "loopback-user",
        authMode: "external",
      });
      expect(requests).toBe(1);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("shares one cold JWKS fetch and rejects invalid JWTs without an immediate refresh", async () => {
    const jwks = await JwksServer.start();
    servers.add(jwks);
    const config = {
      appId: "app-with-cold-jwks",
      jwksUrl: jwks.url,
    };
    const invalidToken = signHs256Jwt(
      {
        sub: "invalid-user",
        iss: "https://issuer.example",
      },
      "different-secret",
    );
    const request = {
      headers: { authorization: `Bearer ${invalidToken}` },
    };

    const attempts = await Promise.allSettled(
      Array.from({ length: 20 }, () => resolveRequestSession(request, config)),
    );

    expect(attempts.every((attempt) => attempt.status === "rejected")).toBe(true);
    expect(jwks.requests).toBe(1);
  });

  it("refreshes a stale JWKS once after provider key rotation and validates the token", async () => {
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const jwks = await JwksServer.start();
    servers.add(jwks);
    const config = {
      appId: "app-with-rotating-jwks",
      jwksUrl: jwks.url,
    };
    const oldToken = signHs256Jwt({
      sub: "old-key-user",
      iss: "https://issuer.example",
    });
    const newSecret = "rotated-backend-request-test-secret";
    const newToken = signHs256Jwt(
      {
        sub: "rotated-key-user",
        iss: "https://issuer.example",
      },
      newSecret,
    );
    const requestFor = (token: string) => ({
      headers: { authorization: `Bearer ${token}` },
    });

    await resolveRequestSession(requestFor(oldToken), config);
    expect(jwks.requests).toBe(1);

    jwks.rotateKey(newSecret);
    now += 30_001;

    await expect(resolveRequestSession(requestFor(newToken), config)).resolves.toMatchObject({
      issuer: "https://issuer.example",
      user_id: "rotated-key-user",
      authMode: "external",
    });
    expect(jwks.requests).toBe(2);
  });

  it("shares a failed cold JWKS fetch and retries after in-flight cleanup", async () => {
    const jwks = await JwksServer.start();
    servers.add(jwks);
    jwks.failNextFetch();
    const config = {
      appId: "app-with-retrying-jwks",
      jwksUrl: jwks.url,
    };
    const token = signHs256Jwt({
      sub: "retry-user",
      iss: "https://issuer.example",
    });
    const request = {
      headers: { authorization: `Bearer ${token}` },
    };

    const attempts = await Promise.allSettled(
      Array.from({ length: 20 }, () => resolveRequestSession(request, config)),
    );

    expect(
      attempts.every(
        (attempt) => attempt.status === "rejected" && String(attempt.reason).includes("HTTP 503"),
      ),
    ).toBe(true);
    expect(jwks.requests).toBe(1);

    await expect(resolveRequestSession(request, config)).resolves.toMatchObject({
      user_id: "retry-user",
      authMode: "external",
    });
    expect(jwks.requests).toBe(2);
  });

  it("coalesces and rate-limits forced JWKS refreshes after bad signatures", async () => {
    const jwks = await JwksServer.start();
    servers.add(jwks);
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const config = {
      appId: "app-with-jwks-refresh-control",
      jwksUrl: jwks.url,
    };
    const validToken = signHs256Jwt({
      sub: "valid-user",
      iss: "https://issuer.example",
    });
    const invalidToken = signHs256Jwt(
      {
        sub: "invalid-user",
        iss: "https://issuer.example",
      },
      "different-secret",
    );
    const requestFor = (token: string) => ({
      headers: { authorization: `Bearer ${token}` },
    });

    await resolveRequestSession(requestFor(validToken), config);
    expect(jwks.requests).toBe(1);
    now += 30_001;

    const attempts = await Promise.allSettled(
      Array.from({ length: 20 }, () => resolveRequestSession(requestFor(invalidToken), config)),
    );
    expect(attempts.every((attempt) => attempt.status === "rejected")).toBe(true);
    expect(jwks.requests).toBe(2);

    await expect(resolveRequestSession(requestFor(invalidToken), config)).rejects.toThrow(
      /Invalid JWT/,
    );
    expect(jwks.requests).toBe(2);
  });

  it("uses a cached JWKS document until its five-minute TTL expires", async () => {
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const jwks = await JwksServer.start();
    servers.add(jwks);
    const config = {
      appId: "app-with-fresh-jwks",
      jwksUrl: jwks.url,
    };
    const token = signHs256Jwt({
      sub: "fresh-cache-user",
      iss: "https://issuer.example",
    });
    const request = {
      headers: { authorization: `Bearer ${token}` },
    };

    await expect(resolveRequestSession(request, config)).resolves.toMatchObject({
      user_id: "fresh-cache-user",
    });
    expect(jwks.requests).toBe(1);

    jwks.rotateKey("replacement-backend-request-test-secret");
    now += 5 * 60 * 1000 - 1;

    await expect(resolveRequestSession(request, config)).resolves.toMatchObject({
      user_id: "fresh-cache-user",
    });
    expect(jwks.requests).toBe(1);
  });

  it("coalesces expiry refreshes and retires the old key without a forced refetch", async () => {
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const jwks = await JwksServer.start();
    servers.add(jwks);
    const config = {
      appId: "app-with-expiring-jwks",
      jwksUrl: jwks.url,
    };
    const oldToken = signHs256Jwt({
      sub: "retired-key-user",
      iss: "https://issuer.example",
    });
    const replacementSecret = "replacement-backend-request-test-secret";
    const replacementToken = signHs256Jwt(
      {
        sub: "replacement-key-user",
        iss: "https://issuer.example",
      },
      replacementSecret,
    );
    const requestFor = (token: string) => ({
      headers: { authorization: `Bearer ${token}` },
    });

    await resolveRequestSession(requestFor(oldToken), config);
    expect(jwks.requests).toBe(1);

    jwks.rotateKey(replacementSecret);
    now += 5 * 60 * 1000;

    const refreshes = await Promise.all(
      Array.from({ length: 20 }, () => resolveRequestSession(requestFor(replacementToken), config)),
    );
    expect(refreshes.every((session) => session.user_id === "replacement-key-user")).toBe(true);
    expect(jwks.requests).toBe(2);

    await expect(resolveRequestSession(requestFor(oldToken), config)).rejects.toThrow(
      /Invalid JWT/,
    );
    expect(jwks.requests).toBe(2);
  });

  it("fails closed on provider outage after the JWKS TTL and retries immediately", async () => {
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const jwks = await JwksServer.start();
    servers.add(jwks);
    const config = {
      appId: "app-with-expired-jwks",
      jwksUrl: jwks.url,
    };
    const token = signHs256Jwt({
      sub: "expired-cache-user",
      iss: "https://issuer.example",
    });
    const request = {
      headers: { authorization: `Bearer ${token}` },
    };

    await expect(resolveRequestSession(request, config)).resolves.toMatchObject({
      user_id: "expired-cache-user",
    });
    expect(jwks.requests).toBe(1);

    now += 5 * 60 * 1000;
    jwks.failNextFetch();

    await expect(resolveRequestSession(request, config)).rejects.toThrow(/HTTP 503/);
    expect(jwks.requests).toBe(2);

    await expect(resolveRequestSession(request, config)).resolves.toMatchObject({
      user_id: "expired-cache-user",
    });
    expect(jwks.requests).toBe(3);
  });

  it("verifies external JWTs via a static JWK and uses JWT sub as the session user", async () => {
    const fetcher = vi.spyOn(globalThis, "fetch");
    const token = signHs256Jwt({
      sub: "user-subject",
      iss: "https://issuer.example",
      role: "editor",
    });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId: "app-with-static-key",
          jwtPublicKey: {
            kty: "oct",
            kid: JWT_KID,
            alg: "HS256",
            k: base64Url(JWT_SECRET),
          },
        },
      ),
    ).resolves.toEqual({
      issuer: "https://issuer.example",
      user_id: "user-subject",
      claims: { role: "editor" },
      authMode: "external",
    });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("rejects a signed external JWT from a different configured issuer", async () => {
    const token = signHs256Jwt({
      sub: "user-subject",
      iss: "https://other-issuer.example",
      aud: "jazz-api",
    });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId: "issuer-bound-app",
          jwtPublicKey: {
            kty: "oct",
            kid: JWT_KID,
            alg: "HS256",
            k: base64Url(JWT_SECRET),
          },
          jwtIssuer: "https://issuer.example",
          jwtAudience: "jazz-api",
        },
      ),
    ).rejects.toThrow(/issuer/i);
  });

  it("rejects a signed external JWT for a different configured audience", async () => {
    const token = signHs256Jwt({
      sub: "user-subject",
      iss: "https://issuer.example",
      aud: "other-service",
    });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId: "audience-bound-app",
          jwtPublicKey: {
            kty: "oct",
            kid: JWT_KID,
            alg: "HS256",
            k: base64Url(JWT_SECRET),
          },
          jwtIssuer: "https://issuer.example",
          jwtAudience: "jazz-api",
        },
      ),
    ).rejects.toThrow(/audience/i);
  });

  it("preserves exact signed external JWT issuer spelling after validation", async () => {
    const token = signHs256Jwt({
      sub: " user-subject ",
      iss: " https://issuer.example ",
      role: "editor",
    });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId: "app-with-static-key-spaced-issuer",
          jwtPublicKey: {
            kty: "oct",
            kid: JWT_KID,
            alg: "HS256",
            k: base64Url(JWT_SECRET),
          },
        },
      ),
    ).resolves.toEqual({
      issuer: " https://issuer.example ",
      user_id: " user-subject ",
      claims: { role: "editor" },
      authMode: "external",
    });
  });

  it("rejects Jazz-reserved issuers on signed external JWTs", async () => {
    for (const issuer of ["urn:jazz:system", "urn:jazz:anonymous", "urn:jazz:static-bearer"]) {
      const token = signHs256Jwt({
        sub: "user-subject",
        iss: issuer,
      });

      await expect(
        resolveRequestSession(
          {
            headers: {
              authorization: `Bearer ${token}`,
            },
          },
          {
            appId: `app-rejects-${issuer}`,
            jwtPublicKey: {
              kty: "oct",
              kid: JWT_KID,
              alg: "HS256",
              k: base64Url(JWT_SECRET),
            },
          },
        ),
      ).rejects.toThrow(/Invalid JWT payload/);
    }

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${signHs256Jwt({
              sub: "user-subject",
              iss: "urn:jazz:local-first",
            })}`,
          },
        },
        {
          appId: "app-rejects-local-first-without-local-first-auth",
          allowLocalFirstAuth: false,
          jwtPublicKey: {
            kty: "oct",
            kid: JWT_KID,
            alg: "HS256",
            k: base64Url(JWT_SECRET),
          },
        },
      ),
    ).rejects.toThrow(/local-first/i);
  });
});
