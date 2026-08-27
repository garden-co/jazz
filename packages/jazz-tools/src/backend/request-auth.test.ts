import { createHmac } from "node:crypto";
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

  static async start(secret = JWT_SECRET): Promise<JwksServer> {
    let instance: JwksServer;
    const server = createHttpServer((request, response) => {
      if (request.url !== "/jwks") {
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

    instance = new JwksServer(server, `http://127.0.0.1:${port}/jwks`, secret);
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

  it("rejects external JWTs when jwksUrl is not configured", async () => {
    const token = signHs256Jwt({
      sub: "user-subject",
      iss: "https://issuer.example",
      claims: { role: "editor" },
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
      claims: {},
      authMode: "local-first",
    });
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
      claims: { role: "editor" },
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
    const token = signHs256Jwt({
      sub: "user-subject",
      iss: "https://issuer.example",
      claims: { role: "editor" },
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
      claims: { role: "editor" },
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
