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

  private constructor(server: HttpServer, url: string) {
    this.server = server;
    this.url = url;
  }

  static async start(secret = JWT_SECRET): Promise<JwksServer> {
    const server = createHttpServer((request, response) => {
      if (request.url !== "/jwks") {
        response.statusCode = 404;
        response.end("not found");
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
              k: base64Url(secret),
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

    return new JwksServer(server, `http://127.0.0.1:${port}/jwks`);
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
    ).rejects.toThrow(/jwksUrl/i);
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
      user_id: userId,
      claims: {
        auth_mode: "local-first",
        subject: userId,
        issuer: "urn:jazz:local-first",
      },
    });
  });

  it("rejects local-first JWTs when allowLocalFirst is disabled", async () => {
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
          allowLocalFirst: false,
        },
      ),
    ).rejects.toThrow(/local-first/i);
  });

  it("verifies external JWTs via JWKS and uses the shared session mapping", async () => {
    const jwks = await JwksServer.start();
    servers.add(jwks);
    const token = signHs256Jwt({
      sub: "user-subject",
      jazz_principal_id: "principal-123",
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
      user_id: "principal-123",
      claims: {
        role: "editor",
        auth_mode: "external",
        subject: "user-subject",
        issuer: "https://issuer.example",
      },
    });
  });
});
