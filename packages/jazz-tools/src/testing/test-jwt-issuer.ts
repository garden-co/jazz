import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { randomUUID } from "node:crypto";
import { exportJWK, generateKeyPair, SignJWT } from "jose";

export interface TestJwtIssuerHandle {
  jwksUrl: string;
  /** Exact issuer configured for tokens minted by this test endpoint. */
  issuer: string;
  /** Exact audience configured for tokens minted by this test endpoint. */
  audience: string;
  jwtForUser: (
    userId: string,
    claims?: Record<string, unknown>,
    options?: {
      expiresInSeconds?: number;
      issuer?: string;
    },
  ) => Promise<string>;
  stop: () => Promise<void>;
}

/**
 * Start a local JWKS endpoint for tests and mint JWTs signed by its key: an
 * identity provider for the app's backend (`createJazz`'s `jwksUrl`) to
 * verify sign-ins against. A core verifies no user tokens.
 */
export async function startTestJwtIssuer(): Promise<TestJwtIssuerHandle> {
  const { privateKey, publicKey } = await generateKeyPair("ES256");
  const kid = randomUUID();
  const jwks = JSON.stringify({ keys: [{ ...(await exportJWK(publicKey)), kid, alg: "ES256" }] });
  const server = createServer((_req, res) => {
    res.setHeader("content-type", "application/json");
    res.end(jwks);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const issuer = `${origin}/`;
  const audience = "jazz-test";
  let stopPromise: Promise<void> | null = null;

  return {
    jwksUrl: `${origin}/.well-known/jwks.json`,
    issuer,
    audience,
    jwtForUser: (userId, claims, options) =>
      new SignJWT({ ...claims })
        .setProtectedHeader({ alg: "ES256", kid })
        .setSubject(userId)
        .setIssuer(options?.issuer ?? issuer)
        .setAudience(audience)
        .setIssuedAt()
        .setExpirationTime(Math.floor(Date.now() / 1000) + (options?.expiresInSeconds ?? 3600))
        .sign(privateKey),
    stop: () =>
      (stopPromise ??= new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      })),
  };
}
