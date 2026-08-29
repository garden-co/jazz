import { TestJwtIssuer } from "jazz-napi";

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
  ) => string;
  stop: () => Promise<void>;
}

/**
 * Start a local JWKS endpoint for tests and mint JWTs signed by its key.
 *
 * Pair the returned `jwksUrl`, `issuer`, and `audience` with
 * {@link startLocalJazzServer} to exercise fail-closed external JWT auth
 * without depending on a real identity provider.
 */
export async function startTestJwtIssuer(): Promise<TestJwtIssuerHandle> {
  const issuer = await TestJwtIssuer.start();
  let stopPromise: Promise<void> | null = null;

  const stop = async () => {
    if (!stopPromise) {
      stopPromise = issuer.stop();
    }
    return await stopPromise;
  };

  return {
    jwksUrl: issuer.jwksUrl,
    issuer: issuer.issuer,
    audience: issuer.audience,
    jwtForUser: (userId, claims, options) => issuer.jwtForUser(userId, claims, options),
    stop,
  };
}
