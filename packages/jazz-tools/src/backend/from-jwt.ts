import { parseJwtPayload, type JwtPayload } from "../runtime/client-session.js";
import type { BackendJwtPublicKey } from "./create-jazz-context.js";
import type { JazzUser } from "./create-jazz.js";
import { isRequestAuthenticationError, verifyExternalJwt } from "./request-auth.js";

export interface FromJWTOptions {
  /** The issuer's JWKS endpoint. HTTPS, or HTTP on loopback in development. */
  jwksUrl?: string;
  /** The issuer's public key (PEM, X.509 or JWK), instead of `jwksUrl`. */
  publicKey?: BackendJwtPublicKey;
  /** The `iss` every accepted token carries. Tokens from any other issuer are refused. */
  issuer: string;
  /** Accepted `aud` values, when the issuer sets one. */
  audience?: string | readonly string[];
  /**
   * Where the token comes from. Defaults to the `Authorization: Bearer`
   * header. Return null when the request carries none.
   */
  token?: (request: Request) => string | null | undefined | Promise<string | null | undefined>;
  /**
   * The claims your permissions read through `session.claims`. Defaults to
   * every claim in the token except the registered ones (`iss`, `sub`,
   * `aud`, `exp`, `nbf`, `iat`, `jti`).
   */
  claims?: (payload: Record<string, unknown>) => Record<string, unknown>;
}

const REGISTERED_CLAIMS = new Set(["iss", "sub", "aud", "exp", "nbf", "iat", "jti"]);

function bearerToken(request: Request): string | null {
  const header = request.headers.get("authorization");
  if (!header?.startsWith("Bearer ")) return null;
  return header.slice("Bearer ".length).trim() || null;
}

function isReservedIssuer(issuer: string): boolean {
  return issuer.startsWith("urn:jazz:") || issuer.startsWith("jazz:app:");
}

/**
 * An `authenticate` for {@link createJazz} that signs users in with JWTs
 * from your identity provider (WorkOS, Auth0, Clerk, any OIDC issuer).
 *
 * It verifies the token's signature, expiry, issuer and audience, and names
 * the user by its `sub`. A request without a token, or with one that does
 * not verify, is signed out (null). A JWKS that cannot be fetched is the
 * server's problem and throws, so clients retry instead of signing out.
 *
 * ```ts
 * createJazz({
 *   app, permissions, store,
 *   authenticate: fromJWT({
 *     jwksUrl: `https://api.workos.com/sso/jwks/${clientId}`,
 *     issuer: `https://api.workos.com/user_management/${clientId}`,
 *   }),
 * });
 * ```
 */
export function fromJWT(options: FromJWTOptions): (request: Request) => Promise<JazzUser | null> {
  if (!options.issuer) throw new Error("fromJWT: issuer is required");
  if (isReservedIssuer(options.issuer)) {
    throw new Error(`fromJWT: ${options.issuer} is reserved for Jazz's own identities`);
  }
  if ((options.jwksUrl === undefined) === (options.publicKey === undefined)) {
    throw new Error("fromJWT: pass exactly one of jwksUrl and publicKey");
  }
  const config = {
    appId: "",
    jwtIssuer: options.issuer,
    ...(options.jwksUrl === undefined ? {} : { jwksUrl: options.jwksUrl }),
    ...(options.publicKey === undefined ? {} : { jwtPublicKey: options.publicKey }),
    ...(options.audience === undefined ? {} : { jwtAudience: options.audience }),
  };
  const readToken = options.token ?? bearerToken;
  const readClaims =
    options.claims ??
    ((payload: JwtPayload) =>
      Object.fromEntries(Object.entries(payload).filter(([name]) => !REGISTERED_CLAIMS.has(name))));

  return async (request) => {
    const token = await readToken(request);
    if (!token) return null;
    const payload = parseJwtPayload(token);
    if (!payload || typeof payload.sub !== "string" || payload.sub === "") return null;
    try {
      await verifyExternalJwt(token, payload, config);
    } catch (error) {
      // A token that does not verify signs nobody in; an unreachable
      // JWKS or an unusable key is the server's problem.
      if (isRequestAuthenticationError(error)) return null;
      throw error;
    }
    if (payload.nbf !== undefined) {
      if (typeof payload.nbf !== "number" || payload.nbf > Math.floor(Date.now() / 1000)) {
        return null;
      }
    }
    return { userId: payload.sub, claims: readClaims(payload) };
  };
}
