import { parseJwtPayload, type JwtPayload } from "../runtime/client-session.js";
import { verifyExternalJwt, type BackendJwtPublicKey } from "./external-jwt.js";
import type { AuthenticatedUser } from "./create-jazz.js";
import { isRequestAuthenticationError } from "./request-auth.js";

export interface FromJWTOptions {
  /** The issuer's JWKS endpoint. HTTPS, or HTTP on loopback in development. */
  jwksUrl?: string;
  /** The issuer's public key (PEM, X.509 or JWK), instead of `jwksUrl`. */
  publicKey?: BackendJwtPublicKey;
  /** The `iss` every accepted token carries. Tokens from any other issuer are refused. */
  issuer: string;
  /**
   * The `aud` values that name this app. Required: an issuer shared by
   * several apps (an Auth0 or Clerk tenant) signs tokens for all of them.
   * Pass `false` only when the issuer itself is this app's alone (WorkOS's
   * issuer names your client id) and its tokens carry no audience.
   */
  audience: string | readonly string[] | false;
  /** Seconds of clock skew allowed on `exp` and `nbf`. Defaults to 60. */
  clockToleranceSeconds?: number;
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
  return issuer.startsWith("urn:jazz:") || issuer.startsWith("jazz:");
}

/**
 * An `authenticate` for {@link createJazz} that signs users in with JWTs
 * from your identity provider (WorkOS, Auth0, Clerk, any OIDC issuer).
 *
 * It verifies the token's signature, expiry (`exp` is required; 60 seconds
 * of clock skew are allowed), `nbf`, issuer and audience, and names the
 * user by its `sub`. A request without a token, or with one that does
 * not verify, is signed out (null). A JWKS that cannot be fetched is the
 * server's problem and throws, so clients retry instead of signing out.
 *
 * The JWKS is cached for up to five minutes, so a key your issuer drops is
 * accepted until then. A token naming a key the cached JWKS doesn't publish
 * fetches it again, at most once every 30 seconds, and is signed out if the
 * JWKS still doesn't publish its key; within 30 seconds of a fetch it is
 * signed out without one. So when rotating keys, publish the new key in
 * your JWKS before your issuer signs with it, as issuers that rotate keys
 * do; a key published less than 30 seconds before its first token may
 * refuse that token.
 *
 * ```ts
 * createJazz({
 *   app, permissions, store,
 *   authenticate: fromJWT({
 *     jwksUrl: "https://my-tenant.auth0.com/.well-known/jwks.json",
 *     issuer: "https://my-tenant.auth0.com/",
 *     audience: "https://api.my-app.com",
 *   }),
 * });
 * ```
 */
export function fromJWT(
  options: FromJWTOptions,
): (request: Request) => Promise<AuthenticatedUser | null> {
  if (!options.issuer) throw new Error("fromJWT: issuer is required");
  if (isReservedIssuer(options.issuer)) {
    throw new Error(`fromJWT: ${options.issuer} is reserved for Jazz's own identities`);
  }
  if ((options.jwksUrl === undefined) === (options.publicKey === undefined)) {
    throw new Error("fromJWT: pass exactly one of jwksUrl and publicKey");
  }
  const audience = options.audience as unknown;
  if (
    audience !== false &&
    !(typeof audience === "string" && audience !== "") &&
    !(
      Array.isArray(audience) &&
      audience.length > 0 &&
      audience.every((a) => typeof a === "string" && a !== "")
    )
  ) {
    throw new Error("fromJWT: audience is required (or false when the issuer is this app's alone)");
  }
  const lifetime = {
    requireExp: true,
    checkNotBefore: true,
    clockToleranceSeconds: options.clockToleranceSeconds ?? 60,
  };
  const config = {
    appId: "",
    jwtIssuer: options.issuer,
    ...(options.jwksUrl === undefined ? {} : { jwksUrl: options.jwksUrl }),
    ...(options.publicKey === undefined ? {} : { jwtPublicKey: options.publicKey }),
    ...(options.audience === false ? {} : { jwtAudience: options.audience }),
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
      await verifyExternalJwt(token, payload, config, lifetime);
    } catch (error) {
      // A token that does not verify signs nobody in; an unreachable
      // JWKS or an unusable key is the server's problem.
      if (isRequestAuthenticationError(error)) return null;
      throw error;
    }
    // Verified with `requireExp`, so the token says until when it holds; it
    // is accepted up to the clock tolerance past that, and so holds as long.
    return {
      userId: payload.sub,
      claims: readClaims(payload),
      expiresAt: (payload.exp as number) + lifetime.clockToleranceSeconds,
    };
  };
}
