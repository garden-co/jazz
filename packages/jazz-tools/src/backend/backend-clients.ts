import { createRemoteJWKSet, customFetch, decodeJwt, jwtVerify, type JWTPayload } from "jose";
import type { JazzUser } from "./create-jazz.js";

/**
 * A backend client the main backend trusts: a machine outside the app's own
 * backend (a CI job, a worker on another cloud, a partner's service) that
 * connects to the main backend's `/jazz` with its platform's OIDC token. See
 * {@link trustWorkload}.
 */
export interface BackendClientTrust {
  readonly issuer: string;
  readonly audience: string;
  readonly claims: Readonly<Record<string, string>>;
  readonly as: "backend" | ((claims: JWTPayload) => JazzUser | null | Promise<JazzUser | null>);
}

/**
 * Trust a workload's OIDC token as a backend client of this main backend.
 *
 * The token comes as `Authorization: Bearer <token>` on every request and
 * socket the backend client opens. It is verified against the issuer's
 * published keys (found through its OpenID discovery document), must be
 * meant for `audience`, must not be expired, and must carry every claim in
 * `claims` with exactly that value, and must have at least a minute left.
 * Name the workload, down to its environment, with claims only it can have
 * (for GitHub Actions `sub`, e.g. `repo:acme/app:environment:production`,
 * plus `job_workflow_ref`; for Vercel the full `sub`, since a Vercel team's
 * tokens all share one audience): `claims` must not be empty, since an
 * issuer's other workloads hold tokens from the same keys. A token from this
 * issuer that fails these checks is refused, never passed to `authenticate`.
 *
 * `as` decides what the backend client may do:
 * - `"backend"`: a trusted machine. It syncs with the main backend's own
 *   authority: your permissions are not checked, and it may act on behalf
 *   of any user, like the main backend itself (writes it makes as a user
 *   are checked against that user's permissions).
 * - a callback from the token's claims to a user: a scoped machine. It acts
 *   as that user, with that user's permissions, exactly as if `authenticate`
 *   had named the user. Return null to refuse the token.
 */
export function trustWorkload(options: {
  issuer: string;
  audience: string;
  claims: Record<string, string>;
  as: BackendClientTrust["as"];
}): BackendClientTrust {
  const issuer = options.issuer;
  let url: URL;
  try {
    url = new URL(issuer);
  } catch {
    throw new Error(`trustWorkload: issuer must be a URL, got ${JSON.stringify(issuer)}`);
  }
  if (url.protocol !== "https:" && !(url.protocol === "http:" && isLoopback(url.hostname))) {
    throw new Error("trustWorkload: issuer must be https (http only on loopback, for tests)");
  }
  if (typeof options.audience !== "string" || !options.audience) {
    throw new Error("trustWorkload: audience is required");
  }
  const claims = { ...options.claims };
  if (Object.keys(claims).length === 0) {
    throw new Error(
      "trustWorkload: claims must name the workload (e.g. its sub); without them any token from the issuer would be trusted",
    );
  }
  for (const [name, value] of Object.entries(claims)) {
    if (typeof value !== "string" || !value) {
      throw new Error(`trustWorkload: claim ${name} must be a non-empty string`);
    }
    if (name === "iss" || name === "aud" || name === "exp" || name === "nbf" || name === "iat") {
      throw new Error(`trustWorkload: ${name} is checked on its own; don't list it in claims`);
    }
  }
  if (options.as !== "backend" && typeof options.as !== "function") {
    throw new Error('trustWorkload: as must be "backend" or a function from claims to a user');
  }
  return Object.freeze({
    issuer,
    audience: options.audience,
    claims: Object.freeze(claims),
    as: options.as,
  });
}

/** What a request's backend-client token admits. */
export type BackendClientAccess =
  | { kind: "user"; user: JazzUser; expiresAt: number }
  | { kind: "backend"; subject: string; expiresAt: number };

// Asymmetric algorithms only: an issuer's keys are public.
const ALGORITHMS = [
  "RS256",
  "RS384",
  "RS512",
  "PS256",
  "PS384",
  "PS512",
  "ES256",
  "ES384",
  "ES512",
  "EdDSA",
];
const CLOCK_TOLERANCE_SECONDS = 30;
/** A token with less time left than this is refused (see `admit`). */
const MIN_REMAINING_SECONDS = 60;
/** jose's codes for a token that fails verification, as opposed to a key outage. */
const REFUSALS = new Set([
  "ERR_JWT_CLAIM_VALIDATION_FAILED",
  "ERR_JWT_EXPIRED",
  "ERR_JWT_INVALID",
  "ERR_JWS_INVALID",
  "ERR_JWS_SIGNATURE_VERIFICATION_FAILED",
  "ERR_JOSE_ALG_NOT_ALLOWED",
  "ERR_JWKS_NO_MATCHING_KEY",
  "ERR_JWKS_MULTIPLE_MATCHING_KEYS",
]);

/**
 * A trusted issuer's keys couldn't be had (discovery or key set unreachable,
 * timed out or malformed), so a backend client's token can't be judged.
 * createJazz answers 503: the client retries rather than reading a refusal.
 */
export class BackendClientsUnavailable extends Error {
  constructor(issuer: string, cause: unknown) {
    super(`Jazz: OIDC keys for ${issuer} are unavailable`, { cause });
    this.name = "BackendClientsUnavailable";
  }
}

function isLoopback(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]";
}

/** The bearer token of `request`, if it has one. */
function bearerOf(request: Request): string | null {
  const header = request.headers.get("authorization");
  if (!header) return null;
  const match = /^Bearer\s+([A-Za-z0-9_\-.]+)$/i.exec(header.trim());
  return match ? match[1]! : null;
}

/**
 * Verifies backend-client tokens for one main backend. `resolve` answers:
 * - null: the request carries no token from a configured issuer, so it is
 *   the app's own auth (`authenticate`) that decides;
 * - "refused": it carries one, and it doesn't verify or match any trust (it
 *   never falls back to `authenticate`);
 * - the access it admits.
 */
export function backendClientVerifier(
  trusts: readonly BackendClientTrust[],
  fetcher: typeof fetch = globalThis.fetch,
) {
  const issuers = new Set(trusts.map((trust) => trust.issuer));
  const keySets = new Map<string, Promise<ReturnType<typeof createRemoteJWKSet>>>();

  function keysOf(issuer: string): Promise<ReturnType<typeof createRemoteJWKSet>> {
    let keys = keySets.get(issuer);
    if (!keys) {
      keys = discover(issuer).catch((error: unknown) => {
        throw new BackendClientsUnavailable(issuer, error);
      });
      keySets.set(issuer, keys);
      // A failed discovery is retried on the next request.
      keys.catch(() => keySets.delete(issuer));
    }
    return keys;
  }

  async function discover(issuer: string): Promise<ReturnType<typeof createRemoteJWKSet>> {
    const response = await fetcher(
      `${issuer.replace(/\/+$/, "")}/.well-known/openid-configuration`,
      { redirect: "error", signal: AbortSignal.timeout(10_000) },
    );
    if (!response.ok) {
      throw new Error(`Jazz: OIDC discovery for ${issuer} failed (${response.status})`);
    }
    const document = (await response.json()) as { issuer?: unknown; jwks_uri?: unknown };
    if (document.issuer !== issuer) {
      throw new Error(`Jazz: OIDC discovery for ${issuer} names another issuer`);
    }
    if (typeof document.jwks_uri !== "string") {
      throw new Error(`Jazz: OIDC discovery for ${issuer} has no jwks_uri`);
    }
    const jwks = new URL(document.jwks_uri);
    if (jwks.protocol !== "https:" && !(jwks.protocol === "http:" && isLoopback(jwks.hostname))) {
      throw new Error(`Jazz: OIDC keys for ${issuer} must be served over https`);
    }
    return createRemoteJWKSet(jwks, {
      timeoutDuration: 10_000,
      [customFetch]: fetcher,
    });
  }

  async function admit(
    trust: BackendClientTrust,
    token: string,
  ): Promise<BackendClientAccess | null> {
    let payload: JWTPayload;
    try {
      ({ payload } = await jwtVerify(token, await keysOf(trust.issuer), {
        issuer: trust.issuer,
        audience: trust.audience,
        algorithms: ALGORITHMS,
        requiredClaims: ["exp"],
        clockTolerance: CLOCK_TOLERANCE_SECONDS,
      }));
    } catch (error) {
      // Only a token that fails verification is refused. A discovery or key
      // outage (unreachable, timed out, malformed key set) is the server's
      // problem: refusing then would read as "not trusted" to the client.
      if (error instanceof BackendClientsUnavailable) throw error;
      const code = (error as { code?: unknown }).code;
      if (typeof code === "string" && REFUSALS.has(code)) return null;
      throw new BackendClientsUnavailable(trust.issuer, error);
    }
    // Too close to expiry to be worth a session: it would end at once, and a
    // client holding only this token would reconnect into the same refusal.
    if (payload.exp! - Date.now() / 1000 < MIN_REMAINING_SECONDS) return null;
    for (const [name, value] of Object.entries(trust.claims)) {
      if (payload[name] !== value) return null;
    }
    const expiresAt = payload.exp!;
    if (trust.as === "backend") {
      return { kind: "backend", subject: String(payload.sub ?? ""), expiresAt };
    }
    const user = await trust.as(payload);
    if (!user) return null;
    if (typeof user.userId !== "string" || !user.userId) {
      throw new Error("Jazz: a backend client's user needs a userId");
    }
    return { kind: "user", user, expiresAt };
  }

  // One verification per request, however many times its auth is asked.
  const resolved = new WeakMap<Request, Promise<BackendClientAccess | "refused" | null>>();

  async function resolveUncached(
    request: Request,
  ): Promise<BackendClientAccess | "refused" | null> {
    const token = bearerOf(request);
    if (!token) return null;
    let issuer: unknown;
    try {
      issuer = decodeJwt(token).iss;
    } catch {
      return null;
    }
    if (typeof issuer !== "string" || !issuers.has(issuer)) return null;
    for (const trust of trusts) {
      if (trust.issuer !== issuer) continue;
      const access = await admit(trust, token);
      if (access) return access;
    }
    return "refused";
  }

  return {
    /** Whether any backend clients are trusted at all. */
    enabled: trusts.length > 0,
    resolve(request: Request): Promise<BackendClientAccess | "refused" | null> {
      if (issuers.size === 0) return Promise.resolve(null);
      let result = resolved.get(request);
      if (!result) {
        result = resolveUncached(request);
        resolved.set(request, result);
      }
      return result;
    },
  };
}
