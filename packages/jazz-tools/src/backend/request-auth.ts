import {
  AccountAuthError,
  requestVouchedAccount,
  readAccountAssignment,
} from "../accounts/registry-client.js";
import type { RequestLike } from "../runtime/client.js";
import {
  LOCAL_FIRST_JWT_ISSUER,
  parseJwtPayload,
  internalSessionFromVerifiedReservedJwtPayload,
  internalSessionFromJwtPayload,
  type JwtPayload,
} from "../runtime/client-session.js";
import type { Session } from "../runtime/context.js";
import { localFirstAccountId } from "../accounts/local-first.js";
import { verifyLocalFirstToken } from "../runtime/local-first-token.js";
import { resolveAllowLocalFirstAuth } from "./local-first-auth.js";

/**
 * The request's credential was missing, malformed or unverifiable, was not a
 * local-first guest's proof, or was rejected by the account registry with one
 * of its rejection codes: the caller should authenticate (HTTP 401).
 *
 * Every other failure (an account-registry outage, a registry answer without
 * a rejection code, a malformed registry answer) is the server's problem and
 * stays a plain `Error`.
 *
 * `jazz-tools/backend` is often loaded outside an app's bundle, so a caller
 * can see this error from a different copy of the class: match it with
 * `isRequestAuthenticationError`, which also compares the name.
 */
export class RequestAuthenticationError extends Error {
  readonly name = "RequestAuthenticationError";

  constructor(
    message: string,
    options: {
      /** The account registry's rejection code, when the registry refused the identity. */
      code?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    if (options.code !== undefined) this.code = options.code;
  }

  readonly code?: string;
}

/** Whether `error` is a `RequestAuthenticationError`, from any copy of jazz-tools. */
export function isRequestAuthenticationError(error: unknown): error is RequestAuthenticationError {
  return (
    error instanceof RequestAuthenticationError ||
    (error instanceof Error && error.name === "RequestAuthenticationError")
  );
}

// The rejection codes of core's account routes (`failure()` and the issuer
// checks in crates/jazz-server/src/server/routes/accounts.rs).
const REGISTRY_REJECTIONS = new Map([
  ["identity_not_assigned", 404],
  ["identity_not_authorized", 403],
  ["identity_already_assigned", 409],
  ["invalid_external_identity", 400],
  ["local_first_auth_disabled", 403],
]);

/**
 * Whether the account registry rejected the caller's identity, as opposed to
 * failing or being misconfigured. Only the registry's own rejection codes
 * count: a refused backend secret (401) is this server's configuration, and
 * core's app gate answers an unknown app id or path with a bare 404.
 */
function isRegistryRejection(error: AccountAuthError): boolean {
  return error.status !== undefined && REGISTRY_REJECTIONS.get(error.code) === error.status;
}

// Only verified requests that completed account admission receive this capability.
const localFirstProofs = new WeakMap<Session, Readonly<{ token: string; appId: string }>>();
export function verifiedLocalFirstRequestProof(session: Session) {
  return localFirstProofs.get(session);
}

/**
 * How a backend client verifies a request it scopes to a user. Only a
 * local-first guest's own proof is verified here: who a signed-in user is,
 * the app's backend decides (`createJazz`'s `authenticate`).
 */
export interface BackendRequestAuthConfig {
  appId: string;
  /** Canonical core registry required by public request-derived contexts. */
  accountRegistry?: string;
  /**
   * The backend secret that vouches, to the core's registry, for the
   * identities this backend verified. Required with `accountRegistry`.
   */
  backendSecret?: string;
  /**
   * Whether request verification accepts local-first bearer JWTs. Defaults to
   * `false` when `NODE_ENV` is `production` (case-insensitive), and `true`
   * otherwise. Explicit `true` or `false` overrides the default; account
   * verification with the configured sync-server registry still applies.
   */
  allowLocalFirstAuth?: boolean;
}

function readHeader(request: RequestLike, name: string): string | undefined {
  const lower = name.toLowerCase();

  const fromMethod = request.header?.(name) ?? request.header?.(lower);
  if (typeof fromMethod === "string") {
    return fromMethod;
  }

  const headers = request.headers;
  if (!headers) {
    return undefined;
  }

  if (typeof Headers !== "undefined" && headers instanceof Headers) {
    return headers.get(name) ?? headers.get(lower) ?? undefined;
  }

  const record = headers as Record<string, string | string[] | undefined>;
  let raw: string | string[] | undefined;
  let matched = false;
  for (const key of Object.keys(record)) {
    if (key.toLowerCase() !== lower) {
      continue;
    }
    if (matched) {
      return undefined;
    }
    matched = true;
    raw = record[key];
  }
  if (Array.isArray(raw)) {
    return raw[0];
  }
  return raw;
}

function readBearerToken(request: RequestLike): string {
  const authHeader = readHeader(request, "authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    throw new RequestAuthenticationError("Missing or invalid Authorization header");
  }

  const token = authHeader.slice("Bearer ".length).trim();
  if (!token) {
    throw new RequestAuthenticationError("Empty bearer token");
  }

  return token;
}

function requireJwtPayload(token: string): JwtPayload {
  const payload = parseJwtPayload(token);
  if (!payload) {
    throw new RequestAuthenticationError("Invalid JWT payload");
  }
  return payload;
}

function requireJwtSession(payload: JwtPayload): Session {
  const session = internalSessionFromJwtPayload(payload);
  if (!session) {
    throw new RequestAuthenticationError("Invalid JWT payload");
  }
  return session;
}

async function verifyLocalFirstIdentityProof(token: string, appId: string): Promise<string> {
  try {
    return verifyLocalFirstToken(token, appId);
  } catch {
    throw new RequestAuthenticationError("Invalid local-first identity proof");
  }
}

function localFirstSessionFromVerifiedProof(payload: JwtPayload, verifiedUserId: string): Session {
  const session = internalSessionFromVerifiedReservedJwtPayload(payload, "local-first");
  if (!session) {
    throw new RequestAuthenticationError("Invalid JWT payload");
  }
  if (session.user_id !== verifiedUserId) {
    throw new RequestAuthenticationError("Invalid local-first identity proof");
  }
  return session;
}

/**
 * Build the session `resolveRequestSession` produces for a local-first bearer
 * token, for trusted in-process harnesses that cannot await registry
 * admission (the policy test app).
 *
 * The account is the registry's deterministic founding account for the
 * verified subject, and native admission re-verifies the token against that
 * exact account on every ingress, so this grants nothing a forged token could
 * use.
 *
 * @internal
 */
export function localFirstSessionFromToken(token: string, appId: string): Session {
  const payload = requireJwtPayload(token);
  let subject: string;
  try {
    if (payload.iss !== LOCAL_FIRST_JWT_ISSUER) throw new Error("not local-first");
    subject = verifyLocalFirstToken(token, appId);
  } catch {
    throw new Error("Invalid local-first identity proof");
  }
  const session = localFirstSessionFromVerifiedProof(payload, subject);
  session.account_id = localFirstAccountId(appId, session.user_id);
  localFirstProofs.set(session, { token, appId });
  return session;
}

export async function resolveRequestSession(
  request: RequestLike,
  config: BackendRequestAuthConfig,
): Promise<Session> {
  const token = readBearerToken(request);
  const payload = requireJwtPayload(token);
  if (payload.iss !== LOCAL_FIRST_JWT_ISSUER) {
    requireJwtSession(payload);
    throw new RequestAuthenticationError(
      "Only a local-first guest's bearer token is verified here, not an external JWT. " +
        "Who a signed-in user is, your backend decides: run Jazz in it with createJazz, whose " +
        "forRequest asks your authenticate.",
    );
  }
  if (!resolveAllowLocalFirstAuth(config.allowLocalFirstAuth)) {
    throw new RequestAuthenticationError(
      "Received local-first JWT, but local-first authentication is disabled. " +
        "Local-first authentication defaults to disabled when NODE_ENV is production. " +
        "Set allowLocalFirstAuth: true to enable it; the sync server must also allow local-first authentication.",
    );
  }
  const verifiedUserId = await verifyLocalFirstIdentityProof(token, config.appId);
  const session = localFirstSessionFromVerifiedProof(payload, verifiedUserId);
  if (config.accountRegistry) {
    // The registry founds (or finds) the guest's deterministic account.
    let assignment: unknown;
    try {
      if (!config.backendSecret)
        throw new Error("Request account admission requires the backend secret");
      assignment = await requestVouchedAccount(
        config.accountRegistry,
        "found-local-first",
        { issuer: session.issuer, subject: session.user_id },
        config.backendSecret,
      );
    } catch (error) {
      // The registry refused this identity (unknown, revoked, not admitted).
      // A registry that is down, slow or failing stays a server error.
      if (error instanceof AccountAuthError && isRegistryRejection(error))
        throw new RequestAuthenticationError(error.message, { code: error.code, cause: error });
      throw error;
    }
    session.account_id = readAccountAssignment(assignment, {
      issuer: session.issuer,
      subject: session.user_id,
    });
  }
  localFirstProofs.set(session, { token, appId: config.appId });
  return session;
}
