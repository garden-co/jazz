import {
  LOCAL_FIRST_JWT_ISSUER,
  parseJwtPayload,
  internalSessionFromVerifiedReservedJwtPayload,
  type JwtPayload,
} from "../runtime/client-session.js";
import type { Session } from "../runtime/context.js";
import { localFirstAccountId } from "../accounts/local-first.js";
import { verifyLocalFirstToken } from "../runtime/local-first-token.js";

/**
 * The request's credential was missing, malformed or unverifiable (as
 * `fromJWT` finds it): the caller should authenticate (HTTP 401). Every other
 * failure, such as an unreachable key set, is the server's problem and stays
 * a plain `Error`.
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
      /** A machine-readable reason, when there is one. */
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

/**
 * The request's credential could not be checked because what checks it is
 * unreachable (`fromJWT`'s JWKS is down, errors or times out): neither the
 * caller's fault nor proof the credential is bad. createJazz answers 503
 * with `Retry-After`, so the client retries and stays signed in.
 */
export class AuthenticationUnavailableError extends Error {
  readonly name = "AuthenticationUnavailableError";
}

/** Whether `error` is an `AuthenticationUnavailableError`, from any copy of jazz-tools. */
export function isAuthenticationUnavailableError(
  error: unknown,
): error is AuthenticationUnavailableError {
  return (
    error instanceof AuthenticationUnavailableError ||
    (error instanceof Error && error.name === "AuthenticationUnavailableError")
  );
}

// Only verified requests that completed account admission receive this capability.
const localFirstProofs = new WeakMap<Session, Readonly<{ token: string; appId: string }>>();
export function verifiedLocalFirstRequestProof(session: Session) {
  return localFirstProofs.get(session);
}

function requireJwtPayload(token: string): JwtPayload {
  const payload = parseJwtPayload(token);
  if (!payload) {
    throw new RequestAuthenticationError("Invalid JWT payload");
  }
  return payload;
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
 * Build the session a local-first guest's bearer token names, for trusted in-process harnesses that cannot await registry
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
