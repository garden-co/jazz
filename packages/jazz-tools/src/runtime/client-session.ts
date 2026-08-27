import type { PublicSession, Session } from "./context.js";
import { isUsableSubject, withCanonicalUser } from "./author-id.js";

export interface ClientSessionInput {
  appId: string;
  jwtToken?: string;
  cookieSession?: Session;
  /** @internal Session produced by a first-party reserved-issuer auth flow. */
  trustedReservedSession?: Session;
}

export type ClientSessionTransport = "bearer" | "cookie";

export interface ClientSessionState {
  transport: ClientSessionTransport | null;
  session: PublicSession | null;
  /** @internal Private transport identity; never exposed as the public session. */
  internalSession: Session | null;
}

export const LOCAL_FIRST_JWT_ISSUER = "urn:jazz:local-first";
export const ANONYMOUS_JWT_ISSUER = "urn:jazz:anonymous";
export const SYSTEM_SESSION_ISSUER = "urn:jazz:system";
export const STATIC_BEARER_SESSION_ISSUER = "urn:jazz:static-bearer";
export const RESERVED_JAZZ_SESSION_ISSUERS = [
  SYSTEM_SESSION_ISSUER,
  LOCAL_FIRST_JWT_ISSUER,
  STATIC_BEARER_SESSION_ISSUER,
  ANONYMOUS_JWT_ISSUER,
] as const;

export function isReservedJazzIssuer(issuer: string): boolean {
  return (RESERVED_JAZZ_SESSION_ISSUERS as readonly string[]).includes(issuer);
}

export const TRUSTED_RESERVED_SESSION_TOKEN_FIELD = "__jazz_trusted_reserved_session";
const trustedReservedSessions = new WeakSet<Session>();
const trustedReservedSessionTokens = new WeakMap<Session, string>();
const trustedReservedSessionTokenValues = new Map<
  string,
  { issuer: string; user_id: string; authMode: Session["authMode"] }
>();

function newTrustedReservedSessionToken(): string {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  if (globalThis.crypto?.getRandomValues) {
    const bytes = new Uint8Array(16);
    globalThis.crypto.getRandomValues(bytes);
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  }
  throw new Error("Trusted reserved sessions require a cryptographically secure runtime");
}

export function markTrustedReservedSession<T extends Session>(session: T): T {
  if (isReservedJazzIssuer(session.issuer)) {
    trustedReservedSessions.add(session);
  }
  return session;
}

export function trustedReservedSessionToken(session: Session): string | undefined {
  if (!trustedReservedSessions.has(session) || !isReservedJazzIssuer(session.issuer)) {
    return undefined;
  }
  let token = trustedReservedSessionTokens.get(session);
  if (!token) {
    token = newTrustedReservedSessionToken();
    trustedReservedSessionTokens.set(session, token);
  }
  trustedReservedSessionTokenValues.set(token, {
    issuer: session.issuer,
    user_id: session.user_id,
    authMode: session.authMode,
  });
  return token;
}

export function isTrustedReservedSession(
  session: Pick<Session, "issuer" | "user_id" | "authMode">,
  token: unknown,
): boolean {
  if (!isReservedJazzIssuer(session.issuer) || typeof token !== "string") return false;
  const trusted = trustedReservedSessionTokenValues.get(token);
  return (
    trusted?.issuer === session.issuer &&
    trusted.user_id === session.user_id &&
    trusted.authMode === session.authMode
  );
}

export interface JwtPayload {
  sub?: unknown;
  iss?: unknown;
  claims?: unknown;
  aud?: unknown;
  exp?: unknown;
}

interface BufferLike {
  from(input: string | Uint8Array, encoding?: string): { toString(encoding?: string): string };
}

function trimOptional(value?: string): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function asUsableSubjectString(value: unknown): string | undefined {
  return typeof value === "string" && isUsableSubject(value) ? value : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function maybeBuffer(): BufferLike | undefined {
  return (globalThis as { Buffer?: BufferLike }).Buffer;
}

function base64UrlToBase64(input: string): string {
  const normalized = input.replace(/-/g, "+").replace(/_/g, "/");
  const padding = normalized.length % 4;
  if (padding === 0) return normalized;
  return normalized + "=".repeat(4 - padding);
}

function decodeBase64ToUtf8(base64: string): string | null {
  const buffer = maybeBuffer();
  if (buffer) {
    try {
      return buffer.from(base64, "base64").toString("utf8");
    } catch {
      return null;
    }
  }

  if (typeof atob === "function") {
    try {
      const binary = atob(base64);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i += 1) {
        bytes[i] = binary.charCodeAt(i);
      }
      return new TextDecoder().decode(bytes);
    } catch {
      return null;
    }
  }

  return null;
}

export function parseJwtPayload(jwtToken: string): JwtPayload | null {
  const token = trimOptional(jwtToken);
  if (!token) return null;

  const parts = token.split(".");
  if (parts.length < 2) return null;
  const payloadPart = parts[1];
  if (payloadPart === undefined) return null;

  const payloadJson = decodeBase64ToUtf8(base64UrlToBase64(payloadPart));
  if (!payloadJson) return null;

  try {
    const parsed = JSON.parse(payloadJson);
    return isRecord(parsed) ? (parsed as JwtPayload) : null;
  } catch {
    return null;
  }
}

export function sessionFromJwtPayload(payload: JwtPayload): PublicSession | null {
  const session = internalSessionFromJwtPayload(payload);
  return session ? withCanonicalUser(session) : null;
}

/** @internal Keep transport identity private from the public session surface. */
export function internalSessionFromJwtPayload(payload: JwtPayload): Session | null {
  const subject = asUsableSubjectString(payload.sub);
  const issuer = asUsableSubjectString(payload.iss);
  if (!subject || !issuer || isReservedJazzIssuer(issuer)) return null;

  const claimsSource = payload.claims;
  const claims: Record<string, unknown> = isRecord(claimsSource) ? { ...claimsSource } : {};

  return {
    issuer,
    user_id: subject,
    claims,
    authMode: "external",
  };
}

export function sessionFromVerifiedReservedJwtPayload(
  payload: JwtPayload,
  authMode: Extract<Session["authMode"], "local-first" | "anonymous">,
): PublicSession | null {
  const subject = asUsableSubjectString(payload.sub);
  const issuer = asUsableSubjectString(payload.iss);
  const expectedIssuer = authMode === "local-first" ? LOCAL_FIRST_JWT_ISSUER : ANONYMOUS_JWT_ISSUER;
  if (!subject || issuer !== expectedIssuer) return null;

  const claimsSource = payload.claims;
  const claims: Record<string, unknown> = isRecord(claimsSource) ? { ...claimsSource } : {};
  const internal = markTrustedReservedSession({
    issuer,
    user_id: subject,
    claims,
    authMode,
  });
  return withCanonicalUser(internal);
}

/** @internal Verified reserved-token form used by transport/auth plumbing. */
export function internalSessionFromVerifiedReservedJwtPayload(
  payload: JwtPayload,
  authMode: Extract<Session["authMode"], "local-first" | "anonymous">,
): Session | null {
  const subject = asUsableSubjectString(payload.sub);
  const issuer = asUsableSubjectString(payload.iss);
  const expectedIssuer = authMode === "local-first" ? LOCAL_FIRST_JWT_ISSUER : ANONYMOUS_JWT_ISSUER;
  if (!subject || issuer !== expectedIssuer) return null;
  const claimsSource = payload.claims;
  const claims: Record<string, unknown> = isRecord(claimsSource) ? { ...claimsSource } : {};
  return markTrustedReservedSession({ issuer, user_id: subject, claims, authMode });
}

export function resolveJwtSession(jwtToken: string): PublicSession | null {
  const payload = parseJwtPayload(jwtToken);
  if (!payload) return null;
  return sessionFromJwtPayload(payload);
}

/**
 * Resolve the client session state that will be used for permission checks.
 *
 * Resolves the JWT bearer token to a session, or returns no session.
 */
export function resolveClientSessionStateSync(config: ClientSessionInput): ClientSessionState {
  if (
    config.jwtToken &&
    config.trustedReservedSession &&
    isTrustedReservedSession(
      config.trustedReservedSession,
      trustedReservedSessionToken(config.trustedReservedSession),
    )
  ) {
    return {
      transport: "bearer",
      session: withCanonicalUser(config.trustedReservedSession),
      internalSession: config.trustedReservedSession,
    };
  }

  const payload = parseJwtPayload(config.jwtToken ?? "");
  const jwtInternal = payload ? internalSessionFromJwtPayload(payload) : null;
  if (jwtInternal) {
    return {
      transport: "bearer",
      session: withCanonicalUser(jwtInternal),
      internalSession: jwtInternal,
    };
  }

  if (
    config.cookieSession &&
    !isReservedJazzIssuer(config.cookieSession.issuer) &&
    isUsableSubject(config.cookieSession.issuer) &&
    isUsableSubject(config.cookieSession.user_id)
  ) {
    return {
      transport: "cookie",
      session: withCanonicalUser(config.cookieSession),
      internalSession: config.cookieSession,
    };
  }

  return {
    transport: null,
    session: null,
    internalSession: null,
  };
}

export function resolveClientSessionSync(config: ClientSessionInput): PublicSession | null {
  return resolveClientSessionStateSync(config).session;
}

/** @internal Transport/runtime identity; never expose this object to applications. */
export function resolveClientInternalSessionSync(config: ClientSessionInput): Session | null {
  return resolveClientSessionStateSync(config).internalSession;
}
