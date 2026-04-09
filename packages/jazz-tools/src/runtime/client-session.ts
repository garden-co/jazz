import type { Session } from "./context.js";

export interface ClientSessionInput {
  appId: string;
  jwtToken?: string;
}

export interface ClientSessionState {
  transport: "bearer" | null;
  session: Session | null;
}

interface JwtPayload {
  sub?: unknown;
  iss?: unknown;
  jazz_principal_id?: unknown;
  claims?: unknown;
}

interface BufferLike {
  from(input: string | Uint8Array, encoding?: string): { toString(encoding?: string): string };
}

function trimOptional(value?: string): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function asNonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" ? trimOptional(value) : undefined;
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

function parseJwtPayload(jwtToken: string): JwtPayload | null {
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

export function resolveJwtSession(jwtToken: string): Session | null {
  const payload = parseJwtPayload(jwtToken);
  if (!payload) return null;

  const subject = asNonEmptyString(payload.sub);
  const issuer = asNonEmptyString(payload.iss);
  const principalId = asNonEmptyString(payload.jazz_principal_id) ?? subject;
  if (!principalId) return null;

  const claimsSource = payload.claims;
  const claims: Record<string, unknown> = isRecord(claimsSource) ? { ...claimsSource } : {};
  claims.auth_mode = "external";
  if (subject) claims.subject = subject;
  if (issuer) claims.issuer = issuer;
  if (!isRecord(claimsSource) && claimsSource !== undefined) {
    claims.raw_claims = claimsSource;
  }

  return {
    user_id: principalId,
    claims,
  };
}

/**
 * Resolve the client session state from a JWT bearer token.
 */
export function resolveClientSessionStateSync(config: ClientSessionInput): ClientSessionState {
  const jwtSession = resolveJwtSession(config.jwtToken ?? "");
  if (jwtSession) {
    return {
      transport: "bearer",
      session: jwtSession,
    };
  }

  return {
    transport: null,
    session: null,
  };
}

export function resolveClientSessionSync(config: ClientSessionInput): Session | null {
  return resolveClientSessionStateSync(config).session;
}
