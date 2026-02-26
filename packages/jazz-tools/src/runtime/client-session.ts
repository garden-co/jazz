import type { LocalAuthMode, Session } from "./context.js";

interface ClientSessionInput {
  appId: string;
  jwtToken?: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
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

  const payloadJson = decodeBase64ToUtf8(base64UrlToBase64(parts[1]));
  if (!payloadJson) return null;

  try {
    const parsed = JSON.parse(payloadJson);
    return isRecord(parsed) ? (parsed as JwtPayload) : null;
  } catch {
    return null;
  }
}

function encodeBase64(bytes: Uint8Array): string {
  const buffer = maybeBuffer();
  if (buffer) {
    return buffer.from(bytes).toString("base64");
  }

  if (typeof btoa === "function") {
    let binary = "";
    for (const byte of bytes) {
      binary += String.fromCharCode(byte);
    }
    return btoa(binary);
  }

  throw new Error("No base64 encoder available in this runtime");
}

async function sha256(input: string): Promise<Uint8Array> {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj?.subtle) {
    const digest = await cryptoObj.subtle.digest("SHA-256", new TextEncoder().encode(input));
    return new Uint8Array(digest);
  }

  throw new Error("No SHA-256 implementation available in this runtime");
}

export async function deriveLocalPrincipalId(
  appId: string,
  mode: LocalAuthMode,
  token: string,
): Promise<string> {
  const input = `${appId}:${mode}:${token}`;
  const digest = await sha256(input);
  const encoded = encodeBase64(digest).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  return `local:${encoded}`;
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
 * Resolve the client session that will be used for permission checks.
 *
 * Priority mirrors request auth headers:
 * 1. JWT (Authorization bearer token)
 * 2. Local anonymous/demo auth (mode + token)
 * 3. No session
 */
export async function resolveClientSession(config: ClientSessionInput): Promise<Session | null> {
  const jwtSession = resolveJwtSession(config.jwtToken ?? "");
  if (jwtSession) return jwtSession;

  const localMode = config.localAuthMode;
  const localToken = trimOptional(config.localAuthToken);
  if (!localMode || !localToken) {
    return null;
  }

  const principalId = await deriveLocalPrincipalId(config.appId, localMode, localToken);
  return {
    user_id: principalId,
    claims: {
      auth_mode: "local",
      local_mode: localMode,
    },
  };
}
