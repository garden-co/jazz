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

const SHA256_K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

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

function utf8Encode(value: string): Uint8Array {
  if (typeof TextEncoder !== "undefined") {
    return new TextEncoder().encode(value);
  }

  const encoded = encodeURIComponent(value);
  const bytes: number[] = [];
  for (let i = 0; i < encoded.length; i += 1) {
    const char = encoded[i]!;
    if (char === "%") {
      bytes.push(Number.parseInt(encoded.slice(i + 1, i + 3), 16));
      i += 2;
    } else {
      bytes.push(char.charCodeAt(0));
    }
  }
  return Uint8Array.from(bytes);
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

function encodeLocalPrincipalId(digest: Uint8Array): string {
  const encoded = encodeBase64(digest).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  return `local:${encoded}`;
}

function rightRotate(value: number, amount: number): number {
  return (value >>> amount) | (value << (32 - amount));
}

function sha256PureJs(input: string): Uint8Array {
  const bytes = utf8Encode(input);
  const bitLength = bytes.length * 8;
  const oneBitAppendedLength = bytes.length + 1;
  const zeroPadLength = (64 - ((oneBitAppendedLength + 8) % 64)) % 64;
  const totalLength = oneBitAppendedLength + zeroPadLength + 8;

  const padded = new Uint8Array(totalLength);
  padded.set(bytes, 0);
  padded[bytes.length] = 0x80;

  const bitLengthHi = Math.floor(bitLength / 0x100000000);
  const bitLengthLo = bitLength >>> 0;
  padded[totalLength - 8] = (bitLengthHi >>> 24) & 0xff;
  padded[totalLength - 7] = (bitLengthHi >>> 16) & 0xff;
  padded[totalLength - 6] = (bitLengthHi >>> 8) & 0xff;
  padded[totalLength - 5] = bitLengthHi & 0xff;
  padded[totalLength - 4] = (bitLengthLo >>> 24) & 0xff;
  padded[totalLength - 3] = (bitLengthLo >>> 16) & 0xff;
  padded[totalLength - 2] = (bitLengthLo >>> 8) & 0xff;
  padded[totalLength - 1] = bitLengthLo & 0xff;

  let h0 = 0x6a09e667;
  let h1 = 0xbb67ae85;
  let h2 = 0x3c6ef372;
  let h3 = 0xa54ff53a;
  let h4 = 0x510e527f;
  let h5 = 0x9b05688c;
  let h6 = 0x1f83d9ab;
  let h7 = 0x5be0cd19;

  const w = new Uint32Array(64);
  for (let offset = 0; offset < totalLength; offset += 64) {
    for (let i = 0; i < 16; i += 1) {
      const index = offset + i * 4;
      w[i] =
        ((padded[index]! << 24) |
          (padded[index + 1]! << 16) |
          (padded[index + 2]! << 8) |
          padded[index + 3]!) >>>
        0;
    }

    for (let i = 16; i < 64; i += 1) {
      const s0 = rightRotate(w[i - 15]!, 7) ^ rightRotate(w[i - 15]!, 18) ^ (w[i - 15]! >>> 3);
      const s1 = rightRotate(w[i - 2]!, 17) ^ rightRotate(w[i - 2]!, 19) ^ (w[i - 2]! >>> 10);
      w[i] = (w[i - 16]! + s0 + w[i - 7]! + s1) >>> 0;
    }

    let a = h0;
    let b = h1;
    let c = h2;
    let d = h3;
    let e = h4;
    let f = h5;
    let g = h6;
    let h = h7;

    for (let i = 0; i < 64; i += 1) {
      const sum1 = rightRotate(e, 6) ^ rightRotate(e, 11) ^ rightRotate(e, 25);
      const choice = (e & f) ^ (~e & g);
      const temp1 = (h + sum1 + choice + SHA256_K[i]! + w[i]!) >>> 0;
      const sum0 = rightRotate(a, 2) ^ rightRotate(a, 13) ^ rightRotate(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (sum0 + majority) >>> 0;

      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }

    h0 = (h0 + a) >>> 0;
    h1 = (h1 + b) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
    h5 = (h5 + f) >>> 0;
    h6 = (h6 + g) >>> 0;
    h7 = (h7 + h) >>> 0;
  }

  const digest = new Uint8Array(32);
  const words = [h0, h1, h2, h3, h4, h5, h6, h7];
  for (let i = 0; i < words.length; i += 1) {
    const value = words[i]!;
    const base = i * 4;
    digest[base] = (value >>> 24) & 0xff;
    digest[base + 1] = (value >>> 16) & 0xff;
    digest[base + 2] = (value >>> 8) & 0xff;
    digest[base + 3] = value & 0xff;
  }

  return digest;
}

async function sha256(input: string): Promise<Uint8Array> {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj?.subtle) {
    try {
      const bytes = utf8Encode(input);
      const digest = await cryptoObj.subtle.digest("SHA-256", bytes as unknown as BufferSource);
      return new Uint8Array(digest);
    } catch {
      // Fall through to pure JS implementation.
    }
  }

  return sha256PureJs(input);
}

export async function deriveLocalPrincipalId(
  appId: string,
  mode: LocalAuthMode,
  token: string,
): Promise<string> {
  const input = `${appId}:${mode}:${token}`;
  const digest = await sha256(input);
  return encodeLocalPrincipalId(digest);
}

export function deriveLocalPrincipalIdSync(
  appId: string,
  mode: LocalAuthMode,
  token: string,
): string {
  const input = `${appId}:${mode}:${token}`;
  return encodeLocalPrincipalId(sha256PureJs(input));
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

export function resolveClientSessionSync(config: ClientSessionInput): Session | null {
  const jwtSession = resolveJwtSession(config.jwtToken ?? "");
  if (jwtSession) return jwtSession;

  const localMode = config.localAuthMode;
  const localToken = trimOptional(config.localAuthToken);
  if (!localMode || !localToken) {
    return null;
  }

  return {
    user_id: deriveLocalPrincipalIdSync(config.appId, localMode, localToken),
    claims: {
      auth_mode: "local",
      local_mode: localMode,
    },
  };
}
