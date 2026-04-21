import {
  compactVerify,
  decodeProtectedHeader,
  importJWK,
  importSPKI,
  importX509,
  type JWK,
} from "jose";
import type { RequestLike } from "../runtime/client.js";
import {
  LOCAL_FIRST_JWT_ISSUER,
  parseJwtPayload,
  sessionFromJwtPayload,
  type JwtPayload,
} from "../runtime/client-session.js";
import type { Session } from "../runtime/context.js";
import type { BackendJwtPublicKey } from "./create-jazz-context.js";

export interface BackendRequestAuthConfig {
  appId: string;
  jwksUrl?: string;
  jwtPublicKey?: BackendJwtPublicKey;
  allowLocalFirstAuth?: boolean;
}

type LocalJwksDocument = {
  keys: Array<Record<string, unknown>>;
};

const jwksDocuments = new Map<string, LocalJwksDocument>();
const staticJwtKeys = new Map<string, Promise<Awaited<ReturnType<typeof importJWK>>>>();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
  const raw = record[name] ?? record[lower];
  if (Array.isArray(raw)) {
    return raw[0];
  }
  return raw;
}

function readBearerToken(request: RequestLike): string {
  const authHeader = readHeader(request, "authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    throw new Error("Missing or invalid Authorization header");
  }

  const token = authHeader.slice("Bearer ".length).trim();
  if (!token) {
    throw new Error("Empty bearer token");
  }

  return token;
}

function parseJwksUrl(jwksUrl: string): URL {
  let parsedUrl: URL;
  try {
    parsedUrl = new URL(jwksUrl);
  } catch {
    throw new Error(`Invalid jwksUrl: ${jwksUrl}`);
  }

  return parsedUrl;
}

async function fetchRemoteJwks(jwksUrl: string): Promise<LocalJwksDocument> {
  const fetchFn = globalThis.fetch;
  if (!fetchFn) {
    throw new Error("Global fetch is required for jwksUrl verification");
  }

  let response: Response;
  try {
    response = await fetchFn(parseJwksUrl(jwksUrl));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Unable to fetch JWKS: ${message}`);
  }

  if (!response.ok) {
    throw new Error(`Unable to fetch JWKS: HTTP ${response.status}`);
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new Error("Unable to parse JWKS response");
  }

  if (!isRecord(body) || !Array.isArray(body.keys) || body.keys.length === 0) {
    throw new Error("Invalid JWKS response");
  }

  return {
    keys: body.keys.filter((key): key is Record<string, unknown> => isRecord(key)),
  };
}

async function getRemoteJwksDocument(
  jwksUrl: string,
  forceRefresh = false,
): Promise<LocalJwksDocument> {
  if (!forceRefresh) {
    const cached = jwksDocuments.get(jwksUrl);
    if (cached) {
      return cached;
    }
  }

  const document = await fetchRemoteJwks(jwksUrl);
  jwksDocuments.set(jwksUrl, document);
  return document;
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function selectJwkCandidates(
  jwks: LocalJwksDocument,
  header: ReturnType<typeof decodeProtectedHeader>,
): Array<Record<string, unknown>> {
  const expectedKid = readString(header.kid);
  const expectedAlg = readString(header.alg);
  if (!expectedAlg) {
    throw new Error("Invalid JWT header");
  }

  return jwks.keys.filter((jwk) => {
    const kid = readString(jwk.kid);
    if (expectedKid && kid !== expectedKid) {
      return false;
    }

    const alg = readString(jwk.alg);
    return !alg || alg === expectedAlg;
  });
}

async function verifyJwtSignatureWithJwks(token: string, jwks: LocalJwksDocument): Promise<void> {
  let header: ReturnType<typeof decodeProtectedHeader>;
  try {
    header = decodeProtectedHeader(token);
  } catch {
    throw new Error("Invalid JWT header");
  }

  const algorithm = readString(header.alg);
  if (!algorithm) {
    throw new Error("Invalid JWT header");
  }

  const candidates = selectJwkCandidates(jwks, header);
  if (candidates.length === 0) {
    throw new Error("No matching JWK found");
  }

  let lastError: Error | null = null;
  for (const candidate of candidates) {
    try {
      const key = await importJWK(candidate as JWK, algorithm);
      await compactVerify(token, key);
      return;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
    }
  }

  throw lastError ?? new Error("JWT signature verification failed");
}

function cacheKeyForStaticJwtPublicKey(
  jwtPublicKey: BackendJwtPublicKey,
  algorithm: string,
): string {
  return typeof jwtPublicKey === "string"
    ? `${algorithm}\0${jwtPublicKey.trim()}`
    : `${algorithm}\0${JSON.stringify(jwtPublicKey)}`;
}

async function importStaticJwtPublicKey(
  jwtPublicKey: BackendJwtPublicKey,
  algorithm: string,
): Promise<Awaited<ReturnType<typeof importJWK>>> {
  if (typeof jwtPublicKey !== "string") {
    return await importJWK(jwtPublicKey, algorithm);
  }

  const trimmed = jwtPublicKey.trim();
  if (!trimmed) {
    throw new Error("Invalid JWT public key");
  }

  if (trimmed.startsWith("{")) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      throw new Error("Invalid JWT public key");
    }

    if (!isRecord(parsed)) {
      throw new Error("Invalid JWT public key");
    }

    return await importJWK(parsed as JWK, algorithm);
  }

  try {
    return await importSPKI(trimmed, algorithm);
  } catch {
    try {
      return await importX509(trimmed, algorithm);
    } catch {
      throw new Error("Invalid JWT public key");
    }
  }
}

async function getStaticJwtPublicKey(
  jwtPublicKey: BackendJwtPublicKey,
  algorithm: string,
): Promise<Awaited<ReturnType<typeof importJWK>>> {
  const cacheKey = cacheKeyForStaticJwtPublicKey(jwtPublicKey, algorithm);
  let promise = staticJwtKeys.get(cacheKey);
  if (!promise) {
    promise = importStaticJwtPublicKey(jwtPublicKey, algorithm);
    staticJwtKeys.set(cacheKey, promise);
  }

  try {
    return await promise;
  } catch (error) {
    staticJwtKeys.delete(cacheKey);
    throw error;
  }
}

async function verifyJwtSignatureWithStaticKey(
  token: string,
  jwtPublicKey: BackendJwtPublicKey,
): Promise<void> {
  let header: ReturnType<typeof decodeProtectedHeader>;
  try {
    header = decodeProtectedHeader(token);
  } catch {
    throw new Error("Invalid JWT header");
  }

  const algorithm = readString(header.alg);
  if (!algorithm) {
    throw new Error("Invalid JWT header");
  }

  const key = await getStaticJwtPublicKey(jwtPublicKey, algorithm);
  await compactVerify(token, key);
}

function requireJwtPayload(token: string): JwtPayload {
  const payload = parseJwtPayload(token);
  if (!payload) {
    throw new Error("Invalid JWT payload");
  }
  return payload;
}

function requireJwtSession(payload: JwtPayload): Session {
  const session = sessionFromJwtPayload(payload);
  if (!session) {
    throw new Error("Invalid JWT payload");
  }
  return session;
}

function ensureJwtNotExpired(payload: JwtPayload): void {
  if (payload.exp === undefined) {
    return;
  }
  if (typeof payload.exp !== "number" || !Number.isInteger(payload.exp) || payload.exp < 0) {
    throw new Error("Invalid JWT payload");
  }

  const nowSeconds = Math.floor(Date.now() / 1000);
  if (payload.exp <= nowSeconds) {
    throw new Error("JWT has expired");
  }
}

async function verifyLocalFirstIdentityProof(token: string, appId: string): Promise<string> {
  const { verifyLocalFirstIdentityProof: verifyToken } = await import("jazz-napi");
  const result = verifyToken(token, appId);
  if (!result.ok) {
    throw new Error("Invalid local-first identity proof");
  }

  return result.id;
}

async function verifyExternalJwt(token: string, config: BackendRequestAuthConfig): Promise<void> {
  if (config.jwtPublicKey !== undefined) {
    try {
      await verifyJwtSignatureWithStaticKey(token, config.jwtPublicKey);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Invalid JWT: ${message}`);
    }

    ensureJwtNotExpired(requireJwtPayload(token));
    return;
  }

  if (config.jwksUrl) {
    let jwks = await getRemoteJwksDocument(config.jwksUrl);
    try {
      await verifyJwtSignatureWithJwks(token, jwks);
    } catch {
      try {
        jwks = await getRemoteJwksDocument(config.jwksUrl, true);
        await verifyJwtSignatureWithJwks(token, jwks);
      } catch (refreshError) {
        const message = refreshError instanceof Error ? refreshError.message : String(refreshError);
        throw new Error(`Invalid JWT: ${message}`);
      }
    }

    ensureJwtNotExpired(requireJwtPayload(token));
    return;
  }

  throw new Error(
    "Received external JWT, but createJazzContext() has no jwksUrl or jwtPublicKey. Configure one of them or verify upstream and call forSession().",
  );
}

export async function resolveRequestSession(
  request: RequestLike,
  config: BackendRequestAuthConfig,
): Promise<Session> {
  const token = readBearerToken(request);
  const payload = requireJwtPayload(token);
  const session = requireJwtSession(payload);
  const allowLocalFirstAuth = config.allowLocalFirstAuth ?? true;

  if (payload.iss === LOCAL_FIRST_JWT_ISSUER) {
    if (!allowLocalFirstAuth) {
      throw new Error(
        "Received local-first JWT, but createJazzContext() has allowLocalFirstAuth disabled.",
      );
    }

    const verifiedUserId = await verifyLocalFirstIdentityProof(token, config.appId);
    if (session.user_id !== verifiedUserId) {
      throw new Error("Invalid local-first identity proof");
    }
    return session;
  }

  await verifyExternalJwt(token, config);
  return session;
}
