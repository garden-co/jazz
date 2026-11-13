import {
  compactVerify,
  decodeProtectedHeader,
  importJWK,
  importSPKI,
  importX509,
  type JWK,
} from "jose";
import { withAuthRequestDeadline } from "../runtime/auth-request-deadline.js";
import type { JwtPayload } from "../runtime/client-session.js";
import { isRecord, toError } from "../runtime/util.js";
import {
  AuthenticationUnavailableError,
  isAuthenticationUnavailableError,
  RequestAuthenticationError,
} from "./request-auth.js";

/**
 * Verifying an identity provider's JWTs, for `fromJWT`: the signature (a JWKS
 * or a static public key), expiry, issuer and audience.
 */

/** A static public key: a JWK, a PEM SPKI key or a PEM X.509 certificate. */
export type BackendJwtPublicKey = JWK | string;

/** @internal What `verifyExternalJwt` checks a token against. */
export interface ExternalJwtConfig {
  jwksUrl?: string;
  jwtPublicKey?: BackendJwtPublicKey;
  jwtIssuer?: string;
  jwtAudience?: string | readonly string[];
}

type LocalJwksDocument = {
  keys: Array<Record<string, unknown>>;
};

type CachedJwksDocument = {
  document: LocalJwksDocument;
  expiresAtMs: number;
};

// Expiry is fail closed: never use keys older than five minutes,
// even when the JWKS provider is unavailable.
const JWKS_CACHE_TTL_MS = 5 * 60 * 1000;
const jwksDocuments = new Map<string, CachedJwksDocument>();
const jwksFetches = new Map<string, Promise<LocalJwksDocument>>();
const jwksRefreshNotBefore = new Map<string, number>();
/** The last forced refresh's failure, rethrown until its cooldown ends. */
const jwksRefreshFailures = new Map<string, unknown>();
const JWKS_FORCED_REFRESH_COOLDOWN_MS = 30_000;

function parseJwksUrl(jwksUrl: string): URL {
  let parsedUrl: URL;
  try {
    parsedUrl = new URL(jwksUrl);
  } catch {
    throw new Error(`Invalid jwksUrl: ${jwksUrl}`);
  }

  // WHATWG URL parsing canonicalises numeric IP spellings and IPv6 compression.
  // Check the parsed hostname, never a textual prefix or a DNS resolution.
  const hostname = parsedUrl.hostname;
  if (
    parsedUrl.protocol !== "https:" &&
    !(
      parsedUrl.protocol === "http:" &&
      (hostname === "localhost" ||
        hostname === "[::1]" ||
        /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(hostname))
    )
  ) {
    throw new Error("Invalid jwksUrl: HTTPS is required except for loopback HTTP development");
  }

  return parsedUrl;
}

async function fetchRemoteJwks(jwksUrl: string): Promise<LocalJwksDocument> {
  const fetchFn = globalThis.fetch;
  if (!fetchFn) {
    throw new Error("Global fetch is required for jwksUrl verification");
  }

  // Only the provider not answering (the network, an error status, the
  // deadline) is an outage the client should wait out. A bad jwksUrl or a
  // document that isn't a JWKS is the app's setup, and stays a plain Error.
  const url = parseJwksUrl(jwksUrl);
  return withAuthRequestDeadline(async (signal) => {
    let response: Response;
    try {
      response = await fetchFn(url, { redirect: "error", signal });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new AuthenticationUnavailableError(`Unable to fetch JWKS: ${message}`, {
        cause: error,
      });
    }

    if (!response.ok) {
      throw new AuthenticationUnavailableError(`Unable to fetch JWKS: HTTP ${response.status}`);
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
  }, new AuthenticationUnavailableError("Unable to fetch JWKS: request timed out"));
}

async function getRemoteJwksDocument(
  jwksUrl: string,
  forceRefresh = false,
): Promise<LocalJwksDocument> {
  const cached = jwksDocuments.get(jwksUrl);
  const now = Date.now();
  if (!forceRefresh && cached && now < cached.expiresAtMs) {
    return cached.document;
  }

  const pending = jwksFetches.get(jwksUrl);
  if (pending) {
    return pending;
  }

  if (
    forceRefresh &&
    cached &&
    now < cached.expiresAtMs &&
    now < (jwksRefreshNotBefore.get(jwksUrl) ?? 0)
  ) {
    // While a failed refresh cools down, the provider is presumed still
    // down: keep failing, so a token with a new key is retried rather than
    // refused by keys that predate it.
    if (jwksRefreshFailures.has(jwksUrl)) {
      const cause = jwksRefreshFailures.get(jwksUrl);
      const message = `JWKS refresh from ${jwksUrl} failed recently; retrying after the cooldown`;
      throw isAuthenticationUnavailableError(cause)
        ? new AuthenticationUnavailableError(message, { cause })
        : new Error(message, { cause });
    }
    return cached.document;
  }

  const fetchPromise = fetchRemoteJwks(jwksUrl).then(
    (document) => {
      const refreshedAtMs = Date.now();
      jwksDocuments.set(jwksUrl, {
        document,
        expiresAtMs: refreshedAtMs + JWKS_CACHE_TTL_MS,
      });
      jwksRefreshNotBefore.set(jwksUrl, refreshedAtMs + JWKS_FORCED_REFRESH_COOLDOWN_MS);
      jwksRefreshFailures.delete(jwksUrl);
      return document;
    },
    (error: unknown) => {
      // A failed refresh also waits out the cooldown, so tokens with
      // unknown keys can't make every request fetch during an outage.
      jwksRefreshNotBefore.set(jwksUrl, Date.now() + JWKS_FORCED_REFRESH_COOLDOWN_MS);
      jwksRefreshFailures.set(jwksUrl, error);
      throw error;
    },
  );
  jwksFetches.set(jwksUrl, fetchPromise);
  try {
    return await fetchPromise;
  } finally {
    if (jwksFetches.get(jwksUrl) === fetchPromise) {
      jwksFetches.delete(jwksUrl);
    }
  }
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
      lastError = toError(error);
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

// Every JWS algorithm jose can verify with a static key.
const STATIC_KEY_ALGORITHMS = [
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
  "Ed25519",
  "HS256",
  "HS384",
  "HS512",
] as const;

type StaticJwtPublicKeys = ReadonlyMap<string, Awaited<ReturnType<typeof importJWK>>>;
const staticJwtKeySets = new Map<string, Promise<StaticJwtPublicKeys>>();

function staticKeyAlgorithmCandidates(jwtPublicKey: BackendJwtPublicKey): readonly string[] {
  if (typeof jwtPublicKey === "string") {
    const trimmed = jwtPublicKey.trim();
    if (!trimmed.startsWith("{")) {
      return STATIC_KEY_ALGORITHMS.filter((alg) => !alg.startsWith("HS"));
    }
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (isRecord(parsed)) jwtPublicKey = parsed as JWK;
      else return STATIC_KEY_ALGORITHMS;
    } catch {
      return STATIC_KEY_ALGORITHMS;
    }
  }
  const jwk = jwtPublicKey as JWK;
  if (typeof jwk.alg === "string") return [jwk.alg];
  // jose accepts an `oct` key for any algorithm, so it has to be narrowed here.
  return STATIC_KEY_ALGORITHMS.filter((alg) => alg.startsWith("HS") === (jwk.kty === "oct"));
}

/**
 * Imports the configured key once for every algorithm it can verify.
 * Rejects when the key is unusable for all of them: that is the server's
 * configuration, not anything a caller sent.
 */
async function importStaticJwtPublicKeys(
  jwtPublicKey: BackendJwtPublicKey,
): Promise<StaticJwtPublicKeys> {
  const keys = new Map<string, Awaited<ReturnType<typeof importJWK>>>();
  let lastError: unknown;
  for (const algorithm of staticKeyAlgorithmCandidates(jwtPublicKey)) {
    try {
      keys.set(algorithm, await importStaticJwtPublicKey(jwtPublicKey, algorithm));
    } catch (error) {
      lastError = error;
    }
  }
  if (keys.size === 0) {
    throw lastError instanceof Error && lastError.message === "Invalid JWT public key"
      ? lastError
      : new Error("Invalid JWT public key", { cause: lastError });
  }
  return keys;
}

async function getStaticJwtPublicKeys(
  jwtPublicKey: BackendJwtPublicKey,
): Promise<StaticJwtPublicKeys> {
  const cacheKey = cacheKeyForStaticJwtPublicKey(jwtPublicKey, "*");
  let promise = staticJwtKeySets.get(cacheKey);
  if (!promise) {
    promise = importStaticJwtPublicKeys(jwtPublicKey);
    staticJwtKeySets.set(cacheKey, promise);
  }

  try {
    return await promise;
  } catch (error) {
    staticJwtKeySets.delete(cacheKey);
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
    throw new RequestAuthenticationError("Invalid JWT: Invalid JWT header");
  }

  const algorithm = readString(header.alg);
  if (!algorithm) {
    throw new RequestAuthenticationError("Invalid JWT: Invalid JWT header");
  }

  // An unusable configured key is the server's problem, not the caller's; a
  // token whose algorithm the usable key does not support is the caller's.
  const key = (await getStaticJwtPublicKeys(jwtPublicKey)).get(algorithm);
  if (!key) {
    throw new RequestAuthenticationError("Invalid JWT: unsupported algorithm for configured key");
  }
  try {
    await compactVerify(token, key);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new RequestAuthenticationError(`Invalid JWT: ${message}`, { cause: error });
  }
}

/** @internal How `verifyExternalJwt` checks a token's lifetime. */
export interface JwtLifetimeOptions {
  /** Refuse a token without `exp`. */
  requireExp?: boolean;
  /** Refuse a token whose `nbf` is still ahead. */
  checkNotBefore?: boolean;
  /** Seconds of clock skew allowed on `exp` and `nbf`. */
  clockToleranceSeconds?: number;
}

function ensureJwtNotExpired(payload: JwtPayload, lifetime: JwtLifetimeOptions = {}): void {
  const tolerance = lifetime.clockToleranceSeconds ?? 0;
  const nowSeconds = Math.floor(Date.now() / 1000);
  if (lifetime.checkNotBefore && payload.nbf !== undefined) {
    if (typeof payload.nbf !== "number" || !Number.isFinite(payload.nbf)) {
      throw new RequestAuthenticationError("Invalid JWT payload");
    }
    if (payload.nbf > nowSeconds + tolerance) {
      throw new RequestAuthenticationError("JWT is not yet valid");
    }
  }
  if (payload.exp === undefined) {
    if (lifetime.requireExp) throw new RequestAuthenticationError("JWT has no expiry");
    return;
  }
  if (typeof payload.exp !== "number" || !Number.isInteger(payload.exp) || payload.exp < 0) {
    throw new RequestAuthenticationError("Invalid JWT payload");
  }

  if (payload.exp <= nowSeconds - tolerance) {
    throw new RequestAuthenticationError("JWT has expired");
  }
}

function ensureJwtDomain(payload: JwtPayload, config: ExternalJwtConfig): void {
  if (config.jwtIssuer !== undefined && payload.iss !== config.jwtIssuer) {
    throw new RequestAuthenticationError("JWT issuer does not match the configured issuer");
  }

  if (config.jwtAudience === undefined) {
    return;
  }
  const expected = Array.isArray(config.jwtAudience) ? config.jwtAudience : [config.jwtAudience];
  const actual =
    typeof payload.aud === "string"
      ? [payload.aud]
      : Array.isArray(payload.aud) && payload.aud.every((audience) => typeof audience === "string")
        ? payload.aud
        : [];
  if (!expected.some((audience) => actual.includes(audience))) {
    throw new RequestAuthenticationError("JWT audience does not match the configured audience");
  }
}

/**
 * The JWKS document. When the provider doesn't answer, the
 * `AuthenticationUnavailableError` names the JWKS for the backend's log.
 */
async function availableJwksDocument(
  jwksUrl: string,
  forceRefresh = false,
): Promise<LocalJwksDocument> {
  try {
    return await getRemoteJwksDocument(jwksUrl, forceRefresh);
  } catch (error) {
    if (!isAuthenticationUnavailableError(error)) throw error;
    throw new AuthenticationUnavailableError(`JWKS ${jwksUrl} is unavailable: ${error.message}`, {
      cause: error,
    });
  }
}

/** @internal Verify `token`'s signature, expiry, issuer and audience. */
export async function verifyExternalJwt(
  token: string,
  payload: JwtPayload,
  config: ExternalJwtConfig,
  lifetime: JwtLifetimeOptions = {},
): Promise<void> {
  if (config.jwtPublicKey !== undefined) {
    await verifyJwtSignatureWithStaticKey(token, config.jwtPublicKey);

    ensureJwtNotExpired(payload, lifetime);
    ensureJwtDomain(payload, config);
    return;
  }

  if (config.jwksUrl) {
    // Fetching the JWKS can fail on the server's side (a bad jwksUrl, a
    // provider outage); only a token that fails verification is the caller's.
    let jwks = await availableJwksDocument(config.jwksUrl);
    try {
      await verifyJwtSignatureWithJwks(token, jwks);
    } catch {
      // A refresh that fails throws, so the client retries, and keeps
      // throwing through its cooldown; only a refresh that succeeds can
      // refuse an unknown key.
      jwks = await availableJwksDocument(config.jwksUrl, true);
      try {
        await verifyJwtSignatureWithJwks(token, jwks);
      } catch (refreshError) {
        const message = refreshError instanceof Error ? refreshError.message : String(refreshError);
        throw new RequestAuthenticationError(`Invalid JWT: ${message}`, { cause: refreshError });
      }
    }

    ensureJwtNotExpired(payload, lifetime);
    ensureJwtDomain(payload, config);
    return;
  }

  throw new Error("An external JWT needs a jwksUrl or a jwtPublicKey to verify it");
}
