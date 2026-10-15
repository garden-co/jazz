import { AuthorSubject } from "./ids.js";
import { appIdFromName, appIdFromString, NAMESPACE_DNS } from "./app_id.js";
import { localFirstAccountId } from "./account_registry.js";
import {
  ed25519PublicKey,
  ed25519Sign,
  ed25519Verify,
  sha512,
  uuidV5,
  uuidToString,
} from "../support/hash.js";
import { concatBytes, fromBase64Url, fromUtf8, toBase64Url, utf8 } from "../support/bytes.js";
import { tryParseUuid } from "./ids.js";

const KEY_NAMESPACE = Uint8Array.of(
  0x6a,
  0x61,
  0x7a,
  0x7a,
  0x2d,
  0x61,
  0x75,
  0x74,
  0x68,
  0x2d,
  0x6b,
  0x65,
  0x79,
  0x2d,
  0x76,
  0x31,
);
const SIGN_DOMAIN = "jazz-auth-sign-v1";

export const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";
export const ANONYMOUS_ISSUER = "urn:jazz:anonymous";
const KNOWN_SELF_SIGNED_ISSUERS = [LOCAL_FIRST_ISSUER, ANONYMOUS_ISSUER];
const DEFAULT_MAX_TTL_SECONDS = 3600;

export interface VerifiedSelfSigned {
  userId: string;
  publicKeyBytes: Uint8Array;
  issuer: string;
}

const nowSecs = () => Math.floor(Date.now() / 1000);

function normalizeAudience(audience: string): string {
  return tryParseUuid(audience) ?? uuidToString(uuidV5(NAMESPACE_DNS, utf8(audience)));
}

export function mintJazzSelfSignedToken(
  seed: Uint8Array,
  issuer: string,
  audience: string,
  ttlSeconds: number,
): string {
  return mintJazzSelfSignedTokenAt(seed, issuer, audience, ttlSeconds, nowSecs());
}

export function mintJazzSelfSignedTokenAt(
  seed: Uint8Array,
  issuer: string,
  audience: string,
  ttlSeconds: number,
  nowSeconds: number,
): string {
  const secret = deriveSigningKey(seed, SIGN_DOMAIN);
  const publicKey = ed25519PublicKey(secret);
  const userId = userIdFromPublicKey(publicKey);
  const headerB64 = toBase64Url(utf8(JSON.stringify({ alg: "EdDSA", typ: "JWT" })));
  const claims = {
    iss: issuer,
    sub: userId,
    aud: normalizeAudience(audience),
    jazz_pub_key: toBase64Url(publicKey),
    iat: nowSeconds,
    exp: nowSeconds + ttlSeconds,
  };
  const claimsB64 = toBase64Url(utf8(JSON.stringify(claims)));
  const signingInput = `${headerB64}.${claimsB64}`;
  return `${signingInput}.${toBase64Url(ed25519Sign(utf8(signingInput), secret))}`;
}

export const verifyJazzSelfSignedProof = (token: string, expectedAudience: string) =>
  verifyJazzSelfSignedProofWithMaxTtlAt(
    token,
    expectedAudience,
    DEFAULT_MAX_TTL_SECONDS,
    nowSecs(),
  );
export const verifyJazzSelfSignedProofWithMaxTtl = (
  token: string,
  expectedAudience: string,
  maxTtl: number,
) => verifyJazzSelfSignedProofWithMaxTtlAt(token, expectedAudience, maxTtl, nowSecs());
export const verifyJazzSelfSignedProofAt = (token: string, expectedAudience: string, now: number) =>
  verifyJazzSelfSignedProofWithMaxTtlAt(token, expectedAudience, DEFAULT_MAX_TTL_SECONDS, now);

/** Verify a self-signed proof and bind it to the exact canonical author claimed. */
export function verifyClientRuntimeAuthorAt(
  token: string,
  expectedAudience: string,
  claimedAuthor: string,
  nowSeconds: number,
): AuthorSubject {
  const verified = verifyJazzSelfSignedProofAt(token, expectedAudience, nowSeconds);
  let author = AuthorSubject.reserved(verified.issuer, verified.userId);
  const claimed = AuthorSubject.fromCanonical(claimedAuthor);
  if (claimed.accountId() !== undefined) {
    if (verified.issuer !== LOCAL_FIRST_ISSUER)
      throw new Error("only local-first proofs can found accounts");
    let app: string;
    try {
      app = appIdFromString(expectedAudience);
    } catch {
      app = appIdFromName(expectedAudience);
    }
    author = author.withAccount(localFirstAccountId(app, verified.userId));
  }
  if (author.canonical() !== claimedAuthor)
    throw new Error(
      `self-signed author mismatch: expected ${JSON.stringify(author.canonical())}, got ${JSON.stringify(claimedAuthor)}`,
    );
  return author;
}

export const verifyClientRuntimeAuthor = (
  token: string,
  expectedAudience: string,
  claimedAuthor: string,
) => verifyClientRuntimeAuthorAt(token, expectedAudience, claimedAuthor, nowSecs());

const CLAIM_KEYS = new Set(["iss", "sub", "aud", "jazz_pub_key", "iat", "exp"]);
const dbg = (v: unknown) => (v === undefined ? "None" : `Some(${JSON.stringify(v)})`);

function decodeJsonPart(b64: string, what: string): any {
  let bytes: Uint8Array;
  try {
    bytes = fromBase64Url(b64);
  } catch (e) {
    throw new Error(`${what} decode: ${e}`);
  }
  try {
    return JSON.parse(fromUtf8(bytes));
  } catch (e) {
    throw new Error(`${what} parse: ${e}`);
  }
}

export function verifyJazzSelfSignedProofWithMaxTtlAt(
  token: string,
  expectedAudience: string,
  maxTtlSeconds: number,
  nowSeconds: number,
): VerifiedSelfSigned {
  const normalizedExpected = normalizeAudience(expectedAudience);
  const first = token.indexOf(".");
  const second = first < 0 ? -1 : token.indexOf(".", first + 1);
  if (second < 0) throw new Error("invalid token: expected 3 parts");
  const headerB64 = token.slice(0, first);
  const claimsB64 = token.slice(first + 1, second);
  const sigB64 = token.slice(second + 1);

  const header = decodeJsonPart(headerB64, "header");
  if (header?.alg !== "EdDSA") throw new Error(`unsupported alg: ${dbg(header?.alg)}`);

  const claims = decodeJsonPart(claimsB64, "claims");
  for (const k of Object.keys(claims ?? {}))
    if (!CLAIM_KEYS.has(k)) throw new Error(`claims parse: unknown field \`${k}\``);

  const issuer = KNOWN_SELF_SIGNED_ISSUERS.find((known) => known === (claims.iss ?? ""));
  if (!issuer) throw new Error(`invalid issuer: ${dbg(claims.iss)}`);
  if (claims.aud !== normalizedExpected)
    throw new Error(
      `audience mismatch: expected ${JSON.stringify(normalizedExpected)}, got ${dbg(claims.aud)}`,
    );

  if (claims.jazz_pub_key === undefined) throw new Error("missing jazz_pub_key");
  let pubKey: Uint8Array;
  try {
    pubKey = fromBase64Url(claims.jazz_pub_key);
  } catch (e) {
    throw new Error(`pub key decode: ${e}`);
  }
  if (pubKey.length !== 32) throw new Error(`pub key must be 32 bytes, got ${pubKey.length}`);

  let sig: Uint8Array;
  try {
    sig = fromBase64Url(sigB64);
  } catch (e) {
    throw new Error(`sig decode: ${e}`);
  }
  if (sig.length !== 64) throw new Error("signature must be 64 bytes");
  if (!ed25519Verify(sig, utf8(`${headerB64}.${claimsB64}`), pubKey))
    throw new Error("signature verification failed: signature error");

  const derivedUserId = userIdFromPublicKey(pubKey);
  if (claims.sub === undefined) throw new Error("missing sub");
  if (claims.sub !== derivedUserId)
    throw new Error(`sub mismatch: expected ${derivedUserId}, got ${claims.sub}`);

  const iat = claims.iat;
  const exp = claims.exp;
  if (iat === undefined) throw new Error("missing iat");
  if (exp === undefined) throw new Error("missing exp");
  if (iat > nowSeconds + 60)
    throw new Error(`token issued in the future: iat=${iat}, now=${nowSeconds}`);
  if (exp <= nowSeconds) throw new Error(`token expired: exp=${exp}, now=${nowSeconds}`);
  const ttl = Math.max(0, exp - iat);
  if (ttl > maxTtlSeconds) throw new Error(`token ttl ${ttl}s exceeds max ${maxTtlSeconds}s`);

  return { userId: derivedUserId, publicKeyBytes: pubKey, issuer };
}

/** SHA-512(domain || seed), first 32 bytes, as Ed25519 secret key material. */
export function deriveSigningKey(seed: Uint8Array, domain: string): Uint8Array {
  return sha512(concatBytes(utf8(domain), seed)).slice(0, 32);
}

export const deriveVerifyingKey = (seed: Uint8Array): Uint8Array =>
  ed25519PublicKey(deriveSigningKey(seed, SIGN_DOMAIN));

export const deriveUserId = (seed: Uint8Array): string =>
  userIdFromPublicKey(deriveVerifyingKey(seed));

/** At least one byte other than ASCII whitespace (no Unicode trimming). */
export function principalIsNonempty(principal: string): boolean {
  for (let i = 0; i < principal.length; i++) {
    const c = principal.charCodeAt(i);
    if (c !== 0x20 && c !== 0x09 && c !== 0x0a && c !== 0x0b && c !== 0x0c && c !== 0x0d)
      return true;
  }
  return false;
}

const userIdFromPublicKey = (pubKey: Uint8Array): string =>
  uuidToString(uuidV5(KEY_NAMESPACE, pubKey));
