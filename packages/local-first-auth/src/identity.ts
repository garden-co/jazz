import { ed25519 } from "@noble/curves/ed25519.js";
import { sha1 } from "@noble/hashes/legacy.js";
import { sha512 } from "@noble/hashes/sha2.js";
import { base64urlDecode, base64urlEncode, formatUuid, fromUtf8, utf8 } from "./encoding.js";
import { parseSecret } from "./secret.js";

/** The `iss` of every local-first token. Kept from the format's origin for compatibility. */
export const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";

const SIGN_DOMAIN = "jazz-auth-sign-v1";
/** UUID namespace for subjects: the ASCII bytes of "jazz-auth-key-v1". */
const SUBJECT_NAMESPACE = utf8("jazz-auth-key-v1");
const DEFAULT_TTL_SECONDS = 3600;
const MAX_CLOCK_SKEW_SECONDS = 60;

export interface LocalFirstIdentity {
  /** Always {@link LOCAL_FIRST_ISSUER}. */
  issuer: typeof LOCAL_FIRST_ISSUER;
  /** A UUID derived from the public key. The same secret always gives the same subject. */
  subject: string;
  /** The raw 32-byte Ed25519 public key. */
  publicKey: Uint8Array;
}

export class LocalFirstTokenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LocalFirstTokenError";
  }
}

function uuidV5(namespace: Uint8Array, name: Uint8Array): string {
  const input = new Uint8Array(namespace.length + name.length);
  input.set(namespace);
  input.set(name, namespace.length);
  const digest = sha1(input).slice(0, 16);
  digest[6] = (digest[6]! & 0x0f) | 0x50;
  digest[8] = (digest[8]! & 0x3f) | 0x80;
  return formatUuid(digest);
}

const DNS_NAMESPACE = Uint8Array.from([
  0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);
const UUID_FORMS = [
  /^([0-9a-f]{8})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{12})$/i,
  /^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$/i,
  /^\{([0-9a-f]{8})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{12})\}$/i,
  // Like Rust's uuid parser, the urn prefix is lowercase only; the hex is any case.
  /^urn:uuid:([0-9a-fA-F]{8})-([0-9a-fA-F]{4})-([0-9a-fA-F]{4})-([0-9a-fA-F]{4})-([0-9a-fA-F]{12})$/,
];

/**
 * The `aud` a token carries for `audience`, the same way Jazz's Rust core does
 * it: a UUID (hyphenated, simple, braced or `urn:uuid:`) becomes its lowercase
 * hyphenated form, and any other string becomes UUIDv5(DNS namespace, audience).
 * Signing and verifying both normalize, so either form of an audience matches.
 * Throws a TypeError for a missing or empty audience: that is a configuration
 * error, not an invalid token.
 */
export function normalizeAudience(audience: string): string {
  if (typeof audience !== "string" || audience.length === 0) {
    throw new TypeError("audience must be a non-empty string");
  }
  for (const form of UUID_FORMS) {
    const match = form.exec(audience);
    if (match) return match.slice(1).join("-").toLowerCase();
  }
  return uuidV5(DNS_NAMESPACE, utf8(audience));
}

function signingKey(secret: string): Uint8Array {
  const seed = parseSecret(secret);
  // SHA-512(domain || seed)[..32] is the Ed25519 secret key, as in Jazz's Rust core.
  const domain = utf8(SIGN_DOMAIN);
  const input = new Uint8Array(domain.length + seed.length);
  input.set(domain);
  input.set(seed, domain.length);
  return sha512(input).slice(0, 32);
}

function subjectOf(publicKey: Uint8Array): string {
  return uuidV5(SUBJECT_NAMESPACE, publicKey);
}

/** The identity a secret stands for. Pure and offline. */
export function identityOf(secret: string): LocalFirstIdentity {
  const publicKey = ed25519.getPublicKey(signingKey(secret));
  return { issuer: LOCAL_FIRST_ISSUER, subject: subjectOf(publicKey), publicKey };
}

export interface SignTokenOptions {
  /**
   * Who the token is for, e.g. `https://app.example/jazz` or an app ID.
   * Required and non-empty; normalized by {@link normalizeAudience}.
   */
  audience: string;
  /**
   * Lifetime in seconds, a positive integer. Defaults to 3600, which is also the
   * verifier's default maximum.
   */
  ttlSeconds?: number;
  /** Override the clock, in Unix seconds. */
  now?: number;
}

const nowSeconds = () => Math.floor(Date.now() / 1000);

/**
 * A compact EdDSA JWT proving control of the secret's identity:
 * `{ iss, sub, aud, jazz_pub_key, iat, exp }`.
 */
export function signToken(secret: string, options: SignTokenOptions): string {
  const aud = normalizeAudience(options.audience);
  const ttl = options.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  if (!Number.isSafeInteger(ttl) || ttl <= 0) {
    throw new LocalFirstTokenError("ttlSeconds must be a positive integer");
  }
  const iat = options.now ?? nowSeconds();
  if (!Number.isSafeInteger(iat) || iat < 0) {
    throw new LocalFirstTokenError("now must be a non-negative integer");
  }
  const key = signingKey(secret);
  const publicKey = ed25519.getPublicKey(key);
  const header = base64urlEncode(utf8(JSON.stringify({ alg: "EdDSA", typ: "JWT" })));
  const claims = base64urlEncode(
    utf8(
      JSON.stringify({
        iss: LOCAL_FIRST_ISSUER,
        sub: subjectOf(publicKey),
        aud,
        jazz_pub_key: base64urlEncode(publicKey),
        iat,
        exp: iat + ttl,
      }),
    ),
  );
  const signature = ed25519.sign(utf8(`${header}.${claims}`), key);
  return `${header}.${claims}.${base64urlEncode(signature)}`;
}

export interface VerifyTokenOptions {
  /** The audience this verifier accepts. Required; normalized like {@link SignTokenOptions.audience}. */
  audience: string;
  /** The longest lifetime (`exp - iat`) accepted. Defaults to 3600. */
  maxTtlSeconds?: number;
  /** Override the clock, in Unix seconds. */
  now?: number;
}

const CLAIMS = new Set(["iss", "sub", "aud", "jazz_pub_key", "iat", "exp"]);

function decodeJson(part: string, what: string): Record<string, unknown> {
  let value: unknown;
  try {
    value = JSON.parse(fromUtf8(base64urlDecode(part)));
  } catch {
    throw new LocalFirstTokenError(`invalid token ${what}`);
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new LocalFirstTokenError(`invalid token ${what}`);
  }
  return value as Record<string, unknown>;
}

function isSmallOrder(bytes: Uint8Array): boolean {
  try {
    return ed25519.Point.fromBytes(bytes).isSmallOrder();
  } catch {
    return true;
  }
}

/**
 * Verify a local-first token and return the identity it proves. Throws
 * {@link LocalFirstTokenError} for anything else. Follows the rules of Jazz's
 * Rust verifier for local-first tokens (`iss` = {@link LOCAL_FIRST_ISSUER}
 * only; Jazz's anonymous issuer is not accepted here).
 */
export function verifyToken(token: string, options: VerifyTokenOptions): LocalFirstIdentity {
  const expectedAudience = normalizeAudience(options.audience);
  if (typeof token !== "string") throw new LocalFirstTokenError("token must be a string");
  const parts = token.split(".");
  if (parts.length !== 3) throw new LocalFirstTokenError("invalid token: expected 3 parts");
  const [headerPart, claimsPart, signaturePart] = parts as [string, string, string];

  const header = decodeJson(headerPart, "header");
  if (header.alg !== "EdDSA") throw new LocalFirstTokenError("unsupported alg");

  const claims = decodeJson(claimsPart, "claims");
  for (const name of Object.keys(claims)) {
    if (!CLAIMS.has(name)) throw new LocalFirstTokenError(`unknown claim ${name}`);
  }
  if (claims.iss !== LOCAL_FIRST_ISSUER) throw new LocalFirstTokenError("invalid issuer");
  if (typeof claims.aud !== "string" || claims.aud !== expectedAudience)
    throw new LocalFirstTokenError("audience mismatch");

  let publicKey: Uint8Array;
  let signature: Uint8Array;
  try {
    publicKey = base64urlDecode(String(claims.jazz_pub_key ?? ""));
    signature = base64urlDecode(signaturePart);
  } catch {
    throw new LocalFirstTokenError("invalid key or signature encoding");
  }
  if (publicKey.length !== 32) throw new LocalFirstTokenError("public key must be 32 bytes");
  if (signature.length !== 64) throw new LocalFirstTokenError("signature must be 64 bytes");
  // Like ed25519-dalek's verify_strict: no small-order key or R, no ZIP-215 leniency.
  if (isSmallOrder(publicKey) || isSmallOrder(signature.subarray(0, 32))) {
    throw new LocalFirstTokenError("signature verification failed");
  }
  let valid = false;
  try {
    valid = ed25519.verify(signature, utf8(`${headerPart}.${claimsPart}`), publicKey, {
      zip215: false,
    });
  } catch {
    valid = false;
  }
  if (!valid) throw new LocalFirstTokenError("signature verification failed");

  const subject = subjectOf(publicKey);
  if (claims.sub !== subject) throw new LocalFirstTokenError("subject does not match key");

  const { iat, exp } = claims;
  if (!Number.isSafeInteger(iat) || !Number.isSafeInteger(exp) || (iat as number) < 0) {
    throw new LocalFirstTokenError("missing iat or exp");
  }
  const now = options.now ?? nowSeconds();
  if ((iat as number) > now + MAX_CLOCK_SKEW_SECONDS) {
    throw new LocalFirstTokenError("token issued in the future");
  }
  if ((exp as number) <= now) throw new LocalFirstTokenError("token expired");
  const ttl = Math.max(0, (exp as number) - (iat as number));
  if (ttl > (options.maxTtlSeconds ?? DEFAULT_TTL_SECONDS)) {
    throw new LocalFirstTokenError("token lifetime too long");
  }
  return { issuer: LOCAL_FIRST_ISSUER, subject, publicKey };
}
