/**
 * Self-signed Jazz tokens in plain JavaScript, so a thin client needs no
 * WASM to sign in. Byte-for-byte the scheme of `mint_jazz_self_signed_token_at`
 * in `crates/jazz/layers/types/src/identity.rs`: an EdDSA JWT whose key is
 * the first 32 bytes of SHA-512(domain, seed), for the user id
 * UUIDv5(KEY_NAMESPACE, public key).
 */
import { ed25519 } from "@noble/curves/ed25519.js";
import { sha512 } from "@noble/hashes/sha2.js";
import { sha1 } from "@noble/hashes/legacy.js";
import { formatUuid } from "../runtime/native-runtime/native-runtime-adapter.js";

export const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";
export const ANONYMOUS_ISSUER = "urn:jazz:anonymous";

const SIGN_DOMAIN = "jazz-auth-sign-v1";
/** `KEY_NAMESPACE`: the bytes of "jazz-auth-key-v1". */
const KEY_NAMESPACE = new TextEncoder().encode("jazz-auth-key-v1");
/** RFC 4122 DNS namespace, for audiences that are not UUIDs. */
const DNS_NAMESPACE = parseUuidText("6ba7b810-9dad-11d1-80b4-00c04fd430c8")!;
const encoder = new TextEncoder();

export interface SelfSignedTokenOptions {
  /** The 32-byte identity seed, base64url-encoded. */
  secret: string;
  audience: string;
  ttlSeconds: number;
  nowSeconds: bigint;
}

export function mintSelfSignedToken(
  issuer: typeof LOCAL_FIRST_ISSUER | typeof ANONYMOUS_ISSUER,
  options: SelfSignedTokenOptions,
): string {
  const signingKey = signingKeyFromSeed(decodeSeed(options.secret));
  const publicKey = ed25519.getPublicKey(signingKey);
  const audienceUuid = parseUuidText(options.audience);
  const audience = formatUuid(
    audienceUuid ?? uuidV5(DNS_NAMESPACE, encoder.encode(options.audience)),
  );
  const header = base64url(encoder.encode(JSON.stringify({ alg: "EdDSA", typ: "JWT" })));
  const iat = Number(options.nowSeconds);
  const claims = base64url(
    encoder.encode(
      JSON.stringify({
        iss: issuer,
        sub: formatUuid(uuidV5(KEY_NAMESPACE, publicKey)),
        aud: audience,
        jazz_pub_key: base64url(publicKey),
        iat,
        exp: iat + options.ttlSeconds,
      }),
    ),
  );
  const signingInput = `${header}.${claims}`;
  const signature = ed25519.sign(encoder.encode(signingInput), signingKey);
  return `${signingInput}.${base64url(signature)}`;
}

/** The user id a seed signs as. */
export function selfSignedUserId(secret: string): string {
  const publicKey = ed25519.getPublicKey(signingKeyFromSeed(decodeSeed(secret)));
  return formatUuid(uuidV5(KEY_NAMESPACE, publicKey));
}

function signingKeyFromSeed(seed: Uint8Array): Uint8Array {
  const input = new Uint8Array(SIGN_DOMAIN.length + seed.length);
  input.set(encoder.encode(SIGN_DOMAIN));
  input.set(seed, SIGN_DOMAIN.length);
  return sha512(input).slice(0, 32);
}

function decodeSeed(secret: string): Uint8Array {
  const padding = "=".repeat((4 - (secret.length % 4)) % 4);
  const binary = atob(secret.replace(/-/g, "+").replace(/_/g, "/") + padding);
  const seed = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  if (seed.length !== 32) throw new Error("seed must be exactly 32 bytes");
  return seed;
}

function uuidV5(namespace: Uint8Array, name: Uint8Array): Uint8Array {
  const input = new Uint8Array(namespace.length + name.length);
  input.set(namespace);
  input.set(name, namespace.length);
  const bytes = sha1(input).slice(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  return bytes;
}

/** A UUID in any spelling `uuid::Uuid::parse_str` accepts. */
function parseUuidText(text: string): Uint8Array | undefined {
  const bare = text.replace(/^urn:uuid:/i, "").replace(/^\{(.*)\}$/, "$1");
  const hex =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(bare) ||
    /^[0-9a-f]{32}$/i.test(bare)
      ? bare.replace(/-/g, "")
      : undefined;
  if (!hex) return undefined;
  return Uint8Array.from({ length: 16 }, (_, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
}

function base64url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
