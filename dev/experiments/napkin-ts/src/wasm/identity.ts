// Local-first self-signed identity tokens.
import { concatBytes, toBase64Url, utf8 } from "../support/bytes.js";
import {
  ed25519PublicKey,
  ed25519Sign,
  sha512,
  uuidFromString,
  uuidToString,
  uuidV5,
} from "../support/hash.js";

const KEY_NAMESPACE = utf8("jazz-auth-key-v1");
const NAMESPACE_DNS = uuidFromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
const SIGN_DOMAIN = "jazz-auth-sign-v1";

export const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";
export const ANONYMOUS_ISSUER = "urn:jazz:anonymous";

const UUID_RE =
  /^(urn:uuid:)?\{?[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}\}?$/i;

export function mintJazzSelfSignedTokenAt(
  seed: Uint8Array,
  issuer: string,
  audience: string,
  ttlSeconds: number,
  nowSeconds: number,
): string {
  const signingKey = deriveSigningKey(seed);
  const publicKey = ed25519PublicKey(signingKey);
  const normalizedAud = UUID_RE.test(audience)
    ? uuidToString(uuidFromString(audience.replace(/^urn:uuid:|[{}]/gi, "")))
    : uuidToString(uuidV5(NAMESPACE_DNS, utf8(audience)));
  const header = toBase64Url(utf8(JSON.stringify({ alg: "EdDSA", typ: "JWT" })));
  const claims = toBase64Url(
    utf8(
      JSON.stringify({
        iss: issuer,
        sub: uuidToString(userIdFromPublicKey(publicKey)),
        aud: normalizedAud,
        jazz_pub_key: toBase64Url(publicKey),
        iat: nowSeconds,
        exp: nowSeconds + ttlSeconds,
      }),
    ),
  );
  const signingInput = `${header}.${claims}`;
  return `${signingInput}.${toBase64Url(ed25519Sign(utf8(signingInput), signingKey))}`;
}

export function deriveUserId(seed: Uint8Array): Uint8Array {
  return userIdFromPublicKey(ed25519PublicKey(deriveSigningKey(seed)));
}

function deriveSigningKey(seed: Uint8Array): Uint8Array {
  return sha512(concatBytes(utf8(SIGN_DOMAIN), seed)).slice(0, 32);
}

function userIdFromPublicKey(publicKey: Uint8Array): Uint8Array {
  return uuidV5(KEY_NAMESPACE, publicKey);
}
