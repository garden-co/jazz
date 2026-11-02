import { verifyToken, type LocalFirstIdentity, type VerifyTokenOptions } from "./identity.js";
import { LOCAL_FIRST_ISSUER } from "./identity.js";
import { base64urlDecode, fromUtf8 } from "./encoding.js";

function bearerOf(request: Request | { headers: Headers }): string | null {
  const header = request.headers.get("authorization");
  const match = header?.match(/^Bearer\s+(\S+)$/i);
  return match ? match[1]! : null;
}

function issuerOf(token: string): unknown {
  try {
    const claims = token.split(".")[1] ?? "";
    const json = fromUtf8(base64urlDecode(claims));
    return (JSON.parse(json) as { iss?: unknown }).iss;
  } catch {
    return undefined;
  }
}

/**
 * The local-first identity proven by the request's `Authorization: Bearer`
 * token. Returns null when there is no local-first bearer, so an
 * `authenticate` function can fall through to other sign-in methods. Throws
 * {@link LocalFirstTokenError} when a local-first bearer is present but invalid.
 */
export function verifyRequest(
  request: Request | { headers: Headers },
  options: VerifyTokenOptions,
): LocalFirstIdentity | null {
  const token = bearerOf(request);
  if (!token || issuerOf(token) !== LOCAL_FIRST_ISSUER) return null;
  return verifyToken(token, options);
}

export { verifyToken, LocalFirstTokenError, LOCAL_FIRST_ISSUER } from "./identity.js";
export type { LocalFirstIdentity, VerifyTokenOptions } from "./identity.js";
