import { SECRET_PREFIX, signToken, verifyToken } from "../local-first-auth/index.js";

/**
 * Local-first tokens for Jazz, minted and verified by
 * the self-contained `src/local-first-auth` module instead of the native runtimes. The audience is
 * the raw app ID; the package normalizes it exactly as the Rust core does (any
 * UUID form parsed, other names as a UUIDv5), so these tokens are
 * byte-identical to the ones the native runtimes used to mint.
 */
export function mintLocalFirstTokenFromSeed(
  seed: string,
  appId: string,
  ttlSeconds: number,
  nowSeconds?: number,
): string {
  return signToken(`${SECRET_PREFIX}${seed}`, {
    audience: appId,
    ttlSeconds,
    now: nowSeconds,
  });
}

/** The subject proven by a local-first token for `appId`; throws if it proves none. */
export function verifyLocalFirstToken(token: string, appId: string): string {
  return verifyToken(token, { audience: appId }).subject;
}
