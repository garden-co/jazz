import { SECRET_PREFIX, signToken, verifyToken } from "@garden-co/local-first-auth";
import { accountAppId } from "../accounts/local-first.js";

/**
 * Local-first tokens for Jazz, minted and verified by
 * `@garden-co/local-first-auth` instead of the native runtimes. The audience is
 * the app ID normalized as the Rust core does (UUIDs as-is, other names as a
 * UUIDv5), so these tokens are byte-identical to the ones the native runtimes
 * used to mint.
 */
export function mintLocalFirstTokenFromSeed(
  seed: string,
  appId: string,
  ttlSeconds: number,
  nowSeconds?: number,
): string {
  return signToken(`${SECRET_PREFIX}${seed}`, {
    audience: accountAppId(appId),
    ttlSeconds,
    now: nowSeconds,
  });
}

/** The subject proven by a local-first token for `appId`; throws if it proves none. */
export function verifyLocalFirstToken(token: string, appId: string): string {
  return verifyToken(token, { audience: accountAppId(appId) }).subject;
}
