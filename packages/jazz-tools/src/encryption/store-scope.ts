import { accountAppId } from "../accounts/local-first.js";

/**
 * The private store scope of `accountId`'s keys in an app: the account domain
 * `json([appId, env, account])` (E2EE_PROTOCOL.md §0), as the runtime names
 * it, with the app id in the runtime's form (a name becomes its UUID).
 */
export function encryptionStoreScope(
  appId: string,
  environment: string,
  accountId: string,
): string {
  return JSON.stringify([accountAppId(appId), environment, accountId]);
}
