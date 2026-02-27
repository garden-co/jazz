import type { LocalAuthMode } from "./context.js";
export interface LocalAuthStorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}
type LocalAuthDefaultsInput = {
  appId: string;
  jwtToken?: string;
  backendSecret?: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
};
interface ResolveLocalAuthDefaultsOptions {
  storage?: LocalAuthStorageLike;
}
export declare function localAuthTokenStorageKey(appId: string, mode: LocalAuthMode): string;
/**
 * Resolve local-auth defaults for client-side DX.
 *
 * Behavior:
 * - If `localAuthToken` is provided without a mode, defaults mode to `anonymous`.
 * - If a mode is set without token, generates one (persisted to localStorage when available).
 * - If no auth is configured and browser storage is available, defaults to anonymous mode
 *   with a persisted per-app device token.
 * - If JWT/backend auth is set and no local auth is explicitly provided, keeps local auth unset.
 */
export declare function resolveLocalAuthDefaults<T extends LocalAuthDefaultsInput>(
  config: T,
  options?: ResolveLocalAuthDefaultsOptions,
): T & {
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
};

//# sourceMappingURL=local-auth.d.ts.map
