import type { LocalAuthMode } from "./context.js";

const LOCAL_AUTH_TOKEN_STORAGE_PREFIX = "jazz-tools:local-auth-token:";

export interface LocalAuthStorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

type LocalAuthDefaultsInput = {
  appId: string;
  auth?: { localFirstSecret: string };
  jwtToken?: string;
  backendSecret?: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
};

interface ResolveLocalAuthDefaultsOptions {
  storage?: LocalAuthStorageLike;
}

function trimOptional(value?: string): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function generateLocalAuthToken(): string {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return cryptoObj.randomUUID();
  }

  return `tok-${Math.random().toString(16).slice(2)}-${Date.now().toString(16)}`;
}

function tryGetStorage(storage?: LocalAuthStorageLike): LocalAuthStorageLike | undefined {
  if (storage) return storage;
  if (typeof globalThis === "undefined") return undefined;

  try {
    const maybeStorage = (globalThis as { localStorage?: LocalAuthStorageLike }).localStorage;
    return maybeStorage;
  } catch {
    return undefined;
  }
}

export function localAuthTokenStorageKey(appId: string, mode: LocalAuthMode): string {
  return `${LOCAL_AUTH_TOKEN_STORAGE_PREFIX}${appId}:${mode}`;
}

function loadOrCreateLocalAuthToken(
  appId: string,
  mode: LocalAuthMode,
  storage?: LocalAuthStorageLike,
): string {
  if (storage) {
    const key = localAuthTokenStorageKey(appId, mode);
    try {
      const existing = trimOptional(storage.getItem(key) ?? undefined);
      if (existing) return existing;
    } catch {
      // Continue with token generation.
    }

    const token = generateLocalAuthToken();
    try {
      storage.setItem(key, token);
    } catch {
      // Ignore write failures (private mode/quota) and still return token.
    }
    return token;
  }

  return generateLocalAuthToken();
}

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
export function resolveLocalAuthDefaults<T extends LocalAuthDefaultsInput>(
  config: T,
  options: ResolveLocalAuthDefaultsOptions = {},
): T & { localAuthMode?: LocalAuthMode; localAuthToken?: string } {
  // Self-signed auth handles its own JWT; skip local auth defaults.
  if (config.auth) {
    return config;
  }

  const storage = tryGetStorage(options.storage);
  const explicitJwtToken = trimOptional(config.jwtToken);
  const explicitBackendSecret = trimOptional(config.backendSecret);
  const explicitMode = config.localAuthMode;
  const explicitToken = trimOptional(config.localAuthToken);

  // Respect external/backend auth unless local auth was explicitly requested.
  if (!explicitMode && !explicitToken && (explicitJwtToken || explicitBackendSecret)) {
    return config;
  }

  let localAuthMode = explicitMode;
  let localAuthToken = explicitToken;

  if (!localAuthMode && localAuthToken) {
    localAuthMode = "anonymous";
  }

  if (!localAuthMode && !localAuthToken && storage) {
    localAuthMode = "anonymous";
  }

  if (!localAuthMode) {
    return config;
  }

  if (!localAuthToken) {
    localAuthToken = loadOrCreateLocalAuthToken(config.appId, localAuthMode, storage);
  }

  return {
    ...config,
    localAuthMode,
    localAuthToken,
  };
}
