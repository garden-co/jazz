import type { LocalAuthMode } from "./runtime/context.js";

const STORAGE_PREFIX = "jazz-tools:synthetic-users:";

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export interface SyntheticUserProfile {
  id: string;
  name: string;
  mode: LocalAuthMode;
  token: string;
}

export interface SyntheticUserStore {
  activeProfileId: string;
  profiles: SyntheticUserProfile[];
}

export interface SyntheticUserStorageOptions {
  storage?: StorageLike;
  storageKey?: string;
  defaultMode?: LocalAuthMode;
}

export interface ActiveSyntheticAuth {
  localAuthMode: LocalAuthMode;
  localAuthToken: string;
  profile: SyntheticUserProfile;
}

function getStorage(storage?: StorageLike): StorageLike | undefined {
  if (storage) return storage;
  if (typeof globalThis === "undefined") return undefined;
  const candidate = (globalThis as { localStorage?: StorageLike }).localStorage;
  return candidate;
}

function randomToken(): string {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return cryptoObj.randomUUID();
  }

  return `tok-${Math.random().toString(16).slice(2)}-${Date.now().toString(16)}`;
}

function defaultStore(defaultMode: LocalAuthMode): SyntheticUserStore {
  const first = createSyntheticUserProfile("User 1", defaultMode);
  const second = createSyntheticUserProfile("User 2", "demo");
  return {
    activeProfileId: first.id,
    profiles: [first, second],
  };
}

function normalizeStore(input: unknown): SyntheticUserStore | null {
  if (!input || typeof input !== "object") return null;
  const maybeStore = input as Partial<SyntheticUserStore>;
  if (!Array.isArray(maybeStore.profiles) || maybeStore.profiles.length === 0) return null;

  const profiles: SyntheticUserProfile[] = [];
  for (const rawProfile of maybeStore.profiles) {
    if (!rawProfile || typeof rawProfile !== "object") continue;
    const profile = rawProfile as Partial<SyntheticUserProfile>;
    if (!profile.id || !profile.name || !profile.token) continue;
    const mode = profile.mode === "anonymous" ? "anonymous" : "demo";
    profiles.push({
      id: profile.id,
      name: profile.name,
      mode,
      token: profile.token,
    });
  }

  if (profiles.length === 0) return null;
  const fallbackProfile = profiles[0];
  if (!fallbackProfile) return null;
  const activeProfileId = profiles.some((p) => p.id === maybeStore.activeProfileId)
    ? (maybeStore.activeProfileId as string)
    : fallbackProfile.id;

  return {
    activeProfileId,
    profiles,
  };
}

export function syntheticUserStorageKey(appId: string, overrideKey?: string): string {
  return overrideKey ?? `${STORAGE_PREFIX}${appId}`;
}

export function createSyntheticUserProfile(
  name: string,
  mode: LocalAuthMode = "demo",
): SyntheticUserProfile {
  const cleanName = name.trim() || "Synthetic User";
  return {
    id: randomToken(),
    name: cleanName,
    mode,
    token: randomToken(),
  };
}

export function loadSyntheticUserStore(
  appId: string,
  options: SyntheticUserStorageOptions = {},
): SyntheticUserStore {
  const storage = getStorage(options.storage);
  const key = syntheticUserStorageKey(appId, options.storageKey);
  const fallbackMode = options.defaultMode ?? "demo";

  if (!storage) return defaultStore(fallbackMode);

  const raw = storage.getItem(key);
  if (!raw) {
    const fallback = defaultStore(fallbackMode);
    storage.setItem(key, JSON.stringify(fallback));
    return fallback;
  }

  try {
    const parsed = JSON.parse(raw);
    const normalized = normalizeStore(parsed);
    if (!normalized) {
      const fallback = defaultStore(fallbackMode);
      storage.setItem(key, JSON.stringify(fallback));
      return fallback;
    }
    return normalized;
  } catch {
    const fallback = defaultStore(fallbackMode);
    storage.setItem(key, JSON.stringify(fallback));
    return fallback;
  }
}

export function saveSyntheticUserStore(
  appId: string,
  store: SyntheticUserStore,
  options: SyntheticUserStorageOptions = {},
): void {
  const storage = getStorage(options.storage);
  if (!storage) return;
  const key = syntheticUserStorageKey(appId, options.storageKey);
  storage.setItem(key, JSON.stringify(store));
}

export function getActiveSyntheticAuth(
  appId: string,
  options: SyntheticUserStorageOptions = {},
): ActiveSyntheticAuth {
  const store = loadSyntheticUserStore(appId, options);
  const fallbackProfile = store.profiles[0];
  if (!fallbackProfile) {
    throw new Error("Synthetic user store must contain at least one profile.");
  }
  const profile =
    store.profiles.find((entry) => entry.id === store.activeProfileId) ?? fallbackProfile;
  return {
    localAuthMode: profile.mode,
    localAuthToken: profile.token,
    profile,
  };
}

export function setActiveSyntheticProfile(
  appId: string,
  profileId: string,
  options: SyntheticUserStorageOptions = {},
): SyntheticUserStore {
  const store = loadSyntheticUserStore(appId, options);
  if (!store.profiles.some((profile) => profile.id === profileId)) {
    return store;
  }
  const nextStore = {
    ...store,
    activeProfileId: profileId,
  };
  saveSyntheticUserStore(appId, nextStore, options);
  return nextStore;
}
