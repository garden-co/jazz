import type { LocalAuthMode } from "./runtime/context.js";
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
export declare function syntheticUserStorageKey(appId: string, overrideKey?: string): string;
export declare function createSyntheticUserProfile(
  name: string,
  mode?: LocalAuthMode,
): SyntheticUserProfile;
export declare function loadSyntheticUserStore(
  appId: string,
  options?: SyntheticUserStorageOptions,
): SyntheticUserStore;
export declare function saveSyntheticUserStore(
  appId: string,
  store: SyntheticUserStore,
  options?: SyntheticUserStorageOptions,
): void;
export declare function getActiveSyntheticAuth(
  appId: string,
  options?: SyntheticUserStorageOptions,
): ActiveSyntheticAuth;
export declare function setActiveSyntheticProfile(
  appId: string,
  profileId: string,
  options?: SyntheticUserStorageOptions,
): SyntheticUserStore;
//# sourceMappingURL=synthetic-users.d.ts.map
