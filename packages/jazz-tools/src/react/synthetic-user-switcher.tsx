import { useMemo, useState } from "react";
import type { LocalAuthMode } from "../runtime/context.js";
import {
  createSyntheticUserProfile,
  loadSyntheticUserStore,
  saveSyntheticUserStore,
  setActiveSyntheticProfile,
  type SyntheticUserProfile,
  type SyntheticUserStorageOptions,
  type SyntheticUserStore,
} from "./synthetic-users.js";

export interface SyntheticUserSwitcherProps extends SyntheticUserStorageOptions {
  appId: string;
  className?: string;
  reloadOnSwitch?: boolean;
  onProfileChange?: (profile: SyntheticUserProfile) => void;
}

function getActiveProfile(store: SyntheticUserStore): SyntheticUserProfile {
  const fallbackProfile = store.profiles[0];
  if (!fallbackProfile) {
    throw new Error("Synthetic user store must contain at least one profile.");
  }
  return store.profiles.find((profile) => profile.id === store.activeProfileId) ?? fallbackProfile;
}

export function SyntheticUserSwitcher({
  appId,
  className,
  reloadOnSwitch = true,
  onProfileChange,
  ...storageOptions
}: SyntheticUserSwitcherProps) {
  const stableStorageOptions = useMemo(
    () => ({
      storage: storageOptions.storage,
      storageKey: storageOptions.storageKey,
      defaultMode: storageOptions.defaultMode,
    }),
    [storageOptions.defaultMode, storageOptions.storage, storageOptions.storageKey],
  );

  const [store, setStore] = useState<SyntheticUserStore>(() =>
    loadSyntheticUserStore(appId, stableStorageOptions),
  );
  const activeProfile = getActiveProfile(store);

  const applyStore = (nextStore: SyntheticUserStore, triggerReload: boolean) => {
    saveSyntheticUserStore(appId, nextStore, stableStorageOptions);
    setStore(nextStore);
    onProfileChange?.(getActiveProfile(nextStore));
    if (triggerReload && reloadOnSwitch && typeof window !== "undefined") {
      window.location.reload();
    }
  };

  const handleSwitch = (profileId: string) => {
    const nextStore = setActiveSyntheticProfile(appId, profileId, stableStorageOptions);
    setStore(nextStore);
    onProfileChange?.(getActiveProfile(nextStore));
    if (reloadOnSwitch && typeof window !== "undefined") {
      window.location.reload();
    }
  };

  const handleModeChange = (mode: LocalAuthMode) => {
    const nextStore: SyntheticUserStore = {
      ...store,
      profiles: store.profiles.map((profile) =>
        profile.id === store.activeProfileId ? { ...profile, mode } : profile,
      ),
    };
    applyStore(nextStore, false);
  };

  const handleAddProfile = () => {
    const suggestedName = `User ${store.profiles.length + 1}`;
    const rawName =
      typeof window !== "undefined"
        ? window.prompt("New synthetic user name", suggestedName)
        : suggestedName;
    if (rawName === null) return;
    const name = rawName.trim() || suggestedName;
    const profile = createSyntheticUserProfile(name, "demo");
    const nextStore: SyntheticUserStore = {
      activeProfileId: profile.id,
      profiles: [...store.profiles, profile],
    };
    applyStore(nextStore, true);
  };

  const handleRemoveProfile = () => {
    if (store.profiles.length <= 1) return;
    const nextProfiles = store.profiles.filter((profile) => profile.id !== store.activeProfileId);
    const nextActiveProfile = nextProfiles[0];
    if (!nextActiveProfile) return;
    const nextStore: SyntheticUserStore = {
      activeProfileId: nextActiveProfile.id,
      profiles: nextProfiles,
    };
    applyStore(nextStore, true);
  };

  return (
    <div className={className}>
      <label>
        Synthetic User{" "}
        <select
          value={store.activeProfileId}
          onChange={(event) => handleSwitch(event.target.value)}
        >
          {store.profiles.map((profile) => (
            <option key={profile.id} value={profile.id}>
              {profile.name} ({profile.mode})
            </option>
          ))}
        </select>
      </label>{" "}
      <label>
        Mode{" "}
        <select
          value={activeProfile.mode}
          onChange={(event) => handleModeChange(event.target.value as LocalAuthMode)}
        >
          <option value="anonymous">anonymous</option>
          <option value="demo">demo</option>
        </select>
      </label>{" "}
      <button type="button" onClick={handleAddProfile}>
        Add
      </button>{" "}
      <button type="button" disabled={store.profiles.length <= 1} onClick={handleRemoveProfile}>
        Remove
      </button>
    </div>
  );
}
