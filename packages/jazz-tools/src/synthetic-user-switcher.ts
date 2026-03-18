import type { LocalAuthMode } from "./runtime/context.js";
import {
  createSyntheticUserProfile,
  loadSyntheticUserStore,
  saveSyntheticUserStore,
  setActiveSyntheticProfile,
  type SyntheticUserProfile,
  type SyntheticUserStorageOptions,
  type SyntheticUserStore,
} from "./synthetic-users.js";

export interface SyntheticUserSwitcherOptions extends SyntheticUserStorageOptions {
  appId: string;
  container: HTMLElement;
  className?: string;
  reloadOnSwitch?: boolean;
  onProfileChange?: (profile: SyntheticUserProfile) => void;
}

export interface SyntheticUserSwitcherHandle {
  destroy(): void;
  getStore(): SyntheticUserStore;
  getActiveProfile(): SyntheticUserProfile;
  rerender(): void;
}

function activeProfile(store: SyntheticUserStore): SyntheticUserProfile {
  const fallbackProfile = store.profiles[0];
  if (!fallbackProfile) {
    throw new Error("Synthetic user store must contain at least one profile.");
  }
  return store.profiles.find((profile) => profile.id === store.activeProfileId) ?? fallbackProfile;
}

function shouldReload(reloadOnSwitch: boolean): boolean {
  return reloadOnSwitch && typeof window !== "undefined";
}

export function createSyntheticUserSwitcher(
  options: SyntheticUserSwitcherOptions,
): SyntheticUserSwitcherHandle {
  const {
    appId,
    container,
    className,
    reloadOnSwitch = true,
    onProfileChange,
    ...storageOptions
  } = options;

  const stableStorageOptions = {
    storage: storageOptions.storage,
    storageKey: storageOptions.storageKey,
    defaultMode: storageOptions.defaultMode,
  };

  let store = loadSyntheticUserStore(appId, stableStorageOptions);

  const root = document.createElement("div");
  if (className) {
    root.className = className;
  }

  const userLabel = document.createElement("label");
  userLabel.textContent = "Synthetic User ";

  const profileSelect = document.createElement("select");
  userLabel.appendChild(profileSelect);

  const modeLabel = document.createElement("label");
  modeLabel.textContent = " Mode ";

  const modeSelect = document.createElement("select");
  const anonymousOption = document.createElement("option");
  anonymousOption.value = "anonymous";
  anonymousOption.textContent = "anonymous";
  modeSelect.appendChild(anonymousOption);

  const demoOption = document.createElement("option");
  demoOption.value = "demo";
  demoOption.textContent = "demo";
  modeSelect.appendChild(demoOption);
  modeLabel.appendChild(modeSelect);

  const addButton = document.createElement("button");
  addButton.type = "button";
  addButton.textContent = "Add";

  const removeButton = document.createElement("button");
  removeButton.type = "button";
  removeButton.textContent = "Remove";

  root.appendChild(userLabel);
  root.appendChild(document.createTextNode(" "));
  root.appendChild(modeLabel);
  root.appendChild(document.createTextNode(" "));
  root.appendChild(addButton);
  root.appendChild(document.createTextNode(" "));
  root.appendChild(removeButton);
  container.appendChild(root);

  const render = () => {
    profileSelect.innerHTML = "";
    for (const profile of store.profiles) {
      const option = document.createElement("option");
      option.value = profile.id;
      option.textContent = `${profile.name} (${profile.mode})`;
      profileSelect.appendChild(option);
    }

    profileSelect.value = store.activeProfileId;
    modeSelect.value = activeProfile(store).mode;
    removeButton.disabled = store.profiles.length <= 1;
  };

  const applyStore = (nextStore: SyntheticUserStore, persist: boolean, triggerReload: boolean) => {
    if (persist) {
      saveSyntheticUserStore(appId, nextStore, stableStorageOptions);
    }

    store = nextStore;
    render();
    onProfileChange?.(activeProfile(store));

    if (triggerReload && shouldReload(reloadOnSwitch)) {
      window.location.reload();
    }
  };

  const onProfileChangeSelect = () => {
    const nextStore = setActiveSyntheticProfile(appId, profileSelect.value, stableStorageOptions);
    applyStore(nextStore, false, true);
  };

  const onModeChange = () => {
    const mode = modeSelect.value as LocalAuthMode;
    const nextStore: SyntheticUserStore = {
      ...store,
      profiles: store.profiles.map((profile) =>
        profile.id === store.activeProfileId ? { ...profile, mode } : profile,
      ),
    };
    applyStore(nextStore, true, false);
  };

  const onAddProfile = () => {
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

    applyStore(nextStore, true, true);
  };

  const onRemoveProfile = () => {
    if (store.profiles.length <= 1) return;

    const nextProfiles = store.profiles.filter((profile) => profile.id !== store.activeProfileId);
    const nextActiveProfile = nextProfiles[0];
    if (!nextActiveProfile) return;
    const nextStore: SyntheticUserStore = {
      activeProfileId: nextActiveProfile.id,
      profiles: nextProfiles,
    };

    applyStore(nextStore, true, true);
  };

  profileSelect.addEventListener("change", onProfileChangeSelect);
  modeSelect.addEventListener("change", onModeChange);
  addButton.addEventListener("click", onAddProfile);
  removeButton.addEventListener("click", onRemoveProfile);

  render();

  return {
    destroy() {
      profileSelect.removeEventListener("change", onProfileChangeSelect);
      modeSelect.removeEventListener("change", onModeChange);
      addButton.removeEventListener("click", onAddProfile);
      removeButton.removeEventListener("click", onRemoveProfile);
      root.remove();
    },
    getStore() {
      return store;
    },
    getActiveProfile() {
      return activeProfile(store);
    },
    rerender() {
      render();
    },
  };
}
