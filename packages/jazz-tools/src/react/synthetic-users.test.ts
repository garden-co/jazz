import { describe, expect, it } from "vitest";
import {
  getActiveSyntheticAuth,
  loadSyntheticUserStore,
  saveSyntheticUserStore,
  setActiveSyntheticProfile,
  type StorageLike,
  type SyntheticUserStore,
} from "./synthetic-users.js";

class MemoryStorage implements StorageLike {
  private data = new Map<string, string>();

  getItem(key: string): string | null {
    return this.data.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.data.set(key, value);
  }
}

describe("synthetic-users", () => {
  it("creates and persists a default store when none exists", () => {
    const storage = new MemoryStorage();
    const store = loadSyntheticUserStore("app-1", { storage });
    expect(store.profiles.length).toBeGreaterThanOrEqual(2);
    expect(store.profiles.some((profile) => profile.id === store.activeProfileId)).toBe(true);
  });

  it("switches active profile and returns updated auth fields", () => {
    const storage = new MemoryStorage();
    const initial = loadSyntheticUserStore("app-2", { storage });
    const target = initial.profiles[1];

    const updated = setActiveSyntheticProfile("app-2", target.id, { storage });
    expect(updated.activeProfileId).toBe(target.id);

    const auth = getActiveSyntheticAuth("app-2", { storage });
    expect(auth.localAuthToken).toBe(target.token);
    expect(auth.localAuthMode).toBe(target.mode);
  });

  it("saves and reloads explicit stores", () => {
    const storage = new MemoryStorage();
    const explicit: SyntheticUserStore = {
      activeProfileId: "user-2",
      profiles: [
        { id: "user-1", name: "Alice", mode: "demo", token: "tok-1" },
        { id: "user-2", name: "Bob", mode: "anonymous", token: "tok-2" },
      ],
    };
    saveSyntheticUserStore("app-3", explicit, { storage });

    const loaded = loadSyntheticUserStore("app-3", { storage });
    expect(loaded).toEqual(explicit);
  });
});
