import { get } from "svelte/store";
import { describe, expect, it } from "vitest";
import type { Db } from "../runtime/db.js";
import type { SyncStatus } from "../runtime/sync-status.js";
import { syncStatus } from "./sync-status.js";

function fakeDb(initial: SyncStatus) {
  let current = initial;
  const listeners = new Set<(status: SyncStatus) => void>();
  const db = {
    getSyncStatus: () => current,
    onSyncStatusChange(listener: (status: SyncStatus) => void) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
  return {
    db: db as unknown as Db,
    listeners,
    set(next: SyncStatus) {
      current = next;
      for (const listener of listeners) listener(next);
    },
  };
}

const connecting: SyncStatus = { link: { state: "connecting" }, pendingUploads: 1, synced: false };
const connected: SyncStatus = { link: { state: "connected" }, pendingUploads: 0, synced: true };

describe("syncStatus", () => {
  it("follows the Db's status only while subscribed", () => {
    const fake = fakeDb(connecting);
    const store = syncStatus(fake.db);
    expect(get(store)).toBe(connecting);
    expect(fake.listeners.size).toBe(0);

    const seen: SyncStatus[] = [];
    const unsubscribe = store.subscribe((status) => seen.push(status));
    fake.set(connected);
    expect(seen).toEqual([connecting, connected]);

    unsubscribe();
    expect(fake.listeners.size).toBe(0);
  });
});
