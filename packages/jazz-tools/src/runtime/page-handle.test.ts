import { afterEach, expect, it } from "vitest";
import { registerPageDb, type PageDb } from "./page-handle.js";
import {
  syncStatus,
  CONNECTED,
  offlineLink,
  type SyncStatus,
  uploadStatusFromCount,
} from "./sync-status.js";

afterEach(() => {
  delete (globalThis as { window?: Window }).window;
});

/** A Db as the handle sees it, with a status the test sets and a wait it settles. */
function fakeDb(initial: SyncStatus) {
  let status = initial;
  let settle: { resolve: () => void; reject: (error: Error) => void } | null = null;
  const db: PageDb & { set(next: SyncStatus): void; settle(error?: Error): void } = {
    getSyncStatus: () => status,
    waitForSync: () =>
      status.pendingUploads === 0
        ? Promise.resolve()
        : new Promise<void>((resolve, reject) => {
            settle = { resolve, reject };
          }),
    set: (next) => {
      status = next;
    },
    settle: (error) => (error ? settle?.reject(error) : settle?.resolve()),
  };
  return db;
}

function page(): Window {
  const window = { document: {} } as unknown as Window;
  (globalThis as { window?: Window }).window = window;
  return window;
}

it("answers the page's sync status, pending writes and every Db", () => {
  const window = page();
  const first = fakeDb(syncStatus(offlineLink("network"), uploadStatusFromCount(2)));
  const second = fakeDb(syncStatus(CONNECTED, uploadStatusFromCount(0)));
  const unlistFirst = registerPageDb(first, "app-1");
  const unlistSecond = registerPageDb(second, null);
  const handle = window.__jazz!;

  expect(handle.version).toBe(1);
  // The page's storage controls share it.
  expect(handle.listLiveStorageNamespaces()).toEqual([]);
  // The Db opened last answers status().
  expect(handle.status()).toBe(second.getSyncStatus());
  expect(handle.pending()).toBe(2);
  expect(handle.dbs()).toEqual([
    { appId: "app-1", status: first.getSyncStatus() },
    { appId: null, status: second.getSyncStatus() },
  ]);

  unlistSecond();
  expect(handle.status()).toBe(first.getSyncStatus());
  unlistFirst();
  expect(handle.status()).toBeNull();
  expect(handle.pending()).toBe(0);
  expect(handle.dbs()).toEqual([]);
});

it("waits for every Db, and rejects with the first refusal", async () => {
  const window = page();
  const first = fakeDb(syncStatus(CONNECTED, uploadStatusFromCount(1)));
  const second = fakeDb(syncStatus(CONNECTED, uploadStatusFromCount(0)));
  registerPageDb(first, "a");
  registerPageDb(second, "b");

  let synced = false;
  const waiting = window.__jazz!.waitForSync().then(() => {
    synced = true;
  });
  await Promise.resolve();
  expect(synced).toBe(false);
  first.settle();
  await waiting;
  expect(synced).toBe(true);

  const refused = window.__jazz!.waitForSync();
  first.settle(new Error("rejected by policy"));
  await expect(refused).rejects.toThrow("rejected by policy");
});

it("lists nothing outside a browser page", () => {
  const db = fakeDb(syncStatus(CONNECTED, uploadStatusFromCount(0)));
  registerPageDb(db, "x")();
  expect((globalThis as { window?: Window }).window).toBeUndefined();
});
