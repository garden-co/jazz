import { afterEach, expect, it } from "vitest";
import {
  ensureWindowJazz,
  registerPageDb,
  WindowJazzConflictError,
  type PageDb,
} from "./page-handle.js";
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

it("refuses a window.__jazz the app set, by name, instead of extending it", () => {
  // Before: `TypeError: Cannot add property clearStorage, object is not
  // extensible` at the upgrade to local-first, for a sealed one; an
  // extensible one had Jazz's members written over its own.
  for (const own of [Object.seal({ debug: true }), { status: "mine" }, "app", null]) {
    const window = page();
    (window as { __jazz?: unknown }).__jazz = own;
    const db = fakeDb(syncStatus(CONNECTED, uploadStatusFromCount(0)));
    expect(() => registerPageDb(db, "app-1")).toThrow(WindowJazzConflictError);
    expect(() => ensureWindowJazz(window)).toThrow(
      /^window\.__jazz is reserved for Jazz \(its page handle: .*\), and this page already has a window\.__jazz that Jazz didn't set \((object|string|null)\)\. Rename your app's global/,
    );
    // Left as the app set it.
    expect((window as { __jazz?: unknown }).__jazz).toBe(own);
  }
});

it("extends the window.__jazz Jazz installed, from any copy of Jazz on the page", () => {
  const window = page();
  const unlist = registerPageDb(fakeDb(syncStatus(CONNECTED, uploadStatusFromCount(0))), "a");
  const installed = window.__jazz!;
  // Another client (or another copy of jazz-tools) installs it again: same object.
  expect(ensureWindowJazz(window)).toBe(installed);
  expect(installed[Symbol.for("jazz-tools.window-jazz") as never]).toBe(true);
  unlist();
});

it("extends a window.__jazz an older Jazz copy set before the ownership mark existed", () => {
  const window = page();
  // alpha.60's handle: version 1, no mark.
  const older = { version: 1, status: () => null, pending: () => 0 };
  (window as { __jazz?: unknown }).__jazz = older;
  const unlist = registerPageDb(fakeDb(syncStatus(CONNECTED, uploadStatusFromCount(0))), "a");
  expect(window.__jazz).toBe(older);
  expect(window.__jazz!.listLiveStorageNamespaces()).toEqual([]);
  expect(older[Symbol.for("jazz-tools.window-jazz") as never]).toBe(true);
  unlist();
  // An object that only looks a little like it is still the app's.
  (window as { __jazz?: unknown }).__jazz = { version: 1 };
  expect(() => ensureWindowJazz(window)).toThrow(WindowJazzConflictError);
});
