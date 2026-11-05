import { IDBFactory } from "fake-indexeddb";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { IndexedDbPageStore } from "./indexeddb-page-store.js";
import {
  acquireBrowserPhysicalDatabaseEpoch,
  type BrowserPhysicalDatabaseEpoch,
} from "./browser-physical-database-epoch.js";

const stores: IndexedDbPageStore[] = [];
const epochs: BrowserPhysicalDatabaseEpoch[] = [];
function runtime() {
  return { flush: vi.fn(async () => {}), close: vi.fn(async () => {}) };
}
async function open(epoch?: BrowserPhysicalDatabaseEpoch) {
  const store = await IndexedDbPageStore.open("alice-notes", {
    owner: "alice",
    format: "sqlite",
    epoch,
  });
  stores.push(store);
  return store;
}
beforeEach(() => {
  const held = new Set<string>();
  vi.stubGlobal("indexedDB", new IDBFactory());
  vi.stubGlobal("navigator", {
    locks: {
      async request(
        name: string,
        _options: unknown,
        callback: (lock: object | null) => Promise<unknown>,
      ) {
        if (held.has(name)) return await callback(null);
        held.add(name);
        try {
          return await callback({});
        } finally {
          held.delete(name);
        }
      },
    },
  });
});
afterEach(async () => {
  await Promise.all(stores.splice(0).map((store) => store.close().catch(() => {})));
  await Promise.all(epochs.splice(0).map((epoch) => epoch.release()));
  vi.unstubAllGlobals();
});

it("retains Alice's physical lock after a failed runtime close until close succeeds on retry", async () => {
  const store = await open();
  const db = runtime();
  db.close.mockRejectedValueOnce(new Error("durable flush failed"));
  await store.openSqliteRuntime(async () => db);
  await expect(store.close()).rejects.toThrow("durable flush failed");
  await expect(open().then(() => undefined)).rejects.toThrow("active in another");
  await store.close();
  expect(db.close).toHaveBeenCalledTimes(2);
  await open();
});

it("retains Alice's runtime reservation after failed retirement and allows a replacement only after retry", async () => {
  const epoch = await acquireBrowserPhysicalDatabaseEpoch("alice-notes");
  epochs.push(epoch);
  const store = await open(epoch);
  const db = runtime();
  db.close.mockRejectedValueOnce(new Error("durable flush failed"));
  await store.openSqliteRuntime(async () => db);
  await expect(store.retireTreeOwnership()).rejects.toThrow("durable flush failed");
  const replacement = vi.fn(async () => runtime());
  await expect(store.openSqliteRuntime(replacement)).rejects.toThrow("already has a runtime owner");
  expect(replacement).not.toHaveBeenCalled();
  await store.retireTreeOwnership();
  expect(db.close).toHaveBeenCalledTimes(2);
  await store.openSqliteRuntime(replacement);
  expect(replacement).toHaveBeenCalledOnce();
});

it("retains a failed opening runtime when both its first flush and cleanup fail", async () => {
  const store = await open();
  const db = runtime();
  db.flush.mockRejectedValueOnce(new Error("first flush failed"));
  db.close.mockRejectedValueOnce(new Error("cleanup failed"));
  await expect(store.openSqliteRuntime(async () => db)).rejects.toThrow();
  const replacement = vi.fn(async () => runtime());
  await expect(store.openSqliteRuntime(replacement)).rejects.toThrow("already has a runtime owner");
  expect(replacement).not.toHaveBeenCalled();
  await expect(open().then(() => undefined)).rejects.toThrow("active in another");
  await store.close();
  expect(db.close).toHaveBeenCalledTimes(2);
  await open();
});
