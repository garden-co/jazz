import { IDBDatabase, IDBFactory, IDBObjectStore } from "fake-indexeddb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createBrowserKeyStore, resolveE2eeStore } from "./browser-key-store.js";

const host = vi.hoisted(() => ({ browser: true }));
vi.mock("../accounts/browser-host.js", () => ({ isBrowserHostRuntime: () => host.browser }));

const scope = {
  registry: "https://registry.test/apps/example/accounts",
  env: "dev",
  accountId: "account-a",
};

beforeEach(() => {
  host.browser = true;
  vi.stubGlobal("indexedDB", new IDBFactory());
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("default E2EE key storage", () => {
  it("selects the browser default while custom stores always take precedence", async () => {
    const store = resolveE2eeStore(undefined, scope);
    await store.update(() => "retained keys");
    expect(await resolveE2eeStore(undefined, scope).read()).toBe("retained keys");

    host.browser = false;
    vi.stubGlobal("indexedDB", undefined);
    expect(resolveE2eeStore(store, scope)).toBe(store);
  });

  it("requires an explicit store on non-browser hosts even with IndexedDB", () => {
    host.browser = false;
    expect(() => resolveE2eeStore(undefined, scope)).toThrow("explicit e2ee.store");
  });

  it("rejects unavailable browser storage without falling back to memory", () => {
    vi.stubGlobal("indexedDB", undefined);
    expect(() => resolveE2eeStore(undefined, scope)).toThrow("requires IndexedDB");
  });

  it("persists concurrent updates across independent connections and reopening", async () => {
    const stores = [createBrowserKeyStore(scope), createBrowserKeyStore(scope)];
    await Promise.all(
      Array.from({ length: 32 }, (_, index) =>
        stores[index % 2]!.update((value) => JSON.stringify([...JSON.parse(value ?? "[]"), index])),
      ),
    );
    const reopened = createBrowserKeyStore(scope);
    expect(JSON.parse((await reopened.read())!).sort((a: number, b: number) => a - b)).toEqual(
      Array.from({ length: 32 }, (_, index) => index),
    );
  });

  it("isolates accounts, environments, applications and server authorities", async () => {
    await createBrowserKeyStore(scope).update(() => "keys");
    for (const otherScope of [
      { ...scope, accountId: "account-b" },
      { ...scope, env: "production" },
      { ...scope, registry: "https://registry.test/apps/other/accounts" },
      { ...scope, registry: "https://another.test/apps/example/accounts" },
    ]) {
      const other = createBrowserKeyStore(otherScope);
      expect(await other.read()).toBeNull();
      await other.update(() => "other keys");
      expect(await createBrowserKeyStore(scope).read()).toBe("keys");
    }
  });

  it("pins the v1 namespace, tuple key and unchanged opaque E2EE value", async () => {
    const value = '{"format":"jazz-e2ee-local-devices-v2","devices":[]}';
    await createBrowserKeyStore(scope).update(() => value);
    const database = await openRaw();
    expect(database.version).toBe(1);
    expect(Array.from(database.objectStoreNames)).toEqual(["accounts"]);
    const transaction = database.transaction("accounts", "readonly");
    const records = transaction.objectStore("accounts");
    const stored = await requestValue(records.getAll());
    const keys = await requestValue(records.getAllKeys());
    expect(stored).toEqual([value]);
    expect(keys).toEqual(['["https://registry.test/apps/example/accounts","dev","account-a"]']);
    database.close();
  });

  it("requests strict transaction durability for every write", async () => {
    const transaction = vi.spyOn(IDBDatabase.prototype, "transaction");
    const store = createBrowserKeyStore(scope);
    await store.update(() => "keys");
    expect(transaction).toHaveBeenCalledWith("accounts", "readwrite", {
      durability: "strict",
    });
    expect(await store.read()).toBe("keys");
  });

  it("aborts a throwing transform without replacing keys, and can retry", async () => {
    const store = createBrowserKeyStore(scope);
    await store.update(() => "original");
    await expect(
      store.update(() => {
        throw new Error("transform refused");
      }),
    ).rejects.toThrow("transform refused");
    expect(await store.read()).toBe("original");
    await store.update((value) => `${value} updated`);
    expect(await store.read()).toBe("original updated");
  });

  it("propagates write failures and retains the original keys", async () => {
    const store = createBrowserKeyStore(scope);
    await store.update(() => "original");
    vi.spyOn(IDBObjectStore.prototype, "put").mockImplementationOnce(() => {
      throw new DOMException("storage full", "QuotaExceededError");
    });
    await expect(store.update(() => "replacement")).rejects.toThrow("storage full");
    expect(await createBrowserKeyStore(scope).read()).toBe("original");
  });

  it("does not treat corrupt persisted state as absent keys", async () => {
    const store = createBrowserKeyStore(scope);
    expect(await store.read()).toBeNull();
    const database = await openRaw();
    const transaction = database.transaction("accounts", "readwrite");
    transaction
      .objectStore("accounts")
      .put({ invalid: true }, JSON.stringify(Object.values(scope)));
    await new Promise<void>((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error);
    });
    database.close();
    const transform = vi.fn(() => "replacement");
    await expect(store.read()).rejects.toThrow("Invalid persisted E2EE key state");
    await expect(store.update(transform)).rejects.toThrow("Invalid persisted E2EE key state");
    expect(transform).not.toHaveBeenCalled();
  });

  it("propagates browser access failures without creating transient key state", async () => {
    vi.spyOn(indexedDB, "open").mockImplementation(() => {
      throw new DOMException("storage denied", "SecurityError");
    });
    const store = createBrowserKeyStore(scope);
    const transform = vi.fn(() => "keys");
    await expect(store.read()).rejects.toThrow("storage denied");
    await expect(store.update(transform)).rejects.toThrow("storage denied");
    expect(transform).not.toHaveBeenCalled();
  });
});

function openRaw(): Promise<IDBDatabase> {
  return requestValue(indexedDB.open("jazz-e2ee-keys-v1", 1));
}

function requestValue<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}
