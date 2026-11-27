import { describe, expect, it, vi } from "vitest";
import { app } from "../fixtures/encrypted/jazz.generated.js";
import { browserPrivateStore } from "../../src/encryption/browser-store.js";
import { buildEncryption } from "../../src/encryption/build.js";
import {
  PrivateStoreConflictError,
  PrivateStoreWipedError,
} from "../../src/encryption/private-store.js";
import { encryptionStoreScope } from "../../src/encryption/store-scope.js";
import { loadWasmModule } from "../../src/runtime/wasm-loader.js";

// The built-in private store in a real browser: IndexedDB, a non-extractable
// WebCrypto key and the page's Web Locks. Two store objects on one database
// name are two tabs of one app; opening a new one is a reload.

const bytes = (...values: number[]) => new Uint8Array(values);
const uniqueName = () => `jazz-private-store-test-${crypto.randomUUID()}`;

describe("the private store in a browser", () => {
  it("survives a reload: documents, the scope's incarnation and the protecting key", async () => {
    const name = uniqueName();
    const before = browserPrivateStore({ name });
    const incarnation = await before.openScope("device");
    const revision = await before.save("device", incarnation, "keys", bytes(1, 2, 3), null);
    const blob = await before.protect(bytes(9, 9, 9));

    const after = browserPrivateStore({ name });
    expect(await after.openScope("device")).toBe(incarnation);
    expect(await after.load("device", "keys")).toEqual({ bytes: bytes(1, 2, 3), revision });
    expect(await after.unprotect(blob)).toEqual(bytes(9, 9, 9));
  });

  it("reads the same documents and revisions in one transaction, only for the current scope", async () => {
    const name = uniqueName();
    const store = browserPrivateStore({ name });
    const incarnation = await store.openScope("device");
    const other = await store.openScope("other");
    await store.save("other", other, "second", bytes(9), null);
    const revision = await store.save("device", incarnation, "first", bytes(1), null);
    await store.save("device", incarnation, "first", bytes(2), revision);
    await store.save("device", incarnation, "second", bytes(3), null);
    const expected = await Promise.all(
      (await store.list("device")).map(async (doc) => ({
        doc,
        ...(await store.load("device", doc)),
      })),
    );

    using transaction = vi.spyOn(IDBDatabase.prototype, "transaction");
    expect(await store.loadAll("device", incarnation)).toEqual(expected);
    expect(transaction).toHaveBeenCalledOnce();
    await store.delete("device");
    await expect(store.loadAll("device", incarnation)).rejects.toBeInstanceOf(
      PrivateStoreWipedError,
    );
    const reopened = await store.openScope("device");
    await expect(store.loadAll("device", incarnation)).rejects.toBeInstanceOf(
      PrivateStoreWipedError,
    );
    expect(await store.loadAll("device", reopened)).toEqual([]);

    // An interrupted wipe leaves the documents without their incarnation.
    const raw = await new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open(name);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    try {
      await new Promise<void>((resolve, reject) => {
        const wiping = raw.transaction("scopes", "readwrite");
        wiping.objectStore("scopes").delete("other");
        wiping.oncomplete = () => resolve();
        wiping.onabort = () => reject(wiping.error);
      });
      await expect(store.loadAll("other", other)).rejects.toBeInstanceOf(PrivateStoreWipedError);
      expect(await store.loadAll("other", await store.openScope("other"))).toEqual([]);
    } finally {
      raw.close();
    }
  });

  it("opens a returning device and its floors with one document read on the browser store", async () => {
    const store = browserPrivateStore({ name: uniqueName() });
    const { EncryptionRuntime: host } = await loadWasmModule();
    expect(host).toBeDefined();
    const appId = crypto.randomUUID();
    const environment = "test";
    const accountId = crypto.randomUUID();
    const options = { host: host!, app, appId, environment, accountId };
    await buildEncryption({ ...options, store });

    const scope = encryptionStoreScope(appId, environment, accountId);
    const incarnation = await store.openScope(scope);
    for (let index = 0; index < 12; index++) {
      // Unrecognised floor records still have to be read and checked on open.
      await store.save(scope, incarnation, `floor-${crypto.randomUUID()}`, bytes(1), null);
    }

    const load = vi.fn(store.load);
    const list = vi.fn(store.list);
    const loadAll = vi.fn(store.loadAll);
    await buildEncryption({ ...options, store: { ...store, load, list, loadAll } });
    expect(loadAll).toHaveBeenCalledExactlyOnceWith(scope, incarnation);
    expect(load).not.toHaveBeenCalled();
    expect(list).not.toHaveBeenCalled();

    await buildEncryption({ ...options, store: { ...store, load, list, loadAll: undefined } });
    expect(loadAll).toHaveBeenCalledOnce();
    expect(list).toHaveBeenCalledOnce();
    expect(load).toHaveBeenCalledTimes(13);
  });

  it("never exposes the protecting key: it cannot be exported", async () => {
    const name = uniqueName();
    const store = browserPrivateStore({ name });
    await store.protect(bytes(1));
    const request = indexedDB.open(name);
    const key = await new Promise<CryptoKey>((resolve, reject) => {
      request.onsuccess = () => {
        const get = request.result.transaction("keys").objectStore("keys").get("wrap");
        get.onsuccess = () => resolve(get.result as CryptoKey);
        get.onerror = () => reject(get.error);
      };
      request.onerror = () => reject(request.error);
    });
    expect(key.extractable).toBe(false);
    await expect(crypto.subtle.exportKey("raw", key)).rejects.toThrow();
  });

  it("two tabs saving at once: one wins, the other conflicts and writes nothing", async () => {
    const name = uniqueName();
    const [one, two] = [browserPrivateStore({ name }), browserPrivateStore({ name })];
    const incarnation = await one.openScope("device");
    await two.openScope("device");
    const settled = await Promise.allSettled([
      one.save("device", incarnation, "nonce", bytes(1), null),
      two.save("device", incarnation, "nonce", bytes(2), null),
    ]);
    expect(settled.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
    const lost = settled.find((outcome) => outcome.status === "rejected") as PromiseRejectedResult;
    expect(lost.reason).toBeInstanceOf(PrivateStoreConflictError);
  });

  it("two tabs first protecting at once share one key, made under a Web Lock", async () => {
    expect(navigator.locks).toBeDefined();
    const name = uniqueName();
    const [one, two] = [browserPrivateStore({ name }), browserPrivateStore({ name })];
    const [a, b] = await Promise.all([one.protect(bytes(1)), two.protect(bytes(2))]);
    expect(await two.unprotect(a)).toEqual(bytes(1));
    expect(await one.unprotect(b)).toEqual(bytes(2));
  });

  it("a Web Lock held by another tab makes a second tab wait for the key, not make its own", async () => {
    const name = uniqueName();
    const one = browserPrivateStore({ name });
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    let acquired!: () => void;
    const taken = new Promise<void>((resolve) => (acquired = resolve));
    // The first tab is in the middle of making the key.
    void navigator.locks.request(`${name}:wrap-key`, async () => {
      acquired();
      await held;
    });
    await taken;
    let done = false;
    const protecting = one.protect(bytes(1)).then((blob) => {
      done = true;
      return blob;
    });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(done).toBe(false);
    release();
    expect((await protecting).length).toBeGreaterThan(1);
  });

  it("a scope another tab deleted refuses the open runtime's save", async () => {
    const name = uniqueName();
    const [one, two] = [browserPrivateStore({ name }), browserPrivateStore({ name })];
    const incarnation = await one.openScope("device");
    await two.delete("device");
    await expect(one.save("device", incarnation, "keys", bytes(1), null)).rejects.toBeInstanceOf(
      PrivateStoreWipedError,
    );
  });
});
