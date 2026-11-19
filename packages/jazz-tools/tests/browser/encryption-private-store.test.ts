import { describe, expect, it } from "vitest";
import { browserPrivateStore } from "../../src/encryption/browser-store.js";
import {
  PrivateStoreConflictError,
  PrivateStoreWipedError,
} from "../../src/encryption/private-store.js";

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
