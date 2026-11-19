import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { browserPrivateStore } from "./browser-store.js";
import { PrivateStoreConflictError, PrivateStoreWipedError } from "./private-store.js";

// The browser's built-in private store, run over an in-memory IndexedDB. Two
// store objects on one factory are two tabs of one app.

const bytes = (...values: number[]) => new Uint8Array(values);

/** A lock manager that runs one request at a time per name, as the browser's does. */
function lockManager() {
  const tails = new Map<string, Promise<unknown>>();
  const held: string[] = [];
  return {
    held,
    request: ((name: string, _options: unknown, work: () => Promise<unknown>) => {
      held.push(name);
      const tail = (tails.get(name) ?? Promise.resolve()).catch(() => {});
      const run = tail.then(work);
      tails.set(name, run);
      return run;
    }) as unknown as LockManager["request"],
  };
}

function tabs(count = 2, locks: ReturnType<typeof lockManager> | null = lockManager()) {
  const indexedDB = new IDBFactory();
  return Array.from({ length: count }, () =>
    browserPrivateStore({ name: "test-store", indexedDB, locks }),
  );
}

describe("the browser private store", () => {
  it("saves with compare-and-swap and loads what it saved", async () => {
    const [store] = tabs(1);
    const incarnation = await store!.openScope("device");
    expect(incarnation).toBeGreaterThan(0n);
    expect(await store!.openScope("device")).toBe(incarnation);
    expect(await store!.load("device", "doc")).toBeNull();
    const first = await store!.save("device", incarnation, "doc", bytes(1), null);
    expect(await store!.load("device", "doc")).toEqual({ bytes: bytes(1), revision: first });
    const second = await store!.save("device", incarnation, "doc", bytes(2), first);
    expect(second).toBeGreaterThan(first);
    expect(await store!.list("device")).toEqual(["doc"]);
  });

  it("refuses a save that names a stale revision, or none for a document that exists", async () => {
    const [store] = tabs(1);
    const incarnation = await store!.openScope("s");
    const first = await store!.save("s", incarnation, "d", bytes(1), null);
    await expect(store!.save("s", incarnation, "d", bytes(9), null)).rejects.toBeInstanceOf(
      PrivateStoreConflictError,
    );
    await store!.save("s", incarnation, "d", bytes(2), first);
    await expect(store!.save("s", incarnation, "d", bytes(9), first)).rejects.toBeInstanceOf(
      PrivateStoreConflictError,
    );
    expect((await store!.load("s", "d"))!.bytes).toEqual(bytes(2));
  });

  it("never creates a scope on save, and a deleted or recreated scope refuses the old incarnation", async () => {
    const [store] = tabs(1);
    await expect(store!.save("none", 5n, "d", bytes(1), null)).rejects.toBeInstanceOf(
      PrivateStoreWipedError,
    );
    expect(await store!.list("none")).toEqual([]);
    const incarnation = await store!.openScope("s");
    await store!.save("s", incarnation, "d", bytes(1), null);
    await store!.delete("s");
    expect(await store!.load("s", "d")).toBeNull();
    await expect(store!.save("s", incarnation, "d", bytes(2), null)).rejects.toBeInstanceOf(
      PrivateStoreWipedError,
    );
    const again = await store!.openScope("s");
    expect(again).not.toBe(incarnation);
    await expect(store!.save("s", incarnation, "d", bytes(2), null)).rejects.toBeInstanceOf(
      PrivateStoreWipedError,
    );
  });

  it("treats documents left without an incarnation as wiped: a new scope never adopts them", async () => {
    const indexedDB = new IDBFactory();
    const store = browserPrivateStore({ name: "test-store", indexedDB, locks: null });
    const incarnation = await store.openScope("device");
    await store.save("device", incarnation, "keys", bytes(1), null);
    // A delete that stopped half way: the incarnation gone, the document left.
    const raw = await new Promise<IDBDatabase>((resolve) => {
      const request = indexedDB.open("test-store");
      request.onsuccess = () => resolve(request.result);
    });
    await new Promise<void>((resolve) => {
      const transaction = raw.transaction("scopes", "readwrite");
      transaction.objectStore("scopes").delete("device");
      transaction.oncomplete = () => resolve();
    });
    raw.close();
    const again = await store.openScope("device");
    expect(again).not.toBe(incarnation);
    expect(await store.load("device", "keys")).toBeNull();
    expect(await store.list("device")).toEqual([]);
  });

  it("two tabs saving the same document: one wins, the other conflicts and writes nothing", async () => {
    const [one, two] = tabs();
    const incarnation = await one!.openScope("device");
    expect(await two!.openScope("device")).toBe(incarnation);
    const settled = await Promise.allSettled([
      one!.save("device", incarnation, "keys", bytes(1), null),
      two!.save("device", incarnation, "keys", bytes(2), null),
    ]);
    expect(settled.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
    const lost = settled.find((outcome) => outcome.status === "rejected") as PromiseRejectedResult;
    expect(lost.reason).toBeInstanceOf(PrivateStoreConflictError);
    const stored = await one!.load("device", "keys");
    const winner = settled[0]!.status === "fulfilled" ? 1 : 2;
    expect(stored!.bytes).toEqual(bytes(winner));
    // The loser reads again and merges.
    await two!.save("device", incarnation, "keys", bytes(3), stored!.revision);
  });

  it("a tab that deletes a scope wipes it for the other tab's open runtime", async () => {
    const [one, two] = tabs();
    const incarnation = await one!.openScope("device");
    await two!.delete("device");
    await expect(one!.save("device", incarnation, "d", bytes(1), null)).rejects.toBeInstanceOf(
      PrivateStoreWipedError,
    );
  });

  it("protects a secret with a key every tab shares, and never stores it in the clear", async () => {
    const [one, two] = tabs();
    const secret = Uint8Array.from({ length: 24 }, (_, at) => 200 - at);
    const blob = await one!.protect(secret);
    const clear = Buffer.from(secret).toString("hex");
    expect(Buffer.from(blob).toString("hex")).not.toContain(clear);
    expect(await two!.unprotect(blob)).toEqual(secret);
    expect(await one!.protect(secret)).not.toEqual(blob);
    const damaged = blob.slice();
    damaged[damaged.length - 1]! ^= 1;
    await expect(one!.unprotect(damaged)).rejects.toThrow();
  });

  it("two tabs first protecting at once end with one key, under a lock", async () => {
    const locks = lockManager();
    const [one, two] = tabs(2, locks);
    const [a, b] = await Promise.all([one!.protect(bytes(1, 2, 3)), two!.protect(bytes(4, 5, 6))]);
    expect(await two!.unprotect(a)).toEqual(bytes(1, 2, 3));
    expect(await one!.unprotect(b)).toEqual(bytes(4, 5, 6));
    expect(locks.held.filter((name) => name.endsWith(":wrap-key"))).not.toHaveLength(0);
  });

  it("without Web Locks the tab that loses the key race uses the winner's key", async () => {
    const [one, two] = tabs(2, null);
    const [a, b] = await Promise.all([one!.protect(bytes(1)), two!.protect(bytes(2))]);
    expect(await two!.unprotect(a)).toEqual(bytes(1));
    expect(await one!.unprotect(b)).toEqual(bytes(2));
  });

  it("refuses a protected secret of an unknown format", async () => {
    const [store] = tabs(1);
    await expect(store!.unprotect(bytes(9, 1, 2, 3))).rejects.toThrow(/unknown format/);
  });
});
