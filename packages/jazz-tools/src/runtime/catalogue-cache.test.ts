import { IDBFactory } from "fake-indexeddb";
import { mkdtemp, rm, writeFile, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  BrowserCatalogueCache,
  EphemeralCatalogueCache,
  decodeCatalogueCache,
  encodeCatalogueCache,
  type CatalogueCacheScope,
} from "./catalogue-cache.js";
import { NodeCatalogueCache } from "./catalogue-cache-node.js";

const scope: CatalogueCacheScope = {
  registryAuthority: "https://registry.example/app/accounts",
  appId: "app",
  environment: "test",
};
const first = Uint8Array.of(0, 255, 19, 128);
const replacement = Uint8Array.of(12, 0, 42);
const directories: string[] = [];
const validateReplacement = (previous: Uint8Array, next: Uint8Array) => {
  if (previous[0]! > next[0]!) throw new Error("regressing catalogue");
};
afterEach(async () => {
  vi.unstubAllGlobals();
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("account-independent authenticated catalogue cache", () => {
  it("survives a new browser adapter and isolates every scope coordinate", async () => {
    vi.stubGlobal("indexedDB", new IDBFactory());
    await new BrowserCatalogueCache().publish(scope, first, validateReplacement);
    const reopened = new BrowserCatalogueCache();
    expect(await reopened.load(scope)).toEqual(first);
    for (const changed of [
      { ...scope, appId: "other" },
      { ...scope, environment: "production" },
      { ...scope, registryAuthority: "https://other.example/app/accounts" },
    ])
      expect(await reopened.load(changed)).toBeNull();
    await reopened.publish(scope, replacement, validateReplacement);
    await expect(reopened.publish(scope, first, validateReplacement)).rejects.toThrow("regressing");
    expect(await new BrowserCatalogueCache().load(scope)).toEqual(replacement);
  });

  it("reopens exact Node bytes after atomic replacement without retaining temporary files", async () => {
    const directory = await mkdtemp(join(tmpdir(), "jazz-catalogue-cache-"));
    directories.push(directory);
    await new NodeCatalogueCache(directory).publish(scope, first, validateReplacement);
    expect(await new NodeCatalogueCache(directory).load(scope)).toEqual(first);
    await new NodeCatalogueCache(directory).publish(scope, replacement, validateReplacement);
    await expect(
      new NodeCatalogueCache(directory).publish(scope, first, validateReplacement),
    ).rejects.toThrow("regressing");
    expect(await new NodeCatalogueCache(directory).load(scope)).toEqual(replacement);
    expect((await readdir(directory)).filter((file) => file.endsWith(".tmp"))).toEqual([]);
    expect(
      await new NodeCatalogueCache(directory).load({ ...scope, environment: "other" }),
    ).toBeNull();
  });

  it("does not turn malformed persisted metadata or I/O failure into a cache miss", async () => {
    const directory = await mkdtemp(join(tmpdir(), "jazz-catalogue-corrupt-"));
    directories.push(directory);
    const cache = new NodeCatalogueCache(directory);
    await cache.publish(scope, first, validateReplacement);
    const file = (await readdir(directory)).find((file) => file.endsWith(".json"));
    await writeFile(join(directory, file!), "not-json");
    await expect(cache.load(scope)).rejects.toThrow();
    const blocked = join(directory, "file-not-directory");
    await writeFile(blocked, "occupied");
    await expect(
      new NodeCatalogueCache(blocked).publish(scope, first, validateReplacement),
    ).rejects.toThrow();
  });

  it("rejects unknown formats, noncanonical bytes and foreign scope envelopes", () => {
    const encoded = encodeCatalogueCache(scope, first);
    expect(encoded).toBe(
      '{"format":"jazz-authenticated-catalogue","version":1,"scope":"[\\"https://registry.example/app/accounts\\",\\"app\\",\\"test\\"]","capture":"AP8TgA=="}',
    );
    expect(() => decodeCatalogueCache({ ...scope, appId: "other" }, encoded)).toThrow(/scope/);
    expect(() =>
      decodeCatalogueCache(scope, encoded.replace('"version":1', '"version":2')),
    ).toThrow();
    expect(() => decodeCatalogueCache(scope, encoded.replace("AP8TgA==", "AP8TgB=="))).toThrow(
      /canonical/,
    );
  });

  it("does not claim process survival for an explicitly ephemeral cache", async () => {
    const cache = new EphemeralCatalogueCache();
    const mutable = first.slice();
    await cache.publish(scope, mutable, validateReplacement);
    mutable[0] = 99;
    expect(await cache.load(scope)).toEqual(Uint8Array.of(0, 255, 19, 128));
    expect(await new EphemeralCatalogueCache().load(scope)).toBeNull();
  });
});

async function snapshotCache(kind: "browser" | "node") {
  if (kind === "browser") {
    vi.stubGlobal("indexedDB", new IDBFactory());
    return new BrowserCatalogueCache();
  }
  const directory = await mkdtemp(join(tmpdir(), "jazz-catalogue-snapshot-"));
  directories.push(directory);
  return new NodeCatalogueCache(directory);
}

it.each(["browser", "node"] as const)(
  "%s validates the exact capture bytes it publishes across awaits",
  async (kind) => {
    const cache = await snapshotCache(kind);
    await cache.publish(scope, Uint8Array.of(1), validateReplacement);
    const callerBytes = Uint8Array.of(0);
    const pending = cache.publish(scope, callerBytes, validateReplacement);
    // Both host implementations have yielded before validation. Mutation must
    // not let validation see generation 2 while generation 0 is persisted.
    callerBytes[0] = 2;
    await expect(pending).rejects.toThrow("regressing");
    expect(await cache.load(scope)).toEqual(Uint8Array.of(1));
  },
);

it.each(["browser", "node"] as const)(
  "%s binds the cache scope before the first await",
  async (kind) => {
    const cache = await snapshotCache(kind);
    const callerScope = { ...scope };
    const pending = cache.publish(callerScope, replacement, validateReplacement);
    callerScope.appId = "other-after-open";
    await pending;
    expect(await cache.load(scope)).toEqual(replacement);
    expect(await cache.load(callerScope)).toBeNull();
  },
);
