import { describe, expect, it } from "vitest";
import { generateAuthSecret } from "../../src/index.js";
import { storageQuotaBrowserCommands } from "./browser-commands.js";
import { createBrowserTestDb as createDb, uniqueDbName, waitForCondition } from "./support.js";
import { todos, allTodos, useSharedWorkerBridgeHarness } from "./worker-bridge-harness.js";

// The quota override is a Chromium DevTools command. It reaches IndexedDB only
// if nothing in the browser has opened this origin's IndexedDB before, so this
// file runs alone in its own browser (pnpm test:browser:storage-quota) and the
// shared browser run excludes it.
const chromium = /Chrome\//.test(navigator.userAgent);

/** A title of about `bytes` that no codec shrinks. */
function incompressible(bytes: number): string {
  const raw = new Uint8Array(Math.ceil((bytes * 3) / 4));
  for (let offset = 0; offset < raw.length; offset += 65536) {
    crypto.getRandomValues(raw.subarray(offset, offset + 65536));
  }
  let binary = "";
  for (const byte of raw) binary += String.fromCharCode(byte);
  return btoa(binary);
}

async function usage(): Promise<number> {
  return (await navigator.storage.estimate()).usage ?? 0;
}

describe.runIf(chromium)("IndexedDB storage quota", () => {
  const { track, untrack, shutdownDbAndWorker } = useSharedWorkerBridgeHarness();

  // rc.4 adopter finding R46: past the quota every write still reported
  // "saved", then rows went missing or the store reopened as "database disk
  // image is malformed".
  it("rejects local waits once IndexedDB is full and keeps the store whole", async () => {
    const quota = storageQuotaBrowserCommands();
    const origin = location.origin;
    const config = {
      appId: crypto.randomUUID(),
      secret: generateAuthSecret(),
      driver: { type: "persistent" as const, dbName: uniqueDbName("storage-quota") },
    };
    // Set before the worker opens IndexedDB: Chromium applies a quota
    // override to storage already open in a realm only lazily.
    await quota.overrideStorageQuota(origin, (await usage()) + 4_000_000);
    let db = track(await createDb(config));
    const saved: string[] = [];
    try {
      await db.insert(todos, { title: "before the quota", done: false }).wait({ tier: "local" });
      saved.push("before the quota");

      let refusal: unknown = null;
      for (let index = 0; index < 40 && refusal === null; index++) {
        const title = `${index} ${incompressible(400_000)}`;
        try {
          await db.insert(todos, { title, done: false }).wait({ tier: "local" });
          saved.push(title);
        } catch (error) {
          refusal = error;
        }
      }
      expect(refusal, "a write past the quota must not report saved").toMatchObject({
        code: "storage",
      });
      expect(String((refusal as Error).message)).toMatch(/IndexedDB/);
      // While storage stays full, later writes are refused locally at once.
      await expect(
        db.insert(todos, { title: "while full", done: false }).wait({ tier: "local" }),
      ).rejects.toMatchObject({ code: "storage" });

      // Reopen while still full: the store is whole and holds every write
      // that was reported saved.
      await shutdownDbAndWorker(db).catch(() => untrack(db));
      db = track(await createDb(config));
      const titles = (await db.all(allTodos, { tier: "local-first" })).map((row) => row.title);
      for (const title of saved) expect(titles).toContain(title);

      // With room again, writes are saved again.
      await quota.overrideStorageQuota(origin, null);
      await waitForCondition(
        async () => {
          try {
            await db.insert(todos, { title: "after", done: false }).wait({ tier: "local" });
            return true;
          } catch {
            return false;
          }
        },
        45_000,
        "writes should be saved once storage has room again",
      );
    } finally {
      await quota.overrideStorageQuota(origin, null);
      await db.shutdown().catch(() => undefined);
      untrack(db);
    }
  }, 120_000);

  // With no further write and no wait, the worker's own retry saves what
  // was refused once storage has room again.
  it("saves refused writes once storage has room, without another write", async () => {
    const quota = storageQuotaBrowserCommands();
    const origin = location.origin;
    const config = {
      appId: crypto.randomUUID(),
      secret: generateAuthSecret(),
      driver: { type: "persistent" as const, dbName: uniqueDbName("storage-quota-retry") },
    };
    await quota.overrideStorageQuota(origin, (await usage()) + 4_000_000);
    let db = track(await createDb(config));
    try {
      let refused: string | null = null;
      for (let index = 0; index < 40 && refused === null; index++) {
        const title = `${index} ${incompressible(400_000)}`;
        await db
          .insert(todos, { title, done: false })
          .wait({ tier: "local" })
          .catch(() => {
            refused = title;
          });
      }
      expect(refused, "a write past the quota is refused").not.toBeNull();
      const full = await usage();

      await quota.overrideStorageQuota(origin, null);
      // Nothing writes or waits now: only the retry timer can save it.
      await waitForCondition(
        async () => (await usage()) > full + 300_000,
        30_000,
        "the refused write should reach IndexedDB once there is room",
      );

      await shutdownDbAndWorker(db);
      db = track(await createDb(config));
      const titles = (await db.all(allTodos, { tier: "local-first" })).map((row) => row.title);
      expect(titles).toContain(refused);
    } finally {
      await quota.overrideStorageQuota(origin, null);
      await db.shutdown().catch(() => undefined);
      untrack(db);
    }
  }, 120_000);
});
