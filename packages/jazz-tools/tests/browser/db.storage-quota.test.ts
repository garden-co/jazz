import { describe, expect, it } from "vitest";
import { generateAuthSecret } from "../../src/index.js";
import { storageQuotaBrowserCommands } from "./browser-commands.js";
import { createBrowserTestDb as createDb, uniqueDbName, waitForCondition } from "./support.js";
import { todos, allTodos, useSharedWorkerBridgeHarness } from "./worker-bridge-harness.js";
import { chromium, incompressible, usage } from "./storage-quota-support.js";

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

      // Close while still full, then reopen with room again: the store is
      // whole and holds every write that was reported saved, and writes are
      // saved again. (Opening records a node lease, so an open with no room
      // at all is refused with code "storage"; the page store's unit tests
      // cover that, as a cap set now would not reliably reach IndexedDB.)
      await shutdownDbAndWorker(db).catch(() => untrack(db));
      await quota.overrideStorageQuota(origin, null);
      db = track(await createDb(config));
      const titles = (await db.all(allTodos, { tier: "local-first" })).map((row) => row.title);
      for (const title of saved) expect(titles).toContain(title);
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
});
