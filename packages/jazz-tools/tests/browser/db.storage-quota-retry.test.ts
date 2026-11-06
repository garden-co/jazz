import { describe, expect, it } from "vitest";
import { generateAuthSecret } from "../../src/index.js";
import { storageQuotaBrowserCommands } from "./browser-commands.js";
import { createBrowserTestDb as createDb, uniqueDbName, waitForCondition } from "./support.js";
import { todos, allTodos, useSharedWorkerBridgeHarness } from "./worker-bridge-harness.js";
import { chromium, incompressible, usage, vfsBytes } from "./storage-quota-support.js";

describe.runIf(chromium)("IndexedDB storage quota retry", () => {
  const { track, untrack, shutdownDbAndWorker } = useSharedWorkerBridgeHarness();

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
      const saved = await vfsBytes();

      await quota.overrideStorageQuota(origin, null);
      // Nothing writes or waits now: only the retry timer can save it.
      await waitForCondition(
        async () => (await vfsBytes()) > saved + 300_000,
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
