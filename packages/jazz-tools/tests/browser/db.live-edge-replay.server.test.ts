import { afterEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { createDb, generateAuthSecret, type Db } from "../../src/index.js";
import { resolveDefaultPersistentDbName } from "../../src/runtime/db.js";
import { deploy } from "../../src/dev/catalogue.js";
import {
  liveEdgeApp as app,
  liveEdgePermissions,
  type LiveEdgeSeed,
} from "./live-edge-replay-schema.js";
import { getJazzServerInfo, type JazzServerInfo } from "./testing-server.js";
import { TestCleanup, uniqueDbName, waitForCondition, withTimeout } from "./support.js";

interface BackendCommands {
  liveEdgeBackendOpen(info: JazzServerInfo): Promise<LiveEdgeSeed>;
  liveEdgeBackendInsert(appId: string, seed: LiveEdgeSeed, title: string): Promise<string>;
  liveEdgeBackendClose(appId: string): Promise<void>;
}
const backend = commands as unknown as BackendCommands;
const cleanup = new TestCleanup();
let backendAppId: string | undefined;
afterEach(async () => {
  await cleanup.cleanup();
  if (backendAppId) await backend.liveEdgeBackendClose(backendAppId);
  backendAppId = undefined;
});

// #2363: a fresh root hydrated successfully but overlapping forward includes
// poisoned it after a live authoritative insert. Keep both carriers active and
// prove subsequent unrelated Edge reads work, not merely backend settlement.
describe("live authoritative overlapping relation replay", () => {
  it("keeps fresh, reopened and reconnected persistent roots usable after live inserts", async () => {
    const info = await getJazzServerInfo(uniqueDbName("live-edge-replay"));
    await deploy({ ...info, schema: app.wasmSchema, permissions: liveEdgePermissions });
    backendAppId = info.appId;
    const seed = await withTimeout(backend.liveEdgeBackendOpen(info), 20_000, "backend seed");
    const expected = [seed.itemId];
    const roots: string[] = [];
    const secret = generateAuthSecret();
    for (let fresh = 0; fresh < 2; fresh++) {
      const config = {
        appId: info.appId,
        serverUrl: info.serverUrl,
        secret,
        driver: { type: "persistent" as const, dbName: uniqueDbName("live-edge-root") },
      };
      const databasesBeforeOpen = (await indexedDB.databases()).map((entry) => entry.name);
      let physical: string | undefined;
      for (let reopen = 0; reopen < 2; reopen++) {
        const db = cleanup.track(await createDb(config));
        await assertUnrelatedRead(db);
        const openedPhysical = resolveDefaultPersistentDbName(db.config);
        if (reopen === 0) {
          expect(databasesBeforeOpen).not.toContain(openedPhysical);
          physical = openedPhysical;
          roots.push(openedPhysical);
        } else {
          expect(openedPhysical).toBe(physical);
        }
        const included = app.items
          .where({ parent_id: seed.parentId })
          .include({ author: true, label: true })
          .orderBy("$createdAt", "desc")
          .limit(250);
        const plain = app.items
          .where({ author_id: seed.authorId })
          .orderBy("$createdAt", "desc")
          .limit(100);
        let includedIds: string[] = [];
        let plainIds: string[] = [];
        let includesComplete = false;
        const stopIncluded = cleanup.trackSubscription(
          db.subscribe(
            included,
            (rows) => {
              includedIds = rows.map((row) => row.id);
              includesComplete = rows.every(
                (row) => row.author?.name === "Author" && row.label?.name === "Label",
              );
            },
            { tier: "edge" },
          ),
        );
        const stopPlain = cleanup.trackSubscription(
          db.subscribe(
            plain,
            (rows) => {
              plainIds = rows.map((row) => row.id);
            },
            { tier: "edge" },
          ),
        );
        const waitForBoth = () =>
          waitForCondition(
            async () =>
              includesComplete &&
              expected.every((id) => includedIds.includes(id) && plainIds.includes(id)),
            15_000,
            `both overlapping carriers (root=${fresh}, reopen=${reopen})`,
          );
        await waitForBoth();
        await assertUnrelatedRead(db);
        expect((await indexedDB.databases()).map((entry) => entry.name)).toContain(physical);
        const live = await withTimeout(
          backend.liveEdgeBackendInsert(info.appId, seed, `live-${fresh}-${reopen}`),
          15_000,
          "live global insert",
        );
        expected.push(live);
        await waitForBoth();
        await assertUnrelatedRead(db);

        await db.disconnect();
        const offline = await withTimeout(
          backend.liveEdgeBackendInsert(info.appId, seed, `offline-${fresh}-${reopen}`),
          15_000,
          "disconnected global insert",
        );
        expected.push(offline);
        await db.reconnect();
        await waitForBoth();
        await assertUnrelatedRead(db);
        const resumed = await withTimeout(
          backend.liveEdgeBackendInsert(info.appId, seed, `resumed-${fresh}-${reopen}`),
          15_000,
          "resumed global insert",
        );
        expected.push(resumed);
        await waitForBoth();
        await assertUnrelatedRead(db);
        stopIncluded();
        stopPlain();
        await db.shutdown();
        cleanup.untrack(db);
      }
      // Retain the previous physical database when opening a genuinely new root.
      expect((await indexedDB.databases()).map((entry) => entry.name)).toEqual(
        expect.arrayContaining(roots),
      );
    }
    expect(new Set(roots).size).toBe(2);
  }, 120_000);
});

async function assertUnrelatedRead(db: Db): Promise<void> {
  expect(
    await withTimeout(
      db.all(app.unrelated, { tier: "edge" }),
      10_000,
      "unrelated read after carrier delivery",
    ),
  ).toMatchObject([{ value: "still usable" }]);
}
