import { describe, expect, it } from "vitest";
import { generateAuthSecret } from "../../src/index.js";
import { createBrowserTestDb as createDb, uniqueDbName, waitForCondition } from "./support.js";

import {
  todos,
  allTodos,
  type Todo,
  useSharedWorkerBridgeHarness,
} from "./worker-bridge-harness.js";

describe("local persistence across reopen", () => {
  const { track, untrack, trackSubscription, shutdownDbAndWorker } = useSharedWorkerBridgeHarness();

  it("settles an empty local read and retains acknowledged writes across repeated reopen", async () => {
    const config = {
      appId: crypto.randomUUID(),
      secret: generateAuthSecret(),
      driver: { type: "persistent" as const, dbName: `local-reopen-${crypto.randomUUID()}` },
    };
    let db = track(await createDb(config));
    const titles: string[] = [];
    try {
      // Safari used to overflow the WASM stack while a projection fetched its
      // already evaluated input. This first read hung before any write was made.
      expect(await db.all(todos, { tier: "local-first" })).toEqual([]);
      for (let cycle = 0; cycle < 3; cycle++) {
        const title = `synthetic task ${cycle}`;
        await db.insert(todos, { title, done: false }).wait({ tier: "local" });
        titles.push(title);
        expect(
          (await db.all(todos, { tier: "local-first" })).map((row) => row.title).sort(),
        ).toEqual(titles);
        await db.shutdown();
        untrack(db);
        db = track(await createDb(config));
        expect(
          (await db.all(todos, { tier: "local-first" })).map((row) => row.title).sort(),
        ).toEqual(titles);
      }
    } finally {
      await db.shutdown();
      untrack(db);
    }
  }, 60_000);

  it("persists data across shutdown and re-create", async () => {
    const dbName = uniqueDbName("persistence");

    const db1 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    db1.insert(todos, { title: "Survive reload", done: true });
    const before = await db1.all(allTodos);
    expect(before.length).toBe(1);
    await db1.shutdown();
    untrack(db1);

    // A new Db with the same namespace reopens the IndexedDB tree.
    const db2 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const after = await db2.all(allTodos, { tier: "local-first" });
    expect(after.length).toBe(1);
    expect(after[0].title).toBe("Survive reload");
    expect(after[0].done).toBe(true);
  });

  it("first local subscription snapshot returns no rows for an empty store", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("empty-snapshot") },
      }),
    );
    const firstSnapshot = new Promise<Todo[]>((resolve) => {
      trackSubscription(db.subscribe(allTodos, resolve, { tier: "local-first" }));
    });
    await expect(firstSnapshot).resolves.toEqual([]);
  });

  it("first local subscription snapshot contains persisted data", async () => {
    const config = {
      appId: "test-app",
      secret: generateAuthSecret(),
      driver: { type: "persistent" as const, dbName: uniqueDbName("first-snapshot-reopen") },
    };
    const seeded = track(await createDb(config));
    const expected: { id: string; title: string; done: boolean }[] = [];
    for (let index = 0; index < 3; index++) {
      const inserted = seeded.insert(todos, { title: `Persisted ${index}`, done: false });
      const row = await inserted.wait({ tier: "local" });
      expected.push({ id: row.id, title: row.title, done: row.done });
    }
    await seeded.all(allTodos, { tier: "local-first" });
    await shutdownDbAndWorker(seeded);

    const reopened = track(await createDb(config));
    const snapshots: (typeof expected)[] = [];
    // Subscribe before any read or readiness wait can warm the reopened
    // runtime. An empty callback followed by the stored rows must fail.
    trackSubscription(
      reopened.subscribe(
        todos.orderBy("title"),
        (rows) => snapshots.push(rows.map(({ id, title, done }) => ({ id, title, done }))),
        { tier: "local-first" },
      ),
    );
    await waitForCondition(
      async () => snapshots.length > 0,
      5000,
      "The initial local snapshot should contain the persisted rows",
    );
    expect(snapshots[0]).toEqual(expected);
  }, 15_000);
});
