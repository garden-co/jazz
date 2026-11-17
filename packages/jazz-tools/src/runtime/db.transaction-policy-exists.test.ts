import { expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { PersistedWriteRejectedError } from "./client.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz } from "../testing/index.js";

// garden-co/jazz#3755: a policy `exists` sees committed rows plus the rows the
// same transaction inserts or updates, so the server accepts the transaction the
// client already applied optimistically. A committed row the transaction deletes
// still counts; a row it both inserts and deletes never does.
const app = testApp({
  tables: {
    shows: { columns: { chiefAccount: "uuid" } },
    tasks: {
      columns: { showId: "uuid", title: "string" },
      relations: { show: { rel: "shows", column: "showId" } },
    },
  },
});
const permissions = {
  format: 1,
  tables: {
    shows: {
      read: "chiefAccount = session.user",
      insert: "chiefAccount = session.user",
      delete: "chiefAccount = session.user",
    },
    tasks: {
      read: "true",
      insert: "exists shows(id = this.showId, chiefAccount = session.user)",
    },
  },
} as const;

async function withChief(
  run: (db: Awaited<ReturnType<typeof createDb>>, me: string) => Promise<void>,
): Promise<void> {
  const jazz = await startTestJazz({ app, permissions });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const account = await localAccountConfig(jazz.appId, jazz.url);
    db = await createDb(account);
    await db.all(app.tasks, { tier: "remote" });
    await run(db, account.account.id);
  } finally {
    await db?.shutdown();
    await jazz.stop();
  }
}

it("accepts a task inserted in the same transaction as the show it requires", async () => {
  await withChief(async (db, me) => {
    const result = await db.transaction((tx) => {
      const show = tx.insert(app.shows, { chiefAccount: me });
      const task = tx.insert(app.tasks, { showId: show.id, title: "Load-in" });
      return { show, task };
    });
    const { show, task } = await result.wait({ tier: "global" });

    expect(await db.all(app.tasks, { tier: "remote" })).toEqual([
      { id: task.id, showId: show.id, title: "Load-in" },
    ]);
    expect(await db.all(app.tasks)).toEqual([{ id: task.id, showId: show.id, title: "Load-in" }]);
  });
}, 60_000);

it("accepts the same rows in one serializable transaction", async () => {
  await withChief(async (db, me) => {
    const tx = db.beginSerializableTransaction();
    const show = tx.insert(app.shows, { chiefAccount: me });
    const task = tx.insert(app.tasks, { showId: show.id, title: "Load-in" });
    await tx.commit().wait();

    expect(await db.all(app.tasks, { tier: "remote" })).toEqual([
      { id: task.id, showId: show.id, title: "Load-in" },
    ]);
  });
}, 60_000);

it("rejects a task whose show the same transaction inserts and deletes, on client and server", async () => {
  await withChief(async (db, me) => {
    const result = await db.transaction((tx) => {
      const show = tx.insert(app.shows, { chiefAccount: me });
      tx.insert(app.tasks, { showId: show.id, title: "Load-in" });
      tx.delete(app.shows, show.id);
    });

    const rejection = await result.wait({ tier: "global" }).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(rejection).toBeInstanceOf(PersistedWriteRejectedError);
    expect(rejection).toMatchObject({ code: "permission_denied" });
    expect(await db.all(app.tasks, { tier: "remote" })).toEqual([]);
    await expect.poll(() => db.all(app.tasks)).toEqual([]);
  });
}, 60_000);

it("accepts a task whose committed show the same transaction deletes", async () => {
  await withChief(async (db, me) => {
    const show = await db.insert(app.shows, { chiefAccount: me }).wait({ tier: "global" });

    const result = await db.transaction((tx) => {
      const task = tx.insert(app.tasks, { showId: show.id, title: "Strike" });
      tx.delete(app.shows, show.id);
      return task;
    });
    const task = await result.wait({ tier: "global" });

    expect(await db.all(app.tasks, { tier: "remote" })).toEqual([
      { id: task.id, showId: show.id, title: "Strike" },
    ]);
    expect(await db.all(app.shows, { tier: "remote" })).toEqual([]);
  });
}, 60_000);
