import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { PersistedWriteRejectedError } from "./client.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

// garden-co/jazz#3755: a policy `exists` sees rows written earlier in the same
// transaction, so the server accepts the transaction the client already applied
// optimistically, and rejects it only when the client's own view would too.
const app = s.defineApp({
  shows: s.table({ chiefAccount: s.uuid() }, {}),
  tasks: s.table({ showId: s.uuid(), title: s.string() }, { show: s.rel("shows", "showId") }),
});
const permissions = definePermissions(app, ({ policy, session }) => {
  policy.shows.allowRead.where({ chiefAccount: session.user.account });
  policy.shows.allowInsert.where({ chiefAccount: session.user.account });
  policy.shows.allowDelete.where({ chiefAccount: session.user.account });
  policy.tasks.allowRead.always();
  policy.tasks.allowInsert.where((task) =>
    policy.shows.exists.where({ id: task.showId, chiefAccount: session.user.account }),
  );
});

async function withChief(
  run: (db: Awaited<ReturnType<typeof createDb>>, me: string) => Promise<void>,
): Promise<void> {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    await db.all(app.tasks, { tier: "global" });
    await run(db, account.account.id);
  } finally {
    await db?.shutdown();
    await server.stop();
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

    expect(await db.all(app.tasks, { tier: "global" })).toEqual([
      { id: task.id, showId: show.id, title: "Load-in" },
    ]);
    expect(await db.all(app.tasks)).toEqual([{ id: task.id, showId: show.id, title: "Load-in" }]);
  });
}, 60_000);

it("accepts the same rows in one exclusive transaction", async () => {
  await withChief(async (db, me) => {
    const tx = db.beginExclusiveTransaction();
    const show = tx.insert(app.shows, { chiefAccount: me });
    const task = tx.insert(app.tasks, { showId: show.id, title: "Load-in" });
    await tx.commit().wait();

    expect(await db.all(app.tasks, { tier: "global" })).toEqual([
      { id: task.id, showId: show.id, title: "Load-in" },
    ]);
  });
}, 60_000);

it("rejects a task whose show the same transaction deletes, on client and server", async () => {
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
    expect(await db.all(app.tasks, { tier: "global" })).toEqual([]);
    await expect.poll(() => db.all(app.tasks)).toEqual([]);
  });
}, 60_000);
