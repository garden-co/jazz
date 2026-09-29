import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { PersistedWriteRejectedError } from "./client.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("inserts an ordinary parent and child atomically after adding a table", async () => {
  const before = { projects: s.table({ title: s.string() }, {}) };
  const after = {
    ...before,
    notes: s.table(
      { projectId: s.uuid(), title: s.string() },
      { project: s.rel("projects", "projectId") },
    ),
  };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const oldPermissions = definePermissions(oldApp, ({ policy }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
  });
  const newPermissions = definePermissions(newApp, ({ policy }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    await db.insert(oldApp.projects, { title: "Existing project" }).wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await deploy({
      ...target,
      schema: newApp,
      permissions: newPermissions,
      migration: s.defineMigration({ from: before, to: after, createTables: { notes: true } }),
    });
    db = await createDb(account);
    await db.all(newApp.projects, { tier: "global" });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(newApp.projects, { title: "New project" });
    const note = tx.insert(newApp.notes, { projectId: project.id, title: "New note" });
    await tx.commit().wait();
    expect(await db.all(newApp.notes, { tier: "global" })).toEqual([
      { id: note.id, projectId: project.id, title: "New note" },
    ]);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);

it.each(["local", "global"] as const)(
  "rejects a matching phantom at %s durability",
  async (tier) => {
    const app = s.defineApp({ notes: s.table({ bucket: s.string(), title: s.string() }, {}) });
    const permissions = definePermissions(app, ({ policy }) => {
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
    });
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
      db = await createDb(await localAccountConfig(server.appId, server.url));
      await db.all(app.notes, { tier: "global" });
      if (tier === "local") await db.disconnect();
      const tx = db.beginExclusiveTransaction();
      expect(await tx.all(app.notes.where({ bucket: "destination" }), { tier: "local" })).toEqual(
        [],
      );
      const phantom = await db
        .insert(app.notes, { bucket: "destination", title: "Concurrent insert" })
        .wait({ tier });
      tx.insert(app.notes, { bucket: "destination", title: "Must not publish" });
      const conflict = await tx
        .commit()
        .wait()
        .then(
          () => undefined,
          (error: unknown) => error,
        );
      expect(conflict).toBeInstanceOf(PersistedWriteRejectedError);
      expect(conflict).toMatchObject({ code: "transaction_conflict" });
      expect(await db.all(app.notes, { tier: "local" })).toEqual([
        { id: phantom.id, bucket: "destination", title: "Concurrent insert" },
      ]);
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);

it("does not reject a filtered transaction read when an unrelated row is inserted", async () => {
  const app = s.defineApp({ notes: s.table({ bucket: s.string(), title: s.string() }, {}) });
  const permissions = definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
  });
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
    db = await createDb(await localAccountConfig(server.appId, server.url));
    await db.all(app.notes, { tier: "global" });
    const tx = db.beginExclusiveTransaction();
    expect(await tx.all(app.notes.where({ bucket: "destination" }))).toEqual([]);
    await db
      .insert(app.notes, { bucket: "unrelated", title: "Other data" })
      .wait({ tier: "global" });
    const note = tx.insert(app.notes, { bucket: "destination", title: "Intended write" });
    await tx.commit().wait();
    expect(await db.all(app.notes.where({ bucket: "destination" }), { tier: "global" })).toEqual([
      { id: note.id, bucket: "destination", title: "Intended write" },
    ]);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);

it.each(["offline", "synced"] as const)(
  "rejects the losing exclusive write on the same row without failing the core tick (%s)",
  async (mode) => {
    const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
    const permissions = definePermissions(app, ({ policy }) => {
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.notes.allowUpdate.always();
    });
    const server =
      mode === "synced"
        ? await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true })
        : undefined;
    const uncaught: unknown[] = [];
    const onUncaught = (error: unknown) => uncaught.push(error);
    process.on("uncaughtException", onUncaught);
    process.on("unhandledRejection", onUncaught);
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    try {
      if (server) {
        await deploy({
          serverUrl: server.url,
          appId: server.appId,
          adminSecret: server.adminSecret,
          schema: app,
          permissions,
        });
        db = await createDb(await localAccountConfig(server.appId, server.url));
      } else {
        db = await createDb({
          ...(await localAccountConfig("test-app")),
          driver: { type: "memory" },
        });
      }
      const row = await db.insert(app.notes, { title: "Shared" }).wait({ tier: "local" });

      // Both transactions read the row before either writes it, so they race
      // on the same snapshot and exactly one can commit.
      let releaseWrites!: () => void;
      const bothRead = new Promise<void>((resolve) => {
        releaseWrites = resolve;
      });
      let reads = 0;
      const race = (title: string) =>
        db!.exclusiveTransaction(async (tx) => {
          await tx.one(app.notes.where({ id: row.id }), { tier: "local" });
          if (++reads === 2) releaseWrites();
          await bothRead;
          tx.update(app.notes, row.id, { title });
        });
      const writes = await Promise.all([race("First"), race("Second")]);
      const results = await Promise.allSettled(writes.map((write) => write.wait()));

      expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
      const [rejected] = results.filter((result) => result.status === "rejected");
      expect(rejected?.reason).toBeInstanceOf(PersistedWriteRejectedError);
      expect(rejected?.reason).toMatchObject({ code: "transaction_conflict" });
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(uncaught).toEqual([]);
    } finally {
      process.off("uncaughtException", onUncaught);
      process.off("unhandledRejection", onUncaught);
      await db?.shutdown();
      await server?.stop();
    }
  },
  60_000,
);
