import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { startTestJazz, testApp } from "../testing/index.js";

it("preserves rows and their relationship through table and reference-column renames", async () => {
  const oldApp = testApp({
    tables: {
      users: { columns: { name: "string" } },
      notes: {
        columns: { title: "string", authorId: "uuid" },
        relations: { author: { rel: "users", column: "authorId" } },
      },
    },
  });
  const newApp = testApp({
    tables: {
      people: { columns: { name: "string" } },
      messages: {
        columns: { title: "string", ownerId: "uuid", reviewerId: "uuid?" },
        relations: {
          owner: { rel: "people", column: "ownerId" },
          reviewer: { rel: "people", column: "reviewerId" },
        },
      },
    },
    history: {
      v1: {
        tables: {
          users: { columns: { name: "string" } },
          notes: {
            columns: { title: "string", authorId: { type: "uuid", references: "users" } },
          },
        },
      },
    },
    migrations: [
      {
        from: "v1",
        renameTables: { people: "users", messages: "notes" },
        migrate: {
          messages: {
            ownerId: { renameFrom: "authorId" },
            reviewerId: { add: "uuid", default: null },
          },
        },
      },
    ],
  });
  const oldPermissions = {
    format: 1,
    tables: { users: { read: "true", insert: "true" }, notes: { read: "true", insert: "true" } },
  };
  const newPermissions = {
    format: 1,
    tables: {
      people: { read: "true", insert: "true" },
      messages: { read: "true", insert: "true" },
    },
  };
  const server = await startTestJazz({
    app: oldApp,
    permissions: oldPermissions,
    coreOptions: { inMemory: true },
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    const owner = await db.insert(oldApp.users, { name: "Alice" }).wait({ tier: "global" });
    const note = await db
      .insert(oldApp.notes, { title: "Before migration", authorId: owner.id })
      .wait({ tier: "global" });
    await db.shutdown();
    db = undefined;

    await server.redeploy({ app: newApp, permissions: newPermissions });
    db = await createDb(account);
    expect(await db.all(newApp.people, { tier: "remote" })).toEqual([owner]);
    expect(
      await db.one(
        newApp.messages.where({ id: note.id }).include({ owner: true, reviewer: true }),
        {
          tier: "remote",
        },
      ),
    ).toMatchObject({
      id: note.id,
      title: note.title,
      ownerId: owner.id,
      reviewerId: null,
      owner,
      reviewer: null,
    });
    const next = await db
      .insert(newApp.messages, {
        title: "After migration",
        ownerId: owner.id,
        reviewerId: owner.id,
      })
      .wait({ tier: "global" });
    expect(
      await db.one(newApp.messages.where({ id: next.id }).include({ reviewer: true }), {
        tier: "remote",
      }),
    ).toMatchObject({ id: next.id, reviewer: owner });
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);
