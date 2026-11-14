import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { migration as m } from "../migration-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { appArtifact, appPermissions } from "../testing/app-artifact.js";
import { startTestJazz } from "../testing/index.js";

it("preserves rows and their relationship through table and reference-column renames", async () => {
  const person = s.table({ name: s.string() }, {});
  const before = {
    users: person,
    notes: s.table(
      { title: s.string(), authorId: s.uuid() },
      { author: s.rel("users", "authorId") },
    ),
  };
  const after = {
    people: person,
    messages: s.table(
      { title: s.string(), ownerId: s.uuid(), reviewerId: s.uuid().optional() },
      { owner: s.rel("people", "ownerId"), reviewer: s.rel("people", "reviewerId") },
    ),
  };
  const migration = m.defineMigration({
    from: before,
    to: after,
    renameTables: {
      people: m.renameTableFrom("users"),
      messages: m.renameTableFrom("notes"),
    },
    migrate: {
      messages: {
        ownerId: m.renameFrom("authorId"),
        reviewerId: m.add.ref("people", { default: null }),
      },
    },
  });
  const oldPermissions = definePermissions(
    s.defineApp(before, { migrations: "none" }),
    ({ policy }) => {
      policy.users.allowRead.always();
      policy.users.allowInsert.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
    },
  );
  const newPermissions = definePermissions(
    s.defineApp(after, { migrations: "none" }),
    ({ policy }) => {
      policy.people.allowRead.always();
      policy.people.allowInsert.always();
      policy.messages.allowRead.always();
      policy.messages.allowInsert.always();
    },
  );
  const oldApp = s.defineApp(before, {
    artifact: await appArtifact(before),
  });
  const newApp = s.defineApp(after, {
    artifact: await appArtifact(before, {
      steps: [{ to: after, migration }],
    }),
  });
  const server = await startTestJazz({
    app: oldApp,
    permissions: await appPermissions(oldApp, oldPermissions),
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

    await server.redeploy({ app: newApp, permissions: await appPermissions(newApp, newPermissions), migration });
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
