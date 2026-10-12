import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import type { Db } from "../runtime/db.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("preserves explicit administration rules and encrypted mutations when composing application policies", async () => {
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] })
      .indexOnly(["projectId"]),
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Db | undefined;
  let retained: string | null = null;
  try {
    const account = await localAccountConfig(server.appId, server.url);
    const administration = definePermissions(app, ({ policy, session }) => {
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({
        authorAccountId: session.user.account,
      });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({
        authorAccountId: session.user.account,
      });
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowInsert.where({
        authorAccountId: session.user.account,
        operation: "add",
      });
      policy.__e2ee_group_deliveries.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_group_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
      policy.__e2ee_group_repairs.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_successors.allowInsert.where({
        authorAccountId: session.user.account,
      });
    });
    const application = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.notes.allowRead.always();
      policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.notes.allowUpdate.where({ "$createdBy.account": session.user.account });
      policy.notes.allowDelete.where({ "$createdBy.account": session.user.account });
    });
    const permissions = { ...administration, ...application };
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb({
      ...account,
      e2ee: {
        app,
        store: {
          async read() {
            return retained;
          },
          async update(transform: (current: string | null) => string) {
            retained = transform(retained);
          },
        },
      },
    });

    await db.e2ee.devices.list();

    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Automatic space" });
    const note = tx.insert(app.notes, { projectId: project.id, body: "Encrypted content" });
    expect(await tx.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual(note);
    await tx.commit().wait({ tier: "global" });
    expect(await db.all(app.notes, { tier: "remote" })).toEqual([note]);
    await db.update(app.notes, note.id, { body: "Updated content" }).wait({ tier: "global" });
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual({
      ...note,
      body: "Updated content",
    });
    await db.upsert(app.notes, note.id, { body: "Upserted content" }).wait({ tier: "global" });
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual({
      ...note,
      body: "Upserted content",
    });
    const newId = crypto.randomUUID();
    await db
      .upsert(app.notes, newId, { projectId: project.id, body: "New upsert" })
      .wait({ tier: "global" });
    expect(await db.one(app.notes.where({ id: newId }), { tier: "remote" })).toEqual({
      id: newId,
      projectId: project.id,
      body: "New upsert",
    });
    await db.delete(app.notes, note.id).wait({ tier: "global" });
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toBeNull();
    await db
      .restore(app.notes, note.id, { projectId: project.id, body: "Restored content" })
      .wait({ tier: "global" });
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual({
      ...note,
      body: "Restored content",
    });

    const group = await db.e2ee.groups.create().wait();
    await expect(
      db.e2ee.groups.remove(group.id, { kind: "account", id: account.account.id }).wait(),
    ).rejects.toMatchObject({
      code: "permission_denied",
    });
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);
