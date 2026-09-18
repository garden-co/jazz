import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("does not let a pending root deletion hide accepted offline encryption history", async () => {
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_spaces.allowDelete.never();
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Db | undefined;
  let saved: string | null = null;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: {
        app,
        store: {
          async read() {
            return saved;
          },
          async update(transform) {
            saved = transform(saved);
          },
        },
      },
    });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Retained accepted scope" });
    const note = tx.insert(app.notes, { projectId: project.id, body: "Before disconnect" });
    await tx.commit().wait({ tier: "global" });
    expect(await db.all(app.notes, { tier: "global" })).toEqual([note]);
    const roots = await db.all(app.__e2ee_spaces, { tier: "global" });
    expect(roots).toHaveLength(1);
    await db.disconnect();
    const deletion = db.delete(app.__e2ee_spaces, roots[0]!.id);
    await deletion.wait({ tier: "local" });
    expect(await db.all(app.__e2ee_spaces, { tier: "local" })).toEqual([]);
    expect(await db.all(app.notes, { tier: "local" })).toEqual([note]);
    const encryptedWrite = db.update(app.notes, note.id, { body: "Accepted history still works" });
    await encryptedWrite.wait({ tier: "local" });
    expect(await db.all(app.notes, { tier: "local" })).toEqual([
      { ...note, body: "Accepted history still works" },
    ]);
    const rejection = deletion.wait({ tier: "global" }).then(
      () => false,
      () => true,
    );
    await db.reconnect();
    expect(await rejection).toBe(true);
    await encryptedWrite.wait({ tier: "global" });
    expect(await db.all(app.__e2ee_spaces, { tier: "global" })).toEqual(roots);
    expect(await db.all(app.notes, { tier: "global" })).toEqual([
      { ...note, body: "Accepted history still works" },
    ]);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);
