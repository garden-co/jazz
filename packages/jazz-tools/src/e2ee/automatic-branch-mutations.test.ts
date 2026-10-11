import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import type { Db } from "../runtime/db.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("upserts and restores encrypted heads using their exact branch scopes", async () => {
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), body: s.string(), branch: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["body"] })
      .indexOnly(["projectId"])
      .branchBy("branch"),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowRead.always();
    policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowUpdate.where({ "$createdBy.account": session.user.account });
    policy.notes.allowDelete.where({ "$createdBy.account": session.user.account });
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Db | undefined;
  let retained: string | null = null;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
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

    const setup = db.beginExclusiveTransaction();
    const baseProject = setup.insert(app.projects, { title: "Base scope" });
    const headProject = setup.insert(app.projects, { title: "Staged head scope" });
    await setup.commit().wait({ tier: "global" });
    const id = crypto.randomUUID();
    const baseRow = await db
      .insert(
        app.notes,
        { projectId: baseProject.id, body: "Inherited", branch: "base" },
        { id, branch: "base" },
      )
      .wait({ tier: "global" });

    const transaction = db.beginTransaction();
    transaction.insert(
      app.notes,
      { projectId: headProject.id, body: "Staged", branch: "draft" },
      { id, branch: "draft" },
    );
    const options: { branch: string; base: string } = { branch: "draft", base: "base" };
    transaction.upsert(app.notes, id, { body: "Updated head" }, options);
    options.base = "other";
    await transaction.commit().wait({ tier: "global" });

    await expect(
      db.one(app.notes.where({ id }), { branch: "draft", tier: "global" }),
    ).resolves.toEqual({
      id,
      projectId: headProject.id,
      body: "Updated head",
      branch: "draft",
    });
    // Continue with these same heads: restoring draft must not use the base
    // branch's different encryption scope or alter its inherited row.
    const firstProject = headProject;
    const otherProject = baseProject;
    const rowId = id;
    const draftRow = { id, projectId: headProject.id, body: "Updated head", branch: "draft" };
    const otherBranchRow = baseRow;
    expect(
      await db.one(app.notes.where({ id: rowId }), { branch: "base", tier: "global" }),
    ).toEqual(otherBranchRow);

    await db.delete(app.notes, rowId, { branch: "draft" }).wait({ tier: "global" });
    await expect(
      db
        .restore(
          app.notes,
          rowId,
          { projectId: otherProject.id, body: "Wrong scope", branch: "draft" },
          { branch: "draft" },
        )
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    expect(
      await db.one(app.notes.where({ id: rowId }), { branch: "draft", tier: "global" }),
    ).toBeNull();
    expect(
      await db.one(app.notes.where({ id: rowId }), { branch: "base", tier: "global" }),
    ).toEqual(otherBranchRow);

    await db
      .restore(
        app.notes,
        rowId,
        { projectId: firstProject.id, body: "Draft restored", branch: "draft" },
        { branch: "draft" },
      )
      .wait({ tier: "global" });
    expect(
      await db.one(app.notes.where({ id: rowId }), { branch: "draft", tier: "global" }),
    ).toEqual({ ...draftRow, body: "Draft restored" });

    const preparedId = crypto.randomUUID();
    const prepared = db.beginTransaction();
    const preparedRow = prepared.insert(
      app.notes,
      { projectId: firstProject.id, body: "Prepared original", branch: "draft" },
      { id: preparedId, branch: "draft" },
    );
    prepared.delete(app.notes, preparedId, { branch: "draft" });
    prepared.restore(
      app.notes,
      preparedId,
      { projectId: firstProject.id, body: "Prepared restored", branch: "draft" },
      { branch: "draft" },
    );
    await prepared.commit().wait({ tier: "global" });
    expect(
      await db.one(app.notes.where({ id: preparedId }), { branch: "draft", tier: "global" }),
    ).toEqual({ ...preparedRow, body: "Prepared restored" });
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 120_000);
