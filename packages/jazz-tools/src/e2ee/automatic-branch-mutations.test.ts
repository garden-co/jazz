import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import type { Db } from "../runtime/db.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("upserts a staged encrypted head using its own scope through a base view", async () => {
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

    const baseProject = await db
      .insert(app.projects, { title: "Base scope" })
      .wait({ tier: "global" });
    const headProject = await db
      .insert(app.projects, { title: "Staged head scope" })
      .wait({ tier: "global" });
    const id = crypto.randomUUID();
    await db
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
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 120_000);

it("restores encrypted rows from their exact branch scope, including prepared writes", async () => {
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

    const firstProject = await db
      .insert(app.projects, { title: "Draft owner" })
      .wait({ tier: "global" });
    const otherProject = await db
      .insert(app.projects, { title: "Other branch owner" })
      .wait({ tier: "global" });
    const rowId = crypto.randomUUID();
    const draftRow = await db
      .insert(
        app.notes,
        { projectId: firstProject.id, body: "Draft original", branch: "draft" },
        { id: rowId, branch: "draft" },
      )
      .wait({ tier: "global" });
    const otherBranchRow = await db
      .insert(
        app.notes,
        { projectId: otherProject.id, body: "Other branch original", branch: "other" },
        { id: rowId, branch: "other" },
      )
      .wait({ tier: "global" });
    expect(
      await db.one(app.notes.where({ id: rowId }), { branch: "other", tier: "global" }),
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
      await db.one(app.notes.where({ id: rowId }), { branch: "other", tier: "global" }),
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
    const transaction = db.beginTransaction();
    const preparedRow = transaction.insert(
      app.notes,
      { projectId: firstProject.id, body: "Prepared original", branch: "draft" },
      { id: preparedId, branch: "draft" },
    );
    transaction.delete(app.notes, preparedId, { branch: "draft" });
    transaction.restore(
      app.notes,
      preparedId,
      { projectId: firstProject.id, body: "Prepared restored", branch: "draft" },
      { branch: "draft" },
    );
    await transaction.commit().wait({ tier: "global" });
    expect(
      await db.one(app.notes.where({ id: preparedId }), { branch: "draft", tier: "global" }),
    ).toEqual({ ...preparedRow, body: "Prepared restored" });
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 120_000);
