import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("reads and searches existing ciphertext after renaming its column", async () => {
  const projects = s.table({ title: s.string() }, {});
  const before = {
    projects,
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  };
  const after = {
    projects,
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"], indexes: { body: "equality" } }),
  };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const migration = s.defineMigration({
    from: before,
    to: after,
    migrate: { notes: { body: s.renameFrom("title") } },
  });
  const permissions = (app: typeof oldApp | typeof newApp) =>
    definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.notes.allowRead.always();
      policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
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
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let retained: string | null = null;
  const store = {
    async read() {
      return retained;
    },
    async update(transform: (current: string | null) => string) {
      retained = transform(retained);
    },
  };
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: permissions(oldApp) });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb({ ...account, e2ee: { app: oldApp, store } });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(oldApp.projects, { title: "Project" });
    const note = tx.insert(oldApp.notes, { projectId: project.id, title: "Private title" });
    await tx.commit().wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await deploy({ ...target, schema: newApp, permissions: permissions(newApp), migration });
    db = await createDb({ ...account, e2ee: { app: newApp, store } });
    expect(await db.all(newApp.notes.where({ projectId: project.id }), { tier: "global" })).toEqual(
      [{ id: note.id, projectId: project.id, body: "Private title" }],
    );
    expect(
      await db.all(newApp.notes.where({ projectId: project.id, body: "Private title" }), {
        tier: "global",
      }),
    ).toEqual([{ id: note.id, projectId: project.id, body: "Private title" }]);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);

it.each(["partial", "full"] as const)(
  "preserves existing ciphertext when adding a second encrypted table with a %s witness",
  async (witness) => {
    const projects = s.table({ title: s.string() }, {});
    const encrypted = s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } });
    const before = { projects, notes: encrypted };
    const after = { ...before, messages: encrypted };
    const oldApp = s.defineApp(before);
    const newApp = s.defineApp(after);
    const migration = s.defineMigration({
      from: witness === "partial" ? { projects } : before,
      to: after,
      createTables: { messages: true },
    });
    const permissions = (app: typeof newApp) =>
      definePermissions(app, ({ policy, session }) => {
        policy.projects.allowRead.always();
        policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
        policy.notes.allowRead.always();
        policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
        policy.messages.allowRead.always();
        policy.messages.allowInsert.where({ "$createdBy.account": session.user.account });
        policy.__e2ee_spaces.allowRead.always();
        policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
        policy.__e2ee_space_grants.allowRead.always();
        policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
        policy.__e2ee_space_deliveries.allowRead.always();
        policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
        policy.__e2ee_space_successors.allowRead.always();
        policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      });
    const { messages: _messages, ...oldPermissions } = permissions(newApp);
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let retained: string | null = null;
    const store = {
      async read() {
        return retained;
      },
      async update(transform: (current: string | null) => string) {
        retained = transform(retained);
      },
    };
    try {
      const target = {
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
      };
      await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
      const account = await localAccountConfig(server.appId, server.url);
      db = await createDb({ ...account, e2ee: { app: oldApp, store } });
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(oldApp.projects, { title: "Project" });
      const note = tx.insert(oldApp.notes, { projectId: project.id, title: "Historical secret" });
      await tx.commit().wait({ tier: "global" });
      await db.shutdown();
      db = undefined;
      await deploy({ ...target, schema: newApp, permissions: permissions(newApp), migration });
      await deploy({ ...target, schema: newApp, permissions: permissions(newApp), migration });
      db = await createDb({ ...account, e2ee: { app: newApp, store } });
      expect(
        await db.all(newApp.notes.where({ projectId: project.id }), { tier: "global" }),
      ).toEqual([{ id: note.id, projectId: project.id, title: "Historical secret" }]);
      expect(
        await db.all(newApp.notes.where({ projectId: project.id, title: "Historical secret" }), {
          tier: "global",
        }),
      ).toEqual([{ id: note.id, projectId: project.id, title: "Historical secret" }]);
      const next = db.beginExclusiveTransaction();
      const message = next.insert(newApp.messages, { projectId: project.id, title: "New secret" });
      await next.commit().wait({ tier: "global" });
      expect(
        await db.all(newApp.messages.where({ projectId: project.id }), { tier: "global" }),
      ).toEqual([{ id: message.id, projectId: project.id, title: "New secret" }]);
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);

it("rejects an explicit added table already present outside the partial source witness", async () => {
  const projects = s.table({ title: s.string() }, {});
  const notes = s
    .table({ projectId: s.uuid(), title: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["title"] });
  const oldApp = s.defineApp({ projects, notes });
  const newApp = s.defineApp({ projects, notes, messages: notes });
  const migration = s.defineMigration({
    from: { projects },
    to: { projects, notes, messages: notes },
    createTables: { notes: true, messages: true },
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: definePermissions(oldApp, () => {}) });
    await expect(
      deploy({
        ...target,
        schema: newApp,
        permissions: definePermissions(newApp, () => {}),
        migration,
      }),
    ).rejects.toThrow(/added table notes.*canonical source/i);
    const unchanged = await deploy({
      ...target,
      schema: oldApp,
      permissions: definePermissions(oldApp, () => {}),
    });
    expect(unchanged.schema.status).toBe("already-stored");
    expect(unchanged.migration).toBeUndefined();
  } finally {
    await server.stop();
  }
}, 60_000);

it("reads and searches existing ciphertext after renaming its scope and referencing table", async () => {
  const projects = s.table({ title: s.string() }, {});
  const before = {
    projects,
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  };
  const after = {
    workspaces: projects,
    records: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("workspaces", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const migration = s.defineMigration({
    from: before,
    to: after,
    renameTables: {
      workspaces: s.renameTableFrom("projects"),
      records: s.renameTableFrom("notes"),
    },
  });
  const oldPermissions = definePermissions(oldApp, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowRead.always();
    policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  });
  const newPermissions = definePermissions(newApp, ({ policy, session }) => {
    policy.workspaces.allowRead.always();
    policy.workspaces.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.records.allowRead.always();
    policy.records.allowInsert.where({ "$createdBy.account": session.user.account });
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
  const store = {
    async read() {
      return retained;
    },
    async update(transform: (current: string | null) => string) {
      retained = transform(retained);
    },
  };
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb({ ...account, e2ee: { app: oldApp, store } });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(oldApp.projects, { title: "Project" });
    const note = tx.insert(oldApp.notes, { projectId: project.id, title: "Historical secret" });
    await tx.commit().wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await deploy({ ...target, schema: newApp, permissions: newPermissions, migration });
    db = await createDb({ ...account, e2ee: { app: newApp, store } });
    expect(await db.all(newApp.workspaces, { tier: "global" })).toEqual([
      { id: project.id, title: "Project" },
    ]);
    expect(
      await db.all(newApp.records.where({ projectId: project.id }), { tier: "global" }),
    ).toEqual([{ id: note.id, projectId: project.id, title: "Historical secret" }]);
    expect(
      await db.all(newApp.records.where({ projectId: project.id, title: "Historical secret" }), {
        tier: "global",
      }),
    ).toEqual([{ id: note.id, projectId: project.id, title: "Historical secret" }]);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);

it("rejects retargeting encrypted rows to an unrelated scope alongside table renames", () => {
  const scope = s.table({ title: s.string() }, {});
  expect(() =>
    s.defineMigration({
      from: {
        projects: scope,
        teams: scope,
        notes: s
          .table(
            { projectId: s.uuid(), title: s.string() },
            { project: s.rel("projects", "projectId") },
          )
          .encrypted({ space: "projectId", columns: ["title"] }),
      },
      to: {
        workspaces: scope,
        teams: scope,
        records: s
          .table(
            { projectId: s.uuid(), title: s.string() },
            { project: s.rel("teams", "projectId") },
          )
          .encrypted({ space: "projectId", columns: ["title"] }),
      },
      renameTables: {
        workspaces: s.renameTableFrom("projects"),
        records: s.renameTableFrom("notes"),
      },
    }),
  ).toThrow(/encryption.*authorised client/i);
});

it("requires a user migration when adding the first encrypted table", async () => {
  const projects = s.table({ title: s.string() }, {});
  const oldApp = s.defineApp({ projects });
  const newApp = s.defineApp({
    projects,
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"] }),
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: definePermissions(oldApp, () => {}) });
    await expect(
      deploy({ ...target, schema: newApp, permissions: definePermissions(newApp, () => {}) }),
    ).rejects.toThrow(/requires a migration/i);
    const unchanged = await deploy({
      ...target,
      schema: oldApp,
      permissions: definePermissions(oldApp, () => {}),
    });
    expect(unchanged.schema.status).toBe("already-stored");
    expect(unchanged.migration).toBeUndefined();
  } finally {
    await server.stop();
  }
}, 60_000);
