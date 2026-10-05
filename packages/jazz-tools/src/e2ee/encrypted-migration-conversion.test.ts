import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("converts encrypted data to a new table and space with an equality index", async () => {
  const before = {
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"] }),
  };
  const after = {
    ...before,
    indexedNotes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const migration = s.defineMigration({
    from: before,
    to: after,
    createTables: { indexedNotes: true },
  });
  const oldPermissions = definePermissions(oldApp, ({ policy, session }) => {
    policy.projects.allowRead.where({ "$createdBy.account": session.user.account });
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowRead.where({ "$createdBy.account": session.user.account });
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
  const destinationPermissions = definePermissions(newApp, ({ policy, session }) => {
    policy.indexedNotes.allowRead.where({ "$createdBy.account": session.user.account });
    policy.indexedNotes.allowInsert.where({ "$createdBy.account": session.user.account });
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
    await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb({ ...account, e2ee: { app: oldApp, store } });
    const project = await db
      .insert(oldApp.projects, { title: "Original space" })
      .wait({ tier: "global" });
    const source = await db
      .insert(oldApp.notes, { projectId: project.id, title: "Re-encrypt this note" })
      .wait({ tier: "global" });
    await db.shutdown();
    db = undefined;

    await deploy({
      ...target,
      schema: newApp,
      permissions: { ...oldPermissions, ...destinationPermissions },
      migration,
    });
    db = await createDb({ ...account, e2ee: { app: newApp, store } });
    const rows = await db.all(newApp.notes.where({ projectId: project.id }), { tier: "global" });
    expect(rows).toEqual([{ id: source.id, projectId: project.id, title: "Re-encrypt this note" }]);
    const tx = db.beginExclusiveTransaction();
    const destination = tx.insert(newApp.projects, { title: "Destination space" });
    for (const row of rows) {
      tx.insert(newApp.indexedNotes, { projectId: destination.id, title: row.title });
    }
    await tx.commit().wait({ tier: "global" });
    await db.shutdown();
    db = undefined;

    db = await createDb({ ...account, e2ee: { app: newApp, store } });
    expect(
      await db.all(
        newApp.indexedNotes.where({ projectId: destination.id, title: "Re-encrypt this note" }),
        { tier: "global" },
      ),
    ).toEqual([
      { id: expect.any(String), projectId: destination.id, title: "Re-encrypt this note" },
    ]);
    expect(
      await db.all(
        newApp.indexedNotes.where({ projectId: project.id, title: "Re-encrypt this note" }),
        { tier: "global" },
      ),
    ).toEqual([]);
    expect(await db.all(newApp.notes.where({ projectId: project.id }), { tier: "global" })).toEqual(
      rows,
    );
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);

it("converts existing plaintext through authorised reads and encrypted writes with wait handles", async () => {
  const before = {
    projects: s.table({ title: s.string() }, {}),
    legacyNotes: s.table(
      { projectId: s.uuid(), title: s.string() },
      { project: s.rel("projects", "projectId") },
    ),
  };
  const after = {
    ...before,
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const migration = s.defineMigration({ from: before, to: after, createTables: { notes: true } });
  const oldPermissions = definePermissions(oldApp, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.legacyNotes.allowRead.where({ "$createdBy.account": session.user.account });
    policy.legacyNotes.allowInsert.where({ "$createdBy.account": session.user.account });
  });
  const newPermissions = definePermissions(newApp, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.legacyNotes.allowRead.where({ "$createdBy.account": session.user.account });
    policy.legacyNotes.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowRead.where({ "$createdBy.account": session.user.account });
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
    await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    const project = await db
      .insert(oldApp.projects, { title: "Existing project" })
      .wait({ tier: "global" });
    const source = await db
      .insert(oldApp.legacyNotes, { projectId: project.id, title: "Convert this note" })
      .wait({ tier: "global" });
    await db.shutdown();
    db = undefined;

    await deploy({ ...target, schema: newApp, permissions: newPermissions, migration });
    db = await createDb({ ...account, e2ee: { app: newApp, store } });
    // This project predates encryption: its verified creator explicitly grants access.
    await db.e2ee.spaces.grant(newApp.projects, project.id, account.account.id).wait();
    const rows = await db.all(newApp.legacyNotes.where({ projectId: project.id }), {
      tier: "global",
    });
    expect(rows).toEqual([{ id: source.id, projectId: project.id, title: "Convert this note" }]);
    const tx = db.beginExclusiveTransaction();
    for (const row of rows) {
      tx.insert(newApp.notes, { projectId: row.projectId, title: row.title });
    }
    await tx.commit().wait({ tier: "global" });
    await db.shutdown();
    db = undefined;

    db = await createDb({ ...account, e2ee: { app: newApp, store } });
    expect(
      await db.all(newApp.notes.where({ projectId: project.id, title: "Convert this note" }), {
        tier: "global",
      }),
    ).toEqual([{ id: expect.any(String), projectId: project.id, title: "Convert this note" }]);
    expect(
      await db.all(newApp.legacyNotes.where({ projectId: project.id }), { tier: "global" }),
    ).toEqual(rows);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);

it("converts encrypted data into a new plaintext table without changing the encrypted source", async () => {
  const before = {
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  };
  const after = {
    ...before,
    exportedNotes: s.table(
      { projectId: s.uuid(), title: s.string() },
      { project: s.rel("projects", "projectId") },
    ),
  };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const migration = s.defineMigration({
    from: before,
    to: after,
    createTables: { exportedNotes: true },
  });
  const oldPermissions = definePermissions(oldApp, ({ policy, session }) => {
    policy.projects.allowRead.where({ "$createdBy.account": session.user.account });
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowRead.where({ "$createdBy.account": session.user.account });
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
  const destinationPermissions = definePermissions(newApp, ({ policy, session }) => {
    policy.exportedNotes.allowRead.where({ "$createdBy.account": session.user.account });
    policy.exportedNotes.allowInsert.where({ "$createdBy.account": session.user.account });
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
    await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb({ ...account, e2ee: { app: oldApp, store } });
    const project = await db
      .insert(oldApp.projects, { title: "Export project" })
      .wait({ tier: "global" });
    const source = await db
      .insert(oldApp.notes, { projectId: project.id, title: "Decrypt this note" })
      .wait({ tier: "global" });
    await db.shutdown();
    db = undefined;

    await deploy({
      ...target,
      schema: newApp,
      permissions: { ...oldPermissions, ...destinationPermissions },
      migration,
    });
    db = await createDb({ ...account, e2ee: { app: newApp, store } });
    const rows = await db.all(newApp.notes.where({ projectId: project.id }), { tier: "global" });
    expect(rows).toEqual([{ id: source.id, projectId: project.id, title: "Decrypt this note" }]);
    const tx = db.beginExclusiveTransaction();
    for (const row of rows) {
      tx.insert(newApp.exportedNotes, { projectId: row.projectId, title: row.title });
    }
    await tx.commit().wait({ tier: "global" });
    expect(
      await db.all(newApp.notes.where({ projectId: project.id, title: "Decrypt this note" }), {
        tier: "global",
      }),
    ).toEqual(rows);
    await db.shutdown();
    db = undefined;

    // Reading the exported destination needs ordinary permissions, not an E2EE key store.
    db = await createDb(account);
    expect(
      await db.all(
        newApp.exportedNotes.where({ projectId: project.id, title: "Decrypt this note" }),
        { tier: "global" },
      ),
    ).toEqual([{ id: expect.any(String), projectId: project.id, title: "Decrypt this note" }]);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);
