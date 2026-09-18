import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import type { DefinedTable } from "../typed-app.js";

it.each(["app", "slice"] as const)(
  "configures space administration without managed schema imports (%s)",
  async (kind) => {
    const definition = {
      projects: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), body: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["body"] })
        .indexOnly(["projectId"]),
    };
    const app =
      kind === "app"
        ? s.defineApp(definition)
        : s.defineSliceableApp(definition).slice("projects", "notes");
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
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
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
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Automatic space" });
      const note = tx.insert(app.notes, { projectId: project.id, body: "Encrypted content" });
      expect(await tx.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(note);
      await tx.commit().wait({ tier: "global" });
      expect(await db.all(app.notes, { tier: "global" })).toEqual([note]);
      await db.update(app.notes, note.id, { body: "Updated content" }).wait({ tier: "global" });
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual({
        ...note,
        body: "Updated content",
      });
      await db.upsert(app.notes, note.id, { body: "Upserted content" }).wait({ tier: "global" });
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual({
        ...note,
        body: "Upserted content",
      });
      const newId = crypto.randomUUID();
      await db
        .upsert(app.notes, newId, { projectId: project.id, body: "New upsert" })
        .wait({ tier: "global" });
      expect(await db.one(app.notes.where({ id: newId }), { tier: "global" })).toEqual({
        id: newId,
        projectId: project.id,
        body: "New upsert",
      });
      await db.delete(app.notes, note.id).wait({ tier: "global" });
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toBeNull();
      await db
        .restore(app.notes, note.id, { projectId: project.id, body: "Restored content" })
        .wait({ tier: "global" });
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual({
        ...note,
        body: "Restored content",
      });
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);

it("does not advertise managed policies on ordinary apps", () => {
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  definePermissions(app, ({ policy }) => {
    // @ts-expect-error Ordinary apps do not contain managed E2EE tables.
    expect(policy.__e2ee_spaces).toBeUndefined();
  });
});

it("slices an automatically registered table from a reusable encrypted schema", () => {
  const definition = s.defineSchema({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] })
      .branchBy("projectId"),
  });
  const app = s.defineSliceableApp(definition).slice("__e2ee_spaces");
  const permissions = definePermissions(app, ({ policy }) => {
    policy.__e2ee_spaces.allowRead.always();
  });
  expect(permissions).toHaveProperty("__e2ee_spaces");
});

it("does not infer encryption from a widened ordinary table type", () => {
  const columns = { title: s.string() };
  const projects: DefinedTable<typeof columns, {}> = s.table(columns, {});
  const app = s.defineApp({ projects });
  expect(Object.keys(app.wasmSchema)).toEqual(["projects"]);
  definePermissions(app, ({ policy }) => {
    // @ts-expect-error Possible encryption is not an encrypted declaration.
    expect(policy.__e2ee_spaces).toBeUndefined();
  });
});

it("keeps migration authoring limited to the declared tables", () => {
  const projects = s.table({ title: s.string() }, {});
  const migration = s.defineMigration({
    fromHash: "aaaaaaaaaaaa",
    toHash: "bbbbbbbbbbbb",
    from: { projects },
    to: {
      projects,
      notes: s
        .table(
          { projectId: s.uuid(), body: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["body"] }),
    },
    createTables: { notes: true },
  });
  expect(Object.keys(migration.from)).toEqual(["projects"]);
  expect(Object.keys(migration.to)).toEqual(["projects", "notes"]);
  expect(migration.forward).toEqual(
    [
      "notes",
      "__e2ee_recovery_protectors",
      "__e2ee_recovery_deliveries",
      "__e2ee_recovery_roots",
      "__e2ee_public_account_successors",
      "__e2ee_public_device_approvals",
      "__e2ee_account_roots",
      "__e2ee_device_keys",
      "__e2ee_account_successors",
      "__e2ee_device_challenges",
      "__e2ee_device_proofs",
      "__e2ee_device_approvals",
      "__e2ee_device_deliveries",
      "__e2ee_account_identities",
      "__e2ee_device_requests",
      "__e2ee_group_recovery_deliveries",
      "__e2ee_group_successors",
      "__e2ee_group_repairs",
      "__e2ee_group_membership",
      "__e2ee_groups",
      "__e2ee_group_deliveries",
      "__e2ee_space_recovery_deliveries",
      "__e2ee_space_successors",
      "__e2ee_spaces",
      "__e2ee_space_grants",
      "__e2ee_space_deliveries",
    ].map((table) => ({ table, added: true, operations: [] })),
  );
});
