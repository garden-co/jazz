import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { e2eeAccountRelation } from "./account-relation.js";
import { encryptionSpacePolicySchema } from "./current-membership-schema.js";

it("keeps Alice's creator ownership when Bob edits a project and resolves both synthetic account relations", async () => {
  const app = s.defineApp({
    ...encryptionSpacePolicySchema("projects"),
    projects: s.table(
      { name: s.string() },
      {
        creator: e2eeAccountRelation("$createdBy.account"),
        updater: e2eeAccountRelation("$updatedBy.account"),
      },
    ),
    notes: s
      .table(
        { projectId: s.uuid(), body: s.string() },
        {
          project: s.rel("projects", "projectId"),
        },
      )
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
  const permissions = definePermissions(app, ({ policy: p, session }) => {
    p.projects.allowRead.always();
    p.projects.allowWrite().always();
    p.notes.fromEncryptionSpace();
    p.notes.space.allowRead.always();
    p.notes.space.allowInsert.where((space) =>
      p.projects.existsIncludingCreated.where({
        id: space.identifier,
        "$createdBy.account": space.creatorAccountId,
      }),
    );
    p.notes.space.allowUpdate.where((space) =>
      p.projects.existsIncludingCreated.where({
        id: space.identifier,
        "$createdBy.account": session.user.account,
      }),
    );
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const open = async () => {
      const account = await localAccountConfig(server.appId, server.url);
      let saved: string | null = null;
      const db = await createDb({
        ...account,
        driver: { type: "memory" },
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
      clients.push(db);
      await expect
        .poll(
          async () =>
            (
              await db.all(app.__e2ee_account_roots.where({ accountId: account.account.id }), {
                tier: "remote",
              })
            ).length,
        )
        .toBe(1);
      return { db, id: account.account.id };
    };
    const alice = await open();
    const bob = await open();
    const project = await alice.db
      .insert(app.projects, { name: "Creator-owned" })
      .wait({ tier: "global" });
    const note = await alice.db
      .insert(app.notes, { projectId: project.id, body: "Private" })
      .wait({ tier: "global" });
    await bob.db
      .update(app.projects, project.id, { name: "Edited by Bob" })
      .wait({ tier: "global" });
    const row = await alice.db.one(
      app.projects.where({ id: project.id }).include({ creator: true, updater: true }),
      { tier: "remote" },
    );
    expect(row).toMatchObject({ creator: { id: alice.id }, updater: { id: bob.id } });
    await expect(
      bob.db.e2ee.spaces.grant(app.projects, project.id, bob.id).wait(),
    ).rejects.toThrow();
    await alice.db.e2ee.spaces.grant(app.projects, project.id, bob.id).wait();
    expect(await bob.db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toMatchObject({
      body: "Private",
    });
    await expect(
      bob.db.e2ee.spaces.revoke(app.projects, project.id, alice.id).wait(),
    ).rejects.toThrow();
    await alice.db.e2ee.spaces.revoke(app.projects, project.id, bob.id).wait();
    expect(await bob.db.all(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual([]);
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 120_000);
