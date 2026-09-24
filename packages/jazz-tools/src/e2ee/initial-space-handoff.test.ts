import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { E2eeDataError } from "./data-error.js";

it("preserves accepted scope and data when later initial recipient delivery is denied", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), body: s.string() },
        {
          project: s.rel("projects", "projectId"),
        },
      )
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    // No delivery insert permission: accepted initial data cannot be undone by handoff denial.
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let creator: Db | undefined;
  let recipient: Db | undefined;
  let creatorStore: string | null = null;
  let recipientStore: string | null = null;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    creator = await createDb({
      ...alice,
      e2ee: {
        app,
        store: {
          async read() {
            return creatorStore;
          },
          async update(transform) {
            creatorStore = transform(creatorStore);
          },
        },
      },
    });
    recipient = await createDb({
      ...bob,
      e2ee: {
        app,
        store: {
          async read() {
            return recipientStore;
          },
          async update(transform) {
            recipientStore = transform(recipientStore);
          },
        },
      },
    });
    await creator.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    const tx = creator.beginExclusiveTransaction();
    const project = tx.insert(
      app.projects,
      { title: "Accepted despite denied handoff" },
      {
        initialRecipients: [bob.account.id],
      },
    );
    const note = tx.insert(app.notes, { projectId: project.id, body: "Accepted ciphertext" });
    await tx.commit().wait({ tier: "global" });
    expect(await creator.one(app.projects.where({ id: project.id }), { tier: "global" })).toEqual(
      project,
    );
    expect(await recipient.all(app.notes.select("id"), { tier: "global" })).toEqual([
      { id: note.id },
    ]);
    const root = await creator.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "global",
    });
    expect(
      (
        await creator.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), { tier: "global" })
      ).map((grant) => grant.recipientId),
    ).toEqual([bob.account.id]);
    expect(await creator.all(app.__e2ee_space_deliveries, { tier: "global" })).toEqual([]);
    await expect(
      recipient.one(app.notes.where({ id: note.id }), { tier: "global" }),
    ).rejects.toEqual(new E2eeDataError("key-unavailable"));
    expect(
      await creator.e2ee.explain({ scope: app.projects, identifier: project.id }),
    ).toMatchObject({ state: "refused", reason: "not-a-space-recipient" });
  } finally {
    await recipient?.shutdown();
    await creator?.shutdown();
    await server.stop();
  }
}, 60_000);
