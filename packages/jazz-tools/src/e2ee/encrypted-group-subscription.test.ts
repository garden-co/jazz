import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { spaceSchema } from "./spaces.js";

it("invalidates encrypted equality when membership is removed from a child group", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
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
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_groups.allowRead.always();
    policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_group_membership.allowRead.always();
    policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_group_successors.allowRead.always();
    policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_group_deliveries.allowRead.always();
    policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_group_repairs.allowRead.always();
    policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const store = () => {
    let value: string | null = null;
    return {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        value = transform(value);
      },
    };
  };
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
    const owner = await createDb({ ...alice, e2ee: { app, store: store() } });
    clients.push(owner);
    const reader = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(reader);
    await owner.e2ee.devices.list();
    await reader.e2ee.devices.list();
    const parent = await owner.e2ee.groups.create().wait();
    const child = await owner.e2ee.groups.create().wait();
    await owner.e2ee.groups.add(parent.id, child.id).wait();
    await owner.e2ee.groups.add(child.id, bob.account.id).wait();
    const tx = owner.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Group project" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Shared note" });
    await tx.commit().wait({ tier: "global" });
    await owner.e2ee.spaces.grant(app.projects, project.id, parent.id).wait();
    expect(
      await reader.all(app.notes.where({ projectId: project.id, title: note.title }), {
        tier: "global",
      }),
    ).toEqual([note]);
    const snapshots: unknown[][] = [];
    let failure: Error | undefined;
    const stop = reader.subscribe(
      app.notes.where({ projectId: project.id, title: note.title }),
      {
        onUpdate: (rows) => snapshots.push(rows),
        onError: (error) => {
          failure = error;
        },
      },
      { tier: "global" },
    );
    try {
      await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 }).toEqual([note]);
      await owner.e2ee.groups.remove(child.id, bob.account.id).wait();
      await expect.poll(() => failure?.name, { timeout: 10_000 }).toBe("E2eeDataError");
      expect(snapshots.some((rows) => rows.length === 0)).toBe(false);
    } finally {
      stop();
    }
  } finally {
    for (const client of clients) await client.shutdown();
    await server.stop();
  }
}, 120_000);
