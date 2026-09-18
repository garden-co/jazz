import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { spaceSchema } from "./spaces.js";

it("initialises a space for a group without implicitly granting its creator", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
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
      permissions: {
        ...deviceRequestPermissions,
        projects: policies.projects!,
        __e2ee_spaces: policies.__e2ee_spaces!,
        __e2ee_space_grants: policies.__e2ee_space_grants!,
        __e2ee_space_deliveries: policies.__e2ee_space_deliveries!,
        __e2ee_space_successors: policies.__e2ee_space_successors!,
        __e2ee_groups: policies.__e2ee_groups!,
        __e2ee_group_membership: policies.__e2ee_group_membership!,
        __e2ee_group_successors: policies.__e2ee_group_successors!,
        __e2ee_group_deliveries: policies.__e2ee_group_deliveries!,
        __e2ee_group_repairs: policies.__e2ee_group_repairs!,
      },
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const creator = await createDb({ ...alice, e2ee: { app, store: store() } });
    const recipient = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(creator, recipient);
    await creator.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    const group = await recipient.e2ee.groups.create().wait();
    const project = await creator
      .insert(app.projects, { title: "Initial group scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, group.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await creator.e2ee.explain(target)).toMatchObject({
      state: "refused",
      reason: "not-a-space-recipient",
    });
    expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
    const root = await creator.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "edge",
    });
    const groupRoot = await creator.one(app.__e2ee_groups.where({ id: group.id }), {
      tier: "edge",
    });
    const grants = await creator.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
      tier: "edge",
    });
    expect(grants).toHaveLength(1);
    expect(grants[0]).toMatchObject({
      id: root!.initialGrantId,
      recipientKind: "group",
      recipientId: group.id,
      recipientEpochId: groupRoot!.epochId,
    });
    const deliveries = await creator.all(
      app.__e2ee_space_deliveries.where({
        spaceId: root!.id,
      }),
      { tier: "edge" },
    );
    expect(deliveries).toHaveLength(1);
    expect(deliveries[0]!.recipientAccountId).toBe(bob.account.id);

    // Emptying the only recipient group seals the space rather than falling
    // back to the account that originally created its secret.
    await recipient.e2ee.groups.leave(group.id).wait();
    expect(await recipient.e2ee.explain(target)).toMatchObject({
      state: "refused",
      reason: "space-sealed",
    });
    expect(await creator.e2ee.explain(target)).toMatchObject({
      state: "refused",
      reason: "space-sealed",
    });
    await expect(
      creator.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait(),
    ).rejects.toThrow();
    expect(
      await creator.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
        tier: "edge",
      }),
    ).toHaveLength(1);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
