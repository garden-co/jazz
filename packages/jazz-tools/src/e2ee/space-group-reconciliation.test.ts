import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { spaceSchema } from "./spaces.js";

it("reconciles a stale recipient group when a remaining member loads the space", async () => {
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
    const group = await creator.e2ee.groups.create().wait();
    await creator.e2ee.groups.add(group.id, bob.account.id).wait();
    const project = await creator
      .insert(app.projects, { title: "Reconciled scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait();
    await creator.e2ee.spaces.grant(app.projects, project.id, group.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
    const root = await creator.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "edge",
    });

    // The departing account can record its departure but cannot receive or
    // author replacement keys. No remaining member has repaired the group yet.
    await recipient.e2ee.groups.leave(group.id).wait();
    expect(await recipient.e2ee.explain(target)).toMatchObject({
      state: "refused",
      reason: "not-a-space-recipient",
    });
    expect(
      await creator.all(app.__e2ee_group_successors.where({ groupId: group.id }), {
        tier: "edge",
      }),
    ).toEqual([]);
    expect(
      await creator.all(app.__e2ee_space_successors.where({ spaceId: root!.id }), {
        tier: "edge",
      }),
    ).toEqual([]);

    // No explicit explain({ groupId }) call: loading the space reconciles its
    // stale recipient group before producing the next space epoch.
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    const groups = await creator.all(app.__e2ee_group_successors.where({ groupId: group.id }), {
      tier: "edge",
    });
    const spaces = await creator.all(app.__e2ee_space_successors.where({ spaceId: root!.id }), {
      tier: "edge",
    });
    expect(groups).toHaveLength(1);
    expect(spaces).toHaveLength(1);
    expect(spaces[0]!.predecessor).toBe(root!.epochId);
    const deliveries = await creator.all(
      app.__e2ee_space_deliveries.where({ spaceId: root!.id, epochId: spaces[0]!.epochId }),
      { tier: "edge" },
    );
    expect(deliveries).toHaveLength(1);
    expect(deliveries[0]!.recipientAccountId).toBe(alice.account.id);
    expect(await recipient.e2ee.explain(target)).toMatchObject({ state: "refused" });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 120_000);
