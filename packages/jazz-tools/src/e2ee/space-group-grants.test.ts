import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { spaceSchema } from "./spaces.js";

it("shares a space through a parent group and follows later child-group additions", async () => {
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
    const parent = await creator.e2ee.groups.create().wait();
    const child = await creator.e2ee.groups.create().wait();
    await creator.e2ee.groups.add(parent.id, child.id).wait();
    const project = await creator
      .insert(app.projects, { title: "Group scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    const grant = creator.e2ee.spaces.grant(app.projects, project.id, parent.id);
    expect(grant).not.toBeInstanceOf(Promise);
    await grant.wait();
    expect(await recipient.e2ee.explain(target)).toMatchObject({ state: "refused" });
    const pending = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(pending);
    const request = (await pending.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    );
    expect(request).toBeDefined();
    await creator.e2ee.groups.add(child.id, bob.account.id).wait();
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await pending.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(
      await creator.all(app.__e2ee_space_deliveries.where({ recipientDeviceId: request!.id }), {
        tier: "edge",
      }),
    ).toEqual([]);
    // An explicit account grant adds an independent path, even for an existing group member.
    await creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    expect(
      await creator.all(
        app.__e2ee_space_grants.where({ recipientKind: "account", recipientId: bob.account.id }),
        { tier: "edge" },
      ),
    ).toHaveLength(1);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
