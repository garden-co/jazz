import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { groupSchema } from "../../src/e2ee/groups.js";
import { spaceSchema } from "../../src/e2ee/spaces.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("browser keeps a space in maintenance when Jazz denies recipient-group rotation", async () => {
  const server = await getJazzServerInfo(`e2ee-denied-reconciliation-${crypto.randomUUID()}`);
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
    // Holding keys must not bypass the missing group-successor insert policy.
    policy.__e2ee_group_deliveries.allowRead.always();
    policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_group_repairs.allowRead.always();
    policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
  });
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
      ...server,
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
    const alice = await acquireBrowserTestAccount({ ...server, key: "creator" });
    const bob = await acquireBrowserTestAccount({ ...server, key: "recipient" });
    expect(alice.id).not.toBe(bob.id);
    for (const account of [alice, bob]) {
      clients.push(
        await createDb({
          appId: server.appId,
          serverUrl: server.serverUrl,
          account,
          driver: { type: "memory" },
          e2ee: { app, store: store() },
        }),
      );
    }
    const [creator, recipient] = clients;
    await creator!.e2ee.devices.list();
    await recipient!.e2ee.devices.list();
    const group = await creator!.e2ee.groups.create().wait();
    await creator!.e2ee.groups.add(group.id, bob.id).wait();
    const project = await creator!
      .insert(app.projects, { title: "Browser reconciled scope" })
      .wait({ tier: "global" });
    await creator!.e2ee.spaces.grant(app.projects, project.id, alice.id).wait();
    await creator!.e2ee.spaces.grant(app.projects, project.id, group.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await recipient!.e2ee.explain(target)).toEqual({ state: "ready" });
    await recipient!.e2ee.groups.leave(group.id).wait();
    const before = await creator!.all(app.__e2ee_space_deliveries, { tier: "edge" });

    expect(await creator!.e2ee.explain(target)).toEqual({
      state: "maintenance-required",
      reason: "group-epoch-stale",
    });
    expect(await creator!.all(app.__e2ee_group_successors, { tier: "edge" })).toEqual([]);
    expect(await creator!.all(app.__e2ee_space_successors, { tier: "edge" })).toEqual([]);
    expect(await creator!.all(app.__e2ee_space_deliveries, { tier: "edge" })).toEqual(before);
    expect(await recipient!.e2ee.explain(target)).toEqual({
      state: "refused",
      reason: "not-a-space-recipient",
    });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await stopJazzServer(server.serverUrl);
  }
}, 60_000);
