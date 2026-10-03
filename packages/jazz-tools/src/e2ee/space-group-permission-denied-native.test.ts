import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { spaceSchema } from "./spaces.js";

it("native Node keeps a space in maintenance when Jazz denies recipient-group rotation", async () => {
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
    // Deliberately no insert permission: holding keys cannot authorise rotation.
    policy.__e2ee_group_deliveries.allowRead.always();
    policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_group_repairs.allowRead.always();
    policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
  });
  const permissions = {
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
  };
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const sessions: Awaited<ReturnType<typeof createJazzSession>>[] = [];
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
      permissions,
    });
    const open = async () => {
      const session = await createJazzSession({
        appId: server.appId,
        serverUrl: server.url,
        app,
        permissions,
        driver: { type: "memory" },
        initial: "local-first",
        store: store(),
        e2ee: { app, store: store() },
      });
      sessions.push(session);
      return session.getSnapshot();
    };
    const alice = await open();
    const bob = await open();
    const creator = alice.client!.db;
    const recipient = bob.client!.db;
    await creator.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    const group = await creator.e2ee.groups.create().wait();
    await creator.e2ee.groups.add(group.id, bob.account!.id).wait();
    const project = await creator
      .insert(app.projects, { title: "Native reconciled scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, alice.account!.id).wait();
    await creator.e2ee.spaces.grant(app.projects, project.id, group.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
    await recipient.e2ee.groups.leave(group.id).wait();
    const before = await creator.all(app.__e2ee_space_deliveries, { tier: "edge" });

    expect(await creator.e2ee.explain(target)).toEqual({
      state: "maintenance-required",
      reason: "group-epoch-stale",
    });
    expect(await creator.all(app.__e2ee_group_successors, { tier: "edge" })).toEqual([]);
    expect(await creator.all(app.__e2ee_space_successors, { tier: "edge" })).toEqual([]);
    expect(await creator.all(app.__e2ee_space_deliveries, { tier: "edge" })).toEqual(before);
    expect(await recipient.e2ee.explain(target)).toEqual({
      state: "refused",
      reason: "not-a-space-recipient",
    });
  } finally {
    await Promise.all(sessions.map((session) => session.close()));
    await server.stop();
  }
}, 120_000);
