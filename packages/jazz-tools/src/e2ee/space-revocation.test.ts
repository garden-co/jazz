import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";

it("revokes a space recipient and delivers the accepted successor only to remaining devices", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
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
      },
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const creator = await createDb({ ...alice, e2ee: { app, store: store() } });
    const removed = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(creator, removed);
    const [first] = await creator.e2ee.devices.list();
    const [bobDevice] = await removed.e2ee.devices.list();
    const remaining = await createDb({ ...alice, e2ee: { app, store: store() } });
    clients.push(remaining);
    const request = (await remaining.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await creator.e2ee.devices.approve(request.id).wait();
    const project = await creator
      .insert(app.projects, { title: "Revocable scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait();
    await creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await removed.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await remaining.e2ee.explain(target)).toEqual({ state: "ready" });
    const root = await creator.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "edge",
    });
    const revoke = creator.e2ee.spaces.revoke(app.projects, project.id, bob.account.id);
    expect(revoke).not.toBeInstanceOf(Promise);
    await revoke.wait();
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await remaining.e2ee.explain(target)).toEqual({ state: "ready" });
    const successors = await creator.all(app.__e2ee_space_successors.where({ spaceId: root!.id }), {
      tier: "edge",
    });
    expect(successors).toHaveLength(1);
    expect(successors[0]!.predecessor).toBe(root!.epochId);
    expect(successors[0]!.epochId).not.toBe(root!.epochId);
    const deliveries = await creator.all(
      app.__e2ee_space_deliveries.where({ spaceId: root!.id, epochId: successors[0]!.epochId }),
      { tier: "edge" },
    );
    expect(deliveries.map((row) => row.recipientDeviceId).sort()).toEqual(
      [first!.id, request.id].sort(),
    );
    expect(deliveries.some((row) => row.recipientDeviceId === bobDevice!.id)).toBe(false);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 120_000);
