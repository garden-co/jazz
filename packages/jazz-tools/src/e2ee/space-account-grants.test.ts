import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";

it("shares a space only after an authorised account grant, excluding pending devices", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const alice = await localAccountConfig(server.appId, server.url);
  const bob = await localAccountConfig(server.appId, server.url);
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
  });
  const policies = definePermissions(app, ({ policy, session, allOf }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where(() =>
      allOf([{ authorAccountId: session.user.account }, { authorAccountId: alice.account.id }]),
    );
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
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
      },
    });
    expect(alice.account.id).not.toBe(bob.account.id);
    const creator = await createDb({ ...alice, e2ee: { app, store: store() } });
    const recipient = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(creator, recipient);
    await creator.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    const project = await creator
      .insert(app.projects, { title: "Shared scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await recipient.e2ee.explain(target)).toMatchObject({ state: "refused" });
    await expect(
      recipient.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait(),
    ).rejects.toThrow();
    expect(await recipient.e2ee.explain(target)).toMatchObject({ state: "refused" });
    const unapproved = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(unapproved);
    const pending = (await unapproved.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    );
    expect(pending).toBeDefined();
    const grant = creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id);
    expect(grant).not.toBeInstanceOf(Promise);
    await grant.wait();
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await unapproved.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(
      await creator.all(app.__e2ee_space_deliveries.where({ recipientDeviceId: pending!.id }), {
        tier: "edge",
      }),
    ).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
