import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";

it("initialises a scoped space explicitly and delivers its key to approved devices", async () => {
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
      },
    });
    const account = await localAccountConfig(server.appId, server.url);
    const creator = await createDb({ ...account, e2ee: { app, store: store() } });
    clients.push(creator);
    await creator.e2ee.devices.list();
    const project = await creator
      .insert(app.projects, { title: "Scoped data" })
      .wait({ tier: "global" });
    const grant = creator.e2ee.spaces.grant(app.projects, project.id, account.account.id);
    expect(grant).not.toBeInstanceOf(Promise);
    await grant.wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });

    const second = await createDb({ ...account, e2ee: { app, store: store() } });
    clients.push(second);
    const pending = (await second.e2ee.devices.list()).find((device) => device.state === "pending");
    expect(pending).toBeDefined();
    expect(await second.e2ee.explain(target)).toMatchObject({ state: "refused" });
    await creator.e2ee.devices.approve(pending!.id).wait();
    // Loading by an authorised holder performs any required key delivery.
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await second.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await creator.all(app.projects, { tier: "edge" })).toMatchObject([
      { id: project.id, title: "Scoped data" },
    ]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
