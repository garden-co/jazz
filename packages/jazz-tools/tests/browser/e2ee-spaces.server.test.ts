import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { spaceSchema } from "../../src/e2ee/spaces.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("shares a space with another browser account and retains access after reopening", async () => {
  const server = await getJazzServerInfo(`e2ee-spaces-${crypto.randomUUID()}`);
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
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const keys = [crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID()];
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
      },
    });
    const alice = await acquireBrowserTestAccount({ ...server, key: "alice" });
    const bob = await acquireBrowserTestAccount({ ...server, key: "bob" });
    expect(alice.id).not.toBe(bob.id);
    const open = async (account: typeof alice, key: string) => {
      const db = await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        account,
        driver: { type: "memory" },
        e2ee: {
          app,
          store: {
            async read() {
              return localStorage.getItem(key);
            },
            async update(transform) {
              await navigator.locks.request(key, () =>
                localStorage.setItem(key, transform(localStorage.getItem(key))),
              );
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    const creator = await open(alice, keys[0]!);
    const recipient = await open(bob, keys[1]!);
    await creator.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    const project = await creator
      .insert(app.projects, { title: "Browser scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, alice.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await recipient.e2ee.explain(target)).toMatchObject({ state: "refused" });
    const pending = await open(bob, keys[2]!);
    const request = (await pending.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    );
    expect(request).toBeDefined();
    const grant = creator.e2ee.spaces.grant(app.projects, project.id, bob.id);
    expect(grant).not.toBeInstanceOf(Promise);
    await grant.wait();
    expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await pending.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(
      await creator.all(app.__e2ee_space_deliveries.where({ recipientDeviceId: request!.id }), {
        tier: "edge",
      }),
    ).toEqual([]);
    await recipient.shutdown();
    const reopened = await open(bob, keys[1]!);
    expect(await reopened.e2ee.explain(target)).toEqual({ state: "ready" });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    for (const key of keys) localStorage.removeItem(key);
    await stopJazzServer(server.serverUrl);
  }
}, 60_000);
