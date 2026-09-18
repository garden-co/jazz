import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { spaceSchema } from "../../src/e2ee/spaces.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("inspects, recovers and reopens a space without a live browser key holder", async () => {
  const server = await getJazzServerInfo(`e2ee-space-recovery-${crypto.randomUUID()}`);
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
  });
  const policies = definePermissions(app, ({ policy, session, allOf }) => {
    const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
    policy.projects.allowRead.where(authenticated);
    policy.projects.allowInsert.where(authenticated);
    policy.__e2ee_spaces.allowRead.where(authenticated);
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.where(authenticated);
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.where(authenticated);
    // Only the original device may publish normal deliveries. Recovery must survive
    // reopening from the private store even when the replacement cannot publish one.
    policy.__e2ee_space_deliveries.allowInsert.where((row) =>
      allOf([
        { senderAccountId: session.user.account },
        policy.__e2ee_spaces.exists.where({ id: row.spaceId, deviceId: row.senderDeviceId }),
      ]),
    );
    policy.__e2ee_space_successors.allowRead.where(authenticated);
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
    policy.__e2ee_space_recovery_deliveries.allowInsert.where({
      senderAccountId: session.user.account,
    });
  });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const keys = [crypto.randomUUID(), crypto.randomUUID()];
  try {
    await deploy({
      ...server,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const account = await acquireBrowserTestAccount(server);
    const open = async (key: string) => {
      const client = await createDb({
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
      clients.push(client);
      return client;
    };
    const first = await open(keys[0]!);
    await first.e2ee.devices.list();
    const project = await first
      .insert(app.projects, { title: "Recovery" })
      .wait({ tier: "global" });
    await first.e2ee.spaces.grant(app.projects, project.id, account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await first.e2ee.explain(target)).toEqual({ state: "ready" });
    const { material } = await first.e2ee.recovery.create().wait();
    const requests = await first.all(app.__e2ee_device_requests, { tier: "edge" });
    const deliveries = await first.all(app.__e2ee_space_recovery_deliveries, { tier: "edge" });
    const [root] = await first.all(app.__e2ee_spaces, { tier: "edge" });
    expect(root).toBeDefined();
    await first.shutdown();
    localStorage.removeItem(keys[0]!);

    const replacement = await open(keys[1]!);
    const status = await replacement.e2ee.recovery.status(material);
    expect(status.spaces).toEqual({
      validation: "checked",
      paths: [
        {
          scopeId: root!.scopeId,
          identifier: project.id,
          spaceId: root!.id,
          epochId: root!.epochId,
          validation: "validated",
        },
      ],
    });
    expect(localStorage.getItem(keys[1]!)).toBeNull();
    expect(await replacement.all(app.__e2ee_device_requests, { tier: "edge" })).toEqual(requests);
    expect(await replacement.all(app.__e2ee_space_recovery_deliveries, { tier: "edge" })).toEqual(
      deliveries,
    );
    const pending = (await replacement.e2ee.devices.list()).find((row) => row.state === "pending");
    expect(pending).toBeDefined();
    expect(await replacement.e2ee.explain(target)).toMatchObject({ state: "refused" });
    await replacement.e2ee.recovery.use(material).wait();
    expect(await replacement.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending!.id, state: "active" }),
    );
    expect(await replacement.e2ee.explain(target)).toEqual({ state: "ready" });
    const normalDeliveries = () =>
      app.__e2ee_space_deliveries.where({ recipientDeviceId: pending!.id });
    expect(await replacement.all(normalDeliveries(), { tier: "edge" })).toEqual([]);
    await replacement.shutdown();
    const reopened = await open(keys[1]!);
    expect(await reopened.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await reopened.all(normalDeliveries(), { tier: "edge" })).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    for (const key of keys) localStorage.removeItem(key);
    await stopJazzServer(server.serverUrl);
  }
}, 90_000);
