import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { groupSchema } from "../../src/e2ee/groups.js";
import { createBrowserKeyEnvelope } from "../../src/e2ee/browser.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it.each(["ordinary", "repair"])(
  "delivers an accepted group key to approved browser devices and survives reopening (%s)",
  async (scenario) => {
    const server = await getJazzServerInfo(`e2ee-groups-${crypto.randomUUID()}`);
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session, allOf }) => {
      policy.__e2ee_group_repairs.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_successors.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      const groups = policy.__e2ee_groups;
      groups.allowRead.where(session.where({ authMode: { in: ["local-first", "external"] } }));
      groups.allowInsert.where({ accountId: session.user.account });
      const deliveries = policy.__e2ee_group_deliveries;
      deliveries.allowRead.where(session.where({ authMode: { in: ["local-first", "external"] } }));
      deliveries.allowInsert.where((row) =>
        allOf([
          { senderAccountId: session.user.account },
          groups.exists.where({ id: row.groupId, accountId: session.user.account }),
        ]),
      );
    });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const keys = [crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID()];
    try {
      await deploy({
        ...server,
        schema: app,
        permissions: {
          ...deviceRequestPermissions,
          __e2ee_groups: policies.__e2ee_groups!,
          __e2ee_group_repairs: policies.__e2ee_group_repairs!,
          __e2ee_group_successors: policies.__e2ee_group_successors!,
          __e2ee_group_deliveries: policies.__e2ee_group_deliveries!,
        },
      });
      const account = await acquireBrowserTestAccount(server);
      const envelope = await createBrowserKeyEnvelope();
      let corruptRecipient: Uint8Array | undefined;
      const open = async (key: string) => {
        const db = await createDb({
          appId: server.appId,
          serverUrl: server.serverUrl,
          account,
          driver: { type: "memory" },
          e2ee: {
            app,
            crypto: {
              keyEnvelope: {
                ...envelope,
                async seal(publicKey, context, secret) {
                  if (
                    corruptRecipient &&
                    publicKey.length === corruptRecipient.length &&
                    publicKey.every((byte, index) => byte === corruptRecipient![index])
                  ) {
                    corruptRecipient = undefined;
                    return new Uint8Array([1]);
                  }
                  return envelope.seal(publicKey, context, secret);
                },
              },
            },
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
      const first = await open(keys[0]!);
      const creator = (await first.e2ee.devices.list())[0]!;
      expect(creator.state).toBe("active");
      const second = await open(keys[1]!);
      const request = (await second.e2ee.devices.list()).find(
        (device) => device.state === "pending",
      )!;
      expect(request).toBeDefined();
      await first.e2ee.devices.approve(request.id).wait();
      const pending = await open(keys[2]!);
      const pendingDevice = (await pending.e2ee.devices.list()).find(
        (device) => device.id !== creator.id && device.id !== request.id,
      )!;
      expect(pendingDevice).toMatchObject({ state: "pending" });

      if (scenario === "repair") corruptRecipient = request.publicKey;
      const group = first.e2ee.groups.create();
      expect(typeof group.id).toBe("string");
      expect(await group.wait()).toEqual({ id: group.id });
      expect(await first.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
      if (scenario === "repair") {
        expect(corruptRecipient).toBeUndefined();
        await expect(second.e2ee.explain({ groupId: group.id })).rejects.toThrow();
        expect(
          await second.all(app.__e2ee_group_repairs.where({ groupId: group.id }), { tier: "edge" }),
        ).toHaveLength(1);
        expect(await first.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
      }
      expect(await second.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
      expect(await pending.e2ee.explain({ groupId: group.id })).toMatchObject({ state: "refused" });
      expect(
        await first.all(
          app.__e2ee_group_deliveries.where({ recipientDeviceId: pendingDevice.id }),
          {
            tier: "edge",
          },
        ),
      ).toEqual([]);

      await second.shutdown();
      const reopened = await open(keys[1]!);
      expect(await reopened.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      for (const key of keys) localStorage.removeItem(key);
      await stopJazzServer(server.serverUrl);
    }
  },
  60000,
);
