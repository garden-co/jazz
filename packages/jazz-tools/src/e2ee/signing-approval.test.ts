import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it.each(["recipient", "approver"])(
  "requires the %s's enrolled signing key before delivering the account key",
  async (role) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: deviceRequestApp,
        permissions: deviceRequestPermissions,
      });
      const account = await localAccountConfig(server.appId, server.url);
      const adapters = await createNativeCrypto();
      let corrupt = false;
      const open = async (pending: boolean) => {
        let saved: string | null = null;
        const db = await createDb({
          ...account,
          e2ee: {
            store: {
              async read() {
                return saved;
              },
              async update(transform) {
                saved = transform(saved);
              },
            },
            crypto: {
              ...adapters,
              deviceSigner: {
                ...adapters.deviceSigner,
                async sign(key, record) {
                  const signature = await adapters.deviceSigner.sign(key, record);
                  if ((role === "recipient" ? pending : !pending) && corrupt)
                    signature[signature.length - 1] ^= 1;
                  return signature;
                },
              },
            },
          },
        });
        clients.push(db);
        return db;
      };
      const first = await open(false);
      const [creator] = await first.e2ee.devices.list();
      const second = await open(true);
      const pending = (await second.e2ee.devices.list()).find(
        (device) => device.id !== creator!.id,
      )!;
      corrupt = true;
      await expect(first.e2ee.devices.approve(pending.id).wait()).rejects.toThrow(/sign|proof/i);
      expect(await first.all(deviceRequestApp.__e2ee_device_deliveries, { tier: "edge" })).toEqual(
        [],
      );
      corrupt = false;
      await first.e2ee.devices.approve(pending.id).wait();
      expect(await second.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active" }),
      );
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  30_000,
);
