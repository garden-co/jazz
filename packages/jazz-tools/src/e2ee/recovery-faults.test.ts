import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";

it.each(["private-signature", "device-envelope"])(
  "rejects a faulty recovery %s before publishing it and permits retry",
  async (fault) => {
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
      let injected = 0;
      const open = async () => {
        let saved: string | null = null;
        const client = await createDb({
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
                  if (
                    corrupt &&
                    fault === "private-signature" &&
                    new TextDecoder().decode(record).includes("approval-signature:")
                  ) {
                    injected++;
                    signature[0] ^= 1;
                  }
                  return signature;
                },
              },
              keyEnvelope: {
                ...adapters.keyEnvelope,
                async seal(key, context, value) {
                  if (
                    corrupt &&
                    fault === "device-envelope" &&
                    new TextDecoder().decode(context).includes("delivery")
                  ) {
                    injected++;
                    return new Uint8Array([1]);
                  }
                  return adapters.keyEnvelope.seal(key, context, value);
                },
              },
            },
          },
        });
        clients.push(client);
        return client;
      };
      const first = await open();
      const [creator] = await first.e2ee.devices.list();
      const { material } = await first.e2ee.recovery.create().wait();
      await first.shutdown();
      const second = await open();
      const pending = (await second.e2ee.devices.list()).find((row) => row.id !== creator!.id)!;
      corrupt = true;
      await expect(second.e2ee.recovery.use(material).wait()).rejects.toThrow();
      expect(injected).toBeGreaterThan(0);
      const table =
        fault === "private-signature"
          ? deviceRequestApp.__e2ee_device_approvals
          : deviceRequestApp.__e2ee_device_deliveries;
      expect(await second.all<{ id: string }>(table, { tier: "edge" })).toEqual([]);
      corrupt = false;
      await second.e2ee.recovery.use(material).wait();
      expect(await second.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active" }),
      );
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);
