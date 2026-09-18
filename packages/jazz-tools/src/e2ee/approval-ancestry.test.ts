import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("rejects descendants when an intermediate delivery cannot be authenticated", async () => {
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
    let rejected: Uint8Array | undefined;
    const open = async () => {
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
            keyEnvelope: {
              ...adapters.keyEnvelope,
              async unwrap(key, context, value) {
                if (
                  rejected &&
                  value.length === rejected.length &&
                  value.every((byte, i) => byte === rejected![i])
                )
                  throw new Error("Unauthenticated intermediate delivery");
                return adapters.keyEnvelope.unwrap(key, context, value);
              },
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    const first = await open();
    const [creator] = await first.e2ee.devices.list();
    const second = await open();
    const secondId = (await second.e2ee.devices.list()).find((d) => d.id !== creator!.id)!.id;
    await first.e2ee.devices.approve(secondId).wait();
    const [intermediate] = await first.all(deviceRequestApp.__e2ee_device_deliveries, {
      tier: "edge",
    });
    const third = await open();
    const thirdId = (await third.e2ee.devices.list()).find(
      (d) => d.id !== creator!.id && d.id !== secondId,
    )!.id;
    await second.e2ee.devices.approve(thirdId).wait();
    expect(await first.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: thirdId, state: "active", keyReadiness: "verified" }),
    );
    rejected = intermediate!.verification;
    const devices = await first.e2ee.devices.list();
    expect(devices).toContainEqual(
      expect.objectContaining({ id: secondId, state: "active", keyReadiness: "not-verified" }),
    );
    expect(devices).toContainEqual(
      expect.objectContaining({ id: thirdId, state: "active", keyReadiness: "not-verified" }),
    );
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: thirdId, state: "active", keyReadiness: "not-verified" }),
    );
    await expect(third.e2ee.devices.approve(creator!.id).wait()).rejects.toThrow(/active|key/i);
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 30_000);
