import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

function memoryStore() {
  let saved: string | null = null;
  return {
    store: {
      async read() {
        return saved;
      },
      async update(transform: (current: string | null) => string) {
        saved = transform(saved);
      },
    },
  };
}

it("retires one recovery root with an epoch rotation and preserves other authority", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const crypto = await createNativeCrypto();
    const open = async () => {
      const retained = memoryStore();
      const client = await createDb({
        ...account,
        e2ee: { store: retained.store, crypto },
      });
      clients.push(client);
      return client;
    };

    const owner = await open();
    const [creator] = await owner.e2ee.devices.list();
    const compromised = await owner.e2ee.recovery.create().wait();
    const retained = await owner.e2ee.recovery.create().wait();
    const compromisedRootId = JSON.parse(compromised.material).rootId as string;
    const retainedRootId = JSON.parse(retained.material).rootId as string;

    const second = await open();
    const secondDevice = (await second.e2ee.devices.list()).find(
      (device) => device.id !== creator!.id,
    )!;
    await owner.e2ee.devices.approve(secondDevice.id).wait();
    const before = await owner.e2ee.recovery.status(compromised.material);

    await owner.e2ee.recovery.revoke(compromisedRootId).wait();

    const after = await owner.e2ee.recovery.status(retained.material);
    expect(after.account.epochId).not.toBe(before.account.epochId);
    expect(after.account.activeDeviceIds).toEqual(before.account.activeDeviceIds);
    expect(after.account.recoveryRootIds).toEqual([retainedRootId]);
    expect(after.account.validatedRootId).toBe(retainedRootId);

    const deliveries = await owner.all(deviceRequestApp.__e2ee_recovery_deliveries, {
      tier: "edge",
    });
    expect(
      deliveries.some(
        (delivery) =>
          delivery.rootId === compromisedRootId && delivery.epochId === after.account.epochId,
      ),
    ).toBe(false);
    expect(
      deliveries.some(
        (delivery) =>
          delivery.rootId === retainedRootId && delivery.epochId === after.account.epochId,
      ),
    ).toBe(true);
    await expect(owner.e2ee.recovery.status(compromised.material)).rejects.toMatchObject({
      code: "recovery-root-mismatch",
    });

    const recovering = await open();
    const pending = (await recovering.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await expect(recovering.e2ee.recovery.use(compromised.material).wait()).rejects.toMatchObject({
      code: "recovery-root-mismatch",
    });
    expect(await recovering.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "pending" }),
    );
    await recovering.e2ee.recovery.use(retained.material).wait();
    expect(await recovering.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
