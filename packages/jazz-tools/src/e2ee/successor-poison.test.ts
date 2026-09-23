import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("does not let an unapproved device permanently occupy the successor slot", async () => {
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
    const open = async () => {
      let saved: string | null = null;
      const db = await createDb({
        ...account,
        e2ee: {
          crypto: adapters,
          store: {
            async read() {
              return saved;
            },
            async update(transform) {
              saved = transform(saved);
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    const first = await open();
    const [creator] = await first.e2ee.devices.list();
    const pending = await open();
    const pendingId = (await pending.e2ee.devices.list()).find((d) => d.id !== creator!.id)!.id;
    const identity = await pending.one(
      deviceRequestApp.__e2ee_account_identities.where({ id: account.account.id }),
      { tier: "edge" },
    );
    await pending
      .insert(
        deviceRequestApp.__e2ee_account_successors,
        {
          accountId: account.account.id,
          predecessor: identity!.epochId,
          epochId: crypto.randomUUID(),
          signerId: pendingId,
          removedDeviceId: creator!.id,
          membership: new Uint8Array([1]),
          revision: new Uint8Array([1]),
          verification: new Uint8Array([1]),
          history: new Uint8Array([1]),
          deliveries: new Uint8Array([1]),
          signature: new Uint8Array([1]),
        },
        { id: identity!.epochId },
      )
      .wait({ tier: "global" });
    await expect(first.e2ee.devices.list()).resolves.toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "active" }),
    );
    await first.e2ee.devices.approve(pendingId).wait();
    await first.e2ee.devices.revoke(pendingId).wait();
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 30_000);
