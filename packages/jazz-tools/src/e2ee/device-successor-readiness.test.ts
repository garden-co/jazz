import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("keeps accepted device membership visible when rotated key delivery fails authentication", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  let corruptSuccessorDelivery = false;
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
    const open = async (faultyDelivery = false) => {
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
              async open(device, context, envelope) {
                const secret = await adapters.keyEnvelope.open(device, context, envelope);
                if (
                  faultyDelivery &&
                  corruptSuccessorDelivery &&
                  new TextDecoder().decode(context).includes("jazz.e2ee.account-successor.v1")
                ) {
                  secret[0] = secret[0]! ^ 1;
                }
                return secret;
              },
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    const first = await open(true);
    const [creator] = await first.e2ee.devices.list();
    const second = await open();
    const removed = (await second.e2ee.devices.list()).find((device) => device.id !== creator!.id)!;
    await first.e2ee.devices.approve(removed.id).wait();
    await first.e2ee.devices.revoke(removed.id).wait();
    corruptSuccessorDelivery = true;
    expect(await first.e2ee.devices.list()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: creator!.id, state: "active", keyReadiness: "not-verified" }),
        expect.objectContaining({ id: removed.id, state: "revoked", keyReadiness: "not-verified" }),
      ]),
    );
    const third = await open();
    const pending = (await third.e2ee.devices.list()).find((device) => device.state === "pending")!;
    await expect(first.e2ee.devices.approve(pending.id).wait()).rejects.toThrow();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "pending" }),
    );
    corruptSuccessorDelivery = false;
    expect(await first.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "active", keyReadiness: "verified" }),
    );
    await first.e2ee.devices.approve(pending.id).wait();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
    );
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);
