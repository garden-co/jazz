import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeKeyEnvelope } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("rejects a correctly sealed but substituted account epoch key", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let record: string | null = null;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const keys = await createNativeKeyEnvelope();
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: {
        store: {
          async read() {
            return record;
          },
          async update(transform) {
            record = transform(record);
          },
        },
        crypto: {
          keyEnvelope: {
            ...keys,
            async seal(publicKey, context, key) {
              // Fault injection at the BYOC seam: valid crypto delivers the wrong
              // secret. This is not a test of the adapter's cryptographic strength.
              const substitute = new TextDecoder()
                .decode(context)
                .includes("__e2ee_account_identities");
              return keys.seal(publicKey, context, substitute ? new Uint8Array(32).fill(7) : key);
            },
          },
        },
      },
    });
    await expect(db.e2ee.devices.list()).rejects.toThrow();
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 15_000);

it("rejects an initial identity referring to another account's device request", async () => {
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
    const alice = await createDb(await localAccountConfig(server.appId, server.url));
    const bobConfig = await localAccountConfig(server.appId, server.url);
    const bob = await createDb(bobConfig);
    clients.push(alice, bob);
    const request = await alice
      .insert(deviceRequestApp.__e2ee_device_requests, {
        publicKey: new Uint8Array(32).fill(1),
        signingPublicKey: new Uint8Array(32).fill(1),
        signingMechanism: "jazz.sodium.sign",
        signingVersion: 1,
        mechanism: "jazz.sodium.key",
        version: 1,
        challenge: new Uint8Array(32).fill(2),
      })
      .wait({ tier: "global" });
    await expect(
      bob
        .insert(
          deviceRequestApp.__e2ee_account_identities,
          {
            deviceId: request.id,
            epochId: crypto.randomUUID(),
            envelope: new Uint8Array([1]),
            verification: new Uint8Array([2]),
          },
          { id: bobConfig.account.id },
        )
        .wait({ tier: "global" }),
    ).rejects.toThrow(/authori|permission/i);
    await expect(
      bob.all(deviceRequestApp.__e2ee_account_identities, { tier: "edge" }),
    ).resolves.toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 15_000);

it("accepts only one first device and never reinitialises an existing account for a new device", async () => {
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
    const config = await localAccountConfig(server.appId, server.url);
    const keyEnvelope = await createNativeKeyEnvelope();
    const openNewDevice = async () => {
      let record: string | null = null;
      const db = await createDb({
        ...config,
        e2ee: {
          crypto: { keyEnvelope },
          store: {
            async read() {
              return record;
            },
            async update(transform) {
              record = transform(record);
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    const [first, second] = await Promise.all([openNewDevice(), openNewDevice()]);
    await Promise.all([first.e2ee.devices.list(), second.e2ee.devices.list()]);
    const devices = await first.e2ee.devices.list();
    expect(devices).toHaveLength(2);
    expect(devices.filter((device) => device.state === "active")).toHaveLength(1);
    expect(devices.filter((device) => device.state === "pending")).toHaveLength(1);
    const activeId = devices.find((device) => device.state === "active")!.id;
    const identities = deviceRequestApp.__e2ee_account_identities;
    const original = await first.one(identities.where({ id: config.account.id }), { tier: "edge" });
    expect(original).not.toBeNull();
    for (const client of [first, second]) {
      const replacement = {
        deviceId: activeId,
        epochId: crypto.randomUUID(),
        envelope: new Uint8Array([1]),
        verification: new Uint8Array([2]),
      };
      for (const replace of [
        () => client.insert(identities, replacement, { id: config.account.id }),
        () => client.upsert(identities, config.account.id, replacement),
        () => client.restore(identities, config.account.id, replacement),
        () => client.update(identities, config.account.id, replacement),
        () => client.delete(identities, config.account.id),
      ]) {
        await expect(async () => replace().wait({ tier: "global" })).rejects.toThrow(
          /already exists|authori|policy denied|not deleted|not_deleted|conflict/i,
        );
      }
    }
    await expect(
      first.one(identities.where({ id: config.account.id }), { tier: "edge" }),
    ).resolves.toEqual(original);
    await first.shutdown();
    await second.shutdown();
    const third = await openNewDevice();
    const afterLoss = await third.e2ee.devices.list();
    expect(afterLoss).toHaveLength(3);
    expect(
      afterLoss.filter((device) => device.state === "active").map((device) => device.id),
    ).toEqual([activeId]);
    expect(afterLoss.filter((device) => device.state === "pending")).toHaveLength(2);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 15_000);
