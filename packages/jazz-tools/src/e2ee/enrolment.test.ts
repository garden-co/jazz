import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { bytes, vectors } from "./fixtures/vectors.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeKeyEnvelope } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it.each(["mismatched", "oversized"])(
  "does not retain a %s generated device keypair and can retry enrolment",
  async (fault) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let persisted: string | null = null;
    let generatedPrivateKey: Uint8Array | undefined;
    let faulty = true;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: deviceRequestApp,
        permissions: deviceRequestPermissions,
      });
      const adapter = await createNativeKeyEnvelope();
      db = await createDb({
        ...(await localAccountConfig(server.appId, server.url)),
        e2ee: {
          store: {
            async read() {
              return persisted;
            },
            async update(transform: (current: string | null) => string) {
              persisted = transform(persisted);
            },
          },
          crypto: {
            keyEnvelope: {
              ...adapter,
              async createKeyPair() {
                const pair = await adapter.createKeyPair();
                if (faulty && fault === "mismatched") pair.privateKey[10]! ^= 128;
                const privateKey = new Uint8Array(faulty && fault === "oversized" ? 65537 : 32);
                privateKey.set(pair.privateKey);
                pair.privateKey.fill(0);
                generatedPrivateKey = privateKey;
                return { publicKey: pair.publicKey, privateKey };
              },
              async open(device, context, sealed) {
                // BYOC may use padded keys; round-trip validity is not storage-format validity.
                return adapter.open(
                  { ...device, privateKey: device.privateKey.subarray(0, 32) },
                  context,
                  sealed,
                );
              },
            },
          },
        },
      });
      await expect(db.e2ee.devices.list()).rejects.toThrow();
      expect(persisted).toBeNull();
      expect(generatedPrivateKey!.every((byte) => byte === 0)).toBe(true);
      expect(await db.all(deviceRequestApp.__e2ee_device_requests, { tier: "edge" })).toEqual([]);
      faulty = false;
      expect(await db.e2ee.devices.list()).toEqual([expect.objectContaining({ state: "active" })]);
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
);

it.each([1, 2])(
  "validates the literal version-%i device fixture without rewriting it",
  async (version) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: deviceRequestApp,
        permissions: deviceRequestPermissions,
      });
      const config = await localAccountConfig(server.appId, server.url);
      const record = readFileSync(
        new URL(`./fixtures/local-device-v${version}.json`, import.meta.url),
        "utf8",
      )
        .trimEnd()
        .replace("__REGISTRY__", `${server.url}/apps/${server.appId}/accounts`)
        .replace("__ACCOUNT__", config.account.id);
      db = await createDb({
        ...config,
        e2ee: {
          crypto: { keyEnvelope: await createNativeKeyEnvelope() },
          store: {
            async read() {
              return record;
            },
            async update() {
              throw new Error("Existing fixture must not be rewritten");
            },
          },
        },
      });
      if (version === 1) {
        await expect(db.e2ee.devices.list()).rejects.toThrow("Invalid persisted E2EE devices");
        return;
      }
      expect(await db.e2ee.devices.list()).toEqual([
        {
          id: "11111111-1111-4111-8111-111111111111",
          keyReadiness: "verified",
          state: "active",
          publicKey: bytes(vectors.publicKey),
          mechanism: { id: "jazz.sodium.key", version: 1 },
        },
      ]);
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
);

it("publishes no device before durable storage succeeds and refuses to replace corrupted retained keys", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  let persisted: string | null = null;
  let rejectWrites = true;
  const saveError = new Error("Device store unavailable");
  const store = {
    async read() {
      return persisted;
    },
    async update(transform: (current: string | null) => string) {
      if (rejectWrites) throw saveError;
      persisted = transform(persisted);
    },
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const config = {
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: { store, crypto: { keyEnvelope: await createNativeKeyEnvelope() } },
    };
    const first = await createDb(config);
    clients.push(first);
    await expect(first.e2ee.devices.list()).rejects.toBe(saveError);
    expect(persisted).toBeNull();
    rejectWrites = false;
    const devices = await first.e2ee.devices.list();
    expect(devices).toHaveLength(1);

    // The versioned host-store record retains raw device keys, not login credentials.
    const damaged = JSON.parse((await store.read())!);
    damaged.devices[0].privateKey[10] ^= 128;
    persisted = JSON.stringify(damaged);
    const corrupted = persisted;
    const second = await createDb(config);
    clients.push(second);
    await expect(second.e2ee.devices.list()).rejects.toThrow();
    expect(persisted).toBe(corrupted);
    expect(await first.e2ee.devices.list()).toEqual(devices);

    persisted = corrupted.slice(0, -1);
    const third = await createDb(config);
    clients.push(third);
    await expect(third.e2ee.devices.list()).rejects.toThrow();
    expect(persisted).toBe(corrupted.slice(0, -1));
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
});

it("retains one active first device across concurrent contexts and reopening without sharing it with another account", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  let persisted: string | null = null;
  const store = {
    async read() {
      return persisted;
    },
    async update(transform: (current: string | null) => string) {
      persisted = transform(persisted);
    },
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const config = await localAccountConfig(server.appId, server.url);
    const e2ee = { store, crypto: { keyEnvelope: await createNativeKeyEnvelope() } };
    const open = async (accountConfig = config) => {
      const db = await createDb({ ...accountConfig, e2ee });
      clients.push(db);
      return db;
    };
    const [first, second] = await Promise.all([open(), open()]);
    const [firstDevices, secondDevices] = await Promise.all([
      first.e2ee.devices.list(),
      second.e2ee.devices.list(),
    ]);
    expect(firstDevices).toHaveLength(1);
    expect(secondDevices).toEqual(firstDevices);
    expect(firstDevices[0]).toMatchObject({ state: "active" });
    expect(firstDevices[0]).not.toHaveProperty("privateKey");
    const saved = persisted;
    await first.shutdown();
    await second.shutdown();
    const reopened = await open();
    expect(await reopened.e2ee.devices.list()).toEqual(firstDevices);
    expect(persisted).toBe(saved);
    const bob = await open(await localAccountConfig(server.appId, server.url));
    const bobDevices = await bob.e2ee.devices.list();
    expect(bobDevices).toHaveLength(1);
    expect(bobDevices[0]!.id).not.toBe(firstDevices[0]!.id);
    expect(bobDevices[0]!.publicKey).not.toEqual(firstDevices[0]!.publicKey);
    await expect(first.e2ee.devices.list()).rejects.toThrow(/closed|shutting down/);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
});
