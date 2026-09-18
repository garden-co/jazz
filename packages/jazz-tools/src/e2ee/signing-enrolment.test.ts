import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("checks signing keys before storage and binds the retained signing identity to its device request", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  let persisted: string | null = null;
  let generated: Uint8Array | undefined;
  let faulty = true;
  const store = {
    async read() {
      return persisted;
    },
    async update(transform: (value: string | null) => string) {
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
    const crypto = await createNativeCrypto();
    const config = {
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: {
        store,
        crypto: {
          ...crypto,
          deviceSigner: {
            ...crypto.deviceSigner,
            async createKeyPair() {
              const pair = await crypto.deviceSigner.createKeyPair();
              generated = pair.privateKey;
              if (faulty) pair.privateKey[63] ^= 1;
              return pair;
            },
          },
        },
      },
    };
    const first = await createDb(config);
    clients.push(first);
    await expect(first.e2ee.devices.list()).rejects.toThrow();
    expect(persisted).toBeNull();
    expect(generated?.every((byte) => byte === 0)).toBe(true);
    expect(await first.all(deviceRequestApp.__e2ee_device_requests, { tier: "edge" })).toEqual([]);
    faulty = false;
    const devices = await first.e2ee.devices.list();
    expect(devices).toEqual([expect.objectContaining({ state: "active" })]);
    const saved = (await store.read())!;
    const record = JSON.parse(saved);
    expect(record.format).toBe("jazz-e2ee-local-devices-v2");
    const request = await first.one(
      deviceRequestApp.__e2ee_device_requests.where({ id: devices[0]!.id }),
    );
    expect(request).toEqual(
      expect.objectContaining({
        signingPublicKey: Uint8Array.from(record.devices[0].signingPublicKey),
        signingMechanism: crypto.deviceSigner.mechanism.id,
        signingVersion: crypto.deviceSigner.mechanism.version,
      }),
    );
    await first.shutdown();
    const reopened = await createDb(config);
    clients.push(reopened);
    expect(await reopened.e2ee.devices.list()).toEqual(devices);
    expect(persisted).toBe(saved);
    await reopened.shutdown();
    // A valid replacement pair is still a different signing identity, not a repair.
    const replacement = await crypto.deviceSigner.createKeyPair();
    record.devices[0].signingPublicKey = Array.from(replacement.publicKey);
    record.devices[0].signingPrivateKey = Array.from(replacement.privateKey);
    persisted = JSON.stringify(record);
    const damaged = persisted;
    const changed = await createDb(config);
    clients.push(changed);
    await expect(changed.e2ee.devices.list()).rejects.toThrow();
    expect(persisted).toBe(damaged);
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 30000);
