import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("revokes a device with a fresh epoch delivered only to remaining devices", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const delivered: { recipient: Uint8Array; secret: Uint8Array }[] = [];
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
              async seal(recipient, context, secret) {
                delivered.push({ recipient: recipient.slice(), secret: secret.slice() });
                return adapters.keyEnvelope.seal(recipient, context, secret);
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
    const removed = (await second.e2ee.devices.list()).find((d) => d.id !== creator!.id)!;
    await first.e2ee.devices.approve(removed.id).wait();
    const oldSecrets = delivered.map((item) => item.secret.slice());
    const before = delivered.length;
    const removal = first.e2ee.devices.revoke(removed.id);
    expect(removal).not.toBeInstanceOf(Promise);
    await removal.wait();
    const rotations = delivered.slice(before);
    expect(rotations).toHaveLength(1);
    expect(rotations[0]!.recipient).toEqual(creator!.publicKey);
    expect(rotations[0]!.secret).toHaveLength(32);
    for (const old of oldSecrets) {
      expect(rotations[0]!.secret).not.toEqual(old);
      old.fill(0);
    }
    for (const db of [first, second])
      expect(await db.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: removed.id, state: "revoked" }),
      );
    const third = await open();
    const pending = (await third.e2ee.devices.list()).find(
      (d) => d.id !== creator!.id && d.id !== removed.id,
    )!;
    await expect(second.e2ee.devices.approve(pending.id).wait()).rejects.toThrow(
      /revok|active|eligible/i,
    );
    await first.e2ee.devices.approve(pending.id).wait();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
  } finally {
    for (const item of delivered) item.secret.fill(0);
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);
