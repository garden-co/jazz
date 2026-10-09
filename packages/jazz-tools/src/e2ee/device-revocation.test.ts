import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("rotates device keys and retains recovery after revoking its registering device", async () => {
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

    // Continue the already-approved device workflow into recovery. The recovery
    // root's author and the remaining approver are both revoked in turn, and
    // every earlier client is closed before each fresh device recovers.
    const { material } = await first.e2ee.recovery.create().wait();
    const roots = await first.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "remote" });
    expect(roots).toHaveLength(1);
    await third.e2ee.devices.revoke(creator!.id).wait();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "revoked" }),
    );
    await Promise.all(clients.map((db) => db.shutdown()));

    const recovered = await open();
    const recovering = (await recovered.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    expect(recovering).toBeDefined();
    await recovered.e2ee.recovery.use(material).wait();
    expect(await recovered.e2ee.devices.list()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: recovering.id, state: "active" }),
        expect.objectContaining({ id: creator!.id, state: "revoked" }),
        expect.objectContaining({ id: removed.id, state: "revoked" }),
      ]),
    );
    expect(await recovered.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "remote" })).toEqual(
      roots,
    );
    await recovered.e2ee.devices.revoke(pending.id).wait();
    await recovered.shutdown();

    const reopened = await open();
    const last = (await reopened.e2ee.devices.list()).find((device) => device.state === "pending")!;
    expect(last).toBeDefined();
    await reopened.e2ee.recovery.use(material).wait();
    expect(await reopened.e2ee.devices.list()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: last.id, state: "active" }),
        expect.objectContaining({ id: creator!.id, state: "revoked" }),
        expect.objectContaining({ id: removed.id, state: "revoked" }),
        expect.objectContaining({ id: pending.id, state: "revoked" }),
      ]),
    );
    expect(await reopened.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "remote" })).toEqual(
      roots,
    );
  } finally {
    for (const item of delivered) item.secret.fill(0);
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);
