import { expect } from "vitest";
import { createDb } from "../../runtime/default-create-db.js";
import { localAccountConfig } from "../../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../../testing/index.js";
import { createBrowserDeviceSigner, createBrowserKeyEnvelope } from "../browser.js";
import { app, permissions } from "./encrypted-offline-fixture-schema.js";

export async function runOfflineDeliveryRetry(
  overlap: "interrupted background read" | "suppressed background read",
  trigger: "ordinary owner read" | "owner reconnect",
) {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const store = () => {
    let value: string | null = null;
    return {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        value = transform(value);
      },
    };
  };
  let release = () => {};
  const barrier = new Promise<void>((resolve) => {
    release = resolve;
  });
  let armed = false;
  let waiting = false;
  let openedKey: Uint8Array | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const owner = await createDb({ ...alice, e2ee: { app, store: store() } });
    clients.push(owner);
    const envelope = await createBrowserKeyEnvelope();
    const signer = await createBrowserDeviceSigner();
    const decoder = new TextDecoder();
    const reader = await createDb({
      ...bob,
      e2ee: {
        app,
        store: store(),
        crypto: {
          keyEnvelope: {
            ...envelope,
            async open(...args) {
              const key = await envelope.open(...args);
              if (
                armed &&
                overlap === "interrupted background read" &&
                decoder.decode(args[1]).includes("jazz.e2ee.space.v1")
              ) {
                armed = false;
                openedKey = key;
                waiting = true;
                await barrier;
              }
              return key;
            },
          },
          deviceSigner: {
            ...signer,
            async verify(...args) {
              if (armed && overlap === "suppressed background read") {
                armed = false;
                waiting = true;
                await barrier;
              }
              return signer.verify(...args);
            },
          },
        },
      },
    });
    clients.push(reader);
    await owner.e2ee.devices.list();
    await reader.e2ee.devices.list();
    const tx = owner.beginExclusiveTransaction();
    const project = tx.insert(
      app.projects,
      { title: "Resumed shared project" },
      { initialRecipients: [alice.account.id, bob.account.id] },
    );
    const note = tx.insert(app.notes, { projectId: project.id, title: "Still shared" });
    await tx.commit().wait({ tier: "global" });
    expect(await reader.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(note);
    await reader.e2ee.explain({ scope: app.projects, identifier: project.id });
    // Leave the original owner offline while another authorised holder retries.
    await owner.shutdown();
    const secondDevice = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(secondDevice);
    const pending = (await secondDevice.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await reader.e2ee.devices.approve(pending.id).wait();
    if (overlap === "interrupted background read") {
      expect(await reader.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(note);
      armed = true;
      await expect.poll(() => waiting, { timeout: 10_000 }).toBe(true);
    } else {
      armed = true;
    }
    const administration = reader.e2ee.spaces
      .grant(app.projects, project.id, crypto.randomUUID())
      .wait();
    const rejected = expect(administration).rejects.toThrow(
      "E2EE recipient account is unavailable",
    );
    if (overlap === "suppressed background read") {
      await expect.poll(() => waiting, { timeout: 10_000 }).toBe(true);
      expect(await reader.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(note);
      release();
    }
    await rejected;
    if (overlap === "interrupted background read") {
      release();
      // A cancelled operation must still wipe the real adapter-owned key.
      await expect
        .poll(() => openedKey?.every((byte) => byte === 0), { timeout: 10_000 })
        .toBe(true);
    }
    // This device became eligible only after the failed foreground operation
    // and cancelled background attempt settled. Its approving device has not
    // used this space, so it cannot have delivered the key in the earlier attempt.
    const recipient = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(recipient);
    const eligible = (await recipient.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await secondDevice.e2ee.devices.approve(eligible.id).wait();
    expect(
      await secondDevice.all(
        app.__e2ee_space_deliveries.where({ recipientDeviceId: eligible.id }),
        { tier: "global" },
      ),
    ).toEqual([]);
    // Failed administration is observable; it must not poison later automatic
    // delivery. Neither retry trigger explicitly calls e2ee.explain().
    if (trigger === "ordinary owner read") {
      expect(await reader.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(note);
    } else {
      await reader.disconnect();
      await reader.reconnect();
    }
    await expect
      .poll(
        () => recipient.one(app.notes.where({ id: note.id }), { tier: "global" }).catch(() => null),
        { timeout: 10_000 },
      )
      .toEqual(note);
  } finally {
    armed = false;
    release();
    for (const client of clients) await client.shutdown();
    await server.stop();
  }
}
