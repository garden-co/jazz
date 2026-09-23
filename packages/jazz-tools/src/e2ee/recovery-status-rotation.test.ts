import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";

it("checks recovery ancestry after rotation without activating the inspecting device", async () => {
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
    let corruptHistory = false;
    let injected = 0;
    const open = async (inspect = false) => {
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
              async unwrap(key, context, envelope) {
                if (
                  inspect &&
                  corruptHistory &&
                  new TextDecoder().decode(context).includes("history")
                ) {
                  injected++;
                  return new Uint8Array(32).fill(9);
                }
                return adapters.keyEnvelope.unwrap(key, context, envelope);
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
    const { material } = await first.e2ee.recovery.create().wait();
    const before = await first.e2ee.recovery.status(material);
    const second = await open();
    const other = (await second.e2ee.devices.list()).find((row) => row.state === "pending")!;
    await first.e2ee.devices.approve(other.id).wait();
    await second.e2ee.devices.revoke(creator!.id).wait();
    await Promise.all([first.shutdown(), second.shutdown()]);
    const observer = await open(true);
    const pending = (await observer.e2ee.devices.list()).find((row) => row.state === "pending")!;
    expect(pending).toBeDefined();
    const requests = await observer.all(deviceRequestApp.__e2ee_device_requests, { tier: "edge" });
    const approvals = await observer.all(deviceRequestApp.__e2ee_public_device_approvals, {
      tier: "edge",
    });
    corruptHistory = true;
    await expect(observer.e2ee.recovery.status(material)).rejects.toMatchObject({
      code: "recovery-delivery-unusable",
      message: "No authenticated recovery delivery for the current account epoch",
    });
    expect(injected).toBeGreaterThan(0);
    corruptHistory = false;
    const checked = await observer.e2ee.recovery.status(material);
    expect(checked.account).toMatchObject({
      validation: "validated",
      activeDeviceIds: [other.id],
      validatedRootId: before.account.validatedRootId,
    });
    expect(checked.account.epochId).not.toBe(before.account.epochId);
    expect(checked.groups.validation).toBe("not-checked");
    expect(await observer.all(deviceRequestApp.__e2ee_device_requests, { tier: "edge" })).toEqual(
      requests,
    );
    expect(
      await observer.all(deviceRequestApp.__e2ee_public_device_approvals, { tier: "edge" }),
    ).toEqual(approvals);
    expect(await observer.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "pending" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 90000);
