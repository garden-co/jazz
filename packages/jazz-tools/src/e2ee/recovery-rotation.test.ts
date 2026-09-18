import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("preserves recovery after revoking the device that registered the root", async () => {
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
    const open = async () => {
      let saved: string | null = null;
      const client = await createDb({
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
        },
      });
      clients.push(client);
      return client;
    };
    const first = await open();
    const [creator] = await first.e2ee.devices.list();
    const { material } = await first.e2ee.recovery.create().wait();
    const roots = await first.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "edge" });
    expect(roots).toHaveLength(1);
    const second = await open();
    const other = (await second.e2ee.devices.list()).find((row) => row.id !== creator!.id)!;
    await first.e2ee.devices.approve(other.id).wait();
    await second.e2ee.devices.revoke(creator!.id).wait();
    expect(await second.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "revoked" }),
    );
    await Promise.all([first.shutdown(), second.shutdown()]);
    const third = await open();
    const pending = (await third.e2ee.devices.list()).find((row) => row.state === "pending")!;
    expect(pending).toBeDefined();
    await third.e2ee.recovery.use(material).wait();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "revoked" }),
    );
    expect(await third.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "edge" })).toEqual(
      roots,
    );
    await third.e2ee.devices.revoke(other.id).wait();
    await third.shutdown();
    const fourth = await open();
    const last = (await fourth.e2ee.devices.list()).find((row) => row.state === "pending")!;
    expect(last).toBeDefined();
    await fourth.e2ee.recovery.use(material).wait();
    expect(await fourth.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: last.id, state: "active" }),
    );
    expect(await fourth.e2ee.devices.list()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: creator!.id, state: "revoked" }),
        expect.objectContaining({ id: other.id, state: "revoked" }),
      ]),
    );
    expect(await fourth.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "edge" })).toEqual(
      roots,
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
