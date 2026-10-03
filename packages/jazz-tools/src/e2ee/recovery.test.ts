import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("recovers an existing account onto a fresh device without another device online", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const store = () => {
    let saved: string | null = null;
    return {
      async read() {
        return saved;
      },
      async update(transform: (current: string | null) => string) {
        saved = transform(saved);
      },
    };
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const first = await createDb({ ...account, e2ee: { store: store() } });
    clients.push(first);
    const [creator] = await first.e2ee.devices.list();
    expect(creator).toMatchObject({ state: "active" });
    const roots = await first.all(
      deviceRequestApp.__e2ee_account_roots.where({ accountId: account.account.id }),
      { tier: "edge" },
    );
    expect(roots).toHaveLength(1);
    const { material } = await first.e2ee.recovery.create().wait();
    expect(typeof material).toBe("string");
    expect(material.length).toBeGreaterThan(0);
    await first.shutdown();

    const retained = store();
    const second = await createDb({ ...account, e2ee: { store: retained } });
    clients.push(second);
    const pending = (await second.e2ee.devices.list()).find((device) => device.id !== creator!.id)!;
    expect(pending).toMatchObject({ state: "pending" });
    await second.e2ee.recovery.use(material).wait();
    expect(await second.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
    await second.shutdown();
    const reopened = await createDb({ ...account, e2ee: { store: retained } });
    clients.push(reopened);
    expect(
      await reopened.all(
        deviceRequestApp.__e2ee_account_roots.where({ accountId: account.account.id }),
        { tier: "edge" },
      ),
    ).toEqual(roots);
    expect(await reopened.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60000);
