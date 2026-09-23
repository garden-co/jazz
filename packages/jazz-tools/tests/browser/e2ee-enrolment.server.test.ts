import { expect, it } from "vitest";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestApp, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("approves, reopens and revokes a second device with browser crypto", async () => {
  const server = await getJazzServerInfo(`e2ee-approval-${crypto.randomUUID()}`);
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const keys = [crypto.randomUUID(), crypto.randomUUID()];
  try {
    await deploy({ ...server, schema: deviceRequestApp, permissions: deviceRequestPermissions });
    const account = await acquireBrowserTestAccount(server);
    const open = async (key: string) => {
      const db = await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        account,
        driver: { type: "memory" },
        e2ee: {
          store: {
            async read() {
              return localStorage.getItem(key);
            },
            async update(transform) {
              await navigator.locks.request(key, () =>
                localStorage.setItem(key, transform(localStorage.getItem(key))),
              );
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    const first = await open(keys[0]!);
    await first.e2ee.devices.list();
    const second = await open(keys[1]!);
    const pending = (await second.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await first.e2ee.devices.approve(pending.id).wait();
    expect(await second.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
    await second.shutdown();
    const reopened = await open(keys[1]!);
    expect(await reopened.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
    await first.e2ee.devices.revoke(pending.id).wait();
    expect(await reopened.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "revoked" }),
    );
    await reopened.shutdown();
    const revoked = await open(keys[1]!);
    expect(await revoked.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "revoked" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    for (const key of keys) localStorage.removeItem(key);
    await stopJazzServer(server.serverUrl);
  }
}, 30000);

it("reopens the active first device with browser crypto and durable atomic browser storage", async () => {
  const server = await getJazzServerInfo(`e2ee-browser-${crypto.randomUUID()}`);
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const key = `e2ee-test-${crypto.randomUUID()}`;
  try {
    await deploy({ ...server, schema: deviceRequestApp, permissions: deviceRequestPermissions });
    const account = await acquireBrowserTestAccount(server);
    const store = {
      async read() {
        return localStorage.getItem(key);
      },
      async update(transform: (current: string | null) => string) {
        await navigator.locks.request(key, () => {
          localStorage.setItem(key, transform(localStorage.getItem(key)));
        });
      },
    };
    const config = {
      appId: server.appId,
      serverUrl: server.serverUrl,
      account,
      driver: { type: "memory" as const },
      e2ee: { store },
    };
    const first = await createDb(config);
    clients.push(first);
    const devices = await first.e2ee.devices.list();
    expect(devices).toHaveLength(1);
    expect(devices[0]).toMatchObject({ state: "active" });
    const saved = await store.read();
    await first.shutdown();
    const second = await createDb(config);
    clients.push(second);
    expect(await second.e2ee.devices.list()).toEqual(devices);
    expect(await store.read()).toBe(saved);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    localStorage.removeItem(key);
    await stopJazzServer(server.serverUrl);
  }
});
