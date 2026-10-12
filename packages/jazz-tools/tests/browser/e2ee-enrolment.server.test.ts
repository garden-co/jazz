import { expect, it } from "vitest";
import { browserTopologyReporter } from "./topology-harness.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestApp, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("reopens the creator and approves, reopens and revokes a second device with browser crypto", async () => {
  const started = performance.now();
  const phase = (label: string) =>
    browserTopologyReporter.phase(
      "start",
      `device enrolment: ${label}`,
      Math.round(performance.now() - started),
    );
  phase("start server");
  const server = await getJazzServerInfo(`e2ee-approval-${crypto.randomUUID()}`);
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const keys = [crypto.randomUUID(), crypto.randomUUID()];
  try {
    phase("deploy schema");
    await deploy({ ...server, schema: deviceRequestApp, permissions: deviceRequestPermissions });
    phase("acquire account");
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
    phase("open creator");
    let first = await open(keys[0]!);
    phase("enrol creator");
    const devices = await first.e2ee.devices.list();
    expect(devices).toHaveLength(1);
    expect(devices[0]).toMatchObject({ state: "active" });
    const saved = localStorage.getItem(keys[0]!);
    phase("close creator");
    await first.shutdown();
    phase("reopen creator");
    first = await open(keys[0]!);
    expect(await first.e2ee.devices.list()).toEqual(devices);
    expect(localStorage.getItem(keys[0]!)).toBe(saved);
    phase("open second device");
    const second = await open(keys[1]!);
    phase("enrol second device");
    const pending = (await second.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    phase("approve second device");
    await first.e2ee.devices.approve(pending.id).wait();
    expect(await second.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
    phase("close approved device");
    await second.shutdown();
    phase("reopen approved device");
    const reopened = await open(keys[1]!);
    expect(await reopened.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
    phase("revoke second device");
    await first.e2ee.devices.revoke(pending.id).wait();
    expect(await reopened.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "revoked" }),
    );
    phase("close revoked device");
    await reopened.shutdown();
    phase("reopen revoked device");
    const revoked = await open(keys[1]!);
    expect(await revoked.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "revoked" }),
    );
  } finally {
    phase("close clients");
    await Promise.all(clients.map((client) => client.shutdown()));
    for (const key of keys) localStorage.removeItem(key);
    phase("stop server");
    await stopJazzServer(server.serverUrl);
    browserTopologyReporter.phase(
      "complete",
      "device enrolment",
      Math.round(performance.now() - started),
    );
  }
}, 30000);
