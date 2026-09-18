import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import {
  deviceRequestApp as app,
  deviceRequestPermissions as permissions,
} from "./device-requests.js";

it("publishes the first accepted device binding without exposing private account material", async () => {
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
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const config = await localAccountConfig(server.appId, server.url);
    const alice = await createDb({ ...config, e2ee: { store: store() } });
    clients.push(alice);
    const [first] = await alice.e2ee.devices.list();
    const bob = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(bob);
    const roots = await bob.all(app.__e2ee_account_roots, { tier: "edge" });
    expect(roots).toHaveLength(1);
    expect(roots[0]).toMatchObject({ accountId: config.account.id, deviceId: first!.id });
    expect(roots[0]).not.toHaveProperty("envelope");
    expect(roots[0]).not.toHaveProperty("verification");
    expect(roots[0]).not.toHaveProperty("challenge");
    expect(await bob.all(app.__e2ee_account_identities, { tier: "edge" })).toEqual([]);
    const pending = await createDb({ ...config, e2ee: { store: store() } });
    clients.push(pending);
    const second = (await pending.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    expect(second).toBeDefined();
    expect(await bob.all(app.__e2ee_account_roots, { tier: "edge" })).toEqual(roots);
    const root = roots[0]!;
    await expect(
      pending
        .insert(app.__e2ee_account_roots, {
          accountId: config.account.id,
          deviceId: second.id,
          epochId: root.epochId,
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      bob
        .insert(app.__e2ee_account_roots, {
          accountId: config.account.id,
          deviceId: first!.id,
          epochId: root.epochId,
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      alice
        .update(app.__e2ee_account_roots, root.id, {
          deviceId: second.id,
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      alice.delete(app.__e2ee_account_roots, root.id).wait({ tier: "global" }),
    ).rejects.toThrow();
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 30_000);
