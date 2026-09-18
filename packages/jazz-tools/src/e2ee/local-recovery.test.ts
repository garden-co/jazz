import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("explicitly restores protected recovery material from a local-first account", async () => {
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
    await first.e2ee.devices.list();
    const { material } = await first.e2ee.recovery.create().wait();
    const protectors = await first.all(deviceRequestApp.__e2ee_recovery_protectors, {
      tier: "edge",
    });
    expect(protectors).toHaveLength(1);
    expect(new TextDecoder().decode(protectors[0]!.material)).not.toContain(material);
    const outsider = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(outsider);
    expect(
      await outsider.all(deviceRequestApp.__e2ee_recovery_protectors, { tier: "edge" }),
    ).toEqual([]);
    await expect(
      outsider
        .insert(deviceRequestApp.__e2ee_recovery_protectors, {
          rootId: protectors[0]!.rootId,
          material: new Uint8Array([1]),
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      first
        .update(deviceRequestApp.__e2ee_recovery_protectors, protectors[0]!.id, {
          material: new Uint8Array([1]),
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await first.shutdown();
    const second = await open();
    const pending = (await second.e2ee.devices.list()).find((row) => row.state === "pending")!;
    expect(pending).toBeDefined();
    await second.e2ee.recovery.use().wait();
    expect(await second.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
