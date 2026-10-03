import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "./device-requests.js";

it("keeps recovery root writes account-owned and immutable while exposing only public records", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: deviceRequestPermissions,
    });
    const accounts = [];
    for (let i = 0; i < 2; i++) {
      const config = await localAccountConfig(server.appId, server.url);
      accounts.push(config.account.id);
      let saved: string | null = null;
      clients.push(
        await createDb({
          ...config,
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
        }),
      );
    }
    const [owner, other] = clients;
    const [device] = await owner!.e2ee.devices.list();
    const [foreignDevice] = await other!.e2ee.devices.list();
    const identity = await owner!.one(app.__e2ee_account_identities, { tier: "edge" });
    const roots = app.__e2ee_recovery_roots;
    // Inert public bytes test ordinary policy, not cryptographic recovery authority.
    const values = {
      accountId: accounts[0]!,
      signerId: device!.id,
      epochId: identity!.epochId,
      signingMechanism: "jazz.sodium.sign",
      signingVersion: 1,
      signingPublicKey: new Uint8Array(32).fill(1),
      mechanism: "jazz.sodium.key",
      version: 1,
      publicKey: new Uint8Array(32).fill(2),
      signature: new Uint8Array(64),
    };
    const root = await owner!.insert(roots, values).wait({ tier: "global" });
    expect(await other!.all(roots, { tier: "edge" })).toEqual([root]);
    await expect(other!.insert(roots, values).wait({ tier: "global" })).rejects.toThrow();
    await expect(
      owner!.insert(roots, { ...values, signerId: foreignDevice!.id }).wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      owner!.insert(roots, { ...values, accountId: accounts[1]! }).wait({ tier: "global" }),
    ).rejects.toThrow();
    for (const client of clients) {
      await expect(
        client
          .update(roots, root.id, { publicKey: new Uint8Array(32).fill(3) })
          .wait({ tier: "global" }),
      ).rejects.toThrow();
      await expect(client.delete(roots, root.id).wait({ tier: "global" })).rejects.toThrow();
    }
    expect(await owner!.one(roots.where({ id: root.id }), { tier: "edge" })).toEqual(root);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 30000);
