import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("keeps device administration and immutable recovery roots account-scoped", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const accounts: string[] = [];
    for (let i = 0; i < 2; i++) {
      const config = await localAccountConfig(server.appId, server.url);
      accounts.push(config.account.id);
      let stored: string | null = null;
      clients.push(
        await createDb({
          ...config,
          e2ee: {
            store: {
              async read() {
                return stored;
              },
              async update(transform: (current: string | null) => string) {
                stored = transform(stored);
              },
            },
          },
        }),
      );
    }
    const [alice, bob] = clients;
    const [aliceDevice] = await alice!.e2ee.devices.list();
    const [bobDevice] = await bob!.e2ee.devices.list();
    expect(bobDevice!.id).not.toBe(aliceDevice!.id);
    expect(await alice!.e2ee.devices.list()).toEqual([aliceDevice]);
    expect(await bob!.e2ee.devices.list()).toEqual([bobDevice]);
    const keys = await bob!.all(deviceRequestApp.__e2ee_device_keys, { tier: "remote" });
    expect(keys.map((row) => row.deviceId).sort()).toEqual([aliceDevice!.id, bobDevice!.id].sort());
    expect(keys.every((row) => !("challenge" in row) && !("envelope" in row))).toBe(true);
    await expect(bob!.e2ee.devices.approve(aliceDevice!.id).wait()).rejects.toThrow();
    await expect(bob!.e2ee.devices.revoke(aliceDevice!.id).wait()).rejects.toThrow();
    const identities = await bob!.all(deviceRequestApp.__e2ee_account_identities, {
      tier: "remote",
    });
    expect(identities).toHaveLength(1);
    expect(identities[0]!.deviceId).toBe(bobDevice!.id);
    expect(await alice!.e2ee.devices.list()).toEqual([aliceDevice]);

    const identity = await alice!.one(deviceRequestApp.__e2ee_account_identities, {
      tier: "remote",
    });
    const roots = deviceRequestApp.__e2ee_recovery_roots;
    // Inert public bytes test ordinary policy, not cryptographic recovery authority.
    const values = {
      accountId: accounts[0]!,
      signerId: aliceDevice!.id,
      epochId: identity!.epochId,
      signingMechanism: "jazz.sodium.sign",
      signingVersion: 1,
      signingPublicKey: new Uint8Array(32).fill(1),
      mechanism: "jazz.sodium.key",
      version: 1,
      publicKey: new Uint8Array(32).fill(2),
      signature: new Uint8Array(64),
    };
    const root = await alice!.insert(roots, values).wait({ tier: "global" });
    expect(await bob!.all(roots, { tier: "remote" })).toEqual([root]);
    await expect(bob!.insert(roots, values).wait({ tier: "global" })).rejects.toThrow();
    await expect(
      alice!.insert(roots, { ...values, signerId: bobDevice!.id }).wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      alice!.insert(roots, { ...values, accountId: accounts[1]! }).wait({ tier: "global" }),
    ).rejects.toThrow();
    for (const client of clients) {
      await expect(
        client
          .update(roots, root.id, { publicKey: new Uint8Array(32).fill(3) })
          .wait({ tier: "global" }),
      ).rejects.toThrow();
      await expect(client.delete(roots, root.id).wait({ tier: "global" })).rejects.toThrow();
    }
    expect(await alice!.one(roots.where({ id: root.id }), { tier: "remote" })).toEqual(root);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 30_000);

it("keeps enrolment requests private to their verified author account and immutable", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const aliceConfig = await localAccountConfig(server.appId, server.url);
    const alice = await createDb(aliceConfig);
    clients.push(alice);
    const bob = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(bob);
    const requests = deviceRequestApp.__e2ee_device_requests;
    // These public bytes exercise record authorisation, not cryptographic approval.
    const request = await alice
      .insert(requests, {
        publicKey: new Uint8Array(32).fill(1),
        signingPublicKey: new Uint8Array(32).fill(1),
        signingMechanism: "jazz.sodium.sign",
        signingVersion: 1,
        mechanism: "jazz.sodium.key",
        version: 1,
        challenge: new Uint8Array(32).fill(2),
      })
      .wait({ tier: "global" });

    await expect(alice.all(requests, { tier: "remote" })).resolves.toEqual([
      expect.objectContaining({ id: request.id, publicKey: new Uint8Array(32).fill(1) }),
    ]);
    await expect(bob.all(requests, { tier: "remote" })).resolves.toEqual([]);
    const directory = deviceRequestApp.__e2ee_device_keys;
    const publicKeys = {
      deviceId: request.id,
      publicKey: new Uint8Array(32).fill(1),
      signingPublicKey: new Uint8Array(32).fill(1),
      signingMechanism: "jazz.sodium.sign",
      signingVersion: 1,
      mechanism: "jazz.sodium.key",
      version: 1,
    };
    await expect(
      bob.insert(directory, publicKeys, { id: request.id }).wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      alice
        .insert(
          directory,
          { ...publicKeys, publicKey: new Uint8Array(32).fill(9) },
          { id: request.id },
        )
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await alice.insert(directory, publicKeys, { id: request.id }).wait({ tier: "global" });
    await expect(bob.all(directory, { tier: "remote" })).resolves.toEqual([
      expect.objectContaining({ id: request.id, ...publicKeys }),
    ]);
    for (const client of [alice, bob]) {
      await expect(
        client
          .update(directory, request.id, {
            publicKey: new Uint8Array(32).fill(9),
          })
          .wait({ tier: "global" }),
      ).rejects.toThrow();
      await expect(client.delete(directory, request.id).wait({ tier: "global" })).rejects.toThrow();
    }
    await expect(
      alice.one(requests.select("$createdBy").where({ id: request.id }), {
        tier: "remote",
      }),
    ).resolves.toMatchObject({ $createdBy: { account: aliceConfig.account.id } });

    for (const client of [alice, bob]) {
      const replacement = {
        publicKey: new Uint8Array(32).fill(3),
        signingPublicKey: new Uint8Array(32).fill(3),
        signingMechanism: "jazz.sodium.sign",
        signingVersion: 1,
        mechanism: "jazz.sodium.key",
        version: 1,
        challenge: new Uint8Array(32).fill(4),
      };
      for (const replace of [
        () => client.insert(requests, replacement, { id: request.id }),
        () => client.upsert(requests, request.id, replacement),
        () => client.restore(requests, request.id, replacement),
      ]) {
        await expect(async () => replace().wait({ tier: "global" })).rejects.toThrow(
          /already exists|authorization|policy denied|not deleted|not_deleted|conflict/i,
        );
      }
      await expect(
        client
          .update(requests, request.id, {
            publicKey: new Uint8Array(32).fill(3),
          })
          .wait({ tier: "global" }),
      ).rejects.toThrow();
      await expect(client.delete(requests, request.id).wait({ tier: "global" })).rejects.toThrow();
    }
    await expect(
      alice.one(requests.where({ id: request.id }), { tier: "remote" }),
    ).resolves.toEqual(request);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
});
