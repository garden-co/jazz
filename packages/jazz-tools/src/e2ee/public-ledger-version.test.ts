import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeDeviceSigner } from "./native.js";
import { readAccountMembership } from "./public-membership.js";
import {
  deviceRequestApp as app,
  deviceRequestPermissions as permissions,
} from "./device-requests.js";

it("refuses an unversioned account rather than treating a backfilled root as complete history", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const owner = await createDb(account);
    const observer = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(owner, observer);
    const signer = await createNativeDeviceSigner();
    const keys = await signer.createKeyPair();
    keys.privateKey.fill(0);
    const request = await owner
      .insert(app.__e2ee_device_requests, {
        publicKey: keys.publicKey,
        mechanism: "fixture",
        version: 1,
        signingPublicKey: keys.publicKey,
        signingMechanism: signer.mechanism.id,
        signingVersion: signer.mechanism.version,
        challenge: new Uint8Array(32),
      })
      .wait({ tier: "global" });
    await owner
      .insert(app.__e2ee_device_keys, {
        deviceId: request.id,
        publicKey: request.publicKey,
        mechanism: request.mechanism,
        version: request.version,
        signingPublicKey: request.signingPublicKey,
        signingMechanism: request.signingMechanism,
        signingVersion: request.signingVersion,
      })
      .wait({ tier: "global" });
    const epochId = crypto.randomUUID();
    // Model an older identity and backfilled public root, without claiming that
    // its private lifecycle was projected. The observer cannot inspect that history.
    await owner
      .insert(
        app.__e2ee_account_identities,
        {
          deviceId: request.id,
          epochId,
          envelope: Uint8Array.of(1),
          verification: Uint8Array.of(2),
        },
        { id: account.account.id },
      )
      .wait({ tier: "global" });
    await owner
      .insert(app.__e2ee_account_roots, {
        accountId: account.account.id,
        deviceId: request.id,
        epochId,
      })
      .wait({ tier: "global" });
    await expect(
      readAccountMembership(observer, account.account.id, "fixture", signer),
    ).rejects.toThrow(/ledger|version|history/i);
    await expect(
      owner
        .insert(app.__e2ee_account_roots, {
          accountId: account.account.id,
          deviceId: request.id,
          epochId,
          ledgerVersion: 1,
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    expect(await observer.all(app.__e2ee_account_identities, { tier: "edge" })).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 30_000);
