import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";
import { recoveryRootBytes } from "./recovery-format.js";
import { readAccountMembership } from "./public-membership.js";

it("accepts recovery registration when immutable public projections are published later", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const crypto = await createNativeCrypto();
  const signer = await crypto.deviceSigner.createKeyPair();
  const recoverySigner = await crypto.deviceSigner.createKeyPair();
  const recoveryKeys = await crypto.keyEnvelope.createKeyPair();
  recoverySigner.privateKey.fill(0);
  recoveryKeys.privateKey.fill(0);
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: deviceRequestPermissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const owner = await createDb(account);
    const observer = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(owner, observer);
    const device = await owner
      .insert(app.__e2ee_device_requests, {
        publicKey: recoveryKeys.publicKey,
        mechanism: crypto.keyEnvelope.mechanism.id,
        version: crypto.keyEnvelope.mechanism.version,
        signingPublicKey: signer.publicKey,
        signingMechanism: crypto.deviceSigner.mechanism.id,
        signingVersion: crypto.deviceSigner.mechanism.version,
        challenge: new Uint8Array(32),
      })
      .wait({ tier: "global" });
    const epochId = globalThis.crypto.randomUUID();
    await owner
      .insert(
        app.__e2ee_account_identities,
        {
          deviceId: device.id,
          epochId,
          ledgerVersion: 1,
          envelope: Uint8Array.of(1),
          verification: Uint8Array.of(2),
        },
        { id: account.account.id },
      )
      .wait({ tier: "global" });
    // The immutable private identity exists. Public projections are deliberately delayed.
    const root = {
      id: globalThis.crypto.randomUUID(),
      accountId: account.account.id,
      signerId: device.id,
      epochId,
      signingPublicKey: recoverySigner.publicKey,
      signingMechanism: crypto.deviceSigner.mechanism.id,
      signingVersion: crypto.deviceSigner.mechanism.version,
      publicKey: recoveryKeys.publicKey,
      mechanism: crypto.keyEnvelope.mechanism.id,
      version: crypto.keyEnvelope.mechanism.version,
    };
    const { id, ...columns } = root;
    const signature = await crypto.deviceSigner.sign(
      signer.privateKey,
      recoveryRootBytes("fixture", root),
    );
    await owner
      .insert(app.__e2ee_recovery_roots, { ...columns, signature }, { id })
      .wait({ tier: "global" });
    await owner
      .insert(app.__e2ee_device_keys, {
        deviceId: device.id,
        publicKey: device.publicKey,
        mechanism: device.mechanism,
        version: device.version,
        signingPublicKey: device.signingPublicKey,
        signingMechanism: device.signingMechanism,
        signingVersion: device.signingVersion,
      })
      .wait({ tier: "global" });
    await owner
      .insert(app.__e2ee_account_roots, {
        accountId: account.account.id,
        deviceId: device.id,
        epochId,
        ledgerVersion: 1,
      })
      .wait({ tier: "global" });
    const result = await readAccountMembership(
      observer,
      account.account.id,
      "fixture",
      crypto.deviceSigner,
    );
    expect(result.recoveryRoots.map((row) => row.id)).toEqual([id]);
    expect(await observer.all(app.__e2ee_account_identities, { tier: "edge" })).toEqual([]);
  } finally {
    signer.privateKey.fill(0);
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 30000);
