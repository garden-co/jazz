import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";
import { readAccountMembership } from "./public-membership.js";
import { recoveryRootBytes } from "./recovery-format.js";

it("retains previously registered recovery authority but rejects registration after device revocation", async () => {
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
      schema: app,
      permissions: deviceRequestPermissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const first = await createDb({ ...account, e2ee: { store: store() } });
    clients.push(first);
    const [creator] = await first.e2ee.devices.list();
    const retained = store();
    const second = await createDb({ ...account, e2ee: { store: retained } });
    clients.push(second);
    const pending = (await second.e2ee.devices.list()).find((d) => d.id !== creator!.id)!;
    await first.e2ee.devices.approve(pending.id).wait();
    const observer = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(observer);
    const crypto = await createNativeCrypto();
    const device = JSON.parse((await retained.read())!).devices[0];
    const signingKey = Uint8Array.from(device.signingPrivateKey);
    const recoverySigner = await crypto.deviceSigner.createKeyPair();
    const recoveryKeys = await crypto.keyEnvelope.createKeyPair();
    recoverySigner.privateKey.fill(0);
    recoveryKeys.privateKey.fill(0);
    const membership = () =>
      readAccountMembership(observer, account.account.id, device.scope, crypto.deviceSigner);
    const register = async (epochId: string) => {
      const root = {
        id: globalThis.crypto.randomUUID(),
        accountId: account.account.id,
        epochId,
        signerId: pending.id,
        signingPublicKey: recoverySigner.publicKey,
        signingMechanism: crypto.deviceSigner.mechanism.id,
        signingVersion: crypto.deviceSigner.mechanism.version,
        publicKey: recoveryKeys.publicKey,
        mechanism: crypto.keyEnvelope.mechanism.id,
        version: crypto.keyEnvelope.mechanism.version,
      };
      const signature = await crypto.deviceSigner.sign(
        signingKey,
        recoveryRootBytes(device.scope, root),
      );
      const { id, ...columns } = root;
      await second
        .insert(app.__e2ee_recovery_roots, { ...columns, signature }, { id })
        .wait({ tier: "global" });
      return id;
    };
    try {
      const initial = await membership();
      const accepted = await register(initial.epochId);
      await first.e2ee.devices.revoke(pending.id).wait();
      const revoked = await membership();
      expect([...revoked.revoked]).toContain(pending.id);
      const rejected = await register(revoked.epochId);
      const result = await membership();
      expect(result.recoveryRoots.map((root) => root.id)).toEqual([accepted]);
      expect(result.recoveryRoots.map((root) => root.id)).not.toContain(rejected);
      expect(await observer.all(app.__e2ee_account_identities, { tier: "edge" })).toEqual([]);
    } finally {
      signingKey.fill(0);
    }
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60000);
