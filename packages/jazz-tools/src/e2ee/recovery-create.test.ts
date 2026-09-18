import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";
import { encodeCryptoContext } from "./context.js";
import { confirmAccountEpoch } from "./first-epoch.js";
import { readAccountMembership } from "./public-membership.js";

it("creates independent recovery material with an accepted account-private key delivery", async () => {
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
    const retained = store();
    const owner = await createDb({ ...account, e2ee: { store: retained } });
    const observer = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(owner, observer);
    const [device] = await owner.e2ee.devices.list();
    const handle = owner.e2ee.recovery.create();
    expect(handle).not.toBeInstanceOf(Promise);
    const { material } = await handle.wait();
    const decoded = JSON.parse(material);
    expect(decoded.format).toBe("jazz-e2ee-recovery-v1");
    const local = JSON.parse((await retained.read())!).devices[0];
    expect(decoded.privateKey).not.toEqual(local.privateKey);
    expect(decoded.signingPrivateKey).not.toEqual(local.signingPrivateKey);
    expect(decoded.scope).toBe(local.scope);
    const crypto = await createNativeCrypto();
    const state = await readAccountMembership(
      observer,
      account.account.id,
      local.scope,
      crypto.deviceSigner,
    );
    expect(state.recoveryRoots).toHaveLength(1);
    const [root] = state.recoveryRoots;
    expect(root).toMatchObject({ id: decoded.rootId, signerId: device!.id });
    const deliveries = await owner.all(app.__e2ee_recovery_deliveries, { tier: "edge" });
    expect(deliveries).toHaveLength(1);
    const delivery = deliveries[0]!;
    expect(delivery).toMatchObject({ rootId: root!.id, epochId: state.epochId });
    expect(await observer.all(app.__e2ee_recovery_deliveries, { tier: "edge" })).toEqual([]);
    const context = encodeCryptoContext({
      application: local.scope,
      policy: "jazz.e2ee.recovery.v1",
      scope: "account",
      identifier: account.account.id,
      table: "__e2ee_recovery_deliveries",
      row: delivery.id,
      column: "envelope",
      epoch: delivery.epochId,
      recipient: root!.id,
    });
    const secret = await crypto.keyEnvelope.open(
      {
        publicKey: Uint8Array.from(decoded.publicKey),
        privateKey: Uint8Array.from(decoded.privateKey),
      },
      context,
      delivery.envelope,
    );
    try {
      const identity = await owner.one(app.__e2ee_account_identities, { tier: "edge" });
      await confirmAccountEpoch(
        crypto.keyEnvelope,
        local.scope,
        account.account.id,
        identity!,
        secret,
      );
    } finally {
      secret.fill(0);
    }
    const pending = await createDb({ ...account, e2ee: { store: store() } });
    clients.push(pending);
    await expect(pending.e2ee.recovery.create().wait()).rejects.toThrow(/active|approved|key/i);
    expect(await owner.all(app.__e2ee_recovery_roots, { tier: "edge" })).toHaveLength(1);
    expect(await owner.all(app.__e2ee_recovery_deliveries, { tier: "edge" })).toHaveLength(1);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60000);
