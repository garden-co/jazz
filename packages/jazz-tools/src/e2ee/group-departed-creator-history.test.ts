// Proposed location: packages/jazz-tools/src/e2ee/group-departed-creator-history.test.ts
import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { createNativeCrypto } from "./native.js";

it("retains full current history checks for departed parent and child creators", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const aliceAccount = await localAccountConfig(server.appId, server.url);
    const bobAccount = await localAccountConfig(server.appId, server.url);
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session, allOf }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowRead.where(authenticated);
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.where(authenticated);
      // Explicit application administration for these two accounts. Alice must
      // legitimately administer Bob's group after its creator leaves.
      policy.__e2ee_group_membership.allowInsert.where(
        allOf([
          { authorAccountId: session.user.account },
          { authorAccountId: { in: [aliceAccount.account.id, bobAccount.account.id] } },
        ]),
      );
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_successors.allowRead.where(authenticated);
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const native = await createNativeCrypto();
    const aliceCrypto = { ...native, deviceSigner: { ...native.deviceSigner } };
    const open = async (account: typeof aliceAccount, crypto = native) => {
      let saved: string | null = null;
      const db = await createDb({
        ...account,
        e2ee: {
          app,
          crypto,
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
      clients.push(db);
      await db.e2ee.devices.list();
      return db;
    };
    const alice = await open(aliceAccount, aliceCrypto);
    const bob = await open(bobAccount);
    const a = await alice.e2ee.groups.create().wait();
    const b = await alice.e2ee.groups.create().wait();
    const c = await bob.e2ee.groups.create().wait();
    await bob.e2ee.groups.add(c.id, { kind: "account", id: aliceAccount.account.id }).wait();
    await bob.e2ee.groups.remove(c.id, { kind: "account", id: bobAccount.account.id }).wait();
    expect(await alice.e2ee.explain({ groupId: c.id })).toEqual({ state: "ready" });
    expect(await bob.e2ee.explain({ groupId: c.id })).toMatchObject({ state: "refused" });
    const priorRoots = await bob.all(app.__e2ee_recovery_roots, { tier: "global" });
    await bob.e2ee.recovery.create().wait();
    const roots = await bob.all(app.__e2ee_recovery_roots, { tier: "global" });
    const oldIds = new Set(priorRoots.map((row) => row.id));
    const added = roots.filter((row) => !oldIds.has(row.id));
    expect(added).toHaveLength(1);
    const lateSignature = added[0]!.signature;
    expect(lateSignature.length).toBeGreaterThan(0);
    const sentinel = new Error("Departed creator's current recovery verifier unavailable");
    let hits = 0;
    aliceCrypto.deviceSigner.verify = async (key, bytes, signature) => {
      if (
        signature.length === lateSignature.length &&
        signature.every((byte, i) => byte === lateSignature[i])
      ) {
        hits++;
        throw sentinel;
      }
      return native.deviceSigner.verify(key, bytes, signature);
    };
    for (const [parent, child] of [
      [a.id, c.id],
      [c.id, b.id],
    ] as const) {
      const query = app.__e2ee_group_membership.where({ groupId: parent });
      const before = await alice.all(query, { tier: "global" });
      const previousHits = hits;
      await expect(alice.e2ee.groups.add(parent, { kind: "group", id: child }).wait()).rejects.toBe(
        sentinel,
      );
      expect(hits).toBeGreaterThan(previousHits);
      expect(await alice.all(query, { tier: "global" })).toEqual(before);
    }
    // New function identity invalidates completed proof caches after restoration.
    aliceCrypto.deviceSigner.verify = (key, bytes, signature) =>
      native.deviceSigner.verify(key, bytes, signature);
    await expect(
      alice.e2ee.groups.add(a.id, { kind: "group", id: c.id }).wait(),
    ).resolves.toBeUndefined();
    await expect(
      alice.e2ee.groups.add(c.id, { kind: "group", id: b.id }).wait(),
    ).resolves.toBeUndefined();
    for (const id of [a.id, b.id, c.id])
      expect(await alice.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 180_000);
