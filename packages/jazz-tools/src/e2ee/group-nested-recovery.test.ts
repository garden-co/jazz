import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { withGroupTopologyPermissions } from "./group-topology.js";

it("does not omit an undelivered inherited group from recovery protection and later restores both paths", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: withGroupTopologyPermissions(app, { ...deviceRequestPermissions, ...policies }),
    });
    const open = async (account: Awaited<ReturnType<typeof localAccountConfig>>) => {
      let saved: string | null = null;
      const db = await createDb({
        ...account,
        e2ee: {
          app,
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
    const alice = await open(await localAccountConfig(server.appId, server.url));
    const bobAccount = await localAccountConfig(server.appId, server.url);
    const bob = await open(bobAccount);
    const admin = await open(await localAccountConfig(server.appId, server.url));
    const parent = alice.e2ee.groups.create();
    await parent.wait();
    const child = bob.e2ee.groups.create();
    await child.wait();
    // An authorised administrator without the parent key can record this edge,
    // but cannot deliver the parent's key to its new inherited member.
    await admin.e2ee.groups.add(parent.id, child.id).wait();
    expect(await admin.e2ee.explain({ groupId: parent.id })).toMatchObject({ state: "refused" });
    expect(
      await bob.all(
        app.__e2ee_group_deliveries.where({
          groupId: parent.id,
          recipientAccountId: bobAccount.account.id,
        }),
        { tier: "edge" },
      ),
    ).toEqual([]);
    expect(await bob.e2ee.explain({ groupId: parent.id })).toMatchObject({ state: "unavailable" });
    await expect(
      bob.e2ee.recovery
        .create()
        .wait()
        .then(() => undefined),
    ).rejects.toThrow("Group key unavailable while creating recovery");
    // Once a capable member supplies the missing key, retry can protect both
    // the direct child and the inherited parent before exporting material.
    expect(await alice.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    const { material } = await bob.e2ee.recovery.create().wait();
    const roots = await bob.all(
      app.__e2ee_recovery_roots.where({ accountId: bobAccount.account.id }),
      { tier: "edge" },
    );
    const protectedGroups = await bob.all(
      app.__e2ee_group_recovery_deliveries.where({ recipientAccountId: bobAccount.account.id }),
      { tier: "edge" },
    );
    expect(roots.length).toBeGreaterThan(0);
    expect(new Set(protectedGroups.map((row) => row.groupId))).toEqual(
      new Set([parent.id, child.id]),
    );
    await alice.shutdown();
    await bob.shutdown();
    await admin.shutdown();
    // A fresh device has no old local store and no other live group key holder.
    const recovered = await open(bobAccount);
    const pending = (await recovered.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    expect(pending).toBeDefined();
    await recovered.e2ee.recovery.use(material).wait();
    expect(await recovered.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
    expect(await recovered.e2ee.explain({ groupId: child.id })).toEqual({ state: "ready" });
    expect(await recovered.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 180000);
