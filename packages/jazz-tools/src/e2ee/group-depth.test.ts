import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { withGroupTopologyPermissions } from "./group-topology.js";
import { createBrowserDeviceSigner } from "./browser.js";
import { groupMembershipBytes } from "./group-format.js";

it("accepts eight group edges but rejects a ninth below existing ancestors, including a signed raw candidate", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session, allOf }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowInsert.where((row) =>
        allOf([
          { authorAccountId: session.user.account },
          policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
        ]),
      );
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
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
    const open = async () => {
      let saved: string | null = null;
      const account = await localAccountConfig(server.appId, server.url);
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
      return { db, accountId: account.account.id, stored: () => saved! };
    };
    const owner = await open();
    const recipient = await open();
    const groups: string[] = [];
    for (let i = 0; i < 9; i++) {
      const group = owner.db.e2ee.groups.create();
      await group.wait();
      groups.push(group.id);
    }
    const extra = recipient.db.e2ee.groups.create();
    await extra.wait();
    for (let i = 0; i < 8; i++) await owner.db.e2ee.groups.add(groups[i]!, groups[i + 1]!).wait();
    const edges = await owner.db.all(app.__e2ee_group_membership, { tier: "edge" });
    expect(edges).toHaveLength(8);
    expect(await owner.db.e2ee.explain({ groupId: groups[0]! })).toEqual({ state: "ready" });
    // The edited leaf has no descendants. Validation must also see its eight ancestors.
    await expect(owner.db.e2ee.groups.add(groups[8]!, extra.id).wait()).rejects.toThrow(
      "cycle or depth",
    );
    expect(await owner.db.all(app.__e2ee_group_membership, { tier: "edge" })).toEqual(edges);
    const root = (await owner.db.one(app.__e2ee_groups.where({ id: groups[8]! }), {
      tier: "edge",
    }))!;
    const device = JSON.parse(owner.stored()).devices[0];
    const signer = await createBrowserDeviceSigner();
    const key = Uint8Array.from(device.signingPrivateKey);
    const candidate = {
      id: crypto.randomUUID(),
      groupId: root.id,
      epochId: root.epochId,
      authorAccountId: root.accountId,
      authorDeviceId: root.deviceId,
      authorEpochId: root.accountEpochId,
      operation: "add",
      memberKind: "group",
      memberId: extra.id,
    };
    try {
      const bytes = groupMembershipBytes(device.scope, candidate);
      const signature = await signer.sign(key, bytes);
      expect(await signer.verify(Uint8Array.from(device.signingPublicKey), bytes, signature)).toBe(
        true,
      );
      const { id, ...values } = candidate;
      await owner.db
        .insert(app.__e2ee_group_membership, { ...values, signature }, { id })
        .wait({ tier: "global" });
    } finally {
      key.fill(0);
    }
    expect(await owner.db.all(app.__e2ee_group_membership, { tier: "edge" })).toHaveLength(9);
    expect(await owner.db.e2ee.explain({ groupId: root.id })).toEqual({ state: "ready" });
    expect(await owner.db.e2ee.explain({ groupId: groups[0]! })).toEqual({ state: "ready" });
    expect(await recipient.db.e2ee.explain({ groupId: root.id })).toMatchObject({
      state: "refused",
    });
    expect(await recipient.db.e2ee.explain({ groupId: groups[0]! })).toMatchObject({
      state: "refused",
    });
    expect(
      await owner.db.all(
        app.__e2ee_group_deliveries.where({
          groupId: { in: groups },
          recipientAccountId: recipient.accountId,
        }),
        { tier: "edge" },
      ),
    ).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 300_000);
