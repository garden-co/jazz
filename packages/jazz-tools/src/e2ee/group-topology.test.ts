import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { withGroupTopologyPermissions } from "./group-topology.js";

it("makes topology readable outside group membership without granting administration or private data access", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...groupSchema,
      notes: s.table({ text: s.string() }, {}),
    });
    const administration = definePermissions(app, ({ policy, session, allOf }) => {
      const own = { "$createdBy.account": session.user.account };
      policy.notes.allowRead.where(own);
      policy.notes.allowInsert.where(own);
      policy.__e2ee_groups.allowRead.where(own);
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.where(own);
      policy.__e2ee_group_membership.allowInsert.where((row) =>
        allOf([
          { authorAccountId: session.user.account },
          policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
        ]),
      );
      policy.__e2ee_group_successors.allowRead.where(own);
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(
        session.where({ authMode: { in: ["local-first", "external"] } }),
      );
      policy.__e2ee_group_deliveries.allowInsert.where((row) =>
        allOf([
          { senderAccountId: session.user.account },
          policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
        ]),
      );
      policy.__e2ee_group_repairs.allowRead.where(own);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: withGroupTopologyPermissions(app, {
        ...deviceRequestPermissions,
        ...administration,
      }),
    });
    const open = async () => {
      const account = await localAccountConfig(server.appId, server.url);
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
      return { db, id: account.account.id };
    };
    const owner = await open();
    const outsider = await open();
    const group = owner.db.e2ee.groups.create();
    await group.wait();
    await owner.db.insert(app.notes, { text: "private" }).wait({ tier: "global" });
    await owner.db.e2ee.groups.add(group.id, outsider.id).wait();
    await owner.db.e2ee.groups.remove(group.id, outsider.id).wait();
    const roots = await owner.db.all(app.__e2ee_groups.where({ id: group.id }), { tier: "edge" });
    const members = await owner.db.all(app.__e2ee_group_membership.where({ groupId: group.id }), {
      tier: "edge",
    });
    const epochs = await owner.db.all(app.__e2ee_group_successors.where({ groupId: group.id }), {
      tier: "edge",
    });
    expect(roots).toHaveLength(1);
    expect(members).toHaveLength(2);
    expect(epochs).toHaveLength(1);
    expect(
      await outsider.db.all(app.__e2ee_groups.where({ id: group.id }), { tier: "edge" }),
    ).toEqual(roots);
    expect(
      await outsider.db.all(app.__e2ee_group_membership.where({ groupId: group.id }), {
        tier: "edge",
      }),
    ).toEqual(members);
    expect(
      await outsider.db.all(app.__e2ee_group_successors.where({ groupId: group.id }), {
        tier: "edge",
      }),
    ).toEqual(epochs);
    expect(await outsider.db.all(app.notes, { tier: "edge" })).toEqual([]);
    expect(await outsider.db.e2ee.explain({ groupId: group.id })).toMatchObject({
      state: "refused",
    });
    await expect(outsider.db.e2ee.groups.add(group.id, outsider.id).wait()).rejects.toThrow();
    expect(
      await owner.db.all(app.__e2ee_group_membership.where({ groupId: group.id }), {
        tier: "edge",
      }),
    ).toEqual(members);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 90000);
