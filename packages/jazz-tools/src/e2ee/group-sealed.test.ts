import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";

it("seals a group after its last account leaves instead of restoring its old lineage", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowRead.where(authenticated);
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.where(authenticated);
      policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_successors.allowRead.where(authenticated);
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const account = await localAccountConfig(server.appId, server.url);
    let saved: string | null = null;
    db = await createDb({
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
    const group = db.e2ee.groups.create();
    await group.wait();
    const roots = await db.all(app.__e2ee_groups.where({ id: group.id }), { tier: "edge" });
    const deliveries = await db.all(app.__e2ee_group_deliveries.where({ groupId: group.id }), {
      tier: "edge",
    });
    await db.e2ee.groups.leave(group.id).wait();
    expect(await db.e2ee.explain({ groupId: group.id })).toMatchObject({ state: "refused" });
    // Ordinary policy still permits this account to administer the group.
    // Empty accepted membership must nevertheless make the lineage terminal.
    await expect(db.e2ee.groups.add(group.id, account.account.id).wait()).rejects.toThrow("sealed");
    expect(await db.e2ee.explain({ groupId: group.id })).toEqual({
      state: "refused",
      reason: "group-sealed",
    });
    expect(await db.all(app.__e2ee_groups.where({ id: group.id }), { tier: "edge" })).toEqual(
      roots,
    );
    expect(
      await db.all(app.__e2ee_group_deliveries.where({ groupId: group.id }), { tier: "edge" }),
    ).toEqual(deliveries);
    expect(
      await db.all(app.__e2ee_group_successors.where({ groupId: group.id }), { tier: "edge" }),
    ).toEqual([]);
    const fresh = db.e2ee.groups.create();
    await fresh.wait();
    expect(fresh.id).not.toBe(group.id);
    expect(await db.e2ee.explain({ groupId: fresh.id })).toEqual({ state: "ready" });
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60000);
