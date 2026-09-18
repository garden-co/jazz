import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("automatically isolates private device records and preserves application group authority", async () => {
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const permissions = definePermissions(app, ({ policy, session, allOf }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowInsert.where(
        allOf([{ authorAccountId: session.user.account }, { authorAccountId: alice.account.id }]),
      );
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const open = async (account: typeof alice) => {
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
    const owner = await open(alice);
    const recipient = await open(bob);
    const ownerRequests = await owner.all(app.__e2ee_device_requests, { tier: "global" });
    const recipientRequests = await recipient.all(app.__e2ee_device_requests, { tier: "global" });
    expect(ownerRequests).toHaveLength(1);
    expect(recipientRequests).toHaveLength(1);
    expect(recipientRequests[0]!.id).not.toBe(ownerRequests[0]!.id);
    expect(
      await recipient.all(app.__e2ee_device_requests.where({ id: ownerRequests[0]!.id }), {
        tier: "global",
      }),
    ).toEqual([]);
    await expect(
      recipient.delete(app.__e2ee_device_requests, ownerRequests[0]!.id).wait({ tier: "global" }),
    ).rejects.toThrow(/permission/i);
    // Public keys are intentionally readable; enrolment handshakes are not.
    expect(
      await recipient.all(app.__e2ee_device_keys.where({ deviceId: ownerRequests[0]!.id }), {
        tier: "global",
      }),
    ).toHaveLength(1);

    const group = await owner.e2ee.groups.create().wait();
    expect(
      await recipient.all(app.__e2ee_groups.where({ id: group.id }), { tier: "global" }),
    ).toHaveLength(1);
    await owner.e2ee.groups.add(group.id, bob.account.id).wait();
    expect(await owner.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
    expect(await recipient.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
    // Bob has the key, but Alice alone has membership administration permission.
    await expect(recipient.e2ee.groups.remove(group.id, alice.account.id).wait()).rejects.toThrow(
      /permission/i,
    );
    expect(await owner.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
  } finally {
    for (const client of clients) await client.shutdown();
    await server.stop();
  }
}, 60_000);
