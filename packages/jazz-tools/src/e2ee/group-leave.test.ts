import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";

it("lets an account leave under self-removal policy without receiving the replacement epoch", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session, allOf, anyOf }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowRead.where(authenticated);
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.where(authenticated);
      policy.__e2ee_group_membership.allowInsert.where(
        allOf([
          { authorAccountId: session.user.account },
          anyOf([
            { authorAccountId: alice.account.id },
            { operation: "remove", memberKind: "account", memberId: session.user.account },
          ]),
        ]),
      );
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where((row) =>
        allOf([
          { senderAccountId: session.user.account },
          policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
        ]),
      );
      policy.__e2ee_group_successors.allowRead.where(authenticated);
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    });
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
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const owner = await createDb({ ...alice, e2ee: { app, store: store() } });
    clients.push(owner);
    const recipient = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(recipient);
    await recipient.e2ee.devices.list();
    const group = owner.e2ee.groups.create();
    await group.wait();
    await owner.e2ee.groups.add(group.id, bob.account.id).wait();
    expect(await recipient.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
    const before = await recipient.all(
      app.__e2ee_group_deliveries.where({ groupId: group.id, recipientAccountId: bob.account.id }),
      { tier: "edge" },
    );
    expect(before).toHaveLength(1);
    await expect(recipient.e2ee.groups.remove(group.id, alice.account.id).wait()).rejects.toThrow();
    const leaving = recipient.e2ee.groups.leave(group.id);
    expect(leaving).not.toBeInstanceOf(Promise);
    await leaving.wait();
    expect(await recipient.e2ee.explain({ groupId: group.id })).toMatchObject({ state: "refused" });
    expect(
      await owner.all(app.__e2ee_group_successors.where({ groupId: group.id }), { tier: "edge" }),
    ).toEqual([]);
    expect(await owner.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
    const successors = await owner.all(app.__e2ee_group_successors.where({ groupId: group.id }), {
      tier: "edge",
    });
    expect(successors).toHaveLength(1);
    expect(successors[0]!.epochId).not.toBe(before[0]!.epochId);
    expect(
      await recipient.all(
        app.__e2ee_group_deliveries.where({
          groupId: group.id,
          recipientAccountId: bob.account.id,
        }),
        { tier: "edge" },
      ),
    ).toEqual(before);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60000);
