import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";

it("inherits child-group access, preserves an alternate path, and rotates after the last path is removed", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session, allOf }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowRead.where(authenticated);
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.where(authenticated);
      policy.__e2ee_group_membership.allowInsert.where((row) =>
        allOf([
          { authorAccountId: session.user.account },
          policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
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
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
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
      return { db, id: account.account.id };
    };
    const { db: owner } = await open();
    const { db: recipient, id: bobId } = await open();
    const parent = owner.e2ee.groups.create();
    await parent.wait();
    const child = owner.e2ee.groups.create();
    await child.wait();
    await owner.e2ee.groups.add(child.id, bobId).wait();
    expect(await recipient.e2ee.explain({ groupId: parent.id })).toMatchObject({
      state: "refused",
    });
    await owner.e2ee.groups.add(parent.id, child.id).wait();
    expect(await recipient.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    const deliveries = await recipient.all(
      app.__e2ee_group_deliveries.where({
        groupId: parent.id,
        recipientAccountId: bobId,
      }),
      { tier: "edge" },
    );
    expect(deliveries.length).toBeGreaterThan(0);
    await owner.e2ee.groups.add(parent.id, bobId).wait();
    await owner.e2ee.groups.remove(child.id, bobId).wait();
    expect(await owner.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    expect(await recipient.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    const before = await recipient.all(
      app.__e2ee_group_deliveries.where({
        groupId: parent.id,
        recipientAccountId: bobId,
      }),
      { tier: "edge" },
    );
    const epochsBefore = await owner.all(
      app.__e2ee_group_successors.where({ groupId: parent.id }),
      { tier: "edge" },
    );
    const childEpochs = await owner.all(app.__e2ee_group_successors.where({ groupId: child.id }), {
      tier: "edge",
    });
    expect(childEpochs).toHaveLength(1);
    const edge = (
      await owner.all(app.__e2ee_group_membership.where({ groupId: parent.id }), { tier: "edge" })
    ).find((row) => row.memberKind === "group")!;
    // IDs are unique within a table, not across tables. Even an invalid raw
    // candidate must remain distinct from the child root in the signed revision.
    await owner
      .insert(
        app.__e2ee_group_membership,
        {
          groupId: parent.id,
          epochId: edge.epochId,
          authorAccountId: edge.authorAccountId,
          authorDeviceId: edge.authorDeviceId,
          authorEpochId: edge.authorEpochId,
          operation: "remove",
          memberKind: "group",
          memberId: child.id,
          signature: new Uint8Array(64),
        },
        { id: child.id },
      )
      .wait({ tier: "global" });
    await owner.e2ee.groups.remove(parent.id, bobId).wait();
    expect(await owner.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    expect(await recipient.e2ee.explain({ groupId: parent.id })).toMatchObject({
      state: "refused",
    });
    expect(
      await owner.all(app.__e2ee_group_successors.where({ groupId: parent.id }), { tier: "edge" }),
    ).toHaveLength(epochsBefore.length + 1);
    const epochsAfter = await owner.all(app.__e2ee_group_successors.where({ groupId: parent.id }), {
      tier: "edge",
    });
    const successor = epochsAfter.find(
      (row) => !epochsBefore.some((prior) => prior.id === row.id),
    )!;
    const revision = JSON.parse(new TextDecoder().decode(successor.revision));
    expect(revision).toContain(child.id);
    expect(revision).toContain(`__e2ee_groups:${child.id}`);
    expect(revision).toContain(`__e2ee_group_successors:${childEpochs[0]!.id}`);
    expect(
      await recipient.all(
        app.__e2ee_group_deliveries.where({
          groupId: parent.id,
          recipientAccountId: bobId,
        }),
        { tier: "edge" },
      ),
    ).toEqual(before);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 120000);
