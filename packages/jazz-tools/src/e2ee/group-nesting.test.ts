import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { createBrowserDeviceSigner } from "./browser.js";
import { groupMembershipBytes } from "./group-format.js";

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
      return { db, id: account.account.id, stored: () => saved! };
    };
    const { db: owner, stored } = await open();
    const { db: recipient, id: bobId } = await open();
    const parent = owner.e2ee.groups.create();
    await parent.wait();
    const child = owner.e2ee.groups.create();
    await child.wait();
    // Account admission is setup here; the public operations below exercise inherited
    // access and removal. Signed records still pass ordinary policy and global acceptance.
    const signer = await createBrowserDeviceSigner();
    const addAccount = async (groupId: string) => {
      const root = (await owner.one(app.__e2ee_groups.where({ id: groupId }), {
        tier: "global",
      }))!;
      const device = JSON.parse(stored()).devices[0];
      const privateKey = Uint8Array.from(device.signingPrivateKey);
      try {
        const record = {
          id: crypto.randomUUID(),
          groupId,
          epochId: root.epochId,
          authorAccountId: root.accountId,
          authorDeviceId: root.deviceId,
          authorEpochId: root.accountEpochId,
          operation: "add",
          memberKind: "account",
          memberId: bobId,
        };
        const bytes = groupMembershipBytes(device.scope, record);
        const signature = await signer.sign(privateKey, bytes);
        expect(
          await signer.verify(Uint8Array.from(device.signingPublicKey), bytes, signature),
        ).toBe(true);
        const { id, ...values } = record;
        await owner
          .insert(app.__e2ee_group_membership, { ...values, signature }, { id })
          .wait({ tier: "global" });
      } finally {
        privateKey.fill(0);
      }
    };
    await addAccount(child.id);
    expect(await recipient.e2ee.explain({ groupId: parent.id })).toMatchObject({
      state: "refused",
    });
    await owner.e2ee.groups.add(parent.id, { kind: "group", id: child.id }).wait();
    expect(await recipient.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    const deliveries = await recipient.all(
      app.__e2ee_group_deliveries.where({
        groupId: parent.id,
        recipientAccountId: bobId,
      }),
      { tier: "remote" },
    );
    expect(deliveries.length).toBeGreaterThan(0);
    await addAccount(parent.id);
    await owner.e2ee.groups.remove(child.id, { kind: "account", id: bobId }).wait();
    expect(await owner.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    expect(await recipient.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    const before = await recipient.all(
      app.__e2ee_group_deliveries.where({
        groupId: parent.id,
        recipientAccountId: bobId,
      }),
      { tier: "remote" },
    );
    const epochsBefore = await owner.all(
      app.__e2ee_group_successors.where({ groupId: parent.id }),
      { tier: "remote" },
    );
    const childEpochs = await owner.all(app.__e2ee_group_successors.where({ groupId: child.id }), {
      tier: "remote",
    });
    expect(childEpochs).toHaveLength(1);
    const edge = (
      await owner.all(app.__e2ee_group_membership.where({ groupId: parent.id }), { tier: "remote" })
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
    await owner.e2ee.groups.remove(parent.id, { kind: "account", id: bobId }).wait();
    expect(await owner.e2ee.explain({ groupId: parent.id })).toEqual({ state: "ready" });
    expect(await recipient.e2ee.explain({ groupId: parent.id })).toMatchObject({
      state: "refused",
    });
    expect(
      await owner.all(app.__e2ee_group_successors.where({ groupId: parent.id }), {
        tier: "remote",
      }),
    ).toHaveLength(epochsBefore.length + 1);
    const epochsAfter = await owner.all(app.__e2ee_group_successors.where({ groupId: parent.id }), {
      tier: "remote",
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
        { tier: "remote" },
      ),
    ).toEqual(before);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 120000);
