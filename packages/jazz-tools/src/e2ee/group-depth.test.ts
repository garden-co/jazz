import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema, type GroupRoot, type GroupMembership } from "./groups.js";
import { withGroupTopologyPermissions } from "./group-topology.js";
import { createBrowserDeviceSigner, createBrowserKeyEnvelope } from "./browser.js";
import { groupMembershipBytes, groupRootBytes, groupContext } from "./group-format.js";

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
    // The boundary needs nine authenticated nodes, not nine creation/delivery workflows.
    // Keep real key-holding endpoints; seed the intermediate signed history through
    // ordinary policy-checked Jazz transactions, each accepted globally.
    const head = await owner.db.e2ee.groups.create().wait();
    const seventh = await owner.db.e2ee.groups.create().wait();
    const leaf = await owner.db.e2ee.groups.create().wait();
    const extra = await recipient.db.e2ee.groups.create().wait();
    const groups = [
      head.id,
      ...Array.from({ length: 6 }, () => crypto.randomUUID()),
      seventh.id,
      leaf.id,
    ];
    const headRoot = (await owner.db.one(app.__e2ee_groups.where({ id: head.id }), {
      tier: "remote",
    }))!;
    const signer = await createBrowserDeviceSigner();
    const keys = await createBrowserKeyEnvelope();
    const fixtureDevice = JSON.parse(owner.stored()).devices[0];
    const fixtureKey = Uint8Array.from(fixtureDevice.signingPrivateKey);
    const roots: GroupRoot[] = [];
    const links: GroupMembership[] = [];
    try {
      for (const id of groups.slice(1, -2)) {
        const epochId = crypto.randomUUID();
        const secret = crypto.getRandomValues(new Uint8Array(32));
        try {
          const record = {
            id,
            accountId: headRoot.accountId,
            deviceId: headRoot.deviceId,
            accountEpochId: headRoot.accountEpochId,
            epochId,
            mechanism: keys.mechanism.id,
            version: keys.mechanism.version,
            verification: await keys.wrap(
              secret,
              groupContext(fixtureDevice.scope, { id, epochId }, "verification"),
              new Uint8Array(32),
            ),
          };
          const bytes = groupRootBytes(fixtureDevice.scope, record);
          const signature = await signer.sign(fixtureKey, bytes);
          expect(
            await signer.verify(Uint8Array.from(fixtureDevice.signingPublicKey), bytes, signature),
          ).toBe(true);
          roots.push({ ...record, signature });
        } finally {
          secret.fill(0);
        }
      }
      const rootsWrite = await owner.db.transaction((tx) => {
        for (const { id, ...values } of roots) tx.insert(app.__e2ee_groups, values, { id });
      });
      await rootsWrite.wait({ tier: "global" });
      const parents = [headRoot, ...roots];
      for (let i = 0; i < parents.length; i++) {
        const root = parents[i]!;
        const record = {
          id: crypto.randomUUID(),
          groupId: root.id,
          epochId: root.epochId,
          authorAccountId: root.accountId,
          authorDeviceId: root.deviceId,
          authorEpochId: root.accountEpochId,
          operation: "add" as const,
          memberKind: "group" as const,
          memberId: groups[i + 1]!,
        };
        const bytes = groupMembershipBytes(fixtureDevice.scope, record);
        const signature = await signer.sign(fixtureKey, bytes);
        expect(
          await signer.verify(Uint8Array.from(fixtureDevice.signingPublicKey), bytes, signature),
        ).toBe(true);
        links.push({ ...record, signature });
      }
      // Roots precede membership in accepted history; no admission or sync is mocked.
      const linksWrite = await owner.db.transaction((tx) => {
        for (const { id, ...values } of links)
          tx.insert(app.__e2ee_group_membership, values, { id });
      });
      await linksWrite.wait({ tier: "global" });
    } finally {
      fixtureKey.fill(0);
    }
    // Keep the original positive boundary: a public eighth edge below seven ancestors,
    // followed by a refused ninth edge below eight. The edited groups hold real keys.
    await owner.db.e2ee.groups.add(groups[7]!, { kind: "group", id: groups[8]! }).wait();
    const edges = await owner.db.all(app.__e2ee_group_membership, { tier: "remote" });
    expect(edges).toHaveLength(8);
    expect(await owner.db.e2ee.explain({ groupId: groups[0]! })).toEqual({ state: "ready" });
    // The edited leaf has no descendants. Validation must also see its eight ancestors.
    await expect(
      owner.db.e2ee.groups.add(groups[8]!, { kind: "group", id: extra.id }).wait(),
    ).rejects.toThrow("cycle or depth");
    expect(await owner.db.all(app.__e2ee_group_membership, { tier: "remote" })).toEqual(edges);
    const root = (await owner.db.one(app.__e2ee_groups.where({ id: groups[8]! }), {
      tier: "remote",
    }))!;
    const device = JSON.parse(owner.stored()).devices[0];
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
    expect(await owner.db.all(app.__e2ee_group_membership, { tier: "remote" })).toHaveLength(9);
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
        { tier: "remote" },
      ),
    ).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 300_000);
