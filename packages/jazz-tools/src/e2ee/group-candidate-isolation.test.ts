import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { withGroupTopologyPermissions } from "./group-topology.js";
import { groupMembershipBytes } from "./group-format.js";
import { createBrowserDeviceSigner } from "./browser.js";

it("ignores a signed add before recipient enrolment without poisoning unrelated groups or granting retroactive access", async () => {
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
      return { db, stored: () => saved! };
    };
    const alice = await open(await localAccountConfig(server.appId, server.url));
    const mallory = await open(await localAccountConfig(server.appId, server.url));
    const future = await localAccountConfig(server.appId, server.url);
    const good = alice.db.e2ee.groups.create();
    await good.wait();
    const other = mallory.db.e2ee.groups.create();
    await other.wait();
    const root = (await mallory.db.one(app.__e2ee_groups.where({ id: other.id }), {
      tier: "edge",
    }))!;
    expect(
      await mallory.db.all(app.__e2ee_account_roots.where({ accountId: future.account.id }), {
        tier: "edge",
      }),
    ).toEqual([]);
    const device = JSON.parse(mallory.stored()).devices[0];
    const signer = await createBrowserDeviceSigner();
    const privateKey = Uint8Array.from(device.signingPrivateKey);
    const record = {
      id: crypto.randomUUID(),
      groupId: other.id,
      epochId: root.epochId,
      authorAccountId: root.accountId,
      authorDeviceId: root.deviceId,
      authorEpochId: root.accountEpochId,
      operation: "add",
      memberKind: "account",
      memberId: future.account.id,
    };
    try {
      const bytes = groupMembershipBytes(device.scope, record);
      const signature = await signer.sign(privateKey, bytes);
      expect(await signer.verify(Uint8Array.from(device.signingPublicKey), bytes, signature)).toBe(
        true,
      );
      const { id, ...values } = record;
      await mallory.db
        .insert(app.__e2ee_group_membership, { ...values, signature }, { id })
        .wait({ tier: "global" });
    } finally {
      privateKey.fill(0);
    }
    // A valid administrator signature is not proof of an accepted recipient root.
    expect(await alice.db.e2ee.explain({ groupId: good.id })).toEqual({ state: "ready" });
    expect(await mallory.db.e2ee.explain({ groupId: other.id })).toEqual({ state: "ready" });
    const recipient = await open(future);
    // Enrolling later must not retroactively authenticate the earlier add.
    expect(await alice.db.e2ee.explain({ groupId: good.id })).toEqual({ state: "ready" });
    expect(await mallory.db.e2ee.explain({ groupId: other.id })).toEqual({ state: "ready" });
    expect(await recipient.db.e2ee.explain({ groupId: other.id })).toMatchObject({
      state: "refused",
    });
    expect(
      await mallory.db.all(
        app.__e2ee_group_deliveries.where({
          groupId: other.id,
          recipientAccountId: future.account.id,
        }),
        { tier: "edge" },
      ),
    ).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 120000);
