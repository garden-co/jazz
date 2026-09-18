import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";

it("supports account-claim checks in group membership administration policies", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  try {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session }) => {
      policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
    });
    await expect(
      deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: {
          ...deviceRequestPermissions,
          __e2ee_group_membership: policies.__e2ee_group_membership!,
        },
      }),
    ).resolves.toBeDefined();
  } finally {
    await server.stop();
  }
});

it.each(["ordinary", "forged-candidate", "forged-malformed-id"])(
  "separates policy-authorised group administration from possession of its key (%s)",
  async (scenario) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    try {
      const alice = await localAccountConfig(server.appId, server.url);
      const bob = await localAccountConfig(server.appId, server.url);
      const admin = await localAccountConfig(server.appId, server.url);
      const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
      const policies = definePermissions(app, ({ policy, session, allOf }) => {
        policy.__e2ee_group_repairs.allowRead.where(
          session.where({ authMode: { in: ["local-first", "external"] } }),
        );
        policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
        policy.__e2ee_group_successors.allowRead.where(
          session.where({ authMode: { in: ["local-first", "external"] } }),
        );
        policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
        const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
        policy.__e2ee_groups.allowRead.where(authenticated);
        policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
        policy.__e2ee_group_membership.allowRead.where(authenticated);
        policy.__e2ee_group_membership.allowInsert.where(
          allOf([{ authorAccountId: session.user.account }, { authorAccountId: admin.account.id }]),
        );
        policy.__e2ee_group_deliveries.allowRead.where(authenticated);
        policy.__e2ee_group_deliveries.allowInsert.where((row) =>
          allOf([
            { senderAccountId: session.user.account },
            policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
          ]),
        );
      });
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: {
          ...deviceRequestPermissions,
          __e2ee_groups: policies.__e2ee_groups!,
          __e2ee_group_membership: policies.__e2ee_group_membership!,
          __e2ee_group_repairs: policies.__e2ee_group_repairs!,
          __e2ee_group_successors: policies.__e2ee_group_successors!,
          __e2ee_group_deliveries: policies.__e2ee_group_deliveries!,
        },
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
      const administrator = await open(admin);
      const { id } = await owner.e2ee.groups.create().wait();
      if (scenario !== "ordinary") {
        const pending = await open(admin);
        const request = (await pending.e2ee.devices.list()).find(
          (device) => device.state === "pending",
        )!;
        const root = await administrator.one(app.__e2ee_groups.where({ id }), { tier: "edge" });
        const accountRoot = await administrator.one(
          app.__e2ee_account_roots.where({ accountId: admin.account.id }),
          { tier: "edge" },
        );
        // Malformed UUIDs fail at insertion. Well-formed proposals still need
        // an active device signature before they can establish membership.
        const proposal = () =>
          pending
            .insert(app.__e2ee_group_membership, {
              groupId: id,
              epochId: root!.epochId,
              authorAccountId: admin.account.id,
              authorDeviceId: request.id,
              authorEpochId: accountRoot!.epochId,
              operation: "add",
              memberKind: "account",
              memberId:
                scenario === "forged-malformed-id" ? "not-an-account-id" : crypto.randomUUID(),
              signature: new Uint8Array(64),
            })
            .wait({ tier: "global" });
        if (scenario === "forged-malformed-id") expect(proposal).toThrow("invalid UUID value");
        else await proposal();
      }
      expect(await administrator.e2ee.explain({ groupId: id })).toMatchObject({ state: "refused" });
      await administrator.e2ee.groups.add(id, bob.account.id).wait();
      // Acceptance changes desired membership; an administrator without the key
      // cannot deliver it. Loading on a capable member performs reconciliation.
      expect(await recipient.e2ee.explain({ groupId: id })).toMatchObject({ state: "unavailable" });
      expect(await owner.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
      expect(await recipient.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
      expect(await administrator.e2ee.explain({ groupId: id })).toMatchObject({ state: "refused" });
      expect(
        await administrator.all(
          app.__e2ee_group_deliveries.where({ recipientAccountId: admin.account.id }),
          { tier: "edge" },
        ),
      ).toEqual([]);
      // Having the group key must not grant this account administration rights.
      await expect(recipient.e2ee.groups.add(id, admin.account.id).wait()).rejects.toThrow();
      expect(await administrator.e2ee.explain({ groupId: id })).toMatchObject({ state: "refused" });
      if (scenario === "ordinary") {
        await administrator.e2ee.groups.add(id, admin.account.id).wait();
        // Bob can read with his accepted key, but policy permits only Alice to
        // deliver the key to the newly added administrator.
        expect(await recipient.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
        expect(await administrator.e2ee.explain({ groupId: id })).toMatchObject({
          state: "unavailable",
        });
        expect(await owner.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
        expect(await administrator.e2ee.explain({ groupId: id })).toEqual({ state: "ready" });
      }
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);
