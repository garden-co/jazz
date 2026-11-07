import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { frontE2eeCore } from "./fixtures/app.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { beginInitialSpaceTransaction, prepareInitialSpaceForTransaction } from "./lifecycle.js";
import { spaceSchema } from "./spaces.js";

it.each([false, true])(
  "backfills both recipients before staging recovery delivery (reverse=%s)",
  async (reverse) => {
    const app = s.defineApp(
      {
        ...deviceRequestSchema,
        ...spaceSchema,
        projects: s.table({ title: s.string() }, {}),
      },
      { migrations: "none" },
    );
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.projects.allowRead.where(authenticated);
      policy.projects.allowInsert.where(authenticated);
      policy.__e2ee_spaces.allowRead.where(authenticated);
      policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.where(authenticated);
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.where(authenticated);
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
    });
    let server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    try {
      const permissions = { ...deviceRequestPermissions, ...policies };
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      server = await frontE2eeCore(server, { schema: app, permissions });
      const accounts = await Promise.all([
        localAccountConfig(server.appId, server.url),
        localAccountConfig(server.appId, server.url),
      ]);
      for (const account of accounts) {
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
        await db.e2ee.recovery.create().wait();
      }
      const owner = clients[0]!;
      const recipientIds = accounts.map((account) => account.account.id);
      if (reverse) recipientIds.reverse();
      const tx = beginInitialSpaceTransaction(owner, recipientIds);
      const project = tx.insert(app.projects, { title: "Two recoverable recipients" });
      await prepareInitialSpaceForTransaction(
        owner,
        tx,
        app.projects,
        project.id,
        async () => {},
        recipientIds,
      );
      await tx.commit().wait({ tier: "global" });
      const [root] = await owner.all(app.__e2ee_spaces, { tier: "remote" });
      expect(root).toBeDefined();
      const grants = await owner.all(app.__e2ee_space_grants, { tier: "remote" });
      expect(grants).toHaveLength(2);
      expect(grants.find((grant) => grant.id === root!.initialGrantId)?.recipientId).toBe(
        recipientIds[0],
      );
      const ordinary = app.__e2ee_space_deliveries.where({ spaceId: root!.id });
      const recovery = app.__e2ee_space_recovery_deliveries.where({ spaceId: root!.id });
      // Both accepted-empty predicates must be read before the first write.
      expect(await owner.all(ordinary, { tier: "remote" })).toEqual([]);
      expect(await owner.all(recovery, { tier: "remote" })).toEqual([]);
      const target = { scope: app.projects, identifier: project.id };
      expect(await owner.e2ee.explain(target)).toEqual({ state: "ready" });
      const envelopes = await owner.all(ordinary, { tier: "remote" });
      const recoveries = await owner.all(recovery, { tier: "remote" });
      const sorted = (ids: string[]) => ids.slice().sort();
      expect(envelopes).toHaveLength(2);
      expect(recoveries).toHaveLength(2);
      expect(sorted(envelopes.map((row) => row.recipientAccountId))).toEqual(sorted(recipientIds));
      expect(sorted(recoveries.map((row) => row.recipientAccountId))).toEqual(sorted(recipientIds));
      expect(envelopes.every((row) => row.epochId === root!.epochId)).toBe(true);
      expect(recoveries.every((row) => row.epochId === root!.epochId)).toBe(true);
      // Public recipient inspection must actually open the delivered keys.
      expect(await clients[1]!.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await owner.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await owner.all(ordinary, { tier: "remote" })).toHaveLength(2);
      expect(await owner.all(recovery, { tier: "remote" })).toHaveLength(2);
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);
