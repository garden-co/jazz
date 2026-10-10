import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { createNativeCrypto } from "./native.js";

it.skipIf(process.env.JAZZ_E2EE_HISTORY_PERF !== "1")(
  "restores nine groups across recovery batches after losing the original device",
  async () => {
    const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowRead.where(authenticated);
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.where(authenticated);
      policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_successors.allowRead.where(authenticated);
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Db[] = [];
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      const account = await localAccountConfig(server.appId, server.url);
      const crypto = await createNativeCrypto();
      const open = async () => {
        let saved: string | null = null;
        const db = await createDb({
          ...account,
          e2ee: {
            app,
            crypto,
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
        return db;
      };
      const owner = await open();
      const ids: string[] = [];
      for (let i = 0; i < 9; i++) ids.push((await owner.e2ee.groups.create().wait()).id);
      const { material } = await owner.e2ee.recovery.create().wait();
      await owner.shutdown();
      const recovered = await open();
      await recovered.e2ee.devices.list();
      const start = performance.now();
      await recovered.e2ee.recovery.use(material).wait();
      console.log(
        "recovery-batch-measurement",
        JSON.stringify({ groups: ids.length, recoveryMs: performance.now() - start }),
      );
      for (const groupId of ids)
        expect(await recovered.e2ee.explain({ groupId })).toEqual({ state: "ready" });
      const devices = await recovered.e2ee.devices.list();
      expect(devices.filter((device) => device.state === "active")).toHaveLength(2);
    } finally {
      await Promise.all(clients.map((db) => db.shutdown()));
      await server.stop();
    }
  },
  180_000,
);
