import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";

it.skipIf(process.env.JAZZ_E2EE_HISTORY_PERF !== "1")(
  "inspects and restores nine spaces across recovery batches after device loss",
  async () => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
    });
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
      for (let i = 0; i < 9; i++) {
        const project = await owner
          .insert(app.projects, { title: "Batch recovery" })
          .wait({ tier: "global" });
        await owner.e2ee.spaces.grant(app.projects, project.id, account.account.id).wait();
        ids.push(project.id);
      }
      const createStart = performance.now();
      const { material } = await owner.e2ee.recovery.create().wait();
      const createMs = performance.now() - createStart;
      await owner.shutdown();
      const observer = await open();
      const statusStart = performance.now();
      const status = await observer.e2ee.recovery.status(material);
      const statusMs = performance.now() - statusStart;
      expect(status.spaces.validation).toBe("checked");
      if (status.spaces.validation !== "checked") throw new Error("Space recovery was not checked");
      expect(status.spaces.paths.map((path) => path.identifier).sort()).toEqual([...ids].sort());
      expect(status.spaces.paths.every((path) => path.validation === "validated")).toBe(true);
      await observer.shutdown();
      const recovered = await open();
      await recovered.e2ee.devices.list();
      const start = performance.now();
      await recovered.e2ee.recovery.use(material).wait();
      console.log(
        "space-recovery-batch-measurement",
        JSON.stringify({
          spaces: ids.length,
          createMs,
          statusMs,
          recoveryMs: performance.now() - start,
        }),
      );
      for (const identifier of ids)
        expect(await recovered.e2ee.explain({ scope: app.projects, identifier })).toEqual({
          state: "ready",
        });
      const devices = await recovered.e2ee.devices.list();
      expect(devices.filter((device) => device.state === "active")).toHaveLength(2);
    } finally {
      await Promise.all(clients.map((db) => db.shutdown()));
      await server.stop();
    }
  },
  240_000,
);
