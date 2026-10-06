import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./index.js";
import { groupSchema } from "./groups.js";
import { spaceSchema } from "./spaces.js";

it.each([
  { devicesFirst: false, schemaOnly: false },
  { devicesFirst: true, schemaOnly: false },
  { devicesFirst: false, schemaOnly: true },
  { devicesFirst: true, schemaOnly: true },
])(
  "composes managed records with application policies (devices first: $devicesFirst, schema only: $schemaOnly)",
  async ({ devicesFirst, schemaOnly }) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      notes: s.table({ body: s.string() }, {}),
    });
    const configuredApp = schemaOnly ? { wasmSchema: app.wasmSchema } : app;
    const applicationPermissions = definePermissions(app, ({ policy, session }) => {
      policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.notes.allowRead.where({ "$createdBy.account": session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let second: Awaited<ReturnType<typeof createDb>> | undefined;
    let retained: string | null = null;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, notes: applicationPermissions.notes! },
      });
      const account = await localAccountConfig(server.appId, server.url);
      db = await createDb({
        ...account,
        e2ee: {
          app: configuredApp,
          store: {
            async read() {
              return retained;
            },
            async update(transform: (current: string | null) => string) {
              retained = transform(retained);
            },
          },
        },
      });
      if (devicesFirst) await db.e2ee.devices.list();
      const note = db.insert(app.notes, { body: "Application policy still applies" });
      const row = await note.wait({ tier: "global" });
      expect(await db.e2ee.devices.list()).toEqual([expect.objectContaining({ state: "active" })]);
      expect(await db.all(app.notes, { tier: "remote" })).toEqual([
        expect.objectContaining({ body: "Application policy still applies" }),
      ]);
      await expect(db.delete(app.notes, row.id).wait({ tier: "global" })).rejects.toThrow(
        /permission/i,
      );
      if (devicesFirst) {
        let secondRetained: string | null = null;
        second = await createDb({
          ...account,
          e2ee: {
            app: configuredApp,
            store: {
              async read() {
                return secondRetained;
              },
              async update(transform: (current: string | null) => string) {
                secondRetained = transform(secondRetained);
              },
            },
          },
        });
        const pending = (await second.e2ee.devices.list()).find(
          (device) => device.state === "pending",
        )!;
        expect(pending).toBeDefined();
        await db.e2ee.devices.approve(pending.id).wait();
        expect(await second.e2ee.devices.list()).toContainEqual(
          expect.objectContaining({ id: pending.id, state: "active" }),
        );
        await db.e2ee.devices.revoke(pending.id).wait();
        expect(await second.e2ee.devices.list()).toContainEqual(
          expect.objectContaining({ id: pending.id, state: "revoked" }),
        );
      }
    } finally {
      await second?.shutdown();
      await db?.shutdown();
      await server.stop();
    }
  },
  60000,
);

it.each([false, true])(
  "rejects partial configured group schemas before recovery can omit group recipients (schema only: %s)",
  async (schemaOnly) => {
    const deployedApp = s.defineApp({
      ...deviceRequestSchema,
      ...groupSchema,
      ...spaceSchema,
    });
    const accountOnlyApp = s.defineApp({ ...deviceRequestSchema, ...spaceSchema });
    const groupRootSchema = deployedApp.wasmSchema.__e2ee_groups;
    if (!groupRootSchema) throw new Error("Deployed schema is missing its group root table");
    // Preserve real builders and relationship metadata while omitting configured tables.
    // Defining a new partial schema would reject its relationships before E2EE sees it.
    const partialApp = {
      ...accountOnlyApp,
      __e2ee_groups: deployedApp.__e2ee_groups,
      wasmSchema: {
        ...accountOnlyApp.wasmSchema,
        __e2ee_groups: groupRootSchema,
      },
    };
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Db[] = [];
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: deployedApp,
        permissions: deviceRequestPermissions,
      });
      const account = await localAccountConfig(server.appId, server.url);
      const open = async () => {
        let saved: string | null = null;
        const db = await createDb({
          ...account,
          e2ee: {
            app: schemaOnly ? { wasmSchema: partialApp.wasmSchema } : partialApp,
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
      await expect(open()).rejects.toThrow(
        'E2EE application is missing managed table "__e2ee_group_recovery_deliveries"',
      );
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);
