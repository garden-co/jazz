import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./index.js";

it.each([false, true])(
  "composes managed records with application policies (devices first: %s)",
  async (devicesFirst) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      notes: s.table({ body: s.string() }, {}),
    });
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
          app,
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
      expect(await db.all(app.notes, { tier: "edge" })).toEqual([
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
            app,
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
