import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { beginDbTransactionAfter } from "../runtime/db.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";

it.each(["switch", "logout"])(
  "retires suspended admission through the native session owner (%s)",
  async (change) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      projects: s.table({ title: s.string() }, {}),
    });
    const policies = definePermissions(app, ({ policy }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
    });
    const permissions = { ...deviceRequestPermissions, ...policies };
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let saved: string | null = null;
    let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      owner = await createJazzSession({
        serverUrl: server.url,
        appId: server.appId,
        app,
        permissions,
        driver: { type: "memory" },
        initial: "local-first",
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
      const original = owner.getSnapshot();
      const oldDb = original.client!.db;
      const oldDevices = await oldDb.e2ee.devices.list();
      expect(oldDevices).toHaveLength(1);
      await oldDb.all(app.projects, { tier: "global" });
      const tx = beginDbTransactionAfter(oldDb, async () => {
        entered();
        await ready;
      });
      const oldRow = tx.insert(app.projects, { title: "Suspended old account" });
      const wait = tx.commit().wait({ tier: "global" });
      wait.catch(() => {});
      await started;
      if (change === "logout") {
        await owner.logout();
        expect(owner.getSnapshot().status).toBe("signed-out");
      }
      await owner.createLocalFirst();
      const replacement = owner.getSnapshot();
      expect(replacement.status).toBe("ready");
      expect(replacement.account!.id).not.toBe(original.account!.id);
      release();
      await expect(wait).rejects.toThrow();
      expect(() => oldDb.insert(app.projects, { title: "Retired context" })).toThrow();
      const currentDb = replacement.client!.db;
      const currentDevices = await currentDb.e2ee.devices.list();
      expect(currentDevices).toHaveLength(1);
      expect(currentDevices[0]!.id).not.toBe(oldDevices[0]!.id);
      expect(
        await currentDb.one(app.projects.where({ id: oldRow.id }), { tier: "global" }),
      ).toBeNull();
      const row = await currentDb
        .insert(app.projects, { title: "Replacement account" })
        .wait({ tier: "global" });
      expect(
        await currentDb.one(app.projects.where({ id: row.id }).select("$createdBy"), {
          tier: "global",
        }),
      ).toMatchObject({
        $createdBy: { account: replacement.account!.id },
      });
      expect(await currentDb.all(app.projects, { tier: "global" })).toEqual([row]);
    } finally {
      release();
      await owner?.close();
      await server.stop();
    }
  },
  60_000,
);
