import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it.each(["explicit", "default"] as const)(
  "automatically configures an encrypted browser app with %s key storage",
  async (storage) => {
    const server = await getJazzServerInfo(`e2ee-automatic-${crypto.randomUUID()}`);
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), body: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["body"] }),
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.notes.allowRead.always();
      policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let saved: string | null = null;
    try {
      await deploy({ ...server, schema: app, permissions });
      const account = await acquireBrowserTestAccount(server);
      const config = {
        appId: server.appId,
        serverUrl: server.serverUrl,
        account,
        driver: { type: "memory" as const },
        e2ee: {
          app,
          ...(storage === "explicit"
            ? {
                store: {
                  async read() {
                    return saved;
                  },
                  async update(transform: (current: string | null) => string) {
                    saved = transform(saved);
                  },
                },
              }
            : {}),
        },
      };
      db = await createDb(config);
      expect(await db.e2ee.devices.list()).toEqual([expect.objectContaining({ state: "active" })]);
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Browser project" });
      const note = tx.insert(app.notes, {
        projectId: project.id,
        body: "Encrypted in the browser",
      });
      await tx.commit().wait({ tier: "global" });
      expect(await db.all(app.notes, { tier: "remote" })).toEqual([note]);
      await expect(db.delete(app.notes, note.id).wait({ tier: "global" })).rejects.toThrow(
        /permission/i,
      );
      if (storage === "default") {
        const devices = await db.e2ee.devices.list();
        await db.shutdown();
        db = await createDb(config);
        expect(await db.e2ee.devices.list()).toEqual(devices);
        expect(await db.all(app.notes, { tier: "remote" })).toEqual([note]);
      }
    } finally {
      await db?.shutdown();
      await stopJazzServer(server.serverUrl);
    }
  },
  60_000,
);
