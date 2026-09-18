import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("automatically configures an encrypted browser app for atomic creation and reads", async () => {
  const server = await getJazzServerInfo(`e2ee-automatic-${crypto.randomUUID()}`);
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
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
    db = await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      account,
      driver: { type: "memory" },
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
    expect(await db.e2ee.devices.list()).toEqual([expect.objectContaining({ state: "active" })]);
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Browser project" });
    const note = tx.insert(app.notes, { projectId: project.id, body: "Encrypted in the browser" });
    await tx.commit().wait({ tier: "global" });
    expect(await db.all(app.notes, { tier: "global" })).toEqual([note]);
    await expect(db.delete(app.notes, note.id).wait({ tier: "global" })).rejects.toThrow(
      /permission/i,
    );
  } finally {
    await db?.shutdown();
    await stopJazzServer(server.serverUrl);
  }
}, 60_000);
