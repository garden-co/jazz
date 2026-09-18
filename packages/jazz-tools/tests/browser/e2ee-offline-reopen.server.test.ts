import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { spaceSchema } from "../../src/e2ee/spaces.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { withTimeout } from "./support.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("reopens accepted encrypted data from IndexedDB and updates it offline in Chromium", async () => {
  const server = await getJazzServerInfo(`e2ee-offline-reopen-${crypto.randomUUID()}`);
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"] }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let stopped = false;
  let saved: string | null = null;
  try {
    await deploy({
      ...server,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...permissions },
    });
    const config = {
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: await acquireBrowserTestAccount(server),
      driver: { type: "persistent" as const, dbName: `e2ee-reopen-${crypto.randomUUID()}` },
      e2ee: {
        app,
        store: {
          async read() {
            return saved;
          },
          async update(transform: (current: string | null) => string) {
            saved = transform(saved);
          },
        },
      },
    };
    db = await createDb(config);
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Persistent offline project" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Before reopening" });
    await tx.commit().wait({ tier: "global" });
    // Close immediately after acceptance, without warming an encrypted read.
    await db.shutdown();
    db = undefined;
    await stopJazzServer(server.serverUrl);
    stopped = true;

    db = await createDb(config);
    await db.disconnect();
    // Plaintext persistence is a control independent of key restoration.
    expect(await db.one(app.projects.where({ id: project.id }), { tier: "local" })).toEqual(
      project,
    );
    const write = db.update(app.notes, note.id, { title: "After reopening offline" });
    expect(write).not.toBeInstanceOf(Promise);
    await withTimeout(write.wait({ tier: "local" }), 5_000, "Offline encrypted write stalled");
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual({
      ...note,
      title: "After reopening offline",
    });
  } finally {
    await db?.shutdown();
    if (!stopped) await stopJazzServer(server.serverUrl);
  }
}, 60_000);
